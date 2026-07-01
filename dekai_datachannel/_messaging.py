from aiortc import RTCDataChannel
from logging import getLogger
from uuid import UUID, uuid7
from typing import Literal, overload, Protocol
from asyncio import Future
from dataclasses import dataclass
from pydantic import TypeAdapter
import asyncio

logger = getLogger("dekai_datachannel")

CHUNK_ID_MASK = 0x000FFFFFFFFFFFFF


@dataclass
class JSONRPCRequest:
    id: str | int | None
    method: str
    params: dict
    jsonrpc: Literal["2.0"] = "2.0"


_jsonrpc_request_adapter = TypeAdapter(JSONRPCRequest)


@dataclass
class JSONRPCErrorBody:
    code: int
    message: str
    data: dict | None = None


@dataclass
class JSONRPCError:
    id: str | int
    error: JSONRPCErrorBody
    jsonrpc: Literal["2.0"] = "2.0"


@dataclass
class JSONRPCResponse:
    id: str | int
    result: dict | None = None
    jsonrpc: Literal["2.0"] = "2.0"


type JSONRPCMessages = JSONRPCRequest | JSONRPCResponse | JSONRPCError

_jsonrpc_messages_adapter = TypeAdapter(JSONRPCMessages)


class StartSessionHandler(Protocol):
    async def __call__(
        self,
        session_id: UUID,
        byte_length: int,
        sha256: str,
        mode: Literal["text", "binary"],
    ) -> None: ...


class RequestChunkHandler(Protocol):
    async def __call__(
        self,
        session_id: UUID,
        chunk_id: int,
        byte_offset: int,
        byte_length: int,
    ) -> None: ...


class ChunkContentHandler(Protocol):
    async def __call__(
        self,
        chunk_id: int,
        data: bytes,
    ) -> None: ...


class TimeoutHandler(Protocol):
    async def __call__(
        self,
        session_id: UUID,
    ) -> None: ...


type MessageHandlers = (
    StartSessionHandler | RequestChunkHandler | ChunkContentHandler | TimeoutHandler
)


class DekaiDataChannelMessaging:
    def __init__(self, channel: RTCDataChannel):
        self.__channel = channel
        self.__handlers: dict[str, MessageHandlers] = {}

        channel.on("message", self._on_message)
        channel.on("open", self._on_open)
        channel.on("close", self._on_close)
        channel.on("error", self._on_error)

        self.__futures: dict[str | int, Future] = {}

    @property
    def channel(self) -> RTCDataChannel:
        return self.__channel

    def _on_open(self):
        logger.info("Data channel is open")

    def _on_close(self):
        logger.info("Data channel is closed")

    def _on_error(self, error):
        logger.error(f"Data channel error: {error}")

    def _on_message(self, message: str | bytes):
        if isinstance(message, str):
            message = message.encode("utf-8")

        if b"{" == message[0:1]:
            self._process_json_message(message)
        elif b"f" == message[0:1]:
            self._process_frame_message(message)
        else:
            logger.warning(f"Unknown message type: {message}")

    def _process_json_message(self, message: bytes):
        try:
            data = _jsonrpc_messages_adapter.validate_json(message)
            if isinstance(data, JSONRPCResponse):
                call_id = data.id
                if call_id in self.__futures:
                    future = self.__futures.pop(call_id)
                    if not future.done():
                        future.set_result(data.result)
            elif isinstance(data, JSONRPCError):
                call_id = data.id
                if call_id in self.__futures:
                    future = self.__futures.pop(call_id)
                    if not future.done():
                        future.set_exception(Exception(data.error))
            elif isinstance(data, JSONRPCRequest):
                coro = self._process_json_request(data)
                asyncio.create_task(coro)

        except Exception as e:
            # TODO: Response or notify with error message
            logger.error(f"Invalid JSON-RPC message: {e}")
            return

    async def _process_json_request(self, request: JSONRPCRequest):
        if request.id is None:
            try:
                await self._emit(request.method, **request.params)
            except Exception as e:
                logger.error(
                    "JSON-RPC notification handler failed: method=%s params=%s error=%s",
                    request.method,
                    request.params,
                    e,
                    exc_info=e,
                )
        else:
            try:
                result = await self._emit(request.method, **request.params)
            except Exception as e:
                error = JSONRPCError(
                    id=request.id,
                    error=JSONRPCErrorBody(code=-32000, message=str(e)),
                    jsonrpc="2.0",
                )
                self.channel.send(_jsonrpc_messages_adapter.dump_json(error))
            else:
                response = JSONRPCResponse(id=request.id, result=result)
                self.channel.send(_jsonrpc_messages_adapter.dump_json(response))

    def _process_frame_message(self, message: bytes):
        message_id = int.from_bytes(message[0:8], "little")
        message_id = message_id >> 8
        message_body = message[8:]
        coro = self._emit("chunk_content", message_id, message_body)
        asyncio.create_task(coro)

    @overload
    def on(self, method: Literal["start_session"], handler: StartSessionHandler): ...
    @overload
    def on(self, method: Literal["request_chunk"], handler: RequestChunkHandler): ...
    @overload
    def on(self, method: Literal["chunk_content"], handler: ChunkContentHandler): ...
    @overload
    def on(self, method: Literal["timeout"], handler: TimeoutHandler): ...
    def on(self, method: str, handler: MessageHandlers):
        existing = self.__handlers.get(method)
        if existing is not None and existing != handler:
            raise ValueError(f"Handler for method '{method}' already exists")
        self.__handlers[method] = handler

    @overload
    def off(self, method: Literal["start_session"], handler: StartSessionHandler): ...
    @overload
    def off(self, method: Literal["request_chunk"], handler: RequestChunkHandler): ...
    @overload
    def off(self, method: Literal["chunk_content"], handler: ChunkContentHandler): ...
    @overload
    def off(self, method: Literal["timeout"], handler: TimeoutHandler): ...
    def off(self, method: str, handler: MessageHandlers):
        if method in self.__handlers:
            existing = self.__handlers[method]
            if existing == handler:
                del self.__handlers[method]
            else:
                raise ValueError(
                    f"Handler for method '{method}' does not match the provided handler"
                )

    async def _emit(self, method: str, *args, **kwargs):
        if method in self.__handlers:
            handler = self.__handlers[method]
            await handler(*args, **kwargs)

    @overload
    def call(
        self,
        method: Literal["start_session"],
        *,
        session_id: UUID,
        byte_length: int,
        sha256: str,
        mode: Literal["text", "binary"],
    ) -> Future: ...

    @overload
    def call(
        self,
        method: Literal["request_chunk"],
        *,
        session_id: UUID,
        chunk_id: int,
        byte_offset: int,
        byte_length: int,
    ) -> Future: ...

    def call(self, method: str, **kwargs) -> Future:
        request_id = str(uuid7())
        request = JSONRPCRequest(
            id=request_id,
            method=method,
            params=kwargs,
        )
        future = Future()
        self.__futures[request_id] = future
        self.channel.send(_jsonrpc_request_adapter.dump_json(request))
        return future

    @overload
    def notify(
        self,
        method: Literal["start_session"],
        *,
        session_id: UUID,
        byte_length: int,
        sha256: str,
        mode: Literal["text", "binary"],
    ) -> None: ...

    @overload
    def notify(
        self,
        method: Literal["request_chunk"],
        *,
        session_id: UUID,
        chunk_id: int,
        byte_offset: int,
        byte_length: int,
    ) -> None: ...

    @overload
    def notify(
        self,
        method: Literal["timeout"],
        *,
        session_id: UUID,
    ) -> None: ...

    def notify(self, method: str, **kwargs) -> None:
        request = JSONRPCRequest(
            id=None,
            method=method,
            params=kwargs,
        )
        self.channel.send(_jsonrpc_request_adapter.dump_json(request))

    async def send_chunk(
        self,
        chunk_id: int,
        data: bytes,
    ):
        if chunk_id < 0 or chunk_id > CHUNK_ID_MASK:
            raise ValueError("chunk_id must fit within 52 bits.")

        frame_id = (chunk_id << 8) | 0x66
        chunk_id_bytes = frame_id.to_bytes(8, "little")
        message = chunk_id_bytes + data
        self.channel.send(message)

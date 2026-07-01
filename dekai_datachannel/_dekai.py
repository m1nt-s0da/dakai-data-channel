from aiortc import RTCDataChannel
from logging import getLogger
import asyncio
from typing import Awaitable, Literal, Callable, overload, Protocol
from io import BytesIO
from hashlib import sha256
from dataclasses import dataclass
from ._messaging import DekaiDataChannelMessaging
from uuid import UUID, uuid7
from ._events import EventTarget

logger = getLogger("dekai_datachannel")

SendPhase = Literal["awaiting_request_chunk", "awaiting_final_response"]
CHUNK_ID_MASK = 0x000FFFFFFFFFFFFF


@dataclass
class RequestedChunk:
    session_id: UUID
    receive: TransferReceive
    offset: int
    timeout_task: asyncio.Task[None]


@dataclass
class SendingSession:
    payload: bytes
    response_future: asyncio.Future
    phase: SendPhase
    timeout_task: asyncio.Task[None] | None


class StartReceivingHandler(Protocol):
    def __call__(self, receiving: TransferReceive): ...


class ReceivedHandler(Protocol):
    def __call__(self, data: str | bytes): ...


class ReceivedProgressHandler(Protocol):
    def __call__(self, received_bytes: int, total_bytes: int): ...


class ReceiverBase(EventTarget):
    def __init__(self, receive: TransferReceive):
        super().__init__()
        self.__receive = receive

    @property
    def receive(self) -> TransferReceive:
        return self.__receive

    @overload
    def on(self, event: Literal["progress"], handler: ReceivedProgressHandler): ...

    def on(self, event: str, handler: Callable):
        super().on(event, handler)


class BufferedReceiver(ReceiverBase):
    def __init__(self, receive: TransferReceive):
        super().__init__(receive)
        self.__buffer = BytesIO()
        self.__stream = StreamReceiver(receive)

    @property
    def _buffer(self) -> BytesIO:
        return self.__buffer

    async def join(self):
        async for chunk in self.__stream:
            self.__buffer.write(chunk)
        return self.__buffer.getvalue()


class StreamReceiver(ReceiverBase):
    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        await self.receive._request_next_chunk()
        return await self.receive._next_chunk()


class TransferReceive(EventTarget):
    byte_length: int
    hash: str
    mode: Literal["text", "binary"]

    __receiver: BufferedReceiver | StreamReceiver | None

    def __init__(
        self,
        byte_length: int,
        hash: str,
        mode: Literal["text", "binary"],
        *,
        frame_body_size: int,
        request_chunk: Callable[[int, int], Awaitable[None]],
        complete: Callable[[], None],
        fail: Callable[[Exception], None],
    ):
        super().__init__()

        self.byte_length = byte_length
        self.hash = hash
        self.mode = mode
        self.__receiver = None
        self.__frame_body_size = frame_body_size
        self.__request_chunk = request_chunk
        self.__complete = complete
        self.__fail = fail
        self.__next_request_offset = 0
        self.__received_byte_length = 0
        self.__digest = sha256()
        self.__buffered_data = bytearray()
        self.__chunk_queue: asyncio.Queue[bytes | None | Exception] = asyncio.Queue()
        self.__done = False

    @property
    def has_receiver(self) -> bool:
        return self.__receiver is not None

    def stream(self) -> StreamReceiver:
        if self.__receiver is None:
            self.__receiver = StreamReceiver(self)
        elif not isinstance(self.__receiver, StreamReceiver):
            raise RuntimeError(
                "Cannot switch to stream mode after buffering has started."
            )
        return self.__receiver

    def buffered(self) -> BufferedReceiver:
        if self.__receiver is None:
            self.__receiver = BufferedReceiver(self)
        elif not isinstance(self.__receiver, BufferedReceiver):
            raise RuntimeError(
                "Cannot switch to buffered mode after streaming has started."
            )
        return self.__receiver

    @overload
    def on(self, event: Literal["received"], handler: ReceivedHandler): ...

    def on(self, event: str, handler: Callable):
        super().on(event, handler)

    async def _request_next_chunk(self):
        if self.__done:
            return
        if self.__next_request_offset >= self.byte_length:
            return

        byte_offset = self.__next_request_offset
        byte_length = min(self.__frame_body_size, self.byte_length - byte_offset)
        self.__next_request_offset += byte_length
        await self.__request_chunk(byte_offset, byte_length)

    async def _next_chunk(self) -> bytes:
        chunk = await self.__chunk_queue.get()
        if isinstance(chunk, Exception):
            raise chunk
        if chunk is None:
            raise StopAsyncIteration
        return chunk

    def _fail(self, error: Exception):
        if self.__done:
            return

        self.__done = True
        self.__chunk_queue.put_nowait(error)
        self.__fail(error)

    def _finalize_if_ready(self):
        if self.__done or self.__received_byte_length != self.byte_length:
            return

        digest = self.__digest.hexdigest()
        if digest != self.hash:
            raise ValueError("Received data hash does not match the announced digest.")

        self.__done = True
        self.__chunk_queue.put_nowait(None)
        data = bytes(self.__buffered_data)
        payload = data.decode("utf-8") if self.mode == "text" else data
        self._emit("received", payload)
        self.__complete()

    def _push_chunk(self, offset: int, chunk: bytes):
        if self.__done:
            raise RuntimeError("Transfer has already completed.")
        if offset != self.__received_byte_length:
            raise ValueError(
                "Chunk offset does not match the expected receive position."
            )
        if offset + len(chunk) > self.byte_length:
            raise ValueError("Chunk exceeds the announced transfer size.")

        self.__received_byte_length += len(chunk)
        self.__digest.update(chunk)
        self.__buffered_data.extend(chunk)
        if self.__receiver is not None:
            self.__receiver._emit(
                "progress", self.__received_byte_length, self.byte_length
            )
        self.__chunk_queue.put_nowait(chunk)
        self._finalize_if_ready()


class DekaiDataChannel(EventTarget):
    def __init__(
        self,
        channel: RTCDataChannel,
        chunk_size: int = 16384,
        timeout_seconds: float = 30.0,
        final_response_timeout_seconds: float | None = None,
    ):
        super().__init__()
        messaging = DekaiDataChannelMessaging(channel)
        self.channel = channel
        self.__chunk_size = chunk_size
        self.__frame_body_size = chunk_size - 8
        self.__timeout_seconds = timeout_seconds
        self.__final_response_timeout_seconds = (
            max(timeout_seconds * 3, timeout_seconds)
            if final_response_timeout_seconds is None
            else final_response_timeout_seconds
        )
        self.__messaging = messaging
        self.__receiving: dict[UUID, tuple[TransferReceive, asyncio.Future[None]]] = {}
        self.__sending: dict[UUID, SendingSession] = {}
        self.__chunk_ids: dict[int, RequestedChunk] = {}

        messaging.on("start_session", self._on_start_session)
        messaging.on("request_chunk", self._on_request_chunk)
        messaging.on("chunk_content", self._on_chunk_content)
        messaging.on("timeout", self._on_timeout)

    def _trace_session(
        self,
        event: str,
        *,
        session_id: UUID,
        chunk_id: int | None = None,
        byte_offset: int | None = None,
        byte_length: int | None = None,
        phase: SendPhase | None = None,
    ):
        logger.debug(
            "Dekai trace event=%s session_id=%s chunk_id=%s byte_offset=%s byte_length=%s phase=%s",
            event,
            session_id,
            "-" if chunk_id is None else chunk_id,
            "-" if byte_offset is None else byte_offset,
            "-" if byte_length is None else byte_length,
            "-" if phase is None else phase,
        )

    def _drop_requested_chunk(
        self, chunk_id: int, reason: str
    ) -> RequestedChunk | None:
        chunk = self.__chunk_ids.pop(chunk_id, None)
        if chunk is None:
            logger.debug(
                "Dekai trace event=chunk_id.missing chunk_id=%s reason=%s",
                chunk_id,
                reason,
            )
            return None

        self._trace_session(
            "chunk_id.drop",
            session_id=chunk.session_id,
            chunk_id=chunk_id,
            byte_offset=chunk.offset,
        )
        logger.debug("Dekai trace chunk_id=%s reason=%s", chunk_id, reason)
        return chunk

    def _send_timeout_seconds(self, phase: SendPhase) -> float:
        if phase == "awaiting_request_chunk":
            return self.__timeout_seconds
        return self.__final_response_timeout_seconds

    async def _ensure_default_receiver(
        self, session_id: UUID, receive: TransferReceive
    ):
        await asyncio.sleep(0)
        if not receive.has_receiver:
            try:
                await receive.buffered().join()
            except Exception as exc:
                logger.error(
                    "Default buffered receiver failed: session_id=%s error=%s",
                    session_id,
                    exc,
                    exc_info=exc,
                )

    def _cancel_requested_chunks(self, session_id: UUID):
        for chunk_id, chunk in list(self.__chunk_ids.items()):
            if chunk.session_id != session_id:
                continue
            chunk.timeout_task.cancel()
            self._drop_requested_chunk(chunk_id, "receive_session_cancelled")

    def _fail_receive_session(self, session_id: UUID, error: Exception):
        session = self.__receiving.get(session_id)
        if session is None:
            return

        receive, future = session
        self._cancel_requested_chunks(session_id)
        receive._fail(error)
        if not future.done():
            future.set_exception(error)

    async def _monitor_chunk_timeout(self, session_id: UUID, chunk_id: int):
        try:
            await asyncio.sleep(self.__timeout_seconds)
            chunk = self._drop_requested_chunk(chunk_id, "chunk_content_timeout")
            if chunk is None:
                return
            self._fail_receive_session(
                session_id,
                TimeoutError(f"Timed out waiting for chunk content: {chunk_id}"),
            )
        except asyncio.CancelledError:
            raise

    async def _monitor_send_timeout(self, session_id: UUID, phase: SendPhase):
        try:
            await asyncio.sleep(self._send_timeout_seconds(phase))
            session = self.__sending.get(session_id)
            if (
                session is None
                or session.response_future.done()
                or session.phase != phase
            ):
                return

            self._trace_session("timeout.send", session_id=session_id, phase=phase)
            self.__messaging.notify("timeout", session_id=session_id)
            if not session.response_future.done():
                session.response_future.set_exception(
                    TimeoutError(f"Send session timed out ({phase}): {session_id}")
                )
        except asyncio.CancelledError:
            raise

    def _arm_send_timeout(self, session_id: UUID, phase: SendPhase):
        session = self.__sending.get(session_id)
        if session is None:
            return

        if session.timeout_task is not None:
            session.timeout_task.cancel()
        session.phase = phase
        session.timeout_task = asyncio.create_task(
            self._monitor_send_timeout(session_id, phase)
        )

    @overload
    def on(self, event: Literal["start_receiving"], handler: StartReceivingHandler): ...
    @overload
    def on(self, event: Literal["received"], handler: ReceivedHandler): ...
    def on(self, event: str, handler: Callable):
        super().on(event, handler)

    async def _on_start_session(
        self,
        session_id: UUID,
        byte_length: int,
        sha256: str,
        mode: Literal["text", "binary"],
    ):
        if not isinstance(session_id, UUID):
            session_id = UUID(str(session_id))

        self._trace_session(
            "start_session.recv",
            session_id=session_id,
            byte_length=byte_length,
        )

        future: asyncio.Future[None] = asyncio.Future()

        def complete_transfer():
            if not future.done():
                future.set_result(None)

        def fail_transfer(error: Exception):
            if not future.done():
                future.set_exception(error)

        receive = TransferReceive(
            byte_length,
            sha256,
            mode,
            frame_body_size=self.__frame_body_size,
            request_chunk=lambda byte_offset, byte_length: self._request_chunk(
                session_id, receive, byte_offset, byte_length
            ),
            complete=complete_transfer,
            fail=fail_transfer,
        )
        receive.on("received", lambda data: self._emit("received", data))
        self.__receiving[session_id] = (receive, future)
        try:
            self._emit("start_receiving", receive)
            if not receive.has_receiver:
                asyncio.create_task(self._ensure_default_receiver(session_id, receive))
            receive._finalize_if_ready()
            await future
        finally:
            self._cancel_requested_chunks(session_id)
            self.__receiving.pop(session_id, None)

    async def _request_chunk(
        self,
        session_id: UUID,
        receive: TransferReceive,
        byte_offset: int,
        byte_length: int,
    ):
        chunk_id = uuid7().int & CHUNK_ID_MASK
        while chunk_id in self.__chunk_ids:
            chunk_id = (chunk_id + 1) & CHUNK_ID_MASK

        timeout_task = asyncio.create_task(
            self._monitor_chunk_timeout(session_id, chunk_id)
        )
        self.__chunk_ids[chunk_id] = RequestedChunk(
            session_id=session_id,
            receive=receive,
            offset=byte_offset,
            timeout_task=timeout_task,
        )
        self._trace_session(
            "request_chunk.send",
            session_id=session_id,
            chunk_id=chunk_id,
            byte_offset=byte_offset,
            byte_length=byte_length,
        )
        self.__messaging.notify(
            "request_chunk",
            session_id=session_id,
            chunk_id=chunk_id,
            byte_offset=byte_offset,
            byte_length=byte_length,
        )

    async def _on_request_chunk(
        self,
        session_id: UUID,
        chunk_id: int,
        byte_offset: int,
        byte_length: int,
    ) -> None:
        if not isinstance(session_id, UUID):
            session_id = UUID(str(session_id))
        if not isinstance(chunk_id, int):
            chunk_id = int(chunk_id)
        if not isinstance(byte_offset, int):
            byte_offset = int(byte_offset)
        if not isinstance(byte_length, int):
            byte_length = int(byte_length)

        session = self.__sending.get(session_id)
        if session is None:
            raise ValueError(f"Unknown session: {session_id}")

        self._trace_session(
            "request_chunk.recv",
            session_id=session_id,
            chunk_id=chunk_id,
            byte_offset=byte_offset,
            byte_length=byte_length,
            phase=session.phase,
        )
        data = session.payload
        if byte_offset < 0 or byte_length < 0:
            raise ValueError("Chunk offset and length must be non-negative.")
        if byte_offset + byte_length > len(data):
            raise ValueError("Requested chunk range exceeds the transfer size.")

        next_phase: SendPhase = (
            "awaiting_final_response"
            if byte_offset + byte_length == len(data)
            else "awaiting_request_chunk"
        )
        await self.__messaging.send_chunk(
            chunk_id,
            data[byte_offset : byte_offset + byte_length],
        )
        self._trace_session(
            "chunk_content.send",
            session_id=session_id,
            chunk_id=chunk_id,
            byte_offset=byte_offset,
            byte_length=byte_length,
            phase=next_phase,
        )
        self._arm_send_timeout(session_id, next_phase)

    async def _on_chunk_content(
        self,
        chunk_id: int,
        data: bytes,
    ):
        chunk = self._drop_requested_chunk(chunk_id, "chunk_content_received")
        if chunk is None:
            logger.debug(
                "Dekai trace event=chunk_content.missing chunk_id=%s",
                chunk_id,
            )
            logger.warning("Received unknown or expired chunk content: %s", chunk_id)
            return

        chunk.timeout_task.cancel()
        self._trace_session(
            "chunk_content.recv",
            session_id=chunk.session_id,
            chunk_id=chunk_id,
            byte_offset=chunk.offset,
            byte_length=len(data),
        )
        try:
            chunk.receive._push_chunk(chunk.offset, data)
        except Exception as exc:
            logger.error(
                "Failed to process received chunk: session_id=%s chunk_id=%s error=%s",
                chunk.session_id,
                chunk_id,
                exc,
                exc_info=exc,
            )
            self._fail_receive_session(chunk.session_id, exc)

    async def _on_timeout(self, session_id: UUID):
        if not isinstance(session_id, UUID):
            session_id = UUID(str(session_id))

        self._trace_session("timeout.recv", session_id=session_id)

        self._fail_receive_session(
            session_id,
            TimeoutError(f"Sender abandoned transfer due to timeout: {session_id}"),
        )

    async def send(self, data: str | bytes, mode: Literal["text", "binary"]):
        payload = data.encode("utf-8") if isinstance(data, str) else data
        session_id = uuid7()
        response_future: asyncio.Future = asyncio.Future()
        sending_session = SendingSession(
            payload=payload,
            response_future=response_future,
            phase="awaiting_request_chunk",
            timeout_task=None,
        )
        self.__sending[session_id] = sending_session
        self._trace_session(
            "start_session.send",
            session_id=session_id,
            byte_length=len(payload),
        )
        rpc_future = self.__messaging.call(
            "start_session",
            session_id=session_id,
            byte_length=len(payload),
            sha256=sha256(payload).hexdigest(),
            mode=mode,
        )
        rpc_future.add_done_callback(
            lambda future: self._settle_send_response_future(response_future, future)
        )
        self._arm_send_timeout(session_id, "awaiting_request_chunk")

        try:
            await response_future
        finally:
            if sending_session.timeout_task is not None:
                sending_session.timeout_task.cancel()
            self.__sending.pop(session_id, None)

    def _settle_send_response_future(
        self,
        response_future: asyncio.Future,
        rpc_future: asyncio.Future,
    ):
        if rpc_future.cancelled():
            if not response_future.done():
                response_future.cancel()
            return

        exception = rpc_future.exception()
        if response_future.done():
            return
        if exception is not None:
            response_future.set_exception(exception)
            return
        response_future.set_result(rpc_future.result())

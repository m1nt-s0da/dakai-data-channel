import asyncio
from typing import Callable, cast

from aiortc import RTCDataChannel
import pytest

from dekai_datachannel._dekai import DekaiDataChannel

TEST_TIMEOUT_SECONDS = 1.0


class FakeRTCDataChannel:
    def __init__(self):
        self._handlers: dict[str, list] = {}
        self._peer: FakeRTCDataChannel | None = None
        self._send_filter: Callable[[str | bytes], bool] | None = None

    def on(self, event: str, handler):
        self._handlers.setdefault(event, []).append(handler)

    def send(self, message: str | bytes):
        if self._peer is None:
            raise RuntimeError("Peer channel is not connected.")
        if self._send_filter is not None and not self._send_filter(message):
            return
        self._peer._deliver(message)

    def _deliver(self, message: str | bytes):
        for handler in self._handlers.get("message", []):
            handler(message)


def create_channel_pair() -> tuple[FakeRTCDataChannel, FakeRTCDataChannel]:
    left = FakeRTCDataChannel()
    right = FakeRTCDataChannel()
    left._peer = right
    right._peer = left
    return left, right


def frame_message(message: str | bytes) -> bool:
    return isinstance(message, bytes) and message[:1] == b"f"


def request_chunk_message(message: str | bytes) -> bool:
    return isinstance(message, bytes) and b'"method":"request_chunk"' in message


def test_send_text_buffered():
    async def scenario():
        sender_channel, receiver_channel = create_channel_pair()
        sender = DekaiDataChannel(cast(RTCDataChannel, sender_channel), chunk_size=32)
        receiver = DekaiDataChannel(
            cast(RTCDataChannel, receiver_channel), chunk_size=32
        )

        received = asyncio.Future()

        def on_start(receiving):
            async def consume():
                data = await receiving.buffered().join()
                if not received.done():
                    received.set_result(data.decode("utf-8"))

            asyncio.create_task(consume())

        receiver.on("start_receiving", on_start)

        await asyncio.wait_for(
            sender.send("hello buffered world", "text"), TEST_TIMEOUT_SECONDS
        )
        assert (
            await asyncio.wait_for(received, TEST_TIMEOUT_SECONDS)
            == "hello buffered world"
        )

    asyncio.run(asyncio.wait_for(scenario(), TEST_TIMEOUT_SECONDS))


def test_send_binary_stream():
    async def scenario():
        sender_channel, receiver_channel = create_channel_pair()
        sender = DekaiDataChannel(cast(RTCDataChannel, sender_channel), chunk_size=24)
        receiver = DekaiDataChannel(
            cast(RTCDataChannel, receiver_channel), chunk_size=24
        )

        payload = bytes(range(32))
        chunks: list[bytes] = []

        def on_start(receiving):
            async def consume():
                async for chunk in receiving.stream():
                    chunks.append(chunk)

            asyncio.create_task(consume())

        receiver.on("start_receiving", on_start)

        await asyncio.wait_for(sender.send(payload, "binary"), TEST_TIMEOUT_SECONDS)
        assert b"".join(chunks) == payload

    asyncio.run(asyncio.wait_for(scenario(), TEST_TIMEOUT_SECONDS))


def test_send_text_default_received_event():
    async def scenario():
        sender_channel, receiver_channel = create_channel_pair()
        sender = DekaiDataChannel(cast(RTCDataChannel, sender_channel), chunk_size=32)
        receiver = DekaiDataChannel(
            cast(RTCDataChannel, receiver_channel), chunk_size=32
        )

        received = asyncio.Future()
        receiver.on("received", lambda data: received.set_result(data))

        await asyncio.wait_for(sender.send("hello event", "text"), TEST_TIMEOUT_SECONDS)
        assert await asyncio.wait_for(received, TEST_TIMEOUT_SECONDS) == "hello event"

    asyncio.run(asyncio.wait_for(scenario(), TEST_TIMEOUT_SECONDS))


def test_buffered_progress_event():
    async def scenario():
        sender_channel, receiver_channel = create_channel_pair()
        sender = DekaiDataChannel(cast(RTCDataChannel, sender_channel), chunk_size=24)
        receiver = DekaiDataChannel(
            cast(RTCDataChannel, receiver_channel), chunk_size=24
        )

        progress_updates: list[tuple[int, int]] = []
        completed = asyncio.Future()

        def on_start(receiving):
            buffered = receiving.buffered()
            buffered.on(
                "progress",
                lambda received, total: progress_updates.append((received, total)),
            )

            async def consume():
                await buffered.join()
                if not completed.done():
                    completed.set_result(None)

            asyncio.create_task(consume())

        receiver.on("start_receiving", on_start)

        await asyncio.wait_for(
            sender.send(bytes(range(32)), "binary"), TEST_TIMEOUT_SECONDS
        )
        await asyncio.wait_for(completed, TEST_TIMEOUT_SECONDS)

        assert progress_updates == [(16, 32), (32, 32)]

    asyncio.run(asyncio.wait_for(scenario(), TEST_TIMEOUT_SECONDS))


def test_stream_progress_event():
    async def scenario():
        sender_channel, receiver_channel = create_channel_pair()
        sender = DekaiDataChannel(cast(RTCDataChannel, sender_channel), chunk_size=24)
        receiver = DekaiDataChannel(
            cast(RTCDataChannel, receiver_channel), chunk_size=24
        )

        progress_updates: list[tuple[int, int]] = []
        completed = asyncio.Future()

        def on_start(receiving):
            stream = receiving.stream()
            stream.on(
                "progress",
                lambda received, total: progress_updates.append((received, total)),
            )

            async def consume():
                async for _ in stream:
                    pass
                if not completed.done():
                    completed.set_result(None)

            asyncio.create_task(consume())

        receiver.on("start_receiving", on_start)

        await asyncio.wait_for(
            sender.send(bytes(range(32)), "binary"), TEST_TIMEOUT_SECONDS
        )
        await asyncio.wait_for(completed, TEST_TIMEOUT_SECONDS)

        assert progress_updates == [(16, 32), (32, 32)]

    asyncio.run(asyncio.wait_for(scenario(), TEST_TIMEOUT_SECONDS))


def test_receive_timeout_raises_error():
    async def scenario():
        sender_channel, receiver_channel = create_channel_pair()
        sender_channel._send_filter = lambda message: not frame_message(message)

        sender = DekaiDataChannel(
            cast(RTCDataChannel, sender_channel), chunk_size=24, timeout_seconds=0.2
        )
        receiver = DekaiDataChannel(
            cast(RTCDataChannel, receiver_channel), chunk_size=24, timeout_seconds=0.05
        )

        def on_start(receiving):
            asyncio.create_task(receiving.buffered().join())

        receiver.on("start_receiving", on_start)

        with pytest.raises(Exception, match="Timed out waiting for chunk content"):
            await asyncio.wait_for(
                sender.send(bytes(range(32)), "binary"), TEST_TIMEOUT_SECONDS
            )

    asyncio.run(asyncio.wait_for(scenario(), TEST_TIMEOUT_SECONDS))


def test_send_timeout_notifies_receiver():
    async def scenario():
        sender_channel, receiver_channel = create_channel_pair()
        sender_channel._send_filter = lambda message: not frame_message(message)

        sender = DekaiDataChannel(
            cast(RTCDataChannel, sender_channel), chunk_size=24, timeout_seconds=0.05
        )
        receiver = DekaiDataChannel(
            cast(RTCDataChannel, receiver_channel), chunk_size=24, timeout_seconds=0.2
        )

        received_error = asyncio.Future()

        def on_start(receiving):
            async def consume():
                try:
                    await receiving.buffered().join()
                except Exception as exc:
                    if not received_error.done():
                        received_error.set_result(str(exc))

            asyncio.create_task(consume())

        receiver.on("start_receiving", on_start)

        with pytest.raises(TimeoutError, match="Send session timed out"):
            await asyncio.wait_for(
                sender.send(bytes(range(32)), "binary"), TEST_TIMEOUT_SECONDS
            )

        assert "Sender abandoned transfer due to timeout" in await asyncio.wait_for(
            received_error, TEST_TIMEOUT_SECONDS
        )

    asyncio.run(asyncio.wait_for(scenario(), TEST_TIMEOUT_SECONDS))

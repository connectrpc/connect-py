from __future__ import annotations

import pytest
from protobuf import Message
from pyqwest import (
    Client,
    Request,
    Response,
    SyncClient,
    SyncRequest,
    SyncResponse,
    SyncTransport,
    Transport,
)
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc.codec import Codec

from .connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClient,
    HaberdasherClientSync,
    HaberdasherSync,
    HaberdasherWSGIApplication,
)
from .connectrpc.example.haberdasher_pb import Hat, Size


class CustomCodec(Codec[Message, Message]):
    def name(self) -> str:
        return "proto"

    def encode(self, message: Message) -> bytes:
        match message:
            case Size(inches=inches):
                return f"{inches}".encode()
            case Hat(size=size, color=color):
                return f"{size}:{color}".encode()
            case _:
                raise ValueError(f"unexpected message type: {type(message)}")

    def decode(self, data: bytes | bytearray, message_class: type[Message]) -> Message:
        s = data.decode()
        message = message_class()
        match message:
            case Size():
                message.inches = int(s)
            case Hat():
                size, color = s.split(":")
                message.size = int(size)
                message.color = color
        return message


class SimpleHaberdasher(Haberdasher):
    async def make_hat(self, request: Size, _ctx):
        return Hat(size=request.inches, color="blue")


class SimpleHaberdasherSync(HaberdasherSync):
    def make_hat(self, request: Size, _ctx):
        return Hat(size=request.inches, color="blue")


@pytest.mark.asyncio
async def test_custom_codec() -> None:
    logged_content: bytes = b""

    class LoggingTransport(Transport):
        def __init__(self, transport: Transport) -> None:
            self._transport = transport
            self.last_request_data: bytes | None = None

        async def execute(self, request: Request) -> Response:
            nonlocal logged_content
            if isinstance(request.content, bytes):
                logged_content = request.content
            else:
                chunks = []
                async for chunk in request.content:
                    chunks.append(chunk)
                logged_content = b"".join(chunks)
            return await self._transport.execute(
                Request(
                    method=request.method,
                    url=request.url,
                    headers=request.headers,
                    content=logged_content,
                )
            )

    transport = LoggingTransport(
        ASGITransport(
            HaberdasherASGIApplication(SimpleHaberdasher(), codecs=[CustomCodec()])
        )
    )
    client = HaberdasherClient(
        "http://localhost",
        http_client=Client(transport),
        codec=CustomCodec(),
        send_compression=None,
    )

    res = await client.make_hat(Size(inches=10))
    assert res.size == 10
    assert res.color == "blue"
    # Should be enough to just log/assert the client side
    assert logged_content == b"10"


def test_custom_codec_sync() -> None:
    logged_content: bytes = b""

    class LoggingSyncTransport(SyncTransport):
        def __init__(self, transport: SyncTransport) -> None:
            self._transport = transport

        def execute_sync(self, request: SyncRequest) -> SyncResponse:
            nonlocal logged_content
            if isinstance(request.content, bytes):
                logged_content = request.content
            else:
                logged_content = b"".join(request.content)
            return self._transport.execute_sync(
                SyncRequest(
                    method=request.method,
                    url=request.url,
                    headers=request.headers,
                    content=logged_content,
                )
            )

    transport = LoggingSyncTransport(
        WSGITransport(
            HaberdasherWSGIApplication(SimpleHaberdasherSync(), codecs=[CustomCodec()])
        )
    )
    client = HaberdasherClientSync(
        "http://localhost",
        http_client=SyncClient(transport=transport),
        codec=CustomCodec(),
        send_compression=None,
    )

    res = client.make_hat(Size(inches=10))
    assert res.size == 10
    assert res.color == "blue"
    # Should be enough to just log/assert the client side
    assert logged_content == b"10"

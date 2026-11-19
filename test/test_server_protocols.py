from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pytest
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from connectrpc.protocol import ProtocolType

from .connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClient,
    HaberdasherClientSync,
    HaberdasherSync,
    HaberdasherWSGIApplication,
)
from .connectrpc.example.haberdasher_pb import Hat, Size

if TYPE_CHECKING:
    from asgiref.typing import (
        ASGIReceiveCallable,
        ASGISendCallable,
        HTTPScope,
        WebSocketScope,
    )

    from connectrpc.request import RequestContext


class _FixedHaberdasher(Haberdasher):
    async def make_hat(self, request: Size, _ctx: RequestContext[Size, Hat], /) -> Hat:
        return Hat(size=request.inches, color="blue")


class _FixedHaberdasherSync(HaberdasherSync):
    def make_hat(self, request: Size, _ctx: RequestContext[Size, Hat], /) -> Hat:
        return Hat(size=request.inches, color="blue")


class _Connection:
    def __init__(self, messages: list[dict[str, Any]]) -> None:
        self._messages = messages
        self.sent: list[dict[str, Any]] = []

    async def receive(self) -> dict[str, Any]:
        return self._messages.pop(0)

    async def send(self, message: dict[str, Any]) -> None:
        self.sent.append(message)


_PATH = "/connectrpc.example.Haberdasher/MakeHat"


def _websocket_scope(*, extension: bool = True) -> dict[str, Any]:
    return {
        "type": "websocket",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "path": _PATH,
        "raw_path": _PATH.encode(),
        "root_path": "",
        "query_string": b"",
        "scheme": "ws",
        "headers": [(b"host", b"example.com")],
        "subprotocols": ["connectrpc.1+json"],
        "extensions": {"websocket.http.response": {}} if extension else {},
    }


def _http_scope(content_type: str) -> dict[str, Any]:
    return {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "path": _PATH,
        "raw_path": _PATH.encode(),
        "root_path": "",
        "query_string": b"",
        "scheme": "http",
        "headers": [
            (b"host", b"example.com"),
            (b"content-type", content_type.encode()),
        ],
        "extensions": {},
    }


def test_empty_protocols_rejected() -> None:
    with pytest.raises(ValueError, match="protocols must not be empty"):
        HaberdasherASGIApplication(_FixedHaberdasher(), protocols=())


@pytest.mark.anyio
async def test_websocket_handshake_rejected() -> None:
    app = HaberdasherASGIApplication(_FixedHaberdasher())
    connection = _Connection([{"type": "websocket.connect"}])

    await app(
        cast("WebSocketScope", _websocket_scope()),
        cast("ASGIReceiveCallable", connection.receive),
        cast("ASGISendCallable", connection.send),
    )

    assert connection.sent == [
        {"type": "websocket.http.response.start", "status": 400, "headers": []},
        {"type": "websocket.http.response.body", "body": b""},
    ]


@pytest.mark.anyio
async def test_websocket_handshake_rejected_without_extension() -> None:
    app = HaberdasherASGIApplication(_FixedHaberdasher())
    connection = _Connection([{"type": "websocket.connect"}])

    await app(
        cast("WebSocketScope", _websocket_scope(extension=False)),
        cast("ASGIReceiveCallable", connection.receive),
        cast("ASGISendCallable", connection.send),
    )

    assert connection.sent == [
        {"type": "websocket.close", "code": 1002, "reason": None}
    ]


@pytest.mark.anyio
async def test_http_protocol_rejected_when_not_served() -> None:
    app = HaberdasherASGIApplication(
        _FixedHaberdasher(), protocols=(ProtocolType.CONNECT,)
    )
    connection = _Connection(
        [{"type": "http.request", "body": b"", "more_body": False}]
    )

    await app(
        cast("HTTPScope", _http_scope("application/grpc-web+proto")),
        cast("ASGIReceiveCallable", connection.receive),
        cast("ASGISendCallable", connection.send),
    )

    start = connection.sent[0]
    assert start["type"] == "http.response.start"
    assert start["status"] == 415


@pytest.mark.anyio
async def test_served_protocol_passes_restriction() -> None:
    app = HaberdasherASGIApplication(
        _FixedHaberdasher(), protocols=(ProtocolType.CONNECT,)
    )
    async with (
        ASGITransport(app) as transport,
        HaberdasherClient("http://localhost", http_client=Client(transport)) as client,
    ):
        hat = await client.make_hat(Size(inches=10))
        assert hat.size == 10


def test_empty_protocols_rejected_sync() -> None:
    with pytest.raises(ValueError, match="protocols must not be empty"):
        HaberdasherWSGIApplication(_FixedHaberdasherSync(), protocols=())


def test_http_protocol_rejected_when_not_served_sync() -> None:
    app = HaberdasherWSGIApplication(
        _FixedHaberdasherSync(), protocols=(ProtocolType.CONNECT,)
    )
    with (
        HaberdasherClientSync(
            "http://localhost",
            protocol=ProtocolType.GRPC_WEB,
            http_client=SyncClient(WSGITransport(app)),
        ) as client,
        pytest.raises(ConnectError) as exc_info,
    ):
        client.make_hat(Size(inches=10))
    assert exc_info.value.code == Code.UNKNOWN


def test_served_protocol_passes_restriction_sync() -> None:
    app = HaberdasherWSGIApplication(
        _FixedHaberdasherSync(), protocols=(ProtocolType.CONNECT,)
    )
    with HaberdasherClientSync(
        "http://localhost", http_client=SyncClient(WSGITransport(app))
    ) as client:
        assert client.make_hat(Size(inches=10)).size == 10

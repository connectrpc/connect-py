from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import anyio
import pytest

from connectrpc.protocol import ProtocolType

from .connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
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


class _Connection:
    def __init__(self, messages: list[dict[str, Any]]) -> None:
        self._messages = messages
        self.sent: list[dict[str, Any]] = []

    async def receive(self) -> dict[str, Any]:
        if self._messages:
            return self._messages.pop(0)
        await anyio.sleep_forever()
        raise AssertionError

    async def send(self, message: dict[str, Any]) -> None:
        self.sent.append(message)


_PATH = "/connectrpc.example.Haberdasher/MakeHat"


def _websocket_scope() -> dict[str, Any]:
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
        "extensions": {"websocket.http.response": {}},
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
async def test_websocket_handshake_rejected_when_not_served() -> None:
    app = HaberdasherASGIApplication(
        _FixedHaberdasher(), protocols=(ProtocolType.CONNECT,)
    )
    connection = _Connection([{"type": "websocket.connect"}])

    with anyio.fail_after(5):
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
async def test_http_rejected_when_websockets_only() -> None:
    app = HaberdasherASGIApplication(
        _FixedHaberdasher(), protocols=(ProtocolType.CONNECT_WEBSOCKETS,)
    )
    connection = _Connection(
        [{"type": "http.request", "body": b"", "more_body": False}]
    )

    with anyio.fail_after(5):
        await app(
            cast("HTTPScope", _http_scope("application/proto")),
            cast("ASGIReceiveCallable", connection.receive),
            cast("ASGISendCallable", connection.send),
        )

    start = connection.sent[0]
    assert start["type"] == "http.response.start"
    assert start["status"] == 426
    assert (b"upgrade", b"websocket") in start["headers"]


@pytest.mark.anyio
async def test_http_protocol_rejected_when_not_served() -> None:
    app = HaberdasherASGIApplication(
        _FixedHaberdasher(), protocols=(ProtocolType.CONNECT,)
    )
    connection = _Connection(
        [{"type": "http.request", "body": b"", "more_body": False}]
    )

    with anyio.fail_after(5):
        await app(
            cast("HTTPScope", _http_scope("application/grpc-web+proto")),
            cast("ASGIReceiveCallable", connection.receive),
            cast("ASGISendCallable", connection.send),
        )

    start = connection.sent[0]
    assert start["type"] == "http.response.start"
    assert start["status"] == 415


@pytest.mark.anyio
async def test_websocket_served_when_websockets_only() -> None:
    app = HaberdasherASGIApplication(
        _FixedHaberdasher(), protocols=(ProtocolType.CONNECT_WEBSOCKETS,)
    )
    connection = _Connection(
        [
            {"type": "websocket.connect"},
            {"type": "websocket.receive", "text": "M[]"},
            {"type": "websocket.receive", "text": 'B{"inches": 10}'},
            {"type": "websocket.receive", "text": "C"},
        ]
    )

    with anyio.fail_after(5):
        await app(
            cast("WebSocketScope", _websocket_scope()),
            cast("ASGIReceiveCallable", connection.receive),
            cast("ASGISendCallable", connection.send),
        )

    texts = [
        m["text"]
        for m in connection.sent
        if m["type"] == "websocket.send" and m.get("text") is not None
    ]
    assert texts == ["M[]", 'B{"size":10,"color":"blue"}', "S{}"]

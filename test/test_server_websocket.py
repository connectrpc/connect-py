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
    from asgiref.typing import ASGIReceiveCallable, ASGISendCallable, WebSocketScope

    from connectrpc.request import RequestContext


class _FixedHaberdasher(Haberdasher):
    def __init__(self) -> None:
        self.protocols: list[ProtocolType] = []

    async def make_hat(self, request: Size, ctx: RequestContext[Size, Hat], /) -> Hat:
        self.protocols.append(ctx.protocol)
        return Hat(size=request.inches, color="blue")


class _Connection:
    """Scripted ASGI websocket events and a recorder for sent events."""

    def __init__(self, messages: list[dict[str, Any]], *, hang: bool = False) -> None:
        self._messages = messages
        # Once the messages run out, the client goes silent rather than
        # disconnecting.
        self._hang = hang

        self.sent: list[dict[str, Any]] = []

    async def receive(self) -> dict[str, Any]:
        if self._messages:
            return self._messages.pop(0)
        if self._hang:
            await anyio.sleep_forever()
        return {"type": "websocket.disconnect", "code": 1005}

    async def send(self, message: dict[str, Any]) -> None:
        self.sent.append(message)


def _scope(
    origin: str | None = None,
    host: str = "example.com",
    *,
    query_string: bytes = b"",
    http_version: str = "1.1",
) -> dict[str, Any]:
    headers = [(b"host", host.encode())]
    if origin is not None:
        headers.append((b"origin", origin.encode()))
    return {
        "type": "websocket",
        "asgi": {"version": "3.0"},
        "http_version": http_version,
        "path": "/connectrpc.example.Haberdasher/MakeHat",
        "raw_path": b"/connectrpc.example.Haberdasher/MakeHat",
        "root_path": "",
        "query_string": query_string,
        "scheme": "ws",
        "headers": headers,
        "subprotocols": ["connectrpc.1+json"],
        "extensions": {"websocket.http.response": {}},
    }


def _rpc_messages() -> list[dict[str, Any]]:
    return [
        {"type": "websocket.connect"},
        {"type": "websocket.receive", "text": "M{}"},
        {"type": "websocket.receive", "text": 'C{"inches": 10}'},
    ]


@pytest.mark.parametrize(
    ("origin", "allowed_origins", "accepted"),
    [
        # A handshake without an Origin header is not from a browser.
        (None, (), True),
        ("https://example.com", (), True),
        ("https://evil.example.org", (), False),
        ("https://app.example.org", ("app.example.org",), True),
        # The comparison ignores scheme and case, but the port must match
        # exactly as written.
        ("http://App.example.ORG", ("app.EXAMPLE.org",), True),
        ("https://app.example.org:8443", ("app.example.org",), False),
        ("https://app.example.org:8443", ("app.example.org:8443",), True),
        ("https://sub.app.example.org", ("*.example.org",), True),
        # A domain wildcard does not match the apex domain itself.
        ("https://example.org", ("*.example.org",), False),
        ("https://anything.example.net", ("*",), True),
        ("null", ("*",), True),
        ("null", ("example.org",), False),
        ("https://[2001:db8::1]:8443", ("[2001:db8::1]:8443",), True),
        ("https://app.example.org", ("other.example.org",), False),
    ],
)
@pytest.mark.anyio
async def test_websocket_origin_check(
    origin: str | None, allowed_origins: tuple[str, ...], accepted: bool
) -> None:
    service = _FixedHaberdasher()
    app = HaberdasherASGIApplication(
        service, websockets_allowed_origins=allowed_origins
    )
    connection = _Connection(_rpc_messages())

    await app(
        cast("WebSocketScope", _scope(origin)),
        cast("ASGIReceiveCallable", connection.receive),
        cast("ASGISendCallable", connection.send),
    )

    if accepted:
        assert {
            "type": "websocket.accept",
            "subprotocol": "connectrpc.1+json",
            "headers": [],
        } in connection.sent
        closes = [m for m in connection.sent if m["type"] == "websocket.close"]
        assert len(closes) == 1
        assert closes[0]["code"] == 1000
        bodies = [
            m["text"]
            for m in connection.sent
            if m["type"] == "websocket.send" and (m.get("text") or "").startswith("B")
        ]
        assert bodies == ['B{"size":10,"color":"blue"}']
        assert service.protocols == [ProtocolType.CONNECT_WEBSOCKETS]
    else:
        assert connection.sent == [
            {"type": "websocket.http.response.start", "status": 403, "headers": []},
            {"type": "websocket.http.response.body", "body": b""},
        ]


@pytest.mark.parametrize("pattern", ["foo.*.com", "*x.example.com", "example.*"])
def test_invalid_allowed_origin_pattern(pattern: str) -> None:
    with pytest.raises(ValueError, match="websockets_allowed_origins"):
        HaberdasherASGIApplication(
            _FixedHaberdasher(), websockets_allowed_origins=(pattern,)
        )


async def _run(
    scope: dict[str, Any], connection: _Connection
) -> tuple[_FixedHaberdasher, list[Any]]:
    """Run one connection, returning the service and what the server sent."""
    service = _FixedHaberdasher()
    app = HaberdasherASGIApplication(service)
    with anyio.fail_after(5):
        await app(
            cast("WebSocketScope", scope),
            cast("ASGIReceiveCallable", connection.receive),
            cast("ASGISendCallable", connection.send),
        )
    sent: list[Any] = []
    for message in connection.sent:
        match message["type"]:
            case "websocket.send":
                sent.append(message["text"])
            case "websocket.close":
                sent.append(message["code"])
    return service, sent


@pytest.mark.anyio
async def test_websocket_deadline_bounds_leading_metadata() -> None:
    # The deadline starts at the handshake, so it ends the wait for a client
    # that upgrades and then never sends its Leading-Metadata message.
    connection = _Connection([{"type": "websocket.connect"}], hang=True)

    service, sent = await _run(
        _scope(query_string=b"connect-timeout-ms=50"), connection
    )

    assert sent == [
        "M{}",
        'S{"error": {"code": "deadline_exceeded", "message": "Request timed out"}}',
        1000,
    ]
    assert service.protocols == []


@pytest.mark.anyio
async def test_websocket_rejects_http2_handshake() -> None:
    connection = _Connection(_rpc_messages())

    service, _ = await _run(_scope(http_version="2"), connection)

    assert connection.sent == [
        {"type": "websocket.http.response.start", "status": 400, "headers": []},
        {"type": "websocket.http.response.body", "body": b""},
    ]
    assert service.protocols == []


@pytest.mark.anyio
async def test_websocket_sends_nothing_after_disconnect() -> None:
    # The client disconnects before ending its stream, so it can no longer
    # receive a response.
    connection = _Connection(
        [{"type": "websocket.connect"}, {"type": "websocket.receive", "text": "M{}"}]
    )

    service, _ = await _run(_scope(), connection)

    assert connection.sent == [
        {"type": "websocket.accept", "subprotocol": "connectrpc.1+json", "headers": []}
    ]
    assert service.protocols == []


@pytest.mark.parametrize("value", ["a\\u0001b", "a\\u007fb", "a\\nb", "a\\u0000b"])
@pytest.mark.anyio
async def test_websocket_rejects_control_characters_in_metadata(value: str) -> None:
    messages = _rpc_messages()
    messages[1] = {"type": "websocket.receive", "text": f'M{{"x-custom": ["{value}"]}}'}

    service, sent = await _run(_scope(), _Connection(messages))

    assert sent == [
        "M{}",
        (
            'S{"error": {"code": "invalid_argument", "message": "metadata key '
            "'x-custom' has a value that is not a valid HTTP field value\"}}"
        ),
        1002,
    ]
    assert service.protocols == []


@pytest.mark.anyio
async def test_websocket_accepts_tab_in_metadata() -> None:
    messages = _rpc_messages()
    messages[1] = {"type": "websocket.receive", "text": 'M{"x-custom": ["a\\tb c"]}'}

    service, sent = await _run(_scope(), _Connection(messages))

    assert sent == ["M{}", 'B{"size":10,"color":"blue"}', "S{}", 1000]
    assert service.protocols == [ProtocolType.CONNECT_WEBSOCKETS]

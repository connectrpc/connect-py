from __future__ import annotations

from io import BytesIO
from typing import TYPE_CHECKING, Any, cast
from wsgiref.util import setup_testing_defaults

import anyio
import pytest
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

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
        if self._messages:
            return self._messages.pop(0)
        await anyio.sleep_forever()
        raise AssertionError

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


_GRPC_CONTENT_TYPES = "application/grpc, application/grpc+proto, application/grpc+json"
_GRPC_WEB_CONTENT_TYPES = (
    "application/grpc-web, application/grpc-web+proto, application/grpc-web+json"
)

# The protocols served, the content type of a request in a protocol that is
# not, and the Accept-Post header of its response.
_UNSERVED_PROTOCOL_CASES = [
    pytest.param(
        (ProtocolType.CONNECT,),
        "application/grpc-web+proto",
        "application/json, application/proto",
        id="grpc-web-to-connect",
    ),
    pytest.param(
        (ProtocolType.CONNECT,),
        "application/grpc",
        "application/json, application/proto",
        id="grpc-to-connect",
    ),
    pytest.param(
        (ProtocolType.GRPC,),
        "application/proto",
        _GRPC_CONTENT_TYPES,
        id="connect-to-grpc",
    ),
    pytest.param(
        (ProtocolType.GRPC, ProtocolType.GRPC_WEB),
        "application/json",
        f"{_GRPC_CONTENT_TYPES}, {_GRPC_WEB_CONTENT_TYPES}",
        id="connect-to-grpc-and-grpc-web",
    ),
]


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
async def test_websocket_handshake_rejected_without_extension() -> None:
    app = HaberdasherASGIApplication(
        _FixedHaberdasher(), protocols=(ProtocolType.CONNECT,)
    )
    connection = _Connection([{"type": "websocket.connect"}])

    with anyio.fail_after(5):
        await app(
            cast("WebSocketScope", _websocket_scope(extension=False)),
            cast("ASGIReceiveCallable", connection.receive),
            cast("ASGISendCallable", connection.send),
        )

    assert connection.sent == [
        {"type": "websocket.close", "code": 1002, "reason": None}
    ]


@pytest.mark.parametrize(
    ("protocols", "content_type", "accept_post"), _UNSERVED_PROTOCOL_CASES
)
@pytest.mark.anyio
async def test_http_protocol_rejected_when_not_served(
    protocols: tuple[ProtocolType, ...], content_type: str, accept_post: str
) -> None:
    app = HaberdasherASGIApplication(_FixedHaberdasher(), protocols=protocols)
    connection = _Connection(
        [{"type": "http.request", "body": b"", "more_body": False}]
    )

    with anyio.fail_after(5):
        await app(
            cast("HTTPScope", _http_scope(content_type)),
            cast("ASGIReceiveCallable", connection.receive),
            cast("ASGISendCallable", connection.send),
        )

    start = connection.sent[0]
    assert start["type"] == "http.response.start"
    assert start["status"] == 415
    assert (b"accept-post", accept_post.encode()) in [
        (name.lower(), value) for name, value in start["headers"]
    ]


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


@pytest.mark.parametrize(
    ("protocols", "content_type", "accept_post"), _UNSERVED_PROTOCOL_CASES
)
def test_http_protocol_rejected_when_not_served_sync(
    protocols: tuple[ProtocolType, ...], content_type: str, accept_post: str
) -> None:
    app = HaberdasherWSGIApplication(_FixedHaberdasherSync(), protocols=protocols)
    environ: dict[str, Any] = {
        "REQUEST_METHOD": "POST",
        "PATH_INFO": _PATH,
        "CONTENT_TYPE": content_type,
        "wsgi.input": BytesIO(),
    }
    setup_testing_defaults(environ)
    responses: list[tuple[str, list[tuple[str, str]]]] = []

    def start_response(
        status: str, headers: list[tuple[str, str]], _exc_info: object = None
    ) -> None:
        responses.append((status, headers))

    list(app(environ, cast("Any", start_response)))

    assert len(responses) == 1
    status, headers = responses[0]
    assert status.startswith("415 ")
    assert ("accept-post", accept_post) in [
        (name.lower(), value) for name, value in headers
    ]


def test_served_protocol_passes_restriction_sync() -> None:
    app = HaberdasherWSGIApplication(
        _FixedHaberdasherSync(), protocols=(ProtocolType.CONNECT,)
    )
    with HaberdasherClientSync(
        "http://localhost", http_client=SyncClient(WSGITransport(app))
    ) as client:
        assert client.make_hat(Size(inches=10)).size == 10

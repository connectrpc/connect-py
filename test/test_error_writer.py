from __future__ import annotations

from io import BytesIO

import pytest
from protobuf import Oneof
from protobuf.wkt import Struct, Value
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

from connectrpc.code import Code
from connectrpc.codec import proto_json_codec
from connectrpc.errors import ConnectError
from connectrpc.protocol import ProtocolType
from connectrpc.server import ErrorWriter

from .connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClient,
    HaberdasherClientSync,
    HaberdasherSync,
    HaberdasherWSGIApplication,
)
from .connectrpc.example.haberdasher_pb import Hat, Size

_ERROR = ConnectError(
    Code.RESOURCE_EXHAUSTED,
    "slow down: 10 requests/s",
    details=[Struct(fields={"limit": Value(kind=Oneof("string_value", "10/s"))})],
)


class _Haberdasher(Haberdasher):
    async def make_hat(self, request, _ctx):
        return Hat(size=request.inches)

    async def make_similar_hats(self, request, _ctx):
        yield Hat(size=request.inches)


class _HaberdasherSync(HaberdasherSync):
    def make_hat(self, request, _ctx):
        return Hat(size=request.inches)

    def make_similar_hats(self, request, _ctx):
        yield Hat(size=request.inches)


def _rejecting_asgi(app):
    """Middleware that rejects requests with an x-reject header."""

    async def middleware(scope, receive, send):
        if scope["type"] == "http" and b"x-reject" in dict(scope["headers"]):
            await ErrorWriter().write_asgi(scope, send, _ERROR)
            return
        await app(scope, receive, send)

    return middleware


def _rejecting_wsgi(app):
    """Middleware that rejects requests with an x-reject header."""

    def middleware(environ, start_response):
        if "HTTP_X_REJECT" in environ:
            return ErrorWriter().write_wsgi(environ, start_response, _ERROR)
        return app(environ, start_response)

    return middleware


class _Recorder(Transport):
    """Records the request's content type and the response's status and content type."""

    def __init__(self, transport: Transport) -> None:
        self._transport = transport
        self.last: tuple[str, int, str] | None = None

    async def execute(self, request: Request) -> Response:
        response = await self._transport.execute(request)
        self.last = (
            request.headers.get("content-type", ""),
            response.status,
            response.headers.get("content-type", ""),
        )
        return response


class _RecorderSync(SyncTransport):
    """Records the request's content type and the response's status and content type."""

    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport
        self.last: tuple[str, int, str] | None = None

    def execute_sync(self, request: SyncRequest) -> SyncResponse:
        response = self._transport.execute_sync(request)
        self.last = (
            request.headers.get("content-type", ""),
            response.status,
            response.headers.get("content-type", ""),
        )
        return response


_CASES = [
    pytest.param(protocol, codec, rpc, id=f"{protocol.name}-{codec_name}-{rpc}")
    for protocol in (ProtocolType.CONNECT, ProtocolType.GRPC, ProtocolType.GRPC_WEB)
    for codec_name, codec in (("proto", None), ("json", proto_json_codec()))
    for rpc in ("unary", "server_stream")
]


def _assert_rejected(
    error: ConnectError, last: tuple[str, int, str] | None, protocol, rpc
) -> None:
    assert error.code == Code.RESOURCE_EXHAUSTED
    assert error.message == "slow down: 10 requests/s"
    detail = error.details[0].value(Struct)
    assert detail is not None
    assert detail.fields["limit"].kind == Oneof("string_value", "10/s")
    # Clients also read a Connect unary error in place of the other formats, so
    # check that the response used the format of the request's protocol.
    assert last is not None
    request_content_type, status, content_type = last
    if protocol == ProtocolType.CONNECT and rpc == "unary":
        assert (status, content_type) == (429, "application/json")
    else:
        assert (status, content_type) == (200, request_content_type)


@pytest.mark.anyio
@pytest.mark.parametrize(("protocol", "codec", "rpc"), _CASES)
async def test_error_writer_asgi(protocol, codec, rpc) -> None:
    app = _rejecting_asgi(HaberdasherASGIApplication(_Haberdasher()))
    recorder = _Recorder(ASGITransport(app))
    async with HaberdasherClient(
        "http://localhost", protocol=protocol, codec=codec, http_client=Client(recorder)
    ) as client:

        async def call(headers: dict[str, str]) -> list[int]:
            if rpc == "unary":
                return [(await client.make_hat(Size(inches=3), headers=headers)).size]
            hats = client.make_similar_hats(Size(inches=3), headers=headers)
            return [hat.size async for hat in hats]

        # A request without the header reaches the application.
        assert await call({}) == [3]
        with pytest.raises(ConnectError) as exc_info:
            await call({"x-reject": "1"})
    _assert_rejected(exc_info.value, recorder.last, protocol, rpc)


@pytest.mark.parametrize(("protocol", "codec", "rpc"), _CASES)
def test_error_writer_wsgi(protocol, codec, rpc) -> None:
    app = _rejecting_wsgi(HaberdasherWSGIApplication(_HaberdasherSync()))
    recorder = _RecorderSync(WSGITransport(app))
    with HaberdasherClientSync(
        "http://localhost",
        protocol=protocol,
        codec=codec,
        http_client=SyncClient(recorder),
    ) as client:

        def call(headers: dict[str, str]) -> list[int]:
            if rpc == "unary":
                return [client.make_hat(Size(inches=3), headers=headers).size]
            hats = client.make_similar_hats(Size(inches=3), headers=headers)
            return [hat.size for hat in hats]

        # A request without the header reaches the application.
        assert call({}) == [3]
        with pytest.raises(ConnectError) as exc_info:
            call({"x-reject": "1"})
    _assert_rejected(exc_info.value, recorder.last, protocol, rpc)


@pytest.mark.parametrize(("server_protocol", "read"), [("HTTP/1.1", 5), ("HTTP/2", 0)])
def test_error_writer_wsgi_drains_http1_body(server_protocol, read) -> None:
    body = BytesIO(b"12345 and what follows the request")
    environ = {
        "REQUEST_METHOD": "POST",
        "SERVER_PROTOCOL": server_protocol,
        "CONTENT_TYPE": "application/proto",
        "CONTENT_LENGTH": "5",
        "wsgi.input": body,
    }

    def start_response(_status, _headers, _exc_info=None):
        return lambda _data: None

    ErrorWriter().write_wsgi(environ, start_response, _ERROR)
    # The body is read up to CONTENT_LENGTH on HTTP/1, as the application does.
    assert body.tell() == read

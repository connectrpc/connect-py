from __future__ import annotations

import io
from typing import TYPE_CHECKING

import pytest
from pyqwest import (
    Client,
    HTTPVersion,
    Request,
    SyncClient,
    SyncRequest,
    SyncTransport,
    Transport,
)
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc._server_sync import _RequestBody
from connectrpc.code import Code
from connectrpc.codec import proto_json_codec
from connectrpc.errors import ConnectError

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
    from pyqwest._pyqwest import Response, SyncResponse

_charset_content_type_cases = [
    "application/json",
    "application/json; charset=utf-8",
    "application/json; charset=UTF-8",
    "application/json;charset=utf-8",
    "application/json;    charset=utf-8",
    "application/json; charset=utf-8; version=1",
    "application/JSON",
]


@pytest.mark.parametrize("header", _charset_content_type_cases)
def test_json_charset_content_type(header: str) -> None:
    class HeadersHaberdasherSync(HaberdasherSync):
        def make_hat(self, _request, _ctx):
            return Hat(size=2)

    transport = WSGITransport(HaberdasherWSGIApplication(HeadersHaberdasherSync()))

    client = SyncClient(transport=transport)

    res = client.post(
        "http://localhost/connectrpc.example.Haberdasher/MakeHat",
        content=b"{}",
        headers={"content-type": header},
    )
    assert res.status == 200
    assert res.json() == {"size": 2}


@pytest.mark.asyncio
@pytest.mark.parametrize("header", _charset_content_type_cases)
async def test_json_charset_content_type_async(header: str) -> None:
    class HeadersHaberdasher(Haberdasher):
        async def make_hat(self, _request, _ctx):
            return Hat(size=2)

    transport = ASGITransport(HaberdasherASGIApplication(HeadersHaberdasher()))

    client = Client(transport=transport)

    res = await client.post(
        "http://localhost/connectrpc.example.Haberdasher/MakeHat",
        content=b"{}",
        headers={"content-type": header},
    )
    assert res.status == 200
    assert res.json() == {"size": 2}


_streaming_charset_content_type_cases = [
    "application/connect+" + h.split("/")[1] for h in _charset_content_type_cases
]


@pytest.mark.parametrize("header", _streaming_charset_content_type_cases)
def test_json_charset_content_type_stream(header: str) -> None:
    class HeadersHaberdasherSync(HaberdasherSync):
        def make_similar_hats(self, _request, _ctx):
            yield Hat(size=2)
            yield Hat(size=3)

    # Difficult to parse an HTTP streaming response so override the header
    # with a real client's transport instead.
    class HeaderTransport(SyncTransport):
        def __init__(self, delegate: SyncTransport):
            self._delegate = delegate

        def execute_sync(self, request: SyncRequest) -> SyncResponse:
            request.headers["content-type"] = header
            return self._delegate.execute_sync(request)

    transport = HeaderTransport(
        WSGITransport(HaberdasherWSGIApplication(HeadersHaberdasherSync()))
    )

    client = HaberdasherClientSync(
        address="http://localhost",
        codec=proto_json_codec(),
        http_client=SyncClient(transport=transport),
    )

    hats = list(client.make_similar_hats(Size(inches=2)))
    assert hats == [Hat(size=2), Hat(size=3)]


@pytest.mark.asyncio
@pytest.mark.parametrize("header", _streaming_charset_content_type_cases)
async def test_json_charset_content_type_stream_async(header: str) -> None:
    class HeadersHaberdasher(Haberdasher):
        async def make_similar_hats(self, _request, _ctx):
            yield Hat(size=2)
            yield Hat(size=3)

    # Difficult to parse an HTTP streaming response so override the header
    # with a real client's transport instead.
    class HeaderTransport(Transport):
        def __init__(self, delegate: Transport):
            self._delegate = delegate

        async def execute(self, request: Request) -> Response:
            request.headers["content-type"] = header
            return await self._delegate.execute(request)

    transport = HeaderTransport(
        ASGITransport(HaberdasherASGIApplication(HeadersHaberdasher()))
    )

    client = HaberdasherClient(
        address="http://localhost",
        codec=proto_json_codec(),
        http_client=Client(transport=transport),
    )

    hats = []
    async for hat in client.make_similar_hats(Size(inches=2)):
        hats.append(hat)
    assert hats == [Hat(size=2), Hat(size=3)]


def test_error_response_does_not_read_past_content_length() -> None:
    body = b"{}"

    class SocketLikeInput(io.BytesIO):
        def read(self, size: int | None = -1, /) -> bytes:
            if self.tell() >= len(body):
                msg = "read past CONTENT_LENGTH"
                raise AssertionError(msg)
            return super().read(size)

    class BoomHaberdasherSync(HaberdasherSync):
        def make_hat(self, _request, _ctx):
            raise ConnectError(Code.INVALID_ARGUMENT, "boom")

    app = HaberdasherWSGIApplication(BoomHaberdasherSync())

    def unbounded_input(environ, start_response):
        environ["wsgi.input"] = SocketLikeInput(environ["wsgi.input"].read())
        return app(environ, start_response)

    transport = WSGITransport(unbounded_input, http_version=HTTPVersion.HTTP1)
    client = SyncClient(transport=transport)

    res = client.post(
        "http://localhost/connectrpc.example.Haberdasher/MakeHat",
        content=body,
        headers={"content-type": "application/json", "content-length": str(len(body))},
    )

    assert transport.app_exception is None
    assert res.status == 400
    assert b"boom" in res.content


def test_request_body_stops_at_content_length() -> None:
    body = b"one\ntwo\nthree"
    unsent = b"the client never sends this"

    def from_environ(**extra: str) -> _RequestBody:
        return _RequestBody.from_environ(
            {"wsgi.input": io.BytesIO(body + unsent), **extra}
        )

    bounded = from_environ(CONTENT_LENGTH=str(len(body)))
    assert bounded.read(3) == b"one"
    assert bounded.read() == body[3:]
    assert bounded.read() == b""

    assert (
        from_environ(CONTENT_LENGTH=str(len(body))).read(len(body) + len(unsent))
        == body
    )

    # With no usable length there is nothing to bound by, so the stream is passed through.
    assert from_environ().read() == body + unsent
    assert from_environ(CONTENT_LENGTH="not a number").read() == body + unsent

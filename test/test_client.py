from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyqwest import (
    Client,
    ReadError,
    RemoteProtocolError,
    Request,
    Response,
    SyncClient,
    SyncRequest,
    SyncResponse,
    SyncTransport,
    Transport,
    WriteError,
)
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc.client import ResponseMetadata
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from connectrpc.protocol import ProtocolType

from ._util import call
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
    from collections.abc import AsyncIterator, Iterator

_default_headers = (
    ("content-type", "application/proto"),
    ("content-encoding", "gzip"),
    ("vary", "Accept-Encoding"),
)
_headers_cases = [
    ([], [], [*_default_headers], []),
    ([("x-animal", "bear")], [], [*_default_headers, ("x-animal", "bear")], []),
    (
        [("x-animal", "bear"), ("X-Animal", "cat")],
        [],
        [*_default_headers, ("x-animal", "bear"), ("x-animal", "cat")],
        [],
    ),
    ([], [("token-cost", "1000")], [*_default_headers], [("token-cost", "1000")]),
    (
        [],
        [("token-cost", "1000"), ("Token-Cost", "500")],
        [*_default_headers],
        [("token-cost", "1000"), ("token-cost", "500")],
    ),
    (
        [("x-animal", "bear"), ("X-Animal", "cat")],
        [("token-cost", "1000"), ("Token-Cost", "500")],
        [*_default_headers, ("x-animal", "bear"), ("x-animal", "cat")],
        [("token-cost", "1000"), ("token-cost", "500")],
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["async", "sync"])
@pytest.mark.parametrize(
    ("headers", "trailers", "response_headers", "response_trailers"), _headers_cases
)
async def test_headers(
    mode, headers, trailers, response_headers, response_trailers
) -> None:
    if mode == "async":

        class HeadersHaberdasher(Haberdasher):
            async def make_hat(self, _request, ctx):
                for key, value in headers:
                    ctx.response_headers.add(key, value)
                for key, value in trailers:
                    ctx.response_trailers.add(key, value)
                return Hat()

        client = HaberdasherClient(
            "http://localhost",
            http_client=Client(
                ASGITransport(HaberdasherASGIApplication(HeadersHaberdasher()))
            ),
        )
    else:

        class HeadersHaberdasherSync(HaberdasherSync):
            def make_hat(self, _request, ctx):
                for key, value in headers:
                    ctx.response_headers.add(key, value)
                for key, value in trailers:
                    ctx.response_trailers.add(key, value)
                return Hat()

        client = HaberdasherClientSync(
            "http://localhost",
            http_client=SyncClient(
                WSGITransport(HaberdasherWSGIApplication(HeadersHaberdasherSync()))
            ),
        )

    with ResponseMetadata() as resp:
        assert resp.http_status is None
        assert list(resp.headers.allitems()) == []
        assert list(resp.trailers.allitems()) == []
        await call(client.make_hat, Size(inches=10))

    assert resp.http_status == 200
    assert list(resp.headers.allitems()) == response_headers
    assert list(resp.trailers.allitems()) == response_trailers


_protocols = [ProtocolType.CONNECT, ProtocolType.GRPC, ProtocolType.GRPC_WEB]


@pytest.mark.asyncio
@pytest.mark.parametrize("protocol", _protocols)
async def test_request_content_async(protocol: ProtocolType) -> None:
    """A request with one message reaches the transport as sized bytes; a stream stays a stream."""

    class SizeHaberdasher(Haberdasher):
        async def make_hat(self, request, _ctx):
            return Hat(size=request.inches)

        async def make_flexible_hat(self, request, _ctx):
            return Hat(size=sum([size.inches async for size in request]))

        async def make_similar_hats(self, request, _ctx):
            yield Hat(size=request.inches)

        async def make_various_hats(self, request, _ctx):
            async for size in request:
                yield Hat(size=size.inches)

    contents: dict[str, bytes | AsyncIterator[bytes]] = {}

    class ContentRecorder(Transport):
        def __init__(self, transport: Transport) -> None:
            self._transport = transport

        async def execute(self, request: Request) -> Response:
            contents[request.url.rsplit("/", 1)[-1]] = request.content
            return await self._transport.execute(request)

    async def sizes() -> AsyncIterator[Size]:
        yield Size(inches=10)

    transport = ContentRecorder(
        ASGITransport(HaberdasherASGIApplication(SizeHaberdasher()))
    )
    async with HaberdasherClient(
        "http://localhost", protocol=protocol, http_client=Client(transport=transport)
    ) as client:
        await client.make_hat(Size(inches=10))
        assert [hat async for hat in client.make_similar_hats(Size(inches=10))]
        await client.make_flexible_hat(sizes())
        assert [hat async for hat in client.make_various_hats(sizes())]

    assert isinstance(contents["MakeHat"], bytes)
    assert isinstance(contents["MakeSimilarHats"], bytes)
    assert not isinstance(contents["MakeFlexibleHat"], bytes)
    assert not isinstance(contents["MakeVariousHats"], bytes)


@pytest.mark.parametrize("protocol", _protocols)
def test_request_content_sync(protocol: ProtocolType) -> None:
    """A request with one message reaches the transport as sized bytes; a stream stays a stream."""

    class SizeHaberdasherSync(HaberdasherSync):
        def make_hat(self, request, _ctx):
            return Hat(size=request.inches)

        def make_flexible_hat(self, request, _ctx):
            return Hat(size=sum(size.inches for size in request))

        def make_similar_hats(self, request, _ctx):
            yield Hat(size=request.inches)

        def make_various_hats(self, request, _ctx):
            for size in [*request]:
                yield Hat(size=size.inches)

    contents: dict[str, bytes | Iterator[bytes]] = {}

    class ContentRecorder(SyncTransport):
        def __init__(self, transport: SyncTransport) -> None:
            self._transport = transport

        def execute_sync(self, request: SyncRequest) -> SyncResponse:
            contents[request.url.rsplit("/", 1)[-1]] = request.content
            return self._transport.execute_sync(request)

    transport = ContentRecorder(
        WSGITransport(HaberdasherWSGIApplication(SizeHaberdasherSync()))
    )
    with HaberdasherClientSync(
        "http://localhost", protocol=protocol, http_client=SyncClient(transport)
    ) as client:
        client.make_hat(Size(inches=10))
        assert list(client.make_similar_hats(Size(inches=10)))
        client.make_flexible_hat(iter([Size(inches=10)]))
        assert list(client.make_various_hats(iter([Size(inches=10)])))

    assert isinstance(contents["MakeHat"], bytes)
    assert isinstance(contents["MakeSimilarHats"], bytes)
    assert not isinstance(contents["MakeFlexibleHat"], bytes)
    assert not isinstance(contents["MakeVariousHats"], bytes)


# Workaround pyqwest testing transports raising the response error instead of request.
# TODO: Cleanup after https://github.com/curioswitch/pyqwest/pull/290


class _FailingTransport(Transport):
    async def execute(self, request: Request) -> Response:
        assert not isinstance(request.content, bytes)
        async for _ in request.content:
            pass
        pytest.fail("request content did not raise")


class _FailingSyncTransport(SyncTransport):
    def execute_sync(self, request: SyncRequest) -> SyncResponse:
        assert not isinstance(request.content, bytes)
        for _ in request.content:
            pass
        pytest.fail("request content did not raise")


@pytest.mark.asyncio
@pytest.mark.parametrize("protocol", _protocols)
async def test_request_iterator_error_async(protocol: ProtocolType) -> None:
    """A request iterator that raises fails the call with INTERNAL, chained from it."""
    error = RuntimeError("Test error")

    async def sizes() -> AsyncIterator[Size]:
        yield Size(inches=10)
        raise error

    async with HaberdasherClient(
        "http://localhost",
        protocol=protocol,
        http_client=Client(transport=_FailingTransport()),
    ) as client:
        with pytest.raises(ConnectError) as exc_info:
            await client.make_flexible_hat(sizes())
        assert exc_info.value.code == Code.INTERNAL
        assert exc_info.value.__cause__ is error
        with pytest.raises(ConnectError) as exc_info:
            async for _ in client.make_various_hats(sizes()):
                pass
        assert exc_info.value.code == Code.INTERNAL
        assert exc_info.value.__cause__ is error


@pytest.mark.parametrize("protocol", _protocols)
def test_request_iterator_error_sync(protocol: ProtocolType) -> None:
    """A request iterator that raises fails the call with INTERNAL, chained from it."""
    error = RuntimeError("Test error")

    def sizes() -> Iterator[Size]:
        yield Size(inches=10)
        raise error

    with HaberdasherClientSync(
        "http://localhost",
        protocol=protocol,
        http_client=SyncClient(_FailingSyncTransport()),
    ) as client:
        with pytest.raises(ConnectError) as exc_info:
            client.make_flexible_hat(sizes())
        assert exc_info.value.code == Code.INTERNAL
        assert exc_info.value.__cause__ is error
        with pytest.raises(ConnectError) as exc_info:
            for _ in client.make_various_hats(sizes()):
                pass
        assert exc_info.value.code == Code.INTERNAL
        assert exc_info.value.__cause__ is error


_transport_errors = [
    (ConnectionError("Test error"), Code.UNAVAILABLE),
    (ReadError("Test error"), Code.UNAVAILABLE),
    (RemoteProtocolError("Test error"), Code.UNAVAILABLE),
    (WriteError("Test error"), Code.UNAVAILABLE),
    (RuntimeError("Test error"), Code.INTERNAL),
]


class _RaisingTransport(Transport):
    def __init__(self, error: Exception) -> None:
        self._error = error

    async def execute(self, request: Request) -> Response:
        del request
        raise self._error


class _RaisingSyncTransport(SyncTransport):
    def __init__(self, error: Exception) -> None:
        self._error = error

    def execute_sync(self, request: SyncRequest) -> SyncResponse:
        del request
        raise self._error


@pytest.mark.asyncio
@pytest.mark.parametrize(("error", "code"), _transport_errors)
async def test_transport_error_async(error: Exception, code: Code) -> None:
    """An I/O failure is UNAVAILABLE and any other transport failure is INTERNAL."""
    async with HaberdasherClient(
        "http://localhost", http_client=Client(transport=_RaisingTransport(error))
    ) as client:
        with pytest.raises(ConnectError) as exc_info:
            await client.make_hat(Size(inches=10))
        assert exc_info.value.code == code
        assert exc_info.value.__cause__ is error
        with pytest.raises(ConnectError) as exc_info:
            async for _ in client.make_similar_hats(Size(inches=10)):
                pass
        assert exc_info.value.code == code
        assert exc_info.value.__cause__ is error


@pytest.mark.parametrize(("error", "code"), _transport_errors)
def test_transport_error_sync(error: Exception, code: Code) -> None:
    """An I/O failure is UNAVAILABLE and any other transport failure is INTERNAL."""
    with HaberdasherClientSync(
        "http://localhost", http_client=SyncClient(_RaisingSyncTransport(error))
    ) as client:
        with pytest.raises(ConnectError) as exc_info:
            client.make_hat(Size(inches=10))
        assert exc_info.value.code == code
        assert exc_info.value.__cause__ is error
        with pytest.raises(ConnectError) as exc_info:
            for _ in client.make_similar_hats(Size(inches=10)):
                pass
        assert exc_info.value.code == code
        assert exc_info.value.__cause__ is error

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
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

from connectrpc.client import ResponseMetadata
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


@pytest.mark.parametrize(
    ("headers", "trailers", "response_headers", "response_trailers"), _headers_cases
)
def test_headers_sync(headers, trailers, response_headers, response_trailers) -> None:
    class HeadersHaberdasherSync(HaberdasherSync):
        def __init__(
            self, headers: list[tuple[str, str]], trailers: list[tuple[str, str]]
        ) -> None:
            self.headers = headers
            self.trailers = trailers

        def make_hat(self, _request, ctx):
            for key, value in self.headers:
                ctx.response_headers.add(key, value)
            for key, value in self.trailers:
                ctx.response_trailers.add(key, value)
            return Hat()

    transport = WSGITransport(
        HaberdasherWSGIApplication(HeadersHaberdasherSync(headers, trailers))
    )

    client = HaberdasherClientSync(
        "http://localhost", http_client=SyncClient(transport=transport)
    )

    with ResponseMetadata() as resp:
        assert resp.http_status is None
        assert list(resp.headers.allitems()) == []
        assert list(resp.trailers.allitems()) == []
        client.make_hat(Size(inches=10))

    assert resp.http_status == 200
    assert list(resp.headers.allitems()) == response_headers
    assert list(resp.trailers.allitems()) == response_trailers


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("headers", "trailers", "response_headers", "response_trailers"), _headers_cases
)
async def test_headers_async(
    headers, trailers, response_headers, response_trailers
) -> None:
    class HeadersHaberdasher(Haberdasher):
        def __init__(
            self, headers: list[tuple[str, str]], trailers: list[tuple[str, str]]
        ) -> None:
            self.headers = headers
            self.trailers = trailers

        async def make_hat(self, _request, ctx):
            for key, value in self.headers:
                ctx.response_headers.add(key, value)
            for key, value in self.trailers:
                ctx.response_trailers.add(key, value)
            return Hat()

    transport = ASGITransport(
        HaberdasherASGIApplication(HeadersHaberdasher(headers, trailers))
    )

    client = HaberdasherClient(
        "http://localhost", http_client=Client(transport=transport)
    )

    with ResponseMetadata() as resp:
        assert resp.http_status is None
        assert list(resp.headers.allitems()) == []
        assert list(resp.trailers.allitems()) == []
        await client.make_hat(Size(inches=10))

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

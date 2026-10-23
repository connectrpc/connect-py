from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc.method import StreamType

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

    from connectrpc.request import RequestContext


class HaberdasherService(Haberdasher):
    async def make_hat(self, request: Size, _ctx: RequestContext[Size, Hat]) -> Hat:
        return Hat(size=request.inches, color="green")

    async def make_flexible_hat(
        self, request: AsyncIterator[Size], _ctx: RequestContext[Size, Hat]
    ) -> Hat:
        size = 0
        async for s in request:
            size += s.inches
        return Hat(size=size, color="red")

    async def make_similar_hats(
        self, request: Size, _ctx: RequestContext[Size, Hat]
    ) -> AsyncIterator[Hat]:
        yield Hat(size=request.inches, color="orange")
        yield Hat(size=request.inches, color="blue")

    async def make_various_hats(
        self, request: AsyncIterator[Size], _ctx: RequestContext[Size, Hat]
    ) -> AsyncIterator[Hat]:
        async for s in request:
            yield Hat(size=s.inches, color="black")


class HaberdasherServiceSync(HaberdasherSync):
    def make_hat(self, request: Size, _ctx: RequestContext[Size, Hat]) -> Hat:
        return Hat(size=request.inches, color="green")

    def make_flexible_hat(
        self, request: Iterator[Size], _ctx: RequestContext[Size, Hat]
    ) -> Hat:
        return Hat(size=sum(s.inches for s in request), color="red")

    def make_similar_hats(
        self, request: Size, _ctx: RequestContext[Size, Hat]
    ) -> Iterator[Hat]:
        yield Hat(size=request.inches, color="orange")
        yield Hat(size=request.inches, color="blue")

    def make_various_hats(
        self, request: Iterator[Size], _ctx: RequestContext[Size, Hat]
    ) -> Iterator[Hat]:
        for s in request:
            yield Hat(size=s.inches, color="black")


class StreamTypeMetadataInterceptor:
    """A metadata interceptor recording the stream type of each RPC it sees."""

    def __init__(self) -> None:
        self.stream_types: list[StreamType] = []

    async def on_start(self, ctx: RequestContext) -> None:
        self.stream_types.append(ctx.method.stream_type)

    async def on_end(
        self, token: None, ctx: RequestContext, error: Exception | None, /
    ) -> None:
        pass


class StreamTypeMetadataInterceptorSync:
    """A sync metadata interceptor recording the stream type of each RPC it sees."""

    def __init__(self) -> None:
        self.stream_types: list[StreamType] = []

    def on_start_sync(self, ctx: RequestContext, /) -> None:
        self.stream_types.append(ctx.method.stream_type)

    def on_end_sync(
        self, token: None, ctx: RequestContext, error: Exception | None, /
    ) -> None:
        pass


_CASES = [
    pytest.param("make_hat", Size(inches=10), StreamType.UNARY, id="unary"),
    pytest.param(
        "make_flexible_hat",
        [Size(inches=10), Size(inches=20)],
        StreamType.CLIENT_STREAM,
        id="client_stream",
    ),
    pytest.param(
        "make_similar_hats",
        Size(inches=15),
        StreamType.SERVER_STREAM,
        id="server_stream",
    ),
    pytest.param(
        "make_various_hats",
        [Size(inches=25), Size(inches=35)],
        StreamType.BIDI_STREAM,
        id="bidi_stream",
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("method_name", "request_", "expected"), _CASES)
async def test_metadata_interceptor_sees_stream_type(
    method_name: str, request_: Size | list[Size], expected: StreamType
) -> None:
    client_interceptor = StreamTypeMetadataInterceptor()
    server_interceptor = StreamTypeMetadataInterceptor()
    app = HaberdasherASGIApplication(
        HaberdasherService(), interceptors=[server_interceptor]
    )
    async with HaberdasherClient(
        "http://localhost",
        http_client=Client(ASGITransport(app)),
        interceptors=[client_interceptor],
    ) as client:
        await call(getattr(client, method_name), request_)
    assert client_interceptor.stream_types == [expected]
    assert server_interceptor.stream_types == [expected]


@pytest.mark.asyncio
@pytest.mark.parametrize(("method_name", "request_", "expected"), _CASES)
async def test_metadata_interceptor_sees_stream_type_sync(
    method_name: str, request_: Size | list[Size], expected: StreamType
) -> None:
    client_interceptor = StreamTypeMetadataInterceptorSync()
    server_interceptor = StreamTypeMetadataInterceptorSync()
    app = HaberdasherWSGIApplication(
        HaberdasherServiceSync(), interceptors=[server_interceptor]
    )
    with HaberdasherClientSync(
        "http://localhost",
        http_client=SyncClient(WSGITransport(app)),
        interceptors=[client_interceptor],
    ) as client:
        await call(getattr(client, method_name), request_)
    assert client_interceptor.stream_types == [expected]
    assert server_interceptor.stream_types == [expected]

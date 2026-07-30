from __future__ import annotations

import itertools
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc.code import Code
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
    from connectrpc.request import RequestContext


class RequestInterceptor:
    def __init__(self) -> None:
        self.result = []

    async def on_start(self, ctx: RequestContext):
        return self.on_start_sync(ctx)

    async def on_end(
        self, token: str, ctx: RequestContext, error: Exception | None
    ) -> None:
        self.on_end_sync(token, ctx, error)

    def on_start_sync(self, ctx: RequestContext) -> str:
        return f"Hello {ctx.method.name}"

    def on_end_sync(
        self, token: str, _ctx: RequestContext, error: Exception | None
    ) -> None:
        msg = f"{token} and goodbye"
        if error is not None:
            msg += f" with error {error}"
        self.result.append(msg)


@pytest.fixture
def client_interceptor():
    return RequestInterceptor()


@pytest.fixture
def server_interceptor():
    return RequestInterceptor()


@pytest_asyncio.fixture
async def client_async(
    client_interceptor: RequestInterceptor, server_interceptor: RequestInterceptor
):
    class SimpleHaberdasher(Haberdasher):
        async def make_hat(self, request, _ctx):
            if request.inches < 0:
                raise ConnectError(Code.INVALID_ARGUMENT, "Size must be non-negative")
            return Hat(size=request.inches, color="green")

        async def make_flexible_hat(self, request, _ctx):
            size = 0
            async for s in request:
                if s.inches < 0:
                    raise ConnectError(
                        Code.INVALID_ARGUMENT, "Size must be non-negative"
                    )
                size += s.inches
            return Hat(size=size, color="red")

        async def make_similar_hats(self, request, _ctx):
            if request.inches < 0:
                raise ConnectError(Code.INVALID_ARGUMENT, "Size must be non-negative")
            yield Hat(size=request.inches, color="orange")
            yield Hat(size=request.inches, color="blue")

        async def make_various_hats(self, request, _ctx):
            colors = itertools.cycle(("black", "white", "gold"))
            async for s in request:
                if s.inches < 0:
                    raise ConnectError(
                        Code.INVALID_ARGUMENT, "Size must be non-negative"
                    )
                yield Hat(size=s.inches, color=next(colors))

    app = HaberdasherASGIApplication(
        SimpleHaberdasher(), interceptors=(server_interceptor,)
    )
    transport = ASGITransport(app)
    async with HaberdasherClient(
        "http://localhost",
        interceptors=(client_interceptor,),
        http_client=Client(transport=transport),
    ) as client:
        yield client


@pytest.mark.asyncio
async def test_intercept_unary_async(
    client_async: HaberdasherClient,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
) -> None:
    result = await client_async.make_hat(Size(inches=10))
    assert result == Hat(size=10, color="green")
    assert client_interceptor.result == ["Hello MakeHat and goodbye"]
    assert server_interceptor.result == ["Hello MakeHat and goodbye"]


@pytest.mark.asyncio
async def test_intercept_unary_async_error(
    client_async: HaberdasherClient,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
) -> None:
    with pytest.raises(ConnectError):
        await client_async.make_hat(Size(inches=-10))
    assert client_interceptor.result == [
        "Hello MakeHat and goodbye with error Size must be non-negative"
    ]
    assert server_interceptor.result == [
        "Hello MakeHat and goodbye with error Size must be non-negative"
    ]


@pytest.mark.asyncio
async def test_intercept_client_stream_async(
    client_async: HaberdasherClient,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
) -> None:
    async def requests():
        yield Size(inches=10)
        yield Size(inches=20)

    result = await client_async.make_flexible_hat(requests())
    assert result == Hat(size=30, color="red")
    assert client_interceptor.result == ["Hello MakeFlexibleHat and goodbye"]
    assert server_interceptor.result == ["Hello MakeFlexibleHat and goodbye"]


@pytest.mark.asyncio
async def test_intercept_client_stream_async_error(
    client_async: HaberdasherClient,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
) -> None:
    async def requests():
        yield Size(inches=-10)
        yield Size(inches=20)

    with pytest.raises(ConnectError):
        await client_async.make_flexible_hat(requests())
    assert client_interceptor.result == [
        "Hello MakeFlexibleHat and goodbye with error Size must be non-negative"
    ]
    assert server_interceptor.result == [
        "Hello MakeFlexibleHat and goodbye with error Size must be non-negative"
    ]


@pytest.mark.asyncio
async def test_intercept_server_stream_async(
    client_async: HaberdasherClient,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
) -> None:
    result = [r async for r in client_async.make_similar_hats(Size(inches=15))]

    assert result == [Hat(size=15, color="orange"), Hat(size=15, color="blue")]
    assert client_interceptor.result == ["Hello MakeSimilarHats and goodbye"]
    assert server_interceptor.result == ["Hello MakeSimilarHats and goodbye"]


@pytest.mark.asyncio
async def test_intercept_server_stream_async_error(
    client_async: HaberdasherClient,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
) -> None:
    with pytest.raises(ConnectError):
        async for _ in client_async.make_similar_hats(Size(inches=-15)):
            pass

    assert client_interceptor.result == [
        "Hello MakeSimilarHats and goodbye with error Size must be non-negative"
    ]
    assert server_interceptor.result == [
        "Hello MakeSimilarHats and goodbye with error Size must be non-negative"
    ]


@pytest.mark.asyncio
async def test_intercept_bidi_stream_async(
    client_async: HaberdasherClient,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
) -> None:
    async def requests():
        yield Size(inches=25)
        yield Size(inches=35)
        yield Size(inches=45)

    result = [r async for r in client_async.make_various_hats(requests())]

    assert result == [
        Hat(size=25, color="black"),
        Hat(size=35, color="white"),
        Hat(size=45, color="gold"),
    ]
    assert client_interceptor.result == ["Hello MakeVariousHats and goodbye"]
    assert server_interceptor.result == ["Hello MakeVariousHats and goodbye"]


@pytest.mark.asyncio
async def test_intercept_bidi_stream_async_error(
    client_async: HaberdasherClient,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
) -> None:
    async def requests():
        yield Size(inches=-25)
        yield Size(inches=35)
        yield Size(inches=45)

    with pytest.raises(ConnectError):
        async for _ in client_async.make_various_hats(requests()):
            pass

    assert client_interceptor.result == [
        "Hello MakeVariousHats and goodbye with error Size must be non-negative"
    ]
    assert server_interceptor.result == [
        "Hello MakeVariousHats and goodbye with error Size must be non-negative"
    ]


@pytest.fixture
def client_sync(
    client_interceptor: RequestInterceptor, server_interceptor: RequestInterceptor
):
    class SimpleHaberdasherSync(HaberdasherSync):
        def make_hat(self, request, _ctx):
            if request.inches < 0:
                raise ConnectError(Code.INVALID_ARGUMENT, "Size must be non-negative")
            return Hat(size=request.inches, color="green")

        def make_flexible_hat(self, request, _ctx):
            size = 0
            for s in request:
                if s.inches < 0:
                    raise ConnectError(
                        Code.INVALID_ARGUMENT, "Size must be non-negative"
                    )
                size += s.inches
            return Hat(size=size, color="red")

        def make_similar_hats(self, request, _ctx):
            if request.inches < 0:
                raise ConnectError(Code.INVALID_ARGUMENT, "Size must be non-negative")
            yield Hat(size=request.inches, color="orange")
            yield Hat(size=request.inches, color="blue")

        def make_various_hats(self, request, _ctx):
            colors = itertools.cycle(("black", "white", "gold"))
            requests = [*request]
            for s in requests:
                if s.inches < 0:
                    raise ConnectError(
                        Code.INVALID_ARGUMENT, "Size must be non-negative"
                    )
                yield Hat(size=s.inches, color=next(colors))

    app = HaberdasherWSGIApplication(
        SimpleHaberdasherSync(), interceptors=(server_interceptor,)
    )
    transport = WSGITransport(app)
    with HaberdasherClientSync(
        "http://localhost",
        interceptors=(client_interceptor,),
        http_client=SyncClient(transport),
    ) as client:
        yield client


def test_intercept_unary_sync(
    client_sync: HaberdasherClientSync,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
) -> None:
    result = client_sync.make_hat(Size(inches=10))
    assert result == Hat(size=10, color="green")
    assert client_interceptor.result == ["Hello MakeHat and goodbye"]
    assert server_interceptor.result == ["Hello MakeHat and goodbye"]


def test_intercept_unary_sync_error(
    client_sync: HaberdasherClientSync,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
) -> None:
    with pytest.raises(ConnectError):
        client_sync.make_hat(Size(inches=-10))
    assert client_interceptor.result == [
        "Hello MakeHat and goodbye with error Size must be non-negative"
    ]
    assert server_interceptor.result == [
        "Hello MakeHat and goodbye with error Size must be non-negative"
    ]


def test_intercept_client_stream_sync(
    client_sync: HaberdasherClientSync,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
) -> None:
    def requests():
        yield Size(inches=10)
        yield Size(inches=20)

    result = client_sync.make_flexible_hat(requests())
    assert result == Hat(size=30, color="red")
    assert client_interceptor.result == ["Hello MakeFlexibleHat and goodbye"]
    assert server_interceptor.result == ["Hello MakeFlexibleHat and goodbye"]


def test_intercept_client_stream_sync_error(
    client_sync: HaberdasherClientSync,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
) -> None:
    def requests():
        yield Size(inches=-10)
        yield Size(inches=20)

    with pytest.raises(ConnectError):
        client_sync.make_flexible_hat(requests())
    assert client_interceptor.result == [
        "Hello MakeFlexibleHat and goodbye with error Size must be non-negative"
    ]
    assert server_interceptor.result == [
        "Hello MakeFlexibleHat and goodbye with error Size must be non-negative"
    ]


def test_intercept_server_stream_sync(
    client_sync: HaberdasherClientSync,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
) -> None:
    result = list(client_sync.make_similar_hats(Size(inches=15)))

    assert result == [Hat(size=15, color="orange"), Hat(size=15, color="blue")]
    assert client_interceptor.result == ["Hello MakeSimilarHats and goodbye"]
    assert server_interceptor.result == ["Hello MakeSimilarHats and goodbye"]


def test_intercept_server_stream_sync_error(
    client_sync: HaberdasherClientSync,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
) -> None:
    with pytest.raises(ConnectError):
        list(client_sync.make_similar_hats(Size(inches=-15)))
    assert client_interceptor.result == [
        "Hello MakeSimilarHats and goodbye with error Size must be non-negative"
    ]
    assert server_interceptor.result == [
        "Hello MakeSimilarHats and goodbye with error Size must be non-negative"
    ]


def test_intercept_bidi_stream_sync(
    client_sync: HaberdasherClientSync,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
) -> None:
    def requests():
        yield Size(inches=25)
        yield Size(inches=35)
        yield Size(inches=45)

    result = list(client_sync.make_various_hats(requests()))

    assert result == [
        Hat(size=25, color="black"),
        Hat(size=35, color="white"),
        Hat(size=45, color="gold"),
    ]
    assert client_interceptor.result == ["Hello MakeVariousHats and goodbye"]
    assert server_interceptor.result == ["Hello MakeVariousHats and goodbye"]


def test_intercept_bidi_stream_sync_error(
    client_sync: HaberdasherClientSync,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
) -> None:
    def requests():
        yield Size(inches=-25)
        yield Size(inches=35)
        yield Size(inches=45)

    with pytest.raises(ConnectError):
        list(client_sync.make_various_hats(requests()))

    assert client_interceptor.result == [
        "Hello MakeVariousHats and goodbye with error Size must be non-negative"
    ]
    assert server_interceptor.result == [
        "Hello MakeVariousHats and goodbye with error Size must be non-negative"
    ]

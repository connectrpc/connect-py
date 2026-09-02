from __future__ import annotations

import itertools
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc.client import ResponseMetadata
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


class _CountingHaberdasher(Haberdasher):
    def __init__(self) -> None:
        self.calls = 0

    async def make_hat(self, request, _ctx):
        self.calls += 1
        return Hat(size=request.inches)

    async def make_similar_hats(self, request, _ctx):
        self.calls += 1
        yield Hat(size=request.inches)


class _CountingHaberdasherSync(HaberdasherSync):
    def __init__(self) -> None:
        self.calls = 0

    def make_hat(self, request, _ctx):
        self.calls += 1
        return Hat(size=request.inches)

    def make_similar_hats(self, request, _ctx):
        self.calls += 1
        yield Hat(size=request.inches)


class _PassthroughUnaryInterceptor:
    async def intercept_unary(self, call_next, request, ctx):
        return await call_next(request, ctx)

    def intercept_unary_sync(self, call_next, request, ctx):
        return call_next(request, ctx)


_MAKE_HAT_URL = "http://localhost/connectrpc.example.Haberdasher/MakeHat"
_MAKE_SIMILAR_HATS_URL = (
    "http://localhost/connectrpc.example.Haberdasher/MakeSimilarHats"
)
# A one-byte envelope containing invalid JSON.
_INVALID_STREAM_BODY = b"\x00" + (1).to_bytes(4, "big") + b"{"


@pytest.mark.asyncio
async def test_metadata_interceptor_unparseable_unary_async(
    server_interceptor: RequestInterceptor,
) -> None:
    """Leading metadata interceptors are invoked even if the message can't be parsed."""
    service = _CountingHaberdasher()
    trailing_interceptor = RequestInterceptor()
    app = HaberdasherASGIApplication(
        service,
        interceptors=(
            server_interceptor,
            _PassthroughUnaryInterceptor(),
            trailing_interceptor,
        ),
    )
    client = Client(transport=ASGITransport(app))

    res = await client.post(
        _MAKE_HAT_URL, content=b"{", headers={"content-type": "application/json"}
    )

    assert res.status != 200
    assert service.calls == 0
    assert len(server_interceptor.result) == 1
    assert server_interceptor.result[0].startswith(
        "Hello MakeHat and goodbye with error"
    )
    # A metadata interceptor after a message interceptor is invoked within the
    # chain, so it never runs if the message can't be parsed.
    assert trailing_interceptor.result == []


def test_metadata_interceptor_unparseable_unary_sync(
    server_interceptor: RequestInterceptor,
) -> None:
    """Leading metadata interceptors are invoked even if the message can't be parsed."""
    service = _CountingHaberdasherSync()
    trailing_interceptor = RequestInterceptor()
    app = HaberdasherWSGIApplication(
        service,
        interceptors=(
            server_interceptor,
            _PassthroughUnaryInterceptor(),
            trailing_interceptor,
        ),
    )
    client = SyncClient(transport=WSGITransport(app))

    res = client.post(
        _MAKE_HAT_URL, content=b"{", headers={"content-type": "application/json"}
    )

    assert res.status != 200
    assert service.calls == 0
    assert len(server_interceptor.result) == 1
    assert server_interceptor.result[0].startswith(
        "Hello MakeHat and goodbye with error"
    )
    assert trailing_interceptor.result == []


@pytest.mark.asyncio
async def test_metadata_interceptor_unparseable_stream_async(
    server_interceptor: RequestInterceptor,
) -> None:
    """Leading metadata interceptors are invoked even if a stream message can't be parsed."""
    service = _CountingHaberdasher()
    app = HaberdasherASGIApplication(service, interceptors=(server_interceptor,))
    client = Client(transport=ASGITransport(app))

    res = await client.post(
        _MAKE_SIMILAR_HATS_URL,
        content=_INVALID_STREAM_BODY,
        headers={"content-type": "application/connect+json"},
    )

    assert res.status == 200
    assert service.calls == 0
    assert len(server_interceptor.result) == 1
    assert server_interceptor.result[0].startswith(
        "Hello MakeSimilarHats and goodbye with error"
    )


def test_metadata_interceptor_unparseable_stream_sync(
    server_interceptor: RequestInterceptor,
) -> None:
    """Leading metadata interceptors are invoked even if a stream message can't be parsed."""
    service = _CountingHaberdasherSync()
    app = HaberdasherWSGIApplication(service, interceptors=(server_interceptor,))
    client = SyncClient(transport=WSGITransport(app))

    res = client.post(
        _MAKE_SIMILAR_HATS_URL,
        content=_INVALID_STREAM_BODY,
        headers={"content-type": "application/connect+json"},
    )

    assert res.status == 200
    assert service.calls == 0
    assert len(server_interceptor.result) == 1
    assert server_interceptor.result[0].startswith(
        "Hello MakeSimilarHats and goodbye with error"
    )


@pytest.mark.asyncio
async def test_metadata_interceptor_ordering_async() -> None:
    """A leading metadata interceptor wraps message interceptors after it."""
    events: list[str] = []

    class EventMetadataInterceptor:
        async def on_start(self, ctx):  # noqa: ARG002
            events.append("metadata start")

        async def on_end(self, _token, _ctx, _error):
            events.append("metadata end")

    class EventUnaryInterceptor:
        async def intercept_unary(self, call_next, request, ctx):
            events.append("unary before")
            try:
                return await call_next(request, ctx)
            finally:
                events.append("unary after")

    class SimpleHaberdasher(Haberdasher):
        async def make_hat(self, request, _ctx):
            events.append("handler")
            return Hat(size=request.inches)

    app = HaberdasherASGIApplication(
        SimpleHaberdasher(),
        interceptors=(EventMetadataInterceptor(), EventUnaryInterceptor()),
    )
    async with HaberdasherClient(
        "http://localhost", http_client=Client(transport=ASGITransport(app))
    ) as client:
        await client.make_hat(Size(inches=10))

    assert events == [
        "metadata start",
        "unary before",
        "handler",
        "unary after",
        "metadata end",
    ]


def test_metadata_interceptor_ordering_sync() -> None:
    """A leading metadata interceptor wraps message interceptors after it."""
    events: list[str] = []

    class EventMetadataInterceptorSync:
        def on_start_sync(self, _ctx):
            events.append("metadata start")

        def on_end_sync(self, _token, _ctx, _error):
            events.append("metadata end")

    class EventUnaryInterceptorSync:
        def intercept_unary_sync(self, call_next, request, ctx):
            events.append("unary before")
            try:
                return call_next(request, ctx)
            finally:
                events.append("unary after")

    class SimpleHaberdasherSync(HaberdasherSync):
        def make_hat(self, request, _ctx):
            events.append("handler")
            return Hat(size=request.inches)

    app = HaberdasherWSGIApplication(
        SimpleHaberdasherSync(),
        interceptors=(EventMetadataInterceptorSync(), EventUnaryInterceptorSync()),
    )
    with HaberdasherClientSync(
        "http://localhost", http_client=SyncClient(WSGITransport(app))
    ) as client:
        client.make_hat(Size(inches=10))

    assert events == [
        "metadata start",
        "unary before",
        "handler",
        "unary after",
        "metadata end",
    ]


class _ResponseMetadataInterceptor:
    async def on_start(self, ctx):
        return self.on_start_sync(ctx)

    async def on_end(self, token, ctx, error) -> None:
        self.on_end_sync(token, ctx, error)

    def on_start_sync(self, _ctx) -> None:
        return None

    def on_end_sync(self, _token, ctx, _error) -> None:
        ctx.response_headers.add("x-interceptor", "ran")
        ctx.response_trailers.add("x-interceptor-trailer", "ran")


@pytest.mark.asyncio
async def test_metadata_interceptor_response_metadata_async() -> None:
    """Response metadata set in on_end is still sent to the client."""

    class SimpleHaberdasher(Haberdasher):
        async def make_hat(self, request, _ctx):
            return Hat(size=request.inches)

        async def make_similar_hats(self, request, _ctx):
            yield Hat(size=request.inches)

    app = HaberdasherASGIApplication(
        SimpleHaberdasher(), interceptors=(_ResponseMetadataInterceptor(),)
    )
    async with HaberdasherClient(
        "http://localhost", http_client=Client(transport=ASGITransport(app))
    ) as client:
        with ResponseMetadata() as resp:
            await client.make_hat(Size(inches=10))
        assert resp.headers.get("x-interceptor") == "ran"
        assert resp.trailers.get("x-interceptor-trailer") == "ran"

        with ResponseMetadata() as resp:
            async for _ in client.make_similar_hats(Size(inches=10)):
                pass
        assert resp.trailers.get("x-interceptor-trailer") == "ran"


def test_metadata_interceptor_response_metadata_sync() -> None:
    """Response metadata set in on_end is still sent to the client."""

    class SimpleHaberdasherSync(HaberdasherSync):
        def make_hat(self, request, _ctx):
            return Hat(size=request.inches)

        def make_similar_hats(self, request, _ctx):
            yield Hat(size=request.inches)

    app = HaberdasherWSGIApplication(
        SimpleHaberdasherSync(), interceptors=(_ResponseMetadataInterceptor(),)
    )
    with HaberdasherClientSync(
        "http://localhost", http_client=SyncClient(WSGITransport(app))
    ) as client:
        with ResponseMetadata() as resp:
            client.make_hat(Size(inches=10))
        assert resp.headers.get("x-interceptor") == "ran"
        assert resp.trailers.get("x-interceptor-trailer") == "ran"

        with ResponseMetadata() as resp:
            for _ in client.make_similar_hats(Size(inches=10)):
                pass
        assert resp.trailers.get("x-interceptor-trailer") == "ran"


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

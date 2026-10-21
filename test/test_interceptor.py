from __future__ import annotations

import asyncio
import contextlib
import itertools
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc.client import ResponseMetadata
from connectrpc.code import Code
from connectrpc.errors import ConnectError

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
    from asgiref.typing import HTTPDisconnectEvent, HTTPRequestEvent, HTTPScope

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


class SimpleHaberdasher(Haberdasher):
    async def make_hat(self, request, _ctx):
        if request.inches < 0:
            raise ConnectError(Code.INVALID_ARGUMENT, "Size must be non-negative")
        return Hat(size=request.inches, color="green")

    async def make_flexible_hat(self, request, _ctx):
        size = 0
        async for s in request:
            if s.inches < 0:
                raise ConnectError(Code.INVALID_ARGUMENT, "Size must be non-negative")
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
                raise ConnectError(Code.INVALID_ARGUMENT, "Size must be non-negative")
            yield Hat(size=s.inches, color=next(colors))


class SimpleHaberdasherSync(HaberdasherSync):
    def make_hat(self, request, _ctx):
        if request.inches < 0:
            raise ConnectError(Code.INVALID_ARGUMENT, "Size must be non-negative")
        return Hat(size=request.inches, color="green")

    def make_flexible_hat(self, request, _ctx):
        size = 0
        for s in request:
            if s.inches < 0:
                raise ConnectError(Code.INVALID_ARGUMENT, "Size must be non-negative")
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
                raise ConnectError(Code.INVALID_ARGUMENT, "Size must be non-negative")
            yield Hat(size=s.inches, color=next(colors))


@pytest_asyncio.fixture(params=["async", "sync"])
async def client(
    request: pytest.FixtureRequest,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
):
    if request.param == "async":
        app = HaberdasherASGIApplication(
            SimpleHaberdasher(), interceptors=(server_interceptor,)
        )
        async with HaberdasherClient(
            "http://localhost",
            http_client=Client(ASGITransport(app)),
            interceptors=(client_interceptor,),
        ) as client:
            yield client
    else:
        app = HaberdasherWSGIApplication(
            SimpleHaberdasherSync(), interceptors=(server_interceptor,)
        )
        with HaberdasherClientSync(
            "http://localhost",
            http_client=SyncClient(WSGITransport(app)),
            interceptors=(client_interceptor,),
        ) as client:
            yield client


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "request_", "expected", "name"),
    [
        pytest.param(
            "make_hat",
            Size(inches=10),
            Hat(size=10, color="green"),
            "MakeHat",
            id="unary",
        ),
        pytest.param(
            "make_flexible_hat",
            [Size(inches=10), Size(inches=20)],
            Hat(size=30, color="red"),
            "MakeFlexibleHat",
            id="client_stream",
        ),
        pytest.param(
            "make_similar_hats",
            Size(inches=15),
            [Hat(size=15, color="orange"), Hat(size=15, color="blue")],
            "MakeSimilarHats",
            id="server_stream",
        ),
        pytest.param(
            "make_various_hats",
            [Size(inches=25), Size(inches=35), Size(inches=45)],
            [
                Hat(size=25, color="black"),
                Hat(size=35, color="white"),
                Hat(size=45, color="gold"),
            ],
            "MakeVariousHats",
            id="bidi_stream",
        ),
    ],
)
async def test_intercept(
    client: HaberdasherClient | HaberdasherClientSync,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
    method: str,
    request_: Size | list[Size],
    expected: Hat | list[Hat],
    name: str,
) -> None:
    assert await call(getattr(client, method), request_) == expected
    assert client_interceptor.result == [f"Hello {name} and goodbye"]
    assert server_interceptor.result == [f"Hello {name} and goodbye"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "request_", "name"),
    [
        pytest.param("make_hat", Size(inches=-10), "MakeHat", id="unary"),
        pytest.param(
            "make_flexible_hat",
            [Size(inches=-10), Size(inches=20)],
            "MakeFlexibleHat",
            id="client_stream",
        ),
        pytest.param(
            "make_similar_hats", Size(inches=-15), "MakeSimilarHats", id="server_stream"
        ),
        pytest.param(
            "make_various_hats",
            [Size(inches=-25), Size(inches=35), Size(inches=45)],
            "MakeVariousHats",
            id="bidi_stream",
        ),
    ],
)
async def test_intercept_error(
    client: HaberdasherClient | HaberdasherClientSync,
    client_interceptor: RequestInterceptor,
    server_interceptor: RequestInterceptor,
    method: str,
    request_: Size | list[Size],
    name: str,
) -> None:
    with pytest.raises(ConnectError):
        await call(getattr(client, method), request_)
    expected = f"Hello {name} and goodbye with error Size must be non-negative"
    assert client_interceptor.result == [expected]
    assert server_interceptor.result == [expected]


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


async def _run_connect_app(
    app: HaberdasherASGIApplication,
    method: str,
    body: bytes,
    *,
    streaming: bool = False,
) -> None:
    path = f"/connectrpc.example.Haberdasher/{method}"
    scope: HTTPScope = {
        "type": "http",
        "asgi": {"spec_version": "2.0", "version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [
            (
                b"content-type",
                b"application/connect+json" if streaming else b"application/json",
            )
        ],
        "client": None,
        "server": None,
        "extensions": None,
    }
    received = False
    disconnected = asyncio.Event()

    async def receive() -> HTTPRequestEvent | HTTPDisconnectEvent:
        nonlocal received
        if not received:
            received = True
            return {"type": "http.request", "body": body, "more_body": False}
        # Keep the server-stream disconnect monitor pending until cancellation.
        await disconnected.wait()
        return {"type": "http.disconnect"}

    async def send(_message) -> None:
        return

    await app(scope, receive, send)


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
        "http://localhost", http_client=Client(ASGITransport(app))
    ) as client:
        await client.make_hat(Size(inches=10))

    assert events == [
        "metadata start",
        "unary before",
        "handler",
        "unary after",
        "metadata end",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "streaming"),
    [
        pytest.param("MakeHat", False, id="unary"),
        pytest.param("MakeFlexibleHat", True, id="client_stream"),
        pytest.param("MakeSimilarHats", True, id="server_stream"),
        pytest.param("MakeVariousHats", True, id="bidi_stream"),
    ],
)
async def test_non_hoisted_metadata_interceptor_reports_cancellation(
    method: str, streaming: bool
) -> None:
    entered = asyncio.Event()
    request_body = b'{"inches":10}'
    if streaming:
        request_body = b"\x00" + len(request_body).to_bytes(4, "big") + request_body

    class BlockingHaberdasher(Haberdasher):
        async def _block(self):
            entered.set()
            await asyncio.Event().wait()

        async def make_hat(self, request, _ctx):
            await self._block()
            return Hat(size=request.inches)

        async def make_flexible_hat(self, request, _ctx):
            async for _ in request:
                pass
            await self._block()
            return Hat(size=10)

        async def make_similar_hats(self, request, _ctx):
            yield Hat(size=request.inches)
            await self._block()

        async def make_various_hats(self, request, _ctx):
            async for _ in request:
                yield Hat(size=10)
                await self._block()

    class PassthroughInterceptor:
        async def intercept_unary(self, call_next, request, ctx):
            return await call_next(request, ctx)

        async def intercept_client_stream(self, call_next, request, ctx):
            return await call_next(request, ctx)

        def intercept_server_stream(self, call_next, request, ctx):
            return call_next(request, ctx)

        def intercept_bidi_stream(self, call_next, request, ctx):
            return call_next(request, ctx)

    class CancellationObserver:
        def __init__(self):
            self.errors = []

        async def on_start(self, ctx):  # noqa: ARG002
            return None

        async def on_end(self, _token, _ctx, error):
            self.errors.append(error)

    observer = CancellationObserver()
    app = HaberdasherASGIApplication(
        BlockingHaberdasher(), interceptors=(PassthroughInterceptor(), observer)
    )
    task = asyncio.create_task(
        _run_connect_app(app, method, request_body, streaming=streaming)
    )
    try:
        await asyncio.wait_for(entered.wait(), timeout=5.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert len(observer.errors) == 1
    error = observer.errors[0]
    assert isinstance(error, ConnectError)
    assert error.code == Code.CANCELED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "middle_raises",
    [pytest.param(False, id="canceled-error"), pytest.param(True, id="cleanup-error")],
)
async def test_hoisted_metadata_interceptors_unwind_after_cancellation(
    middle_raises: bool,
) -> None:
    cleanup_entered = asyncio.Event()
    cleanup_release = asyncio.Event()
    middle_error = ValueError("middle cleanup failed") if middle_raises else None

    class CancellationObserver:
        def __init__(
            self,
            *,
            suspend_on_end: bool = False,
            error_to_raise: Exception | None = None,
        ):
            self.suspend_on_end = suspend_on_end
            self.error_to_raise = error_to_raise
            self.errors = []

        async def on_start(self, ctx):  # noqa: ARG002
            return None

        async def on_end(self, _token, _ctx, error):
            self.errors.append(error)
            if self.suspend_on_end:
                cleanup_entered.set()
                await cleanup_release.wait()
            if self.error_to_raise is not None:
                raise self.error_to_raise

    inner = CancellationObserver(suspend_on_end=True)
    middle = CancellationObserver(error_to_raise=middle_error)
    outer = CancellationObserver()
    app = HaberdasherASGIApplication(
        SimpleHaberdasher(), interceptors=(outer, middle, inner)
    )
    task = asyncio.create_task(_run_connect_app(app, "MakeHat", b'{"inches":10}'))
    try:
        await asyncio.wait_for(cleanup_entered.wait(), timeout=5.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert inner.errors == [None]
    assert len(middle.errors) == 1
    middle_reported = middle.errors[0]
    assert isinstance(middle_reported, ConnectError)
    assert middle_reported.code == Code.CANCELED
    assert len(outer.errors) == 1
    if middle_error is None:
        outer_reported = outer.errors[0]
        assert isinstance(outer_reported, ConnectError)
        assert outer_reported.code == Code.CANCELED
    else:
        assert outer.errors[0] is middle_error


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


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["MakeSimilarHats", "MakeVariousHats"])
async def test_metadata_interceptor_closes_stream_async(method: str) -> None:
    """An early end of the response closes the handler's stream."""
    events: list[str] = []

    class EventMetadataInterceptor:
        async def on_start(self, ctx):  # noqa: ARG002
            events.append("metadata start")

        async def on_end(self, _token, _ctx, _error):
            events.append("metadata end")

    class EndlessHaberdasher(Haberdasher):
        async def make_similar_hats(self, request, _ctx):
            try:
                while True:
                    yield Hat(size=request.inches)
                    await asyncio.sleep(0)
            finally:
                await asyncio.sleep(0)
                events.append("handler closed")

        def make_various_hats(self, _request, ctx):
            return self.make_similar_hats(Size(inches=10), ctx)

    app = HaberdasherASGIApplication(
        EndlessHaberdasher(),
        interceptors=(_PassthroughUnaryInterceptor(), EventMetadataInterceptor()),
    )
    path = f"/connectrpc.example.Haberdasher/{method}"
    scope: HTTPScope = {
        "type": "http",
        "asgi": {"spec_version": "2.0", "version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"content-type", b"application/connect+proto")],
        "client": None,
        "server": None,
        "extensions": None,
    }
    request = Size(inches=10).to_binary()
    body = b"\x00" + len(request).to_bytes(4, "big") + request
    disconnected = asyncio.Event()
    sent_request = False

    async def receive() -> HTTPRequestEvent | HTTPDisconnectEvent:
        nonlocal sent_request
        if not sent_request:
            sent_request = True
            return {"type": "http.request", "body": body, "more_body": False}
        await disconnected.wait()
        return {"type": "http.disconnect"}

    async def send(message) -> None:
        if message["type"] == "http.response.body" and message["more_body"]:
            disconnected.set()
            if method == "MakeVariousHats":
                # Only a server stream watches receive() for the disconnect; a bidi stream
                # learns of it from a failed send.
                raise ConnectError(Code.CANCELED, "Client disconnected")

    await asyncio.wait_for(app(scope, receive, send), timeout=5.0)

    assert events == ["metadata start", "handler closed", "metadata end"]


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
        "http://localhost", http_client=Client(ASGITransport(app))
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

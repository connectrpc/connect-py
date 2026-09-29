"""The ASGI application served under trio.

Hypercorn's trio worker serves the application on a background thread, and the
sync client calls it over pyqwest's SyncHTTPTransport. An ASGI wrapper around
the application records what it sends and can cancel it, as the server does
when it shuts down with the request in flight.
"""

from __future__ import annotations

import gc
import gzip
import json
import subprocess
import sys
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import trio
import trio.lowlevel
from hypercorn.config import Config
from hypercorn.trio import serve
from pyqwest import HTTPVersion, SyncClient, SyncHTTPTransport

from connectrpc import _server_trio
from connectrpc.code import Code
from connectrpc.errors import ConnectError

from .connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClientSync,
)
from .connectrpc.example.haberdasher_pb import Hat, Size

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator

    from connectrpc.request import RequestContext

# An envelope is a byte of flags and four bytes of length, then the message.
ENVELOPE_HEADER_LEN = 5
COMPRESSED = 1
END_STREAM = 2

CLEANUP_TIMEOUT = 0.2

CANCELLED = {"code": "canceled", "message": "Request was cancelled"}


class TrioHaberdasher(Haberdasher):
    async def make_hat(self, request: Size, _ctx: RequestContext[Size, Hat], /) -> Hat:
        # Fails unless the handler runs under trio.
        await trio.lowlevel.checkpoint()
        if request.inches < 0:
            raise ConnectError(Code.INVALID_ARGUMENT, "inches must not be negative")
        return Hat(size=request.inches, color="blue", name="bowler")

    async def make_flexible_hat(
        self, request: AsyncIterator[Size], _ctx: RequestContext[Size, Hat], /
    ) -> Hat:
        total = 0
        async for size in request:
            total += size.inches
        return Hat(size=total, color="red", name="flexible")

    async def make_similar_hats(
        self, request: Size, _ctx: RequestContext[Size, Hat], /
    ) -> AsyncIterator[Hat]:
        for i in range(3):
            await trio.lowlevel.checkpoint()
            if request.inches < 0 and i == 1:
                raise ConnectError(Code.RESOURCE_EXHAUSTED, "no more hats")
            yield Hat(size=request.inches + i, color="green", name=f"hat{i}")

    async def make_various_hats(
        self, request: AsyncIterator[Size], _ctx: RequestContext[Size, Hat], /
    ) -> AsyncIterator[Hat]:
        async for size in request:
            yield Hat(size=size.inches, color="black", name="echo")


class Wrapper:
    """Wraps the application served by hypercorn.

    Records what the application sends, can block a send, and can cancel the
    application from the test's thread.
    """

    def __init__(self) -> None:
        self.app: HaberdasherASGIApplication = HaberdasherASGIApplication(
            TrioHaberdasher()
        )
        self.token: trio.lowlevel.TrioToken | None = None
        self._in_flight = 0
        self.idle = threading.Event()
        self.idle.set()
        self.reset()

    def reset(self) -> None:
        # A request finishes after the client has its response, so one from the
        # previous test may still be running.
        assert self.idle.wait(10)
        self.sent: list[dict[str, Any]] = []
        self.on_send: Callable[[dict[str, Any]], Any] | None = None
        self.raised: BaseException | None = None
        self.cancelled_at = 0.0
        self.finished_at = 0.0
        self.done = threading.Event()
        self.blocked = threading.Event()
        self._scope: trio.CancelScope | None = None

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        self._in_flight += 1
        self.idle.clear()

        async def recording_send(message: Any) -> None:
            self.sent.append(message)
            if self.on_send is not None:
                await self.on_send(message)
            await send(message)

        try:
            with trio.CancelScope() as self._scope:
                await self.app(scope, receive, recording_send)
        except BaseException as e:
            self.raised = e
            raise
        finally:
            self.finished_at = trio.current_time()
            self.done.set()
            self._in_flight -= 1
            if self._in_flight == 0:
                self.idle.set()

    def cancel(self) -> None:
        def cancel_on_loop() -> None:
            assert self._scope is not None
            self.cancelled_at = trio.current_time()
            self._scope.cancel()

        assert self.token is not None
        trio.from_thread.run_sync(cancel_on_loop, trio_token=self.token)

    def types(self) -> list[str]:
        return [m["type"].removeprefix("http.response.") for m in self.sent]

    def bodies(self) -> list[bytes]:
        return [m["body"] for m in self.sent if m["type"] == "http.response.body"]

    def end_error(self) -> dict[str, str] | None:
        """The error of a Connect stream's end message, or None for a success."""
        (end,) = [body for body in self.bodies() if body and body[0] & END_STREAM]
        data = end[ENVELOPE_HEADER_LEN:]
        if end[0] & COMPRESSED:
            data = gzip.decompress(data)
        return json.loads(data).get("error")

    def error(self) -> dict[str, str] | None:
        """The error of the response: a unary error response, or a stream's end."""
        if self.sent[0]["status"] != 200:
            return json.loads(self.bodies()[0])
        return self.end_error()

    async def block_on_first_message(self, message: dict[str, Any]) -> None:
        if message["type"] == "http.response.body" and not self.blocked.is_set():
            self.blocked.set()
            await trio.sleep_forever()

    async def block_in_nursery_on_first_message(self, message: dict[str, Any]) -> None:
        """Block inside a nursery, as a server's send() may."""
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.block_on_first_message, message)


@pytest.fixture(scope="module")
def transport() -> Iterator[SyncHTTPTransport]:
    with SyncHTTPTransport(http_version=HTTPVersion.HTTP1) as transport:
        yield transport


@pytest.fixture(scope="module")
def server() -> Iterator[tuple[str, Wrapper]]:
    wrapper = Wrapper()
    stop = threading.Event()
    started: Future[str] = Future()

    async def stopped() -> None:
        await trio.to_thread.run_sync(stop.wait, abandon_on_cancel=True)

    async def main() -> None:
        wrapper.token = trio.lowlevel.current_trio_token()
        config = Config()
        config.bind = ["127.0.0.1:0"]
        async with trio.open_nursery() as nursery:
            binds = await nursery.start(
                partial(serve, wrapper, config, shutdown_trigger=stopped)
            )
            started.set_result(binds[0])

    def run() -> None:
        try:
            trio.run(main)
        except BaseException as e:
            if not started.done():
                started.set_exception(e)
            raise

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        yield started.result(timeout=10), wrapper
    finally:
        stop.set()
        thread.join(10)
    assert not thread.is_alive(), "server did not stop"


@pytest.fixture
def url(server: tuple[str, Wrapper]) -> str:
    return server[0]


@pytest.fixture
def wrapper(server: tuple[str, Wrapper]) -> Iterator[Wrapper]:
    wrapper = server[1]
    wrapper.reset()
    yield wrapper
    wrapper.app = HaberdasherASGIApplication(TrioHaberdasher())


@pytest.fixture
def client(url: str, transport: SyncHTTPTransport) -> Iterator[HaberdasherClientSync]:
    with HaberdasherClientSync(url, http_client=SyncClient(transport)) as client:
        yield client


@pytest.fixture
def executor() -> Iterator[ThreadPoolExecutor]:
    with ThreadPoolExecutor() as executor:
        yield executor


@pytest.fixture
def cleanup_timeout(monkeypatch: pytest.MonkeyPatch) -> float:
    monkeypatch.setattr(_server_trio, "_CLEANUP_TIMEOUT", CLEANUP_TIMEOUT)
    return CLEANUP_TIMEOUT


def sizes() -> Iterator[Size]:
    for inches in (1, 2, 3):
        yield Size(inches=inches)


def call(client: HaberdasherClientSync, method: str) -> Any:
    """Call the method with one request message, consuming any response stream."""
    match method:
        case "MakeHat":
            return client.make_hat(Size(inches=10))
        case "MakeFlexibleHat":
            return client.make_flexible_hat(iter([Size(inches=10)]))
        case "MakeSimilarHats":
            return list(client.make_similar_hats(Size(inches=10)))
        case "MakeVariousHats":
            return list(client.make_various_hats(iter([Size(inches=10)])))
        case _:
            raise ValueError(method)


def test_unary(client: HaberdasherClientSync) -> None:
    assert client.make_hat(Size(inches=10)) == Hat(size=10, color="blue", name="bowler")


def test_unary_error(client: HaberdasherClientSync) -> None:
    with pytest.raises(ConnectError) as exc_info:
        client.make_hat(Size(inches=-1))
    assert exc_info.value.code == Code.INVALID_ARGUMENT


def test_client_stream(client: HaberdasherClientSync) -> None:
    assert client.make_flexible_hat(sizes()).size == 6


def test_server_stream(client: HaberdasherClientSync) -> None:
    hats = list(client.make_similar_hats(Size(inches=10)))
    assert [hat.size for hat in hats] == [10, 11, 12]


def test_server_stream_error(client: HaberdasherClientSync) -> None:
    hats = []
    with pytest.raises(ConnectError) as exc_info:
        hats.extend(client.make_similar_hats(Size(inches=-5)))
    assert exc_info.value.code == Code.RESOURCE_EXHAUSTED
    assert [hat.size for hat in hats] == [-5]


def test_bidi_stream(client: HaberdasherClientSync) -> None:
    hats = list(client.make_various_hats(sizes()))
    assert [hat.size for hat in hats] == [1, 2, 3]


class Recorder:
    """A metadata interceptor whose on_end awaits before it finishes."""

    def __init__(self, events: list[str], seconds: float = 0) -> None:
        self._events = events
        self._seconds = seconds

    async def on_start(self, ctx: RequestContext) -> None:  # noqa: ARG002
        return None

    async def on_end(
        self, _token: None, _ctx: RequestContext, error: Exception | None, /
    ) -> None:
        code = error.code.value if isinstance(error, ConnectError) else error
        self._events.append(f"on_end started {code}")
        await trio.sleep(self._seconds)
        self._events.append("on_end finished")


class EndlessHats(Haberdasher):
    """Streams hats until it is closed, and awaits while it cleans up."""

    def __init__(
        self,
        events: list[str],
        cleanup_error: Exception | None = None,
        cleanup_seconds: float = 0,
    ) -> None:
        self._events = events
        self._cleanup_error = cleanup_error
        self._cleanup_seconds = cleanup_seconds

    async def make_similar_hats(
        self, request: Size, _ctx: RequestContext[Size, Hat], /
    ) -> AsyncIterator[Hat]:
        try:
            while True:
                yield Hat(size=request.inches)
                await trio.sleep(0.01)
        finally:
            await self._clean_up()

    async def make_various_hats(
        self, request: AsyncIterator[Size], _ctx: RequestContext[Size, Hat], /
    ) -> AsyncIterator[Hat]:
        try:
            async for size in request:
                yield Hat(size=size.inches)
        finally:
            await self._clean_up()

    async def _clean_up(self) -> None:
        await trio.sleep(self._cleanup_seconds)
        self._events.append("handler cleaned up")
        if self._cleanup_error is not None:
            raise self._cleanup_error


@pytest.mark.parametrize(
    ("cleanup_error", "end_error"),
    [
        (None, {"code": "canceled", "message": "Client disconnected"}),
        (
            ConnectError(Code.ABORTED, "cleanup failed"),
            {"code": "aborted", "message": "cleanup failed"},
        ),
    ],
    ids=["clean", "cleanup-error"],
)
def test_disconnect_ends_server_stream(
    url: str,
    wrapper: Wrapper,
    cleanup_error: Exception | None,
    end_error: dict[str, str],
) -> None:
    events: list[str] = []
    wrapper.app = HaberdasherASGIApplication(
        EndlessHats(events, cleanup_error), interceptors=[Recorder(events)]
    )
    # The client's own connection, so that closing it disconnects the request.
    with (
        SyncHTTPTransport(http_version=HTTPVersion.HTTP1) as transport,
        HaberdasherClientSync(url, http_client=SyncClient(transport)) as client,
    ):
        hats = client.make_similar_hats(Size(inches=10))
        assert [next(hats).size for _ in range(3)] == [10, 10, 10]
        del hats
    gc.collect()

    assert wrapper.done.wait(10)
    assert wrapper.end_error() == end_error
    assert events == [
        "handler cleaned up",
        f"on_end started {end_error['code']}",
        "on_end finished",
    ]


@pytest.mark.parametrize(
    ("method", "cleanup_error", "send_in_nursery"),
    [
        ("MakeSimilarHats", None, False),
        ("MakeSimilarHats", ConnectError(Code.ABORTED, "cleanup failed"), False),
        ("MakeVariousHats", ConnectError(Code.ABORTED, "cleanup failed"), False),
        ("MakeVariousHats", None, True),
    ],
)
def test_cancelled_while_sending(
    client: HaberdasherClientSync,
    wrapper: Wrapper,
    executor: ThreadPoolExecutor,
    method: str,
    cleanup_error: Exception | None,
    send_in_nursery: bool,
) -> None:
    events: list[str] = []
    wrapper.app = HaberdasherASGIApplication(
        EndlessHats(events, cleanup_error), interceptors=[Recorder(events)]
    )
    wrapper.on_send = (
        wrapper.block_in_nursery_on_first_message
        if send_in_nursery
        else wrapper.block_on_first_message
    )

    response = executor.submit(call, client, method)
    assert wrapper.blocked.wait(10)
    wrapper.cancel()
    assert wrapper.done.wait(10)
    response.exception(10)

    assert wrapper.raised is None
    assert events == [
        "handler cleaned up",
        "on_end started canceled",
        "on_end finished",
    ]
    assert wrapper.types() == ["start", "body", "body"]
    assert wrapper.sent[1]["more_body"]
    assert wrapper.end_error() == CANCELLED


class BlockedHats(Haberdasher):
    """Blocks forever; records when it is cancelled."""

    def __init__(
        self,
        blocked: threading.Event,
        events: list[str],
        *,
        in_nursery: bool,
        cleanup_error: Exception | None = None,
    ):
        self._blocked = blocked
        self._events = events
        self._in_nursery = in_nursery
        self._cleanup_error = cleanup_error

    async def make_hat(self, request: Size, _ctx: RequestContext[Size, Hat], /) -> Hat:
        await self._block()
        return Hat(size=request.inches)

    async def make_flexible_hat(
        self, _request: AsyncIterator[Size], _ctx: RequestContext[Size, Hat], /
    ) -> Hat:
        await self._block()
        return Hat()

    async def make_similar_hats(
        self, _request: Size, _ctx: RequestContext[Size, Hat], /
    ) -> AsyncIterator[Hat]:
        await self._block()
        yield Hat()

    async def make_various_hats(
        self, _request: AsyncIterator[Size], _ctx: RequestContext[Size, Hat], /
    ) -> AsyncIterator[Hat]:
        await self._block()
        yield Hat()

    async def _block(self) -> None:
        try:
            self._blocked.set()
            if self._in_nursery:
                # A cancelled nursery raises the cancellation in a group.
                async with trio.open_nursery() as nursery:
                    nursery.start_soon(trio.sleep_forever)
                    nursery.start_soon(self._fail_when_cancelled)
            else:
                await trio.sleep_forever()
        finally:
            self._events.append("handler cleaned up")

    async def _fail_when_cancelled(self) -> None:
        try:
            await trio.sleep_forever()
        finally:
            if self._cleanup_error is not None:
                raise self._cleanup_error


@pytest.mark.parametrize(
    ("method", "in_nursery"),
    [
        ("MakeHat", False),
        ("MakeHat", True),
        ("MakeFlexibleHat", True),
        ("MakeSimilarHats", True),
        ("MakeVariousHats", True),
    ],
)
def test_cancelled_in_handler(
    client: HaberdasherClientSync,
    wrapper: Wrapper,
    executor: ThreadPoolExecutor,
    method: str,
    in_nursery: bool,
) -> None:
    events: list[str] = []
    blocked = threading.Event()
    wrapper.app = HaberdasherASGIApplication(
        BlockedHats(blocked, events, in_nursery=in_nursery),
        interceptors=[Recorder(events)],
    )

    response = executor.submit(call, client, method)
    assert blocked.wait(10)
    wrapper.cancel()
    assert wrapper.done.wait(10)
    response.exception(10)

    assert wrapper.raised is None
    assert events == [
        "handler cleaned up",
        "on_end started canceled",
        "on_end finished",
    ]
    assert wrapper.types() == ["start", "body"]
    assert wrapper.error() == CANCELLED


def leaves(error: BaseException) -> list[BaseException]:
    nested = getattr(error, "exceptions", None)
    if nested is None:
        return [error]
    return [leaf for e in nested for leaf in leaves(e)]


@pytest.mark.parametrize(
    "method", ["MakeHat", "MakeFlexibleHat", "MakeSimilarHats", "MakeVariousHats"]
)
def test_cancelled_in_handler_whose_cleanup_fails(
    client: HaberdasherClientSync,
    wrapper: Wrapper,
    executor: ThreadPoolExecutor,
    method: str,
) -> None:
    """A cancellation beside a handler error is handled as a cancellation.

    The client is told CANCELED, and the handler's error still escapes the
    application for the server to log.
    """
    events: list[str] = []
    blocked = threading.Event()
    cleanup_error = OSError("cleanup failed")
    wrapper.app = HaberdasherASGIApplication(
        BlockedHats(blocked, events, in_nursery=True, cleanup_error=cleanup_error),
        interceptors=[Recorder(events)],
    )

    response = executor.submit(call, client, method)
    assert blocked.wait(10)
    wrapper.cancel()
    assert wrapper.done.wait(10)
    response.exception(10)

    assert wrapper.raised is not None
    assert leaves(wrapper.raised) == [cleanup_error]
    assert events == [
        "handler cleaned up",
        "on_end started canceled",
        "on_end finished",
    ]
    assert wrapper.types() == ["start", "body"]
    assert wrapper.error() == CANCELLED


def test_handler_cleanup_after_cancellation_is_bounded(
    client: HaberdasherClientSync,
    wrapper: Wrapper,
    executor: ThreadPoolExecutor,
    cleanup_timeout: float,
) -> None:
    events: list[str] = []
    wrapper.app = HaberdasherASGIApplication(EndlessHats(events, cleanup_seconds=60))
    wrapper.on_send = wrapper.block_on_first_message

    response = executor.submit(call, client, "MakeSimilarHats")
    assert wrapper.blocked.wait(10)
    wrapper.cancel()
    assert wrapper.done.wait(10)
    response.exception(10)

    assert events == []
    assert cleanup_timeout <= wrapper.finished_at - wrapper.cancelled_at < 5


def test_on_end_after_cancellation_is_bounded(
    client: HaberdasherClientSync,
    wrapper: Wrapper,
    executor: ThreadPoolExecutor,
    cleanup_timeout: float,
) -> None:
    events: list[str] = []
    wrapper.app = HaberdasherASGIApplication(
        EndlessHats(events), interceptors=[Recorder(events, seconds=60)]
    )
    wrapper.on_send = wrapper.block_on_first_message

    response = executor.submit(call, client, "MakeSimilarHats")
    assert wrapper.blocked.wait(10)
    wrapper.cancel()
    assert wrapper.done.wait(10)
    response.exception(10)

    assert events == ["handler cleaned up", "on_end started canceled"]
    assert cleanup_timeout <= wrapper.finished_at - wrapper.cancelled_at < 5


@pytest.mark.parametrize("method", ["MakeHat", "MakeSimilarHats"])
def test_on_end_is_not_bounded_without_cancellation(
    client: HaberdasherClientSync, wrapper: Wrapper, cleanup_timeout: float, method: str
) -> None:
    events: list[str] = []
    wrapper.app = HaberdasherASGIApplication(
        TrioHaberdasher(), interceptors=[Recorder(events, seconds=2 * cleanup_timeout)]
    )

    call(client, method)

    assert events == ["on_end started None", "on_end finished"]


def test_handler_timeout_on_request_stream(
    client: HaberdasherClientSync, wrapper: Wrapper, executor: ThreadPoolExecutor
) -> None:
    timed_out: list[bool] = []

    class Service(Haberdasher):
        async def make_flexible_hat(
            self, request: AsyncIterator[Size], _ctx: RequestContext[Size, Hat], /
        ) -> Hat:
            total = 0
            with trio.move_on_after(0.5) as scope:
                async for size in request:
                    total += size.inches
            timed_out.append(scope.cancelled_caught)
            return Hat(size=total)

    wrapper.app = HaberdasherASGIApplication(Service())
    release = threading.Event()

    def stalling_sizes() -> Iterator[Size]:
        # One message, then the client stalls without ending the body.
        yield Size(inches=7)
        release.wait(10)

    response = executor.submit(client.make_flexible_hat, stalling_sizes())
    assert wrapper.done.wait(10)
    release.set()

    assert timed_out == [True]
    assert response.result(10) == Hat(size=7)


def test_bidi_handler_returns_early(
    client: HaberdasherClientSync, wrapper: Wrapper
) -> None:
    """The request stream is closed by the server, not the garbage collector."""

    class FirstSizeOnly(Haberdasher):
        async def make_various_hats(
            self, request: AsyncIterator[Size], _ctx: RequestContext[Size, Hat], /
        ) -> AsyncIterator[Hat]:
            async for size in request:
                yield Hat(size=size.inches)
                return

    wrapper.app = HaberdasherASGIApplication(FirstSizeOnly())

    hats = list(client.make_various_hats(sizes()))

    assert [hat.size for hat in hats] == [1]


SERVER_STREAM_ON_ASYNCIO = """
import asyncio
import sys

from pyqwest import Client
from pyqwest.testing import ASGITransport

from test.connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClient,
)
from test.connectrpc.example.haberdasher_pb import Hat, Size


class Service(Haberdasher):
    async def make_similar_hats(self, request, ctx):
        yield Hat(size=request.inches)


async def main():
    transport = ASGITransport(HaberdasherASGIApplication(Service()))
    async with HaberdasherClient("http://localhost", http_client=Client(transport)) as client:
        hats = [hat async for hat in client.make_similar_hats(Size(inches=10))]
    assert hats == [Hat(size=10)], hats


asyncio.run(main())
loaded = sorted(name for name in sys.modules if name.split(".")[0] == "trio")
assert not loaded, loaded
"""


def test_asyncio_does_not_import_trio() -> None:
    result = subprocess.run(
        [sys.executable, "-c", SERVER_STREAM_ON_ASYNCIO],
        cwd=Path(__file__).parents[1],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

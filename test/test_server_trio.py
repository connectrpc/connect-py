"""The ASGI application served under trio.

The async client calls the application through pyqwest's ASGITransport. An
ASGI wrapper around the application records what it sends, can block a send,
can report a client disconnect, and can cancel the application as a server
does when it shuts down with the request in flight.
"""

from __future__ import annotations

import contextlib
import gzip
import json
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("trio")

import trio
import trio.lowlevel
from pyqwest import Client
from pyqwest.testing import ASGITransport

from connectrpc import _loop_trio
from connectrpc.code import Code
from connectrpc.errors import ConnectError

from .connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClient,
)
from .connectrpc.example.haberdasher_pb import Hat, Size

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable

    from connectrpc.request import RequestContext

pytestmark = pytest.mark.anyio

# An envelope is a byte of flags and four bytes of length, then the message.
ENVELOPE_HEADER_LEN = 5
COMPRESSED = 1
END_STREAM = 2

CLEANUP_TIMEOUT = 0.2

CANCELLED = {"code": "canceled", "message": "Request was cancelled"}
TIMED_OUT = {"code": "deadline_exceeded", "message": "Request timed out"}


@pytest.fixture
def anyio_backend() -> str:
    """Run these tests on trio only, since they use trio directly."""
    return "trio"


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
    """Wraps the application under test.

    Records what the application sends, can block a send, can report a client
    disconnect once the request has been read, and can cancel the application.
    """

    def __init__(self) -> None:
        self.app: Callable[[Any, Any, Any], Awaitable[None]] = (
            HaberdasherASGIApplication(TrioHaberdasher())
        )
        self.sent: list[dict[str, Any]] = []
        self.on_send: Callable[[dict[str, Any]], Any] | None = None
        self.raised: BaseException | None = None
        self.cancelled_at = 0.0
        self.finished_at = 0.0
        self.done = trio.Event()
        self.blocked = trio.Event()
        self._disconnected = trio.Event()
        self._scope: trio.CancelScope | None = None

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        body_read = False

        async def disconnecting_receive() -> Any:
            nonlocal body_read
            if body_read:
                await self._disconnected.wait()
                return {"type": "http.disconnect"}
            message = await receive()
            body_read = not message.get("more_body", False)
            return message

        async def recording_send(message: Any) -> None:
            self.sent.append(message)
            if self.on_send is not None:
                await self.on_send(message)
            await send(message)

        try:
            # ASGITransport cancels the application once the client has the
            # whole response. A server lets it finish, so the shield blocks
            # that cancellation.
            with trio.CancelScope(shield=True), trio.CancelScope() as self._scope:
                await self.app(scope, disconnecting_receive, recording_send)
        except BaseException as e:
            self.raised = e
            raise
        finally:
            self.finished_at = trio.current_time()
            self.done.set()

    def cancel(self) -> None:
        assert self._scope is not None
        self.cancelled_at = trio.current_time()
        self._scope.cancel()

    def disconnect(self) -> None:
        self._disconnected.set()

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


@pytest.fixture
def wrapper() -> Wrapper:
    return Wrapper()


@pytest.fixture
async def client(wrapper: Wrapper) -> AsyncIterator[HaberdasherClient]:
    async with HaberdasherClient(
        "http://localhost", http_client=Client(ASGITransport(wrapper))
    ) as client:
        yield client


@pytest.fixture
def cleanup_timeout(monkeypatch: pytest.MonkeyPatch) -> float:
    monkeypatch.setattr(_loop_trio, "_CLEANUP_TIMEOUT", CLEANUP_TIMEOUT)
    return CLEANUP_TIMEOUT


async def sizes(*inches: int) -> AsyncIterator[Size]:
    for i in inches:
        yield Size(inches=i)


async def call(
    client: HaberdasherClient, method: str, headers: dict[str, str] | None = None
) -> Any:
    """Call the method with one request message, consuming any response stream."""
    match method:
        case "MakeHat":
            return await client.make_hat(Size(inches=10), headers=headers)
        case "MakeFlexibleHat":
            return await client.make_flexible_hat(sizes(10), headers=headers)
        case "MakeSimilarHats":
            return [
                hat
                async for hat in client.make_similar_hats(
                    Size(inches=10), headers=headers
                )
            ]
        case "MakeVariousHats":
            return [
                hat
                async for hat in client.make_various_hats(sizes(10), headers=headers)
            ]
        case _:
            raise ValueError(method)


async def call_ignoring_error(client: HaberdasherClient, method: str) -> None:
    with contextlib.suppress(ConnectError):
        await call(client, method)


async def cancel_when_blocked(
    client: HaberdasherClient, wrapper: Wrapper, method: str, blocked: trio.Event
) -> None:
    """Call the method, cancel the application once blocked is set, and wait for
    the application to finish."""
    with trio.fail_after(10):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(call_ignoring_error, client, method)
            await blocked.wait()
            wrapper.cancel()
            await wrapper.done.wait()
            # A cleanup timeout can end the application before it finishes the
            # response, which leaves the client waiting.
            nursery.cancel_scope.cancel()


async def test_unary(client: HaberdasherClient) -> None:
    hat = await client.make_hat(Size(inches=10))
    assert hat == Hat(size=10, color="blue", name="bowler")


async def test_unary_error(client: HaberdasherClient) -> None:
    with pytest.raises(ConnectError) as exc_info:
        await client.make_hat(Size(inches=-1))
    assert exc_info.value.code == Code.INVALID_ARGUMENT


async def test_client_stream(client: HaberdasherClient) -> None:
    assert (await client.make_flexible_hat(sizes(1, 2, 3))).size == 6


async def test_server_stream(client: HaberdasherClient) -> None:
    hats = [hat async for hat in client.make_similar_hats(Size(inches=10))]
    assert [hat.size for hat in hats] == [10, 11, 12]


async def test_server_stream_error(client: HaberdasherClient) -> None:
    hats = []
    with pytest.raises(ConnectError) as exc_info:
        async for hat in client.make_similar_hats(Size(inches=-5)):
            hats.append(hat)
    assert exc_info.value.code == Code.RESOURCE_EXHAUSTED
    assert [hat.size for hat in hats] == [-5]


async def test_bidi_stream(client: HaberdasherClient) -> None:
    hats = [hat async for hat in client.make_various_hats(sizes(1, 2, 3))]
    assert [hat.size for hat in hats] == [1, 2, 3]


class Recorder:
    """A metadata interceptor whose on_end awaits before it finishes or fails."""

    def __init__(
        self, events: list[str], seconds: float = 0, error: Exception | None = None
    ) -> None:
        self._events = events
        self._seconds = seconds
        self._error = error

    async def on_start(self, ctx: RequestContext) -> None:  # noqa: ARG002
        return None

    async def on_end(
        self, _token: None, _ctx: RequestContext, error: Exception | None, /
    ) -> None:
        code = error.code.value if isinstance(error, ConnectError) else error
        self._events.append(f"on_end started {code}")
        await trio.sleep(self._seconds)
        self._events.append("on_end finished")
        if self._error is not None:
            raise self._error


class PassThrough:
    """A message interceptor, after which metadata interceptors wrap the handler."""

    def intercept_server_stream(
        self,
        call_next: Callable[[Size, RequestContext], AsyncIterator[Hat]],
        request: Size,
        ctx: RequestContext,
        /,
    ) -> AsyncIterator[Hat]:
        return call_next(request, ctx)


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
        # Reads the whole request, then streams like make_similar_hats.
        try:
            size = Size()
            async for message in request:
                size = message
            while True:
                yield Hat(size=size.inches)
                await trio.sleep(0.01)
        finally:
            await self._clean_up()

    async def _clean_up(self) -> None:
        await trio.sleep(self._cleanup_seconds)
        self._events.append("handler cleaned up")
        if self._cleanup_error is not None:
            raise self._cleanup_error


async def read_hats_then_disconnect(
    client: HaberdasherClient, wrapper: Wrapper, method: str = "MakeSimilarHats"
) -> None:
    if method == "MakeSimilarHats":
        stream = client.make_similar_hats(Size(inches=10))
    else:
        stream = client.make_various_hats(sizes(10))
    # Generated stream methods are typed as AsyncIterator but return async
    # generators.
    hats = cast("AsyncGenerator[Hat]", stream)
    async with contextlib.aclosing(hats):
        assert [(await anext(hats)).size for _ in range(3)] == [10, 10, 10]
        wrapper.disconnect()
        with trio.fail_after(10):
            await wrapper.done.wait()


@pytest.mark.parametrize("method", ["MakeSimilarHats", "MakeVariousHats"])
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
async def test_disconnect_ends_stream(
    client: HaberdasherClient,
    wrapper: Wrapper,
    method: str,
    cleanup_error: Exception | None,
    end_error: dict[str, str],
) -> None:
    """A disconnect after the request has been read ends the response stream."""
    events: list[str] = []
    wrapper.app = HaberdasherASGIApplication(
        EndlessHats(events, cleanup_error), interceptors=[Recorder(events)]
    )
    await read_hats_then_disconnect(client, wrapper, method)

    assert wrapper.end_error() == end_error
    assert events == [
        "handler cleaned up",
        f"on_end started {end_error['code']}",
        "on_end finished",
    ]


async def test_disconnect_cancels_blocked_unary(
    client: HaberdasherClient, wrapper: Wrapper
) -> None:
    """A unary handler has nothing to poll, so a disconnect cancels it like a deadline."""
    events: list[str] = []
    blocked = trio.Event()
    wrapper.app = HaberdasherASGIApplication(
        BlockedHats(blocked, events, in_nursery=False), interceptors=[Recorder(events)]
    )

    with trio.fail_after(10):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(call_ignoring_error, client, "MakeHat")
            await blocked.wait()
            wrapper.disconnect()
            await wrapper.done.wait()

    assert wrapper.raised is None
    assert events == [
        "handler cleaned up",
        "on_end started canceled",
        "on_end finished",
    ]
    assert wrapper.error() == {"code": "canceled", "message": "Client disconnected"}


class ScopedHats(Haberdasher):
    """Streams hats from inside a cancel scope of its own."""

    async def make_similar_hats(
        self, request: Size, _ctx: RequestContext[Size, Hat], /
    ) -> AsyncIterator[Hat]:
        with trio.move_on_after(60):
            while True:
                yield Hat(size=request.inches)
                await trio.sleep(0.01)


async def test_disconnect_ends_server_stream_in_cancel_scope(
    client: HaberdasherClient, wrapper: Wrapper
) -> None:
    """A disconnect cleanly closes a handler suspended inside its own cancel scope."""
    wrapper.app = HaberdasherASGIApplication(ScopedHats())
    await read_hats_then_disconnect(client, wrapper)

    assert wrapper.raised is None
    assert wrapper.end_error() == {"code": "canceled", "message": "Client disconnected"}


@pytest.mark.parametrize(
    ("method", "cleanup_error", "send_in_nursery"),
    [
        ("MakeSimilarHats", None, False),
        ("MakeSimilarHats", ConnectError(Code.ABORTED, "cleanup failed"), False),
        ("MakeVariousHats", ConnectError(Code.ABORTED, "cleanup failed"), False),
        ("MakeVariousHats", None, True),
    ],
)
async def test_cancelled_while_sending(
    client: HaberdasherClient,
    wrapper: Wrapper,
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

    await cancel_when_blocked(client, wrapper, method, wrapper.blocked)

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
        blocked: trio.Event,
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
async def test_cancelled_in_handler(
    client: HaberdasherClient, wrapper: Wrapper, method: str, in_nursery: bool
) -> None:
    events: list[str] = []
    blocked = trio.Event()
    wrapper.app = HaberdasherASGIApplication(
        BlockedHats(blocked, events, in_nursery=in_nursery),
        interceptors=[Recorder(events)],
    )

    await cancel_when_blocked(client, wrapper, method, blocked)

    assert wrapper.raised is None
    assert events == [
        "handler cleaned up",
        "on_end started canceled",
        "on_end finished",
    ]
    assert wrapper.types() == ["start", "body"]
    assert wrapper.error() == CANCELLED


@pytest.mark.parametrize(
    "method", ["MakeHat", "MakeFlexibleHat", "MakeSimilarHats", "MakeVariousHats"]
)
@pytest.mark.parametrize(
    ("interceptors", "on_end_events"),
    [
        # The server ends the leading metadata interceptors after the deadline.
        pytest.param(
            lambda events: [Recorder(events)],
            ["on_end started deadline_exceeded", "on_end finished"],
            id="leading",
        ),
        # Wrapping the handler, the interceptor sees the deadline's cancellation.
        pytest.param(
            lambda events: [PassThrough(), Recorder(events)],
            ["on_end started canceled"],
            id="after_message_interceptor",
        ),
    ],
)
async def test_timeout_in_handler(
    client: HaberdasherClient,
    wrapper: Wrapper,
    method: str,
    interceptors: Callable[[list[str]], list[Any]],
    on_end_events: list[str],
) -> None:
    events: list[str] = []
    wrapper.app = HaberdasherASGIApplication(
        BlockedHats(trio.Event(), events, in_nursery=True),
        interceptors=interceptors(events),
    )

    # The header alone, so that only the server enforces the timeout.
    with pytest.raises(ConnectError) as exc_info:
        await call(client, method, headers={"connect-timeout-ms": "100"})
    with trio.fail_after(10):
        await wrapper.done.wait()

    assert exc_info.value.code == Code.DEADLINE_EXCEEDED
    assert wrapper.raised is None
    assert events == ["handler cleaned up", *on_end_events]
    assert wrapper.types() == ["start", "body"]
    assert wrapper.error() == TIMED_OUT


def leaves(error: BaseException) -> list[BaseException]:
    nested = getattr(error, "exceptions", None)
    if nested is None:
        return [error]
    return [leaf for e in nested for leaf in leaves(e)]


@pytest.mark.parametrize(
    "method", ["MakeHat", "MakeFlexibleHat", "MakeSimilarHats", "MakeVariousHats"]
)
async def test_cancelled_in_handler_whose_cleanup_fails(
    client: HaberdasherClient, wrapper: Wrapper, method: str
) -> None:
    """A cancellation beside a handler error is handled as a cancellation.

    The client is told CANCELED, and the handler's error still escapes the
    application for the server to log.
    """
    events: list[str] = []
    blocked = trio.Event()
    cleanup_error = OSError("cleanup failed")
    wrapper.app = HaberdasherASGIApplication(
        BlockedHats(blocked, events, in_nursery=True, cleanup_error=cleanup_error),
        interceptors=[Recorder(events)],
    )

    await cancel_when_blocked(client, wrapper, method, blocked)

    assert wrapper.raised is not None
    assert leaves(wrapper.raised) == [cleanup_error]
    assert events == [
        "handler cleaned up",
        "on_end started canceled",
        "on_end finished",
    ]
    assert wrapper.types() == ["start", "body"]
    assert wrapper.error() == CANCELLED


@pytest.mark.parametrize(
    "method", ["MakeHat", "MakeFlexibleHat", "MakeSimilarHats", "MakeVariousHats"]
)
async def test_cancelled_in_handler_whose_on_end_fails(
    client: HaberdasherClient, wrapper: Wrapper, method: str
) -> None:
    """The cancellation still reaches the server when an on_end hook fails."""
    events: list[str] = []
    blocked = trio.Event()
    wrapper.app = HaberdasherASGIApplication(
        BlockedHats(blocked, events, in_nursery=True),
        interceptors=[Recorder(events, error=RuntimeError("on_end failed"))],
    )

    await cancel_when_blocked(client, wrapper, method, blocked)

    # The wrapper's cancel scope absorbs the cancellation; the hook's error
    # raised in its place would escape the scope.
    assert wrapper.raised is None
    assert events == [
        "handler cleaned up",
        "on_end started canceled",
        "on_end finished",
    ]
    assert wrapper.types() == ["start", "body"]
    assert wrapper.error() == {"code": "unknown", "message": "on_end failed"}


async def test_handler_cleanup_after_cancellation_is_bounded(
    client: HaberdasherClient, wrapper: Wrapper, cleanup_timeout: float
) -> None:
    events: list[str] = []
    wrapper.app = HaberdasherASGIApplication(EndlessHats(events, cleanup_seconds=60))
    wrapper.on_send = wrapper.block_on_first_message

    await cancel_when_blocked(client, wrapper, "MakeSimilarHats", wrapper.blocked)

    assert events == []
    assert cleanup_timeout <= wrapper.finished_at - wrapper.cancelled_at < 5


@pytest.mark.parametrize(
    ("interceptors", "on_ends"),
    [
        pytest.param(
            lambda events: [Recorder(events, seconds=60)], 1, id="one_interceptor"
        ),
        # Each interceptor's on_end runs, though cancelled at its first await.
        pytest.param(
            lambda events: [Recorder(events), Recorder(events, seconds=60)],
            2,
            id="two_interceptors",
        ),
        # The server closes the handler's stream, which the interceptor wraps.
        pytest.param(
            lambda events: [PassThrough(), Recorder(events, seconds=60)],
            1,
            id="after_message_interceptor",
        ),
    ],
)
async def test_on_end_after_cancellation_is_bounded(
    client: HaberdasherClient,
    wrapper: Wrapper,
    cleanup_timeout: float,
    interceptors: Callable[[list[str]], list[Any]],
    on_ends: int,
) -> None:
    events: list[str] = []
    wrapper.app = HaberdasherASGIApplication(
        EndlessHats(events), interceptors=interceptors(events)
    )
    wrapper.on_send = wrapper.block_on_first_message

    await cancel_when_blocked(client, wrapper, "MakeSimilarHats", wrapper.blocked)

    assert events == ["handler cleaned up", *["on_end started canceled"] * on_ends]
    assert cleanup_timeout <= wrapper.finished_at - wrapper.cancelled_at < 5


@pytest.mark.parametrize("method", ["MakeHat", "MakeSimilarHats"])
async def test_on_end_is_bounded_by_deadline(
    client: HaberdasherClient, wrapper: Wrapper, method: str
) -> None:
    events: list[str] = []
    wrapper.app = HaberdasherASGIApplication(
        TrioHaberdasher(), interceptors=[Recorder(events, seconds=5)]
    )

    with pytest.raises(ConnectError) as exc_info:
        await call(client, method, headers={"connect-timeout-ms": "100"})
    with trio.fail_after(10):
        await wrapper.done.wait()

    assert exc_info.value.code == Code.DEADLINE_EXCEEDED
    assert wrapper.raised is None
    # The deadline can pass before on_end on a slow machine.
    assert events in (
        ["on_end started None"],
        ["on_end started deadline_exceeded", "on_end finished"],
    )
    assert wrapper.error() == TIMED_OUT


@pytest.mark.parametrize("method", ["MakeHat", "MakeSimilarHats"])
async def test_on_end_is_not_bounded_without_cancellation(
    client: HaberdasherClient, wrapper: Wrapper, cleanup_timeout: float, method: str
) -> None:
    events: list[str] = []
    wrapper.app = HaberdasherASGIApplication(
        TrioHaberdasher(), interceptors=[Recorder(events, seconds=2 * cleanup_timeout)]
    )

    await call(client, method)
    with trio.fail_after(10):
        await wrapper.done.wait()

    assert events == ["on_end started None", "on_end finished"]


@pytest.mark.parametrize("method", ["MakeHat", "MakeSimilarHats"])
async def test_base_exception_in_handler(
    client: HaberdasherClient, wrapper: Wrapper, method: str
) -> None:
    """A SystemExit from the handler reaches the server and is reported as an error."""
    events: list[str] = []
    raised: list[BaseException] = []

    class ExitingHats(Haberdasher):
        async def make_hat(self, _request: Size, _ctx: RequestContext, /) -> Hat:
            raise SystemExit(1)

        async def make_similar_hats(
            self, _request: Size, _ctx: RequestContext, /
        ) -> AsyncIterator[Hat]:
            yield Hat(size=1)
            raise SystemExit(1)

    app = HaberdasherASGIApplication(ExitingHats(), interceptors=[Recorder(events)])

    async def catching_app(scope: Any, receive: Any, send: Any) -> None:
        # A SystemExit escaping the application would end the trio run.
        try:
            await app(scope, receive, send)
        except SystemExit as e:
            raised.append(e)

    wrapper.app = catching_app

    with pytest.raises(ConnectError):
        await call(client, method)
    with trio.fail_after(10):
        await wrapper.done.wait()

    assert [type(e) for e in raised] == [SystemExit]
    assert events == ["on_end started unknown", "on_end finished"]
    if method == "MakeSimilarHats":
        assert wrapper.end_error() == {
            "code": "unknown",
            "message": "Request was interrupted",
        }


async def test_handler_timeout_on_request_stream(
    client: HaberdasherClient, wrapper: Wrapper
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

    async def stalling_sizes() -> AsyncIterator[Size]:
        # One message, then the client stalls without ending the body.
        yield Size(inches=7)
        await trio.sleep_forever()

    with trio.fail_after(10):
        hat = await client.make_flexible_hat(stalling_sizes())

    assert timed_out == [True]
    assert hat == Hat(size=7)


async def test_bidi_handler_returns_early(
    client: HaberdasherClient, wrapper: Wrapper
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

    hats = [hat async for hat in client.make_various_hats(sizes(1, 2, 3))]

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

"""Disconnects, cancellation and cleanup of the ASGI application under trio.

The tests call the application directly, so that each one controls when the
client disconnects and when the server cancels the request. Behaviour common to
both event loops is tested on both.
"""

from __future__ import annotations

import asyncio
import gc
import json
import struct
import subprocess
import sys
import warnings
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

import anyio
import pytest
import trio
from trio.testing import MockClock

from connectrpc import _server_anyio
from connectrpc.code import Code
from connectrpc.errors import ConnectError

from .connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
)
from .connectrpc.example.haberdasher_pb import Hat, Size

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from connectrpc.request import RequestContext

CONNECT_STREAM = "application/connect+proto"
GRPC = "application/grpc+proto"

# An envelope is a byte of flags and four bytes of length, then the message.
ENVELOPE_HEADER_LEN = 5
END_STREAM = 2

both_backends = pytest.mark.parametrize("backend", ["asyncio", "trio"])


def envelope(message: Size) -> bytes:
    data = message.to_binary()
    return struct.pack(">BI", 0, len(data)) + data


class Exchange:
    """One request to the application, and what it sent back."""

    def __init__(
        self,
        method: str,
        content_type: str,
        *chunks: bytes,
        more_body: bool = False,
        receive_error: Exception | None = None,
    ) -> None:
        path = f"/connectrpc.example.Haberdasher/{method}"
        self.scope: dict[str, Any] = {
            "type": "http",
            "asgi": {"spec_version": "2.0", "version": "3.0"},
            "http_version": "2",
            "method": "POST",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "root_path": "",
            "headers": [(b"content-type", content_type.encode())],
            "client": None,
            "server": None,
            "extensions": {"http.response.trailers": {}},
        }
        self._chunks = list(chunks)
        self._more_body = more_body
        self._receive_error = receive_error
        self.sent: list[dict[str, Any]] = []
        self.on_send: Callable[[dict[str, Any]], Awaitable[None]] | None = None

    # Created on first use so that they belong to the running event loop.
    @cached_property
    def blocked(self) -> anyio.Event:
        return anyio.Event()

    @cached_property
    def _disconnect(self) -> anyio.Event:
        return anyio.Event()

    @cached_property
    def _disconnect_received(self) -> anyio.Event:
        return anyio.Event()

    async def receive(self) -> dict[str, Any]:
        if self._chunks:
            body = self._chunks.pop(0)
            more_body = self._more_body or bool(self._chunks)
            return {"type": "http.request", "body": body, "more_body": more_body}
        if self._receive_error is not None:
            raise self._receive_error
        await self._disconnect.wait()
        self._disconnect_received.set()
        return {"type": "http.disconnect"}

    async def disconnect(self) -> None:
        self._disconnect.set()
        await self._disconnect_received.wait()

    async def send(self, message: dict[str, Any]) -> None:
        self.sent.append(message)
        if self.on_send is not None:
            await self.on_send(message)

    async def block_on_first_message(self, message: dict[str, Any]) -> None:
        if message["type"] == "http.response.body":
            self.blocked.set()
            await anyio.sleep_forever()

    async def block_in_nursery_on_first_message(self, message: dict[str, Any]) -> None:
        """Block inside a nursery, as a server's send() may."""
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.block_on_first_message, message)

    async def call(self, app: HaberdasherASGIApplication) -> None:
        await app(
            self.scope,  # ty: ignore[invalid-argument-type] - plain dicts, not asgiref TypedDicts
            self.receive,  # ty: ignore[invalid-argument-type]
            self.send,  # ty: ignore[invalid-argument-type]
        )

    def run(self, app: HaberdasherASGIApplication, backend: str = "trio") -> None:
        async def main() -> None:
            with anyio.fail_after(60):
                await self.call(app)

        if backend == "trio":
            trio.run(main, clock=MockClock(autojump_threshold=0))
        else:
            asyncio.run(main())

    def run_until_cancelled(
        self, app: HaberdasherASGIApplication
    ) -> tuple[trio.CancelScope, float]:
        """Cancel the request once it blocks; return its scope and the seconds it took to end."""
        scope = trio.CancelScope()

        async def request() -> None:
            with scope:
                await self.call(app)

        async def main() -> float:
            with trio.fail_after(60):
                async with trio.open_nursery() as nursery:
                    nursery.start_soon(request)
                    await self.blocked.wait()
                    cancelled_at = trio.current_time()
                    scope.cancel()
            return trio.current_time() - cancelled_at

        return scope, trio.run(main, clock=MockClock(autojump_threshold=0))

    def types(self) -> list[str]:
        return [m["type"].removeprefix("http.response.") for m in self.sent]

    def bodies(self) -> list[bytes]:
        return [m["body"] for m in self.sent if m["type"] == "http.response.body"]

    def messages(self) -> list[Hat]:
        return [
            Hat.from_binary(body[ENVELOPE_HEADER_LEN:])
            for body in self.bodies()
            if body and not body[0] & END_STREAM
        ]

    def end_error(self) -> dict[str, str] | None:
        """The error of a Connect stream's end message, or None for a success."""
        (end,) = [body for body in self.bodies() if body and body[0] & END_STREAM]
        return json.loads(end[ENVELOPE_HEADER_LEN:]).get("error")

    def trailers(self) -> dict[bytes, bytes]:
        (trailers,) = [m for m in self.sent if m["type"] == "http.response.trailers"]
        assert not trailers["more_trailers"]
        return dict(trailers["headers"])


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
        await anyio.sleep(self._seconds)
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
        await anyio.sleep(self._cleanup_seconds)
        self._events.append("handler cleaned up")
        if self._cleanup_error is not None:
            raise self._cleanup_error


class Hats(Haberdasher):
    """Rejects negative sizes with INVALID_ARGUMENT."""

    async def make_hat(self, request: Size, _ctx: RequestContext[Size, Hat], /) -> Hat:
        if request.inches < 0:
            raise ConnectError(Code.INVALID_ARGUMENT, "negative")
        return Hat(size=request.inches)

    async def make_similar_hats(
        self, request: Size, _ctx: RequestContext[Size, Hat], /
    ) -> AsyncIterator[Hat]:
        if request.inches < 0:
            raise ConnectError(Code.INVALID_ARGUMENT, "negative")
        yield Hat(size=request.inches)


@both_backends
def test_disconnect_ends_server_stream(backend: str) -> None:
    events: list[str] = []
    app = HaberdasherASGIApplication(
        EndlessHats(events), interceptors=[Recorder(events)]
    )
    exchange = Exchange("MakeSimilarHats", CONNECT_STREAM, envelope(Size(inches=10)))

    async def disconnect_after_three(_message: dict[str, Any]) -> None:
        if len(exchange.messages()) == 3:
            await exchange.disconnect()

    exchange.on_send = disconnect_after_three

    exchange.run(app, backend)

    assert len(exchange.messages()) == 3
    assert exchange.end_error() == {
        "code": "canceled",
        "message": "Client disconnected",
    }
    assert events == [
        "handler cleaned up",
        "on_end started canceled",
        "on_end finished",
    ]


@both_backends
def test_cleanup_error_after_disconnect(backend: str) -> None:
    events: list[str] = []
    service = EndlessHats(events, ConnectError(Code.ABORTED, "cleanup failed"))
    app = HaberdasherASGIApplication(service)
    exchange = Exchange("MakeSimilarHats", CONNECT_STREAM, envelope(Size(inches=10)))

    async def disconnect_after_first(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.body" and message["more_body"]:
            await exchange.disconnect()

    exchange.on_send = disconnect_after_first

    exchange.run(app, backend)

    assert exchange.end_error() == {"code": "aborted", "message": "cleanup failed"}


@both_backends
@pytest.mark.parametrize(
    ("content_type", "method", "inches", "sent", "error"),
    [
        (GRPC, "MakeHat", 10, ["start", "body", "last body", "trailers"], None),
        (GRPC, "MakeSimilarHats", 10, ["start", "body", "last body", "trailers"], None),
        (GRPC, "MakeHat", -1, ["start", "last body", "trailers"], "negative"),
        (GRPC, "MakeSimilarHats", -1, ["start", "last body", "trailers"], "negative"),
        (CONNECT_STREAM, "MakeSimilarHats", 10, ["start", "body", "last body"], None),
        (CONNECT_STREAM, "MakeSimilarHats", -1, ["start", "last body"], "negative"),
    ],
)
def test_response_end(
    backend: str,
    content_type: str,
    method: str,
    inches: int,
    sent: list[str],
    error: str | None,
) -> None:
    exchange = Exchange(method, content_type, envelope(Size(inches=inches)))

    exchange.run(HaberdasherASGIApplication(Hats()), backend)

    assert [
        "body" if m.get("more_body") else "last body" if t == "body" else t
        for t, m in zip(exchange.types(), exchange.sent, strict=True)
    ] == sent
    assert exchange.sent[0]["status"] == 200
    assert exchange.messages() == ([Hat(size=inches)] if error is None else [])
    if content_type == GRPC:
        assert exchange.bodies()[-1] == b""
        trailers = exchange.trailers()
        assert trailers[b"grpc-status"] == (b"0" if error is None else b"3")
        assert trailers.get(b"grpc-message") == (error and error.encode())
    elif error is None:
        assert exchange.end_error() is None
    else:
        assert exchange.end_error() == {"code": "invalid_argument", "message": error}


class UnencodableHats(Haberdasher):
    async def make_similar_hats(
        self, _request: Size, _ctx: RequestContext[Size, Hat], /
    ) -> AsyncIterator[Hat]:
        yield "not a hat"  # ty: ignore[invalid-yield]


@both_backends
def test_message_cannot_be_encoded(backend: str) -> None:
    exchange = Exchange("MakeSimilarHats", CONNECT_STREAM, envelope(Size(inches=10)))

    with pytest.raises(AttributeError):
        exchange.run(HaberdasherASGIApplication(UnencodableHats()), backend)

    # The headers were sent before the message was encoded, and are not resent.
    assert exchange.types() == ["start", "body"]
    assert exchange.end_error() == {
        "code": "unknown",
        "message": "'str' object has no attribute 'to_binary'",
    }


class Stop(BaseException):
    pass


class StoppedHats(Haberdasher):
    """Raises a BaseException, like a KeyboardInterrupt."""

    async def make_hat(self, _request: Size, _ctx: RequestContext[Size, Hat], /) -> Hat:
        raise Stop

    async def make_similar_hats(
        self, request: Size, _ctx: RequestContext[Size, Hat], /
    ) -> AsyncIterator[Hat]:
        yield Hat(size=request.inches)
        raise Stop


@both_backends
@pytest.mark.parametrize(
    ("method", "content_type", "sent"),
    [
        ("MakeHat", GRPC, ["start", "body", "trailers"]),
        ("MakeSimilarHats", CONNECT_STREAM, ["start", "body", "body"]),
    ],
)
def test_handler_raises_base_exception(
    backend: str, method: str, content_type: str, sent: list[str]
) -> None:
    events: list[str] = []
    app = HaberdasherASGIApplication(StoppedHats(), interceptors=[Recorder(events)])
    exchange = Exchange(method, content_type, envelope(Size(inches=10)))

    with pytest.raises(Stop):
        exchange.run(app, backend)

    assert events == ["on_end started None", "on_end finished"]
    assert exchange.types() == sent


def test_handler_raises_base_exception_in_nursery() -> None:
    async def stop() -> None:
        raise Stop

    class Service(Haberdasher):
        async def make_hat(
            self, _request: Size, _ctx: RequestContext[Size, Hat], /
        ) -> Hat:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(stop)
            return Hat()

    events: list[str] = []
    app = HaberdasherASGIApplication(Service(), interceptors=[Recorder(events)])
    exchange = Exchange("MakeHat", GRPC, envelope(Size(inches=10)))

    with pytest.RaisesGroup(Stop):
        exchange.run(app)

    assert events == ["on_end started None", "on_end finished"]
    assert exchange.types() == ["start", "body", "trailers"]


@pytest.mark.parametrize(
    ("method", "cleanup_error", "send_in_nursery"),
    [
        ("MakeSimilarHats", None, False),
        ("MakeSimilarHats", ConnectError(Code.ABORTED, "cleanup failed"), False),
        ("MakeVariousHats", ConnectError(Code.ABORTED, "cleanup failed"), False),
        ("MakeVariousHats", None, True),
    ],
)
def test_stream_cancelled(
    method: str, cleanup_error: Exception | None, send_in_nursery: bool
) -> None:
    events: list[str] = []
    app = HaberdasherASGIApplication(
        EndlessHats(events, cleanup_error), interceptors=[Recorder(events)]
    )
    exchange = Exchange(
        method,
        CONNECT_STREAM,
        envelope(Size(inches=10)),
        more_body=method == "MakeVariousHats",
    )
    exchange.on_send = (
        exchange.block_in_nursery_on_first_message
        if send_in_nursery
        else exchange.block_on_first_message
    )

    scope, _ = exchange.run_until_cancelled(app)

    assert scope.cancelled_caught
    assert events == [
        "handler cleaned up",
        "on_end started canceled",
        "on_end finished",
    ]
    assert exchange.types() == ["start", "body"]
    assert exchange.sent[1]["more_body"]


class BlockedHats(Haberdasher):
    """Blocks forever; records when it is cancelled."""

    def __init__(
        self,
        exchange: Exchange,
        events: list[str],
        *,
        in_nursery: bool,
        cleanup_error: Exception | None = None,
    ):
        self._exchange = exchange
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
            self._exchange.blocked.set()
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
def test_cancelled_in_handler(method: str, in_nursery: bool) -> None:
    events: list[str] = []
    exchange = Exchange(
        method,
        GRPC,
        envelope(Size(inches=10)),
        more_body=method in ("MakeFlexibleHat", "MakeVariousHats"),
    )
    app = HaberdasherASGIApplication(
        BlockedHats(exchange, events, in_nursery=in_nursery),
        interceptors=[Recorder(events)],
    )

    scope, _ = exchange.run_until_cancelled(app)

    assert scope.cancelled_caught
    assert events == [
        "handler cleaned up",
        "on_end started canceled",
        "on_end finished",
    ]
    assert exchange.sent == []


def leaves(error: BaseException) -> list[BaseException]:
    nested = getattr(error, "exceptions", None)
    if nested is None:
        return [error]
    return [leaf for e in nested for leaf in leaves(e)]


@pytest.mark.parametrize(
    "method", ["MakeHat", "MakeFlexibleHat", "MakeSimilarHats", "MakeVariousHats"]
)
def test_cancelled_in_handler_whose_cleanup_fails(method: str) -> None:
    """A cancellation that arrives beside an error is still a cancellation."""
    events: list[str] = []
    exchange = Exchange(
        method,
        GRPC,
        envelope(Size(inches=10)),
        more_body=method in ("MakeFlexibleHat", "MakeVariousHats"),
    )
    cleanup_error = OSError("cleanup failed")
    app = HaberdasherASGIApplication(
        BlockedHats(exchange, events, in_nursery=True, cleanup_error=cleanup_error),
        interceptors=[Recorder(events)],
    )

    with pytest.raises(Exception, match="Exceptions from Trio nursery") as raised:
        exchange.run_until_cancelled(app)

    assert leaves(raised.value) == [cleanup_error]
    assert events == [
        "handler cleaned up",
        "on_end started canceled",
        "on_end finished",
    ]
    assert exchange.sent == []


def test_handler_cleanup_after_cancellation_is_bounded() -> None:
    events: list[str] = []
    app = HaberdasherASGIApplication(EndlessHats(events, cleanup_seconds=3600))
    exchange = Exchange("MakeSimilarHats", CONNECT_STREAM, envelope(Size(inches=10)))
    exchange.on_send = exchange.block_on_first_message

    scope, seconds = exchange.run_until_cancelled(app)

    assert scope.cancelled_caught
    assert events == []
    assert seconds == _server_anyio._CLEANUP_TIMEOUT


def test_on_end_after_cancellation_is_bounded() -> None:
    events: list[str] = []
    app = HaberdasherASGIApplication(
        EndlessHats(events), interceptors=[Recorder(events, seconds=3600)]
    )
    exchange = Exchange("MakeSimilarHats", CONNECT_STREAM, envelope(Size(inches=10)))
    exchange.on_send = exchange.block_on_first_message

    scope, seconds = exchange.run_until_cancelled(app)

    assert scope.cancelled_caught
    assert events == ["handler cleaned up", "on_end started canceled"]
    assert seconds == _server_anyio._CLEANUP_TIMEOUT


@pytest.mark.parametrize(
    ("method", "content_type"), [("MakeHat", GRPC), ("MakeSimilarHats", CONNECT_STREAM)]
)
def test_on_end_is_not_bounded_without_cancellation(
    method: str, content_type: str
) -> None:
    events: list[str] = []
    seconds = 10 * _server_anyio._CLEANUP_TIMEOUT
    app = HaberdasherASGIApplication(Hats(), interceptors=[Recorder(events, seconds)])
    exchange = Exchange(method, content_type, envelope(Size(inches=10)))

    exchange.run(app)

    assert events == ["on_end started None", "on_end finished"]
    assert exchange.messages() == [Hat(size=10)]


def test_handler_timeout_on_request_stream() -> None:
    timed_out: list[bool] = []

    class Service(Haberdasher):
        async def make_flexible_hat(
            self, request: AsyncIterator[Size], _ctx: RequestContext[Size, Hat], /
        ) -> Hat:
            total = 0
            with trio.move_on_after(1) as scope:
                async for size in request:
                    total += size.inches
            timed_out.append(scope.cancelled_caught)
            return Hat(size=total)

    app = HaberdasherASGIApplication(Service())
    # The client sends one message, then stalls without ending the body.
    exchange = Exchange(
        "MakeFlexibleHat", CONNECT_STREAM, envelope(Size(inches=7)), more_body=True
    )

    exchange.run(app)

    assert timed_out == [True]
    assert exchange.messages() == [Hat(size=7)]
    assert exchange.end_error() is None


def test_receive_error_while_streaming() -> None:
    class Service(Haberdasher):
        async def make_similar_hats(
            self, request: Size, _ctx: RequestContext[Size, Hat], /
        ) -> AsyncIterator[Hat]:
            for i in range(5):
                await trio.sleep(1)
                yield Hat(size=request.inches + i)

    app = HaberdasherASGIApplication(Service())
    exchange = Exchange(
        "MakeSimilarHats",
        CONNECT_STREAM,
        envelope(Size(inches=10)),
        receive_error=OSError("connection reset"),
    )

    exchange.run(app)

    assert [hat.size for hat in exchange.messages()] == [10, 11, 12, 13, 14]
    assert exchange.end_error() is None


class FirstSizeOnly(Haberdasher):
    """Reads one size, makes one hat and returns."""

    async def make_various_hats(
        self, request: AsyncIterator[Size], _ctx: RequestContext[Size, Hat], /
    ) -> AsyncIterator[Hat]:
        async for size in request:
            yield Hat(size=size.inches)
            return


def test_request_stream_is_closed() -> None:
    app = HaberdasherASGIApplication(FirstSizeOnly())
    exchange = Exchange(
        "MakeVariousHats", CONNECT_STREAM, envelope(Size(inches=7)), more_body=True
    )

    async def main() -> None:
        await exchange.call(app)
        gc.collect()

    # trio warns of a generator that it finds left open.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        trio.run(main)

    assert caught == []
    assert exchange.messages() == [Hat(size=7)]
    assert exchange.end_error() is None


def test_anyio_not_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    # None in sys.modules makes the import fail as it does for a missing package.
    monkeypatch.setitem(sys.modules, "anyio", None)
    exchange = Exchange("MakeSimilarHats", CONNECT_STREAM, envelope(Size(inches=10)))

    with pytest.raises(ImportError, match=r"connectrpc\[trio\]") as exc_info:
        trio.run(exchange.call, HaberdasherASGIApplication(Hats()))
    assert exc_info.value.name == "anyio"
    assert exchange.sent == []


SERVER_STREAM_ON_ASYNCIO = """
import asyncio
import struct

from test.connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
)
from test.connectrpc.example.haberdasher_pb import Hat, Size


class Service(Haberdasher):
    async def make_similar_hats(self, request, ctx):
        yield Hat(size=request.inches)


async def main():
    data = Size(inches=10).to_binary()
    path = "/connectrpc.example.Haberdasher/MakeSimilarHats"
    scope = {
        "type": "http",
        "asgi": {"spec_version": "2.0", "version": "3.0"},
        "http_version": "2",
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
    sent_request = False
    sent = []

    async def receive():
        nonlocal sent_request
        if not sent_request:
            sent_request = True
            body = struct.pack(">BI", 0, len(data)) + data
            return {"type": "http.request", "body": body, "more_body": False}
        await asyncio.Event().wait()

    async def send(message):
        sent.append(message["type"])

    await HaberdasherASGIApplication(Service())(scope, receive, send)
    assert sent.count("http.response.body") == 2, sent


asyncio.run(main())
"""


def test_asyncio_does_not_import_anyio() -> None:
    script = (
        SERVER_STREAM_ON_ASYNCIO
        + """
import sys

loaded = sorted(
    name
    for name in sys.modules
    if name == "connectrpc._server_anyio" or name.split(".")[0] in ("anyio", "trio")
)
assert not loaded, loaded
"""
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).parents[1],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

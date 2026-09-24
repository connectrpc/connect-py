"""The ASGI application served under trio.

Hypercorn's trio worker serves the Haberdasher application on a background
thread, and the sync client calls it over pyqwest's SyncHTTPTransport.
"""

from __future__ import annotations

import threading
from concurrent.futures import Future
from functools import partial
from typing import TYPE_CHECKING

import pytest
import trio
import trio.lowlevel
from hypercorn.config import Config
from hypercorn.trio import serve
from pyqwest import HTTPVersion, SyncClient, SyncHTTPTransport

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from connectrpc.protocol import ProtocolType

from .connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClientSync,
)
from .connectrpc.example.haberdasher_pb import Hat, Size

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from connectrpc.request import RequestContext


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


@pytest.fixture(scope="module")
def transports() -> Iterator[dict[str, SyncHTTPTransport]]:
    with (
        SyncHTTPTransport(http_version=HTTPVersion.HTTP1) as http1,
        SyncHTTPTransport(http_version=HTTPVersion.HTTP2) as http2,
    ):
        yield {"h1": http1, "h2": http2}


# Depends on `transports` so that the server stops before the connections close.
# Closing an HTTP/2 connection while hypercorn's trio worker is sending on it can
# stop the worker.
@pytest.fixture(scope="module")
def url(transports: dict[str, SyncHTTPTransport]) -> Iterator[str]:  # noqa: ARG001
    app = HaberdasherASGIApplication(TrioHaberdasher())
    stop = threading.Event()
    started: Future[str] = Future()

    async def stopped() -> None:
        await trio.to_thread.run_sync(stop.wait, abandon_on_cancel=True)

    async def main() -> None:
        config = Config()
        config.bind = ["127.0.0.1:0"]
        async with trio.open_nursery() as nursery:
            binds = await nursery.start(
                partial(
                    serve,
                    app,  # ty: ignore[invalid-argument-type] - hypercorn vs asgiref scope TypedDicts
                    config,
                    shutdown_trigger=stopped,
                )
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
        yield started.result(timeout=10)
    finally:
        stop.set()
        thread.join(10)
    assert not thread.is_alive(), "server did not stop"


@pytest.fixture(
    params=[
        (ProtocolType.CONNECT, "h1"),
        (ProtocolType.CONNECT, "h2"),
        (ProtocolType.GRPC, "h2"),
        (ProtocolType.GRPC_WEB, "h1"),
        (ProtocolType.GRPC_WEB, "h2"),
    ],
    ids=["connect-h1", "connect-h2", "grpc-h2", "grpcweb-h1", "grpcweb-h2"],
)
def client(
    url: str, transports: dict[str, SyncHTTPTransport], request: pytest.FixtureRequest
) -> Iterator[HaberdasherClientSync]:
    protocol, http_version = request.param
    with HaberdasherClientSync(
        url, protocol=protocol, http_client=SyncClient(transports[http_version])
    ) as client:
        yield client


def sizes() -> Iterator[Size]:
    for inches in (1, 2, 3):
        yield Size(inches=inches)


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

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, Any, Literal

from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc._compression import IdentityCompression
from connectrpc.compression.brotli import BrotliCompression
from connectrpc.compression.gzip import GzipCompression
from connectrpc.compression.zstd import ZstdCompression

from .connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClient,
    HaberdasherClientSync,
    HaberdasherSync,
    HaberdasherWSGIApplication,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from pyqwest import SyncTransport, Transport

    from connectrpc.compression import Compression
    from connectrpc.request import RequestContext

    from .connectrpc.example.haberdasher_pb import Hat, Size


def resolve_compression(encoding: str) -> Compression:
    match encoding:
        case "gzip":
            return GzipCompression()
        case "br":
            return BrotliCompression()
        case "zstd":
            return ZstdCompression()
        case "identity":
            return IdentityCompression()
        case _:
            msg = f"unknown encoding '{encoding}'"
            raise ValueError(msg)


def haberdasher_client(transport: Transport, **kwargs) -> HaberdasherClient:
    return HaberdasherClient(
        "http://localhost", http_client=Client(transport), **kwargs
    )


def haberdasher_client_sync(
    transport: SyncTransport, **kwargs
) -> HaberdasherClientSync:
    return HaberdasherClientSync(
        "http://localhost", http_client=SyncClient(transport), **kwargs
    )


async def call(
    client: HaberdasherClient | HaberdasherClientSync,
    method: str,
    request: Size | list[Size],
    **kwargs,
) -> Hat | list[Hat]:
    """Calls method on client, passing and returning streams as lists.

    A sync client runs in a worker thread, as it would in a sync program.
    """
    if isinstance(client, HaberdasherClientSync):

        def run() -> Hat | list[Hat]:
            req = iter(request) if isinstance(request, list) else request
            result = getattr(client, method)(req, **kwargs)
            return list(result) if isinstance(result, Iterator) else result

        return await asyncio.to_thread(run)

    async def stream(requests: list[Size]) -> AsyncIterator[Size]:
        for r in requests:
            yield r

    result = getattr(client, method)(
        stream(request) if isinstance(request, list) else request, **kwargs
    )
    if isinstance(result, AsyncIterator):
        return [r async for r in result]
    return await result


def unary_client(
    mode: Literal["async", "sync"],
    make_hat: Callable[[Size, RequestContext], Hat],
    app_options: Mapping[str, Any] | None = None,
    **kwargs,
) -> HaberdasherClient | HaberdasherClientSync:
    """Returns a client of an ASGI or WSGI server whose MakeHat calls make_hat.

    app_options are passed to the application and kwargs to the client.
    """
    if mode == "async":

        class UnaryHaberdasher(Haberdasher):
            async def make_hat(self, request, ctx):
                return make_hat(request, ctx)

        app = HaberdasherASGIApplication(UnaryHaberdasher(), **(app_options or {}))
        return haberdasher_client(ASGITransport(app), **kwargs)

    class UnaryHaberdasherSync(HaberdasherSync):
        def make_hat(self, request, ctx):
            return make_hat(request, ctx)

    app = HaberdasherWSGIApplication(UnaryHaberdasherSync(), **(app_options or {}))
    return haberdasher_client_sync(WSGITransport(app), **kwargs)

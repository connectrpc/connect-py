from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING

from pyqwest import Client, SyncClient

from connectrpc._compression import IdentityCompression
from connectrpc.compression.brotli import BrotliCompression
from connectrpc.compression.gzip import GzipCompression
from connectrpc.compression.zstd import ZstdCompression

from .connectrpc.example.haberdasher_connect import (
    HaberdasherClient,
    HaberdasherClientSync,
)

if TYPE_CHECKING:
    from pyqwest import SyncTransport, Transport

    from connectrpc.compression import Compression

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
) -> Hat | list[Hat]:
    """Calls method on client, passing and returning streams as lists.

    A sync client runs in a worker thread, as it would in a sync program.
    """
    if isinstance(client, HaberdasherClientSync):

        def run() -> Hat | list[Hat]:
            req = iter(request) if isinstance(request, list) else request
            result = getattr(client, method)(req)
            return list(result) if isinstance(result, Iterator) else result

        return await asyncio.to_thread(run)

    async def stream(requests: list[Size]) -> AsyncIterator[Size]:
        for r in requests:
            yield r

    result = getattr(client, method)(
        stream(request) if isinstance(request, list) else request
    )
    if isinstance(result, AsyncIterator):
        return [r async for r in result]
    return await result

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING

import anyio

from connectrpc._compression import IdentityCompression
from connectrpc.compression.brotli import BrotliCompression
from connectrpc.compression.gzip import GzipCompression
from connectrpc.compression.zstd import ZstdCompression

from .connectrpc.example.haberdasher_connect import HaberdasherClientSync

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import MethodType

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


async def call(method: MethodType, request: Size | list[Size]) -> Hat | list[Hat]:
    """Calls a client method, passing and returning streams as lists."""
    if isinstance(method.__self__, HaberdasherClientSync):

        def run() -> Hat | list[Hat]:
            result = method(iter(request) if isinstance(request, list) else request)
            return list(result) if isinstance(result, Iterator) else result

        return await anyio.to_thread.run_sync(run)

    async def stream(requests: list[Size]) -> AsyncIterator[Size]:
        for r in requests:
            yield r

    result = method(stream(request) if isinstance(request, list) else request)
    if isinstance(result, AsyncIterator):
        return [r async for r in result]
    return await result


async def cancel_when(
    event: anyio.Event, func: Callable[[], Awaitable[object]]
) -> BaseException | None:
    """Runs func until the event is set, then cancels it as a server would.

    Servers on asyncio cancel the request's task, which delivers the
    cancellation once, while trio cancels a scope, which delivers it at every
    checkpoint until the scope exits. Returns what func raised, which is the
    backend's cancellation exception when func propagates the cancellation.
    """
    raised: BaseException | None = None

    async def run() -> None:
        nonlocal raised
        try:
            await func()
        except BaseException as e:
            raised = e
            raise

    with anyio.fail_after(5):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            async with anyio.create_task_group() as tg:
                tg.start_soon(run)
                await event.wait()
                tg.cancel_scope.cancel()
        else:
            task = asyncio.ensure_future(run())
            try:
                await event.wait()
            finally:
                task.cancel()
                await asyncio.wait((task,))
    return raised

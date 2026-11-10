"""The event loop operations that differ between asyncio and trio."""

from __future__ import annotations

import contextlib
from asyncio import (
    CancelledError,
    Event,
    Task,
    create_task,
    get_running_loop,
    sleep,
    timeout_at as asyncio_timeout_at,
    wait,
)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from asyncio import Timeout
    from collections.abc import AsyncGenerator
    from contextlib import AbstractContextManager

    from ._server_async import _Event, _RequestReceiver
    from ._server_trio import TrioLoop, TrioTimeout


class AsyncioLoop:
    """Event loop operations under asyncio, for the server and client."""

    async def checkpoint(self) -> None:
        """Give the loop a chance to deliver a cancellation."""
        await sleep(0)

    def time(self) -> float:
        """Return the loop's clock, which deadlines are measured on."""
        return get_running_loop().time()

    def event(self) -> _Event:
        """Return an event of the loop."""
        return Event()

    def timeout_at(self, deadline: float | None) -> Timeout:
        """Return a context whose body raises TimeoutError once the deadline passes.

        The context's expired() tells whether that happened, since the body can
        raise a TimeoutError of its own.
        """
        return asyncio_timeout_at(deadline)

    def is_cancellation(self, error: BaseException) -> bool:
        """Whether the error is a loop cancellation."""
        return isinstance(error, CancelledError)

    def async_cleanup(self) -> AbstractContextManager[object]:
        """Return a context in which cleanup can await after a cancellation."""
        # asyncio only delivers cancellation once so async logic works as normal during cleanup
        return contextlib.nullcontext()

    @contextlib.asynccontextmanager
    async def watch_for_disconnect(
        self,
        receive: _RequestReceiver,
        scope: Timeout | TrioTimeout,
        *,
        cancel: bool = False,  # noqa: ARG002 # asyncio always cancels
    ) -> AsyncGenerator[None]:
        """Cut the request's deadline short when the client disconnects.

        A handler blocked at an await is cancelled there, as by the deadline,
        so the body has nothing to poll and the watch yields None. asyncio
        delivers the cancellation once, so the handler's cleanup still runs
        as normal.
        """
        task: Task[None] | None = None

        async def watch() -> None:
            await receive.body_done.wait()
            # An error from receive() would otherwise replace the response's
            # outcome when the watch ends.
            with contextlib.suppress(Exception):
                while not receive.disconnected:
                    await receive()
            # The deadline may have fired in the meantime, and a scope that is
            # expiring can't be rescheduled; its cancellation is already on
            # its way.
            if receive.disconnected and not scope.expired():
                scope.reschedule(get_running_loop().time())

        def start() -> None:
            nonlocal task
            task = create_task(watch())

        # Only a body that suspends can be cancelled, so start the watch from
        # the loop, which runs once the body suspends. A body that completes
        # without suspending never pays for the task.
        handle = get_running_loop().call_soon(start)
        try:
            yield
        finally:
            handle.cancel()
            if task is not None:
                task.cancel()
                # Unlike awaiting the task, this lets a cancellation of the
                # request itself, such as by its deadline, propagate.
                await wait((task,))


_ASYNCIO_LOOP = AsyncioLoop()


def current_loop() -> AsyncioLoop | TrioLoop:
    try:
        get_running_loop()
    except RuntimeError:
        # Not asyncio, so trio: the only other event loop supported.
        from ._server_trio import TrioLoop  # noqa: PLC0415

        return TrioLoop()
    return _ASYNCIO_LOOP

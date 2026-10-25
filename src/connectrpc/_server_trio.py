"""The stream handler's event loop operations under trio.

Imported the first time a request runs under trio, so that applications on
asyncio never import trio.
"""

from __future__ import annotations

import contextlib
import math
from typing import TYPE_CHECKING, Self

import trio
import trio.lowlevel

if TYPE_CHECKING:
    from asyncio import Timeout
    from collections.abc import AsyncGenerator, Callable
    from types import TracebackType

    from ._server_async import _Event, _RequestReceiver

# Seconds allowed for each cleanup step that runs after a cancellation: closing
# the handler's generator, then the on_end hooks and the end of the response. A
# cancelled request therefore delays the app server by at most twice this. The
# value is a guess at how long cleanup that does not wait on the client can
# take, and is unavoidable to support cleanup on cancellation with trio.
_CLEANUP_TIMEOUT = 1.0


class TrioLoop:
    """See _server_async.AsyncioLoop for what each operation is for."""

    async def checkpoint(self) -> None:
        await trio.lowlevel.checkpoint()

    def time(self) -> float:
        return trio.current_time()

    def event(self) -> _Event:
        return trio.Event()

    def timeout_at(self, deadline: float | None) -> TrioTimeout:
        return TrioTimeout(deadline)

    def is_cancellation(self, error: BaseException) -> bool:
        if isinstance(error, trio.Cancelled):
            return True
        # A cancelled nursery raises its cancellation in an exception group,
        # beside any error its tasks raised while they were being cancelled.
        return isinstance(error, BaseExceptionGroup) and any(
            self.is_cancellation(e) for e in error.exceptions
        )

    def async_cleanup(self) -> trio.CancelScope:
        # Once the app server has cancelled the request, every checkpoint raises
        # Cancelled again, so async cleanup can only run in a cancel scope shielded
        # from that. Since the cleanup logic itself can't be canceled, we
        # apply a deadline to prevent hangs.
        scope = trio.move_on_after(_CLEANUP_TIMEOUT)
        scope.shield = True
        return scope

    @contextlib.asynccontextmanager
    async def watch_for_disconnect(
        self,
        receive: _RequestReceiver,
        scope: Timeout | TrioTimeout,
        *,
        cancel: bool = False,
    ) -> AsyncGenerator[Callable[[], bool]]:
        # Unless asked to cancel, the watch yields a flag for the body to poll
        # instead of cutting the deadline short: trio raises Cancelled at every
        # checkpoint of a cancelled scope, which would cut short a handler's
        # unshielded async cleanup, while a poll closes the handler's generator
        # normally. A unary handler has nothing to poll the flag, and is cut
        # short as the deadline would.
        async def watch() -> None:
            await receive.body_done.wait()
            # An error raised in a nursery cancels the rest of it, so an error
            # from receive() would end the response.
            with contextlib.suppress(Exception):
                while not receive.disconnected:
                    await receive()
            if cancel and receive.disconnected:
                scope.reschedule(trio.current_time())

        def disconnected() -> bool:
            return receive.disconnected

        error: BaseException | None = None
        try:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(watch)
                try:
                    yield disconnected
                except BaseException as e:  # noqa: BLE001
                    # Raised after the nursery exits; raised inside, the nursery
                    # would wrap it in an ExceptionGroup.
                    error = e
                finally:
                    nursery.cancel_scope.cancel()
        except BaseException:
            # A nursery whose enclosing scope was cancelled raises a group of
            # its own cancellations, which would drop an error that the
            # handler raised beside the cancellation.
            if error is not None and self.is_cancellation(error):
                raise error from None
            raise
        if error is not None:
            raise error


class TrioTimeout:
    """A cancel scope with a deadline that raises TimeoutError, like asyncio.Timeout.

    The handler then treats a deadline the same way under both loops.
    """

    def __init__(self, deadline: float | None) -> None:
        self._scope = trio.CancelScope(
            deadline=math.inf if deadline is None else deadline
        )

    def expired(self) -> bool:
        """Whether the deadline passed while the body ran."""
        return self._scope.cancelled_caught

    def reschedule(self, when: float) -> None:
        """Move the deadline, like asyncio.Timeout.reschedule."""
        self._scope.deadline = when

    def when(self) -> float | None:
        """Return the deadline, like asyncio.Timeout.when."""
        deadline = self._scope.deadline
        return None if deadline == math.inf else deadline

    async def __aenter__(self) -> Self:
        self._scope.__enter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        swallowed = self._scope.__exit__(exc_type, exc_val, exc_tb)
        if exc_type is not None and swallowed:
            # The scope swallowed its own cancellation, so the body is done.
            raise TimeoutError from exc_val
        return False

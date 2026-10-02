"""The stream handler's event loop operations under trio.

Imported the first time a request runs under trio, so that applications on
asyncio never import trio.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import trio
import trio.lowlevel

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from asgiref.typing import ASGIReceiveCallable

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

    def is_cancellation(self, error: BaseException) -> bool:
        if isinstance(error, trio.Cancelled):
            return True
        # A cancelled nursery raises its cancellation in an exception group,
        # beside any error its tasks raised while they were being cancelled.
        # TODO: Use ExceptionGroup when updating Python floor to 3.11.
        exceptions = getattr(error, "exceptions", None)
        return isinstance(exceptions, tuple) and any(
            self.is_cancellation(e) for e in exceptions
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
        self, receive: ASGIReceiveCallable
    ) -> AsyncGenerator[Callable[[], bool]]:
        disconnected = trio.Event()

        async def watch() -> None:
            # An error raised in a nursery cancels the rest of it, so an error
            # from receive() would end the response.
            with contextlib.suppress(Exception):
                while True:
                    msg = await receive()
                    if msg["type"] == "http.disconnect":
                        disconnected.set()
                        return

        error: BaseException | None = None
        try:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(watch)
                try:
                    yield disconnected.is_set
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

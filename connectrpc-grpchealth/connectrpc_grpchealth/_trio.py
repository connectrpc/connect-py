"""Watch's wakeup under trio.

Imported the first time a Watch runs under trio, so that applications on asyncio
never import trio.
"""

from __future__ import annotations

import contextlib

import trio
import trio.lowlevel


class TrioWaiter:
    """Wakes a Watch on a trio run when its service's status changes."""

    def __init__(self) -> None:
        self._token = trio.lowlevel.current_trio_token()
        self._event = trio.Event()

    def notify(self) -> None:
        # Called from any thread. A run that has finished has no watch to wake.
        with contextlib.suppress(trio.RunFinishedError):
            self._token.run_sync_soon(self._set)

    def _set(self) -> None:
        self._event.set()

    def reset(self) -> None:
        # A trio.Event can't be cleared. _set runs on the run's thread after the
        # status changes, so it sets whichever event is current then, and a
        # change is never missed, only sometimes reported by a spurious wakeup.
        self._event = trio.Event()

    async def wait(self) -> None:
        await self._event.wait()

from __future__ import annotations

import asyncio
import contextlib
import threading
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from connectrpc.code import Code
from connectrpc.errors import ConnectError

from ._gen.grpc.health.v1.health_connect import Health, HealthSync
from ._gen.grpc.health.v1.health_pb import (
    HealthCheckRequest,
    HealthCheckResponse,
    HealthListRequest,
    HealthListResponse,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator, Mapping

    from connectrpc.request import RequestContext

    from ._trio import TrioWaiter as _TrioWaiter

ServingStatus = HealthCheckResponse.ServingStatus
"""The serving status of a service, as reported by the health service."""


class Checker(Protocol):
    """Reports the health of each service to [HealthService][connectrpc_grpchealth.HealthService].

    The methods may be called concurrently. To support Watch, the checker also
    implements [Watcher][connectrpc_grpchealth.Watcher].
    """

    async def check(self, service: str) -> ServingStatus | None:
        """Return the status of a service, or None if the service is unknown.

        Args:
            service: The fully-qualified protobuf service name, or the empty
                string for the status of the server as a whole.

        """
        ...

    async def list(self) -> Mapping[str, ServingStatus]:
        """Return the status of every known service, keyed by service name."""
        ...


class CheckerSync(Protocol):
    """Reports the health of each service to [HealthServiceSync][connectrpc_grpchealth.HealthServiceSync].

    The methods may be called concurrently from any thread. To support Watch,
    the checker also implements [Watcher][connectrpc_grpchealth.Watcher].
    """

    def check_sync(self, service: str) -> ServingStatus | None:
        """Return the status of a service, or None if the service is unknown.

        Args:
            service: The fully-qualified protobuf service name, or the empty
                string for the status of the server as a whole.

        """
        ...

    def list_sync(self) -> Mapping[str, ServingStatus]:
        """Return the status of every known service, keyed by service name."""
        ...


@runtime_checkable
class Watcher(Protocol):
    """Notifies the health service when the status of a service may change.

    A [Checker][connectrpc_grpchealth.Checker] or
    [CheckerSync][connectrpc_grpchealth.CheckerSync] that also implements Watcher
    supports the Watch RPC. Otherwise, Watch fails with
    [Code.UNIMPLEMENTED][connectrpc.code.Code.UNIMPLEMENTED], and gRPC clients
    that check health with Watch treat the server as always serving.
    """

    def watch(self, service: str, on_change: Callable[[], None]) -> Callable[[], None]:
        """Call on_change from any thread whenever the service's status may change.

        It may be called concurrently from any thread, and under ASGI it is
        called on the event loop, so it must not block.

        Args:
            service: The fully-qualified protobuf service name, or the empty
                string for the status of the server as a whole.
            on_change: Called after each change. It returns quickly and may be
                called after the watch stops.

        Returns:
            A function that stops the watch.

        """
        ...


class StaticChecker(Checker, CheckerSync, Watcher):
    """Health statuses set explicitly for each service.

    The checker reports [ServingStatus.SERVING][connectrpc_grpchealth.ServingStatus]
    for the server as a whole (the empty service name) unless a different status is
    set for it. Services that have no status set are unknown.

    It is safe to call [set_status][connectrpc_grpchealth.StaticChecker.set_status]
    from any thread while requests are being served, including from synchronous code
    while an asynchronous application serves the checker.
    """

    def __init__(self, *services: str) -> None:
        """Create a checker where each of the given services is serving.

        Args:
            *services: Fully-qualified protobuf service names, such as
                `"acme.user.v1.UserService"`.

        """
        self._lock = threading.Lock()
        self._statuses: dict[str, ServingStatus] = dict.fromkeys(
            services, ServingStatus.SERVING
        )
        self._watchers: dict[str, set[Callable[[], None]]] = {}

    def set_status(self, service: str, status: ServingStatus) -> None:
        """Set the status of a service, registering it if it is unknown.

        Active watches of the service receive the new status.

        Args:
            service: The fully-qualified protobuf service name, or the empty
                string for the status of the server as a whole.
            status: The new status of the service.

        """
        with self._lock:
            self._statuses[service] = status
            callbacks = list(self._watchers.get(service, ()))
        for callback in callbacks:
            callback()

    async def check(self, service: str) -> ServingStatus | None:
        return self.check_sync(service)

    async def list(self) -> Mapping[str, ServingStatus]:
        return self.list_sync()

    def check_sync(self, service: str) -> ServingStatus | None:
        with self._lock:
            status = self._statuses.get(service)
        if status is None and not service:
            return ServingStatus.SERVING
        return status

    def list_sync(self) -> Mapping[str, ServingStatus]:
        with self._lock:
            return {"": ServingStatus.SERVING, **self._statuses}

    def watch(self, service: str, on_change: Callable[[], None]) -> Callable[[], None]:
        with self._lock:
            self._watchers.setdefault(service, set()).add(on_change)

        def stop() -> None:
            with self._lock:
                callbacks = self._watchers[service]
                callbacks.discard(on_change)
                if not callbacks:
                    del self._watchers[service]

        return stop


class HealthService(Health):
    """Asynchronous service implementation for gRPC health checking."""

    def __init__(self, checker: Checker) -> None:
        """Create a service for gRPC health checking.

        Serve it with [HealthASGIApplication][connectrpc_grpchealth.HealthASGIApplication].

        Watch is supported if the checker implements
        [Watcher][connectrpc_grpchealth.Watcher]. Under asyncio, a call to Watch
        ends as soon as its client disconnects. Under trio, the server notices a
        disconnect only when the call next sends a status, so the call ends at
        its deadline or at the next status change of the watched service,
        whichever comes first.

        Args:
            checker: The checker that reports the status of each service.

        """
        self._checker = checker

    async def check(
        self,
        request: HealthCheckRequest,
        _ctx: RequestContext[HealthCheckRequest, HealthCheckResponse],
    ) -> HealthCheckResponse:
        return _check_response(await self._checker.check(request.service), request)

    async def list(
        self,
        _request: HealthListRequest,
        _ctx: RequestContext[HealthListRequest, HealthListResponse],
    ) -> HealthListResponse:
        return _list_response(await self._checker.list())

    async def watch(
        self,
        request: HealthCheckRequest,
        _ctx: RequestContext[HealthCheckRequest, HealthCheckResponse],
    ) -> AsyncIterator[HealthCheckResponse]:
        watcher = _watcher(self._checker)
        waiter = _current_waiter()
        stop = watcher.watch(request.service, waiter.notify)
        try:
            last: ServingStatus | None = None
            while True:
                waiter.reset()
                status = _watch_status(await self._checker.check(request.service))
                if status != last:
                    last = status
                    yield HealthCheckResponse(status=status)
                # The server ends the call at its deadline.
                await waiter.wait()
        finally:
            stop()


class HealthServiceSync(HealthSync):
    """Synchronous service implementation for gRPC health checking."""

    def __init__(self, checker: CheckerSync) -> None:
        """Create a service for gRPC health checking.

        Serve it with [HealthWSGIApplication][connectrpc_grpchealth.HealthWSGIApplication].

        Watch is supported if the checker implements
        [Watcher][connectrpc_grpchealth.Watcher]. Each call to Watch occupies a
        server thread while it is open. The server notices a disconnect only
        when the call next sends a status, so a call whose client disconnects
        ends at its deadline or at the next status change of the watched
        service, whichever comes first.

        Args:
            checker: The checker that reports the status of each service.

        """
        self._checker = checker

    def check(
        self,
        request: HealthCheckRequest,
        _ctx: RequestContext[HealthCheckRequest, HealthCheckResponse],
    ) -> HealthCheckResponse:
        return _check_response(self._checker.check_sync(request.service), request)

    def list(
        self,
        _request: HealthListRequest,
        _ctx: RequestContext[HealthListRequest, HealthListResponse],
    ) -> HealthListResponse:
        return _list_response(self._checker.list_sync())

    def watch(
        self,
        request: HealthCheckRequest,
        ctx: RequestContext[HealthCheckRequest, HealthCheckResponse],
    ) -> Iterator[HealthCheckResponse]:
        watcher = _watcher(self._checker)
        changed = threading.Event()
        stop = watcher.watch(request.service, changed.set)
        try:
            last: ServingStatus | None = None
            while True:
                changed.clear()
                status = _watch_status(self._checker.check_sync(request.service))
                if status != last:
                    last = status
                    yield HealthCheckResponse(status=status)
                if not changed.wait(_remaining_seconds(ctx)):
                    raise _deadline_exceeded()
        finally:
            stop()


class _AsyncioWaiter:
    """Wakes a Watch on an asyncio loop when its service's status changes."""

    def __init__(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._event = asyncio.Event()

    def notify(self) -> None:
        # Called from any thread. A loop that has closed has no watch to wake.
        with contextlib.suppress(RuntimeError):
            self._loop.call_soon_threadsafe(self._event.set)

    def reset(self) -> None:
        self._event.clear()

    async def wait(self) -> None:
        await self._event.wait()


def _current_waiter() -> _AsyncioWaiter | _TrioWaiter:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # Not asyncio, so trio: the only other event loop that ASGI servers run.
        from ._trio import TrioWaiter  # noqa: PLC0415

        return TrioWaiter()
    return _AsyncioWaiter()


def _watcher(checker: Checker | CheckerSync) -> Watcher:
    if not isinstance(checker, Watcher):
        raise ConnectError(Code.UNIMPLEMENTED, "watching health state is not supported")
    return checker


def _check_response(
    status: ServingStatus | None, request: HealthCheckRequest
) -> HealthCheckResponse:
    if status is None:
        raise ConnectError(Code.NOT_FOUND, f"unknown service {request.service}")
    return HealthCheckResponse(status=status)


def _list_response(statuses: Mapping[str, ServingStatus]) -> HealthListResponse:
    return HealthListResponse(
        statuses={
            service: HealthCheckResponse(status=status)
            for service, status in statuses.items()
        }
    )


# Unlike Check, Watch reports an unknown service in the stream and keeps it open
# so the client sees the service once it's registered.
def _watch_status(status: ServingStatus | None) -> ServingStatus:
    return ServingStatus.SERVICE_UNKNOWN if status is None else status


def _remaining_seconds(
    ctx: RequestContext[HealthCheckRequest, HealthCheckResponse],
) -> float | None:
    timeout_ms = ctx.timeout_ms
    if timeout_ms is None:
        return None
    return max(timeout_ms, 0) / 1000


def _deadline_exceeded() -> ConnectError:
    return ConnectError(Code.DEADLINE_EXCEEDED, "deadline exceeded")

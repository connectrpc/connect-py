from __future__ import annotations

import threading
from contextlib import aclosing
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import anyio
import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc_grpchealth import (
    Checker,
    CheckerSync,
    HealthASGIApplication,
    HealthService,
    HealthServiceSync,
    HealthWSGIApplication,
    ServingStatus,
    StaticChecker,
    _service,
)
from connectrpc_grpchealth._gen.grpc.health.v1.health_connect import (
    HealthClient,
    HealthClientSync,
)
from connectrpc_grpchealth._gen.grpc.health.v1.health_pb import (
    HealthCheckRequest,
    HealthCheckResponse,
    HealthListRequest,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Mapping

ELIZA = "connectrpc.eliza.v1.ElizaService"
HABERDASHER = "connectrpc.example.Haberdasher"

AnyHealthClient = HealthClient | HealthClientSync


@pytest.fixture
def checker() -> StaticChecker:
    return StaticChecker(ELIZA, HABERDASHER)


@pytest.fixture(params=[pytest.param("sync", marks=pytest.mark.asyncio_only), "async"])
async def client(
    request: pytest.FixtureRequest, checker: StaticChecker
) -> AsyncIterator[AnyHealthClient]:
    match request.param:
        case "sync":
            app = HealthWSGIApplication(HealthServiceSync(checker))
            with HealthClientSync(
                "http://localhost", http_client=SyncClient(WSGITransport(app=app))
            ) as client:
                yield client
        case "async":
            app = HealthASGIApplication(HealthService(checker))
            async with HealthClient(
                "http://localhost", http_client=Client(ASGITransport(app))
            ) as client:
                yield client


async def _check(client: AnyHealthClient, service: str) -> ServingStatus:
    request = HealthCheckRequest(service=service)
    if isinstance(client, HealthClientSync):
        return client.check(request).status
    return (await client.check(request)).status


# A synchronous Watch occupies a server thread until its deadline, so every watch
# has one to let the test transport shut down.
async def _watch(
    client: AnyHealthClient, service: str, timeout_ms: int = 1000
) -> AsyncIterator[ServingStatus]:
    request = HealthCheckRequest(service=service)
    if isinstance(client, HealthClientSync):
        for response in client.watch(request, timeout_ms=timeout_ms):
            yield response.status
        return
    # Generated stream methods are typed as AsyncIterator but return async
    # generators.
    responses = cast(
        "AsyncGenerator[HealthCheckResponse]",
        client.watch(request, timeout_ms=timeout_ms),
    )
    async with aclosing(responses):
        async for response in responses:
            yield response.status


@pytest.mark.anyio
async def test_check_serving(client: AnyHealthClient) -> None:
    assert await _check(client, ELIZA) == ServingStatus.SERVING
    assert await _check(client, "") == ServingStatus.SERVING


@pytest.mark.anyio
async def test_check_not_serving(
    client: AnyHealthClient, checker: StaticChecker
) -> None:
    checker.set_status(ELIZA, ServingStatus.NOT_SERVING)
    checker.set_status("", ServingStatus.NOT_SERVING)

    assert await _check(client, ELIZA) == ServingStatus.NOT_SERVING
    assert await _check(client, "") == ServingStatus.NOT_SERVING
    assert await _check(client, HABERDASHER) == ServingStatus.SERVING


@pytest.mark.anyio
async def test_check_unknown_service(client: AnyHealthClient) -> None:
    with pytest.raises(ConnectError, match=r"unknown service missing\.Service") as e:
        await _check(client, "missing.Service")
    assert e.value.code == Code.NOT_FOUND


@pytest.mark.anyio
async def test_list(client: AnyHealthClient, checker: StaticChecker) -> None:
    checker.set_status(HABERDASHER, ServingStatus.NOT_SERVING)

    if isinstance(client, HealthClientSync):
        response = client.list(HealthListRequest())
    else:
        response = await client.list(HealthListRequest())

    assert {
        service: status.status for service, status in response.statuses.items()
    } == {
        "": ServingStatus.SERVING,
        ELIZA: ServingStatus.SERVING,
        HABERDASHER: ServingStatus.NOT_SERVING,
    }


class _OnDemandChecker(Checker, CheckerSync):
    """Computes each status when it's asked for, and can't be watched."""

    async def check(self, service: str) -> ServingStatus | None:
        return self.check_sync(service)

    async def list(self) -> Mapping[str, ServingStatus]:
        return self.list_sync()

    def check_sync(self, service: str) -> ServingStatus | None:
        return ServingStatus.NOT_SERVING if service == ELIZA else None

    def list_sync(self) -> Mapping[str, ServingStatus]:
        return {ELIZA: ServingStatus.NOT_SERVING}


@pytest.mark.anyio
@pytest.mark.parametrize("checker", [_OnDemandChecker()])
async def test_custom_checker(client: AnyHealthClient) -> None:
    assert await _check(client, ELIZA) == ServingStatus.NOT_SERVING
    with pytest.raises(ConnectError) as e:
        await _check(client, HABERDASHER)
    assert e.value.code == Code.NOT_FOUND
    if isinstance(client, HealthClientSync):
        response = client.list(HealthListRequest())
    else:
        response = await client.list(HealthListRequest())
    assert dict(response.statuses) == {
        ELIZA: HealthCheckResponse(status=ServingStatus.NOT_SERVING)
    }
    with pytest.raises(
        ConnectError, match="watching health state is not supported"
    ) as e:
        await anext(_watch(client, ELIZA))
    assert e.value.code == Code.UNIMPLEMENTED


@pytest.mark.anyio
async def test_watch_update(client: AnyHealthClient, checker: StaticChecker) -> None:
    statuses = _watch(client, ELIZA)

    assert await anext(statuses) == ServingStatus.SERVING
    # Setting an unchanged status sends nothing.
    checker.set_status(ELIZA, ServingStatus.SERVING)
    checker.set_status(ELIZA, ServingStatus.NOT_SERVING)
    assert await anext(statuses) == ServingStatus.NOT_SERVING
    checker.set_status(ELIZA, ServingStatus.SERVING)
    assert await anext(statuses) == ServingStatus.SERVING
    await statuses.aclose()


@pytest.mark.anyio
async def test_watch_unknown_service(
    client: AnyHealthClient, checker: StaticChecker
) -> None:
    statuses = _watch(client, "acme.v1.NewService")

    assert await anext(statuses) == ServingStatus.SERVICE_UNKNOWN
    checker.set_status("acme.v1.NewService", ServingStatus.SERVING)
    assert await anext(statuses) == ServingStatus.SERVING
    await statuses.aclose()


class _EarlyEvent(threading.Event):
    """An event whose timed waits end early, as they can on Windows."""

    def wait(self, timeout: float | None = None) -> bool:
        if timeout is None:
            return super().wait()
        return super().wait(max(timeout - 0.05, 0))


@pytest.mark.anyio
@pytest.mark.parametrize("early_wait", [False, True])
async def test_watch_deadline(
    monkeypatch: pytest.MonkeyPatch, client: AnyHealthClient, early_wait: bool
) -> None:
    if early_wait:
        monkeypatch.setattr(_service, "threading", SimpleNamespace(Event=_EarlyEvent))
    statuses = _watch(client, ELIZA, timeout_ms=200)

    assert await anext(statuses) == ServingStatus.SERVING
    with pytest.raises(ConnectError) as e:
        await anext(statuses)
    assert e.value.code == Code.DEADLINE_EXCEEDED


@pytest.mark.anyio
async def test_watch_stops_on_close(
    client: AnyHealthClient, checker: StaticChecker
) -> None:
    statuses = _watch(client, ELIZA)
    assert await anext(statuses) == ServingStatus.SERVING
    assert checker._watchers
    await statuses.aclose()
    # Well before the call's deadline, which would also stop it.
    for _ in range(50):
        if not checker._watchers:
            break
        await anyio.sleep(0.01)
    assert not checker._watchers

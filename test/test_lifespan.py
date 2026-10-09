from __future__ import annotations

from collections import Counter

import pytest
from pyqwest import Client
from pyqwest.testing import ASGITransport

from connectrpc.code import Code
from connectrpc.errors import ConnectError

from .connectrpc.example.empty_connect import NoMethods, NoMethodsASGIApplication
from .connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClient,
)
from .connectrpc.example.haberdasher_pb import Hat, Size


class CountingHaberdasher(Haberdasher):
    def __init__(self, counter: Counter) -> None:
        self._counter = counter

    async def make_hat(self, request, _ctx):
        self._counter["requests"] += 1
        return Hat(size=request.inches, color="blue")


@pytest.mark.asyncio
async def test_lifespan() -> None:
    final_count = None

    async def counting_haberdasher():
        counter = Counter()
        try:
            haberdasher = CountingHaberdasher(counter)
            yield haberdasher
        finally:
            nonlocal final_count
            final_count = counter["requests"]

    app = HaberdasherASGIApplication(counting_haberdasher())
    async with (
        ASGITransport(app) as transport,
        HaberdasherClient("http://localhost", http_client=Client(transport)) as client,
    ):
        for _ in range(5):
            hat = await client.make_hat(Size(inches=10))
            assert hat.size == 10
            assert hat.color == "blue"

    assert final_count == 5


@pytest.mark.asyncio
async def test_lifespan_empty_services() -> None:
    class NoMethodsService(NoMethods):
        pass

    async def no_methods():
        yield NoMethodsService()

    app = NoMethodsASGIApplication(no_methods())
    async with (
        ASGITransport(app) as transport,
        # No method to invoke, so we just send an unrelated request and make sure
        # the error code matches expected.
        HaberdasherClient("http://localhost", http_client=Client(transport)) as client,
    ):
        with pytest.raises(ConnectError) as exc_info:
            await client.make_hat(Size(inches=10))
    assert exc_info.value.code == Code.UNIMPLEMENTED


@pytest.mark.asyncio
async def test_lifespan_startup_error() -> None:
    final_count = None

    async def counting_haberdasher():
        counter = Counter()
        if True:
            msg = "Haberdasher failed to start"
            raise RuntimeError(msg)
        # Unreachable code below but keep it to make this an async generator
        try:
            haberdasher = CountingHaberdasher(counter)
            yield haberdasher
        finally:
            nonlocal final_count
            final_count = counter["requests"]

    app = HaberdasherASGIApplication(counting_haberdasher())
    with pytest.raises(
        RuntimeError, match="ASGI application failed to start up"
    ) as exc_info:
        async with ASGITransport(app):
            pass

    assert "Haberdasher failed to start" in str(exc_info.value)


@pytest.mark.asyncio
async def test_lifespan_shutdown_error() -> None:
    async def counting_haberdasher():
        counter = Counter()
        try:
            haberdasher = CountingHaberdasher(counter)
            yield haberdasher
        finally:
            msg = "Haberdasher failed to shut down"
            raise RuntimeError(msg)

    app = HaberdasherASGIApplication(counting_haberdasher())
    with pytest.raises(
        RuntimeError, match="ASGI application failed to shut down"
    ) as exc_info:
        async with ASGITransport(app):
            pass
    assert "Haberdasher failed to shut down" in str(exc_info.value)


@pytest.mark.asyncio
async def test_lifespan_not_supported() -> None:
    final_count = None

    async def counting_haberdasher():
        counter = Counter()
        try:
            haberdasher = CountingHaberdasher(counter)
            yield haberdasher
        finally:
            nonlocal final_count
            final_count = counter["requests"]

    app = HaberdasherASGIApplication(counting_haberdasher())
    transport = ASGITransport(app)
    async with HaberdasherClient(
        "http://localhost", http_client=Client(transport)
    ) as client:
        with pytest.raises(ConnectError):
            await client.make_hat(Size(inches=10))
    assert (
        "ASGI server does not support lifespan but async generator passed for service."
        in str(transport.app_exception)
    )

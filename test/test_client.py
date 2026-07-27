from __future__ import annotations

import pytest
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc.client import ResponseMetadata

from .connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClient,
    HaberdasherClientSync,
    HaberdasherSync,
    HaberdasherWSGIApplication,
)
from .connectrpc.example.haberdasher_pb import Hat, Size

_default_headers = (
    ("content-type", "application/proto"),
    ("content-encoding", "gzip"),
    ("vary", "Accept-Encoding"),
)
_headers_cases = [
    ([], [], [*_default_headers], []),
    ([("x-animal", "bear")], [], [*_default_headers, ("x-animal", "bear")], []),
    (
        [("x-animal", "bear"), ("X-Animal", "cat")],
        [],
        [*_default_headers, ("x-animal", "bear"), ("x-animal", "cat")],
        [],
    ),
    ([], [("token-cost", "1000")], [*_default_headers], [("token-cost", "1000")]),
    (
        [],
        [("token-cost", "1000"), ("Token-Cost", "500")],
        [*_default_headers],
        [("token-cost", "1000"), ("token-cost", "500")],
    ),
    (
        [("x-animal", "bear"), ("X-Animal", "cat")],
        [("token-cost", "1000"), ("Token-Cost", "500")],
        [*_default_headers, ("x-animal", "bear"), ("x-animal", "cat")],
        [("token-cost", "1000"), ("token-cost", "500")],
    ),
]


@pytest.mark.parametrize(
    ("headers", "trailers", "response_headers", "response_trailers"), _headers_cases
)
def test_headers_sync(headers, trailers, response_headers, response_trailers) -> None:
    class HeadersHaberdasherSync(HaberdasherSync):
        def __init__(
            self, headers: list[tuple[str, str]], trailers: list[tuple[str, str]]
        ) -> None:
            self.headers = headers
            self.trailers = trailers

        def make_hat(self, _request, ctx):
            for key, value in self.headers:
                ctx.response_headers.add(key, value)
            for key, value in self.trailers:
                ctx.response_trailers.add(key, value)
            return Hat()

    transport = WSGITransport(
        HaberdasherWSGIApplication(HeadersHaberdasherSync(headers, trailers))
    )

    client = HaberdasherClientSync(
        "http://localhost", http_client=SyncClient(transport=transport)
    )

    with ResponseMetadata() as resp:
        client.make_hat(Size(inches=10))

    assert list(resp.headers.allitems()) == response_headers
    assert list(resp.trailers.allitems()) == response_trailers


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("headers", "trailers", "response_headers", "response_trailers"), _headers_cases
)
async def test_headers_async(
    headers, trailers, response_headers, response_trailers
) -> None:
    class HeadersHaberdasher(Haberdasher):
        def __init__(
            self, headers: list[tuple[str, str]], trailers: list[tuple[str, str]]
        ) -> None:
            self.headers = headers
            self.trailers = trailers

        async def make_hat(self, _request, ctx):
            for key, value in self.headers:
                ctx.response_headers.add(key, value)
            for key, value in self.trailers:
                ctx.response_trailers.add(key, value)
            return Hat()

    transport = ASGITransport(
        HaberdasherASGIApplication(HeadersHaberdasher(headers, trailers))
    )

    client = HaberdasherClient(
        "http://localhost", http_client=Client(transport=transport)
    )

    with ResponseMetadata() as resp:
        await client.make_hat(Size(inches=10))

    assert list(resp.headers.allitems()) == response_headers
    assert list(resp.trailers.allitems()) == response_trailers

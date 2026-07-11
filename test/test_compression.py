from __future__ import annotations

import pytest
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc.client import ResponseMetadata
from connectrpc.compression.brotli import BrotliCompression
from connectrpc.compression.gzip import GzipCompression
from connectrpc.compression.zstd import ZstdCompression

from ._util import resolve_compression
from .connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClient,
    HaberdasherClientSync,
    HaberdasherSync,
    HaberdasherWSGIApplication,
)
from .connectrpc.example.haberdasher_pb import Hat, Size


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("compressions", "encoding"),
    [
        pytest.param((), "identity", id="none"),
        pytest.param(("gzip",), "gzip", id="gzip"),
        pytest.param(("zstd",), "zstd", id="zstd"),
        pytest.param(("br",), "br", id="br"),
        pytest.param(("gzip", "br", "zstd"), "zstd", id="all"),
    ],
)
async def test_server_compressions_async(
    compressions: tuple[str], encoding: str
) -> None:
    class SimpleHaberdasher(Haberdasher):
        async def make_hat(self, request, ctx):
            return Hat(size=10, color="blue")

    app = HaberdasherASGIApplication(
        SimpleHaberdasher(), compressions=[resolve_compression(c) for c in compressions]
    )
    with ResponseMetadata() as meta:
        client = HaberdasherClient(
            "http://localhost",
            http_client=Client(ASGITransport(app)),
            accept_compression=(
                ZstdCompression(),
                GzipCompression(),
                BrotliCompression(),
            ),
            send_compression=None,
        )
        res = await client.make_hat(Size(inches=10))
    assert res.size == 10
    assert res.color == "blue"
    assert meta.headers.get("content-encoding") == encoding


@pytest.mark.parametrize(
    ("compressions", "encoding"),
    [
        pytest.param((), "identity", id="none"),
        pytest.param(("gzip",), "gzip", id="gzip"),
        pytest.param(("zstd",), "zstd", id="zstd"),
        pytest.param(("br",), "br", id="br"),
        pytest.param(("gzip", "br", "zstd"), "zstd", id="all"),
    ],
)
def test_server_compressions_sync(compressions: tuple[str], encoding: str) -> None:
    class SimpleHaberdasher(HaberdasherSync):
        def make_hat(self, request, ctx):
            return Hat(size=10, color="blue")

    app = HaberdasherWSGIApplication(
        SimpleHaberdasher(), compressions=[resolve_compression(c) for c in compressions]
    )
    client = HaberdasherClientSync(
        "http://localhost",
        http_client=SyncClient(WSGITransport(app)),
        accept_compression=(ZstdCompression(), GzipCompression(), BrotliCompression()),
        send_compression=None,
    )
    with ResponseMetadata() as meta:
        res = client.make_hat(Size(inches=10))
    assert res.size == 10
    assert res.color == "blue"
    assert meta.headers.get("content-encoding") == encoding

from __future__ import annotations

import pytest
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc._compression import IdentityCompression

from connectrpc.client import ResponseMetadata
from connectrpc.compression.brotli import BrotliCompression
from connectrpc.compression.gzip import GzipCompression
from connectrpc.compression.zstd import ZstdCompression

from ._util import resolve_compression
from .haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClient,
    HaberdasherClientSync,
    HaberdasherSync,
    HaberdasherWSGIApplication,
)
from .haberdasher_pb import Hat, Size


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


class TestIdentityCompression:
    def setup_method(self):
        self.ic = IdentityCompression()

    def test_name(self):
        assert self.ic.name() == "identity"

    def test_compress_bytes_returns_same_object(self):
        """bytes input must be returned as-is without copying."""
        data = b"hello"
        result = self.ic.compress(data)
        assert result is data

    def test_decompress_bytes_returns_same_object(self):
        """bytes input must be returned as-is without copying."""
        data = b"hello"
        result = self.ic.decompress(data)
        assert result is data

    def test_compress_bytearray_copies(self):
        """bytearray input must be copied into a new bytes object."""
        data = bytearray(b"hello")
        result = self.ic.compress(data)
        assert result == b"hello"
        assert isinstance(result, bytes)
        assert not isinstance(result, bytearray)

    def test_decompress_bytearray_copies(self):
        """bytearray input must be copied into a new bytes object."""
        data = bytearray(b"hello")
        result = self.ic.decompress(data)
        assert result == b"hello"
        assert isinstance(result, bytes)

    def test_compress_memoryview_copies(self):
        """memoryview input must be copied into a new bytes object."""
        data = memoryview(b"hello")
        result = self.ic.compress(data)
        assert result == b"hello"
        assert isinstance(result, bytes)

    def test_decompress_memoryview_copies(self):
        """memoryview input must be copied into a new bytes object."""
        data = memoryview(b"hello")
        result = self.ic.decompress(data)
        assert result == b"hello"
        assert isinstance(result, bytes)

    def test_compress_empty_bytes(self):
        data = b""
        assert self.ic.compress(data) is data

    def test_decompress_empty_bytes(self):
        data = b""
        assert self.ic.decompress(data) is data

from __future__ import annotations

from typing import TYPE_CHECKING

import brotli as brotli_lib
import pytest
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc._compression import IdentityCompression
from connectrpc.client import ResponseMetadata
from connectrpc.code import Code
from connectrpc.compression.brotli import BrotliCompression
from connectrpc.compression.gzip import GzipCompression
from connectrpc.compression.zstd import ZstdCompression
from connectrpc.errors import ConnectError

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

if TYPE_CHECKING:
    from connectrpc.compression import Compression


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
        async def make_hat(self, _request, _ctx):
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
        def make_hat(self, _request, _ctx):
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
    def test_name(self):
        assert IdentityCompression().name() == "identity"

    def test_bytes(self):
        data = b"hello"
        compression = IdentityCompression()
        compressed = compression.compress(data)
        assert compressed is data
        decompressed = compression.decompress(compressed)
        assert decompressed is data

    @pytest.mark.parametrize("ctor", [bytearray, memoryview])
    def test_not_bytes(self, ctor: type[bytearray | memoryview]) -> None:
        data = ctor(b"hello")
        compression = IdentityCompression()
        compressed = compression.compress(data)
        assert compressed == b"hello"
        assert isinstance(compressed, bytes)
        decompressed = compression.decompress(compressed)
        assert decompressed == b"hello"
        assert isinstance(decompressed, bytes)

    def test_read_max_bytes_exceeded(self) -> None:
        with pytest.raises(ConnectError) as exc_info:
            IdentityCompression().decompress(b"hello", 4)
        assert exc_info.value.code == Code.RESOURCE_EXHAUSTED


_READ_MAX_BYTES = 100


@pytest.mark.parametrize(
    "compression",
    [GzipCompression(), ZstdCompression(), BrotliCompression()],
    ids=["gzip", "zstd", "br"],
)
class TestDecompressReadMaxBytes:
    def test_at_limit(self, compression: Compression) -> None:
        data = b"a" * _READ_MAX_BYTES
        compressed = compression.compress(data)
        assert compression.decompress(compressed, _READ_MAX_BYTES) == data

    def test_over_limit(self, compression: Compression) -> None:
        data = b"a" * (_READ_MAX_BYTES + 1)
        compressed = compression.compress(data)
        with pytest.raises(ConnectError) as exc_info:
            compression.decompress(compressed, _READ_MAX_BYTES)
        assert exc_info.value.code == Code.RESOURCE_EXHAUSTED
        assert (
            exc_info.value.message
            == f"message is larger than configured max {_READ_MAX_BYTES}"
        )

    def test_over_limit_incompressible(self, compression: Compression) -> None:
        data = bytes(range(256)) * ((_READ_MAX_BYTES // 256) + 2)
        compressed = compression.compress(data)
        with pytest.raises(ConnectError) as exc_info:
            compression.decompress(compressed, _READ_MAX_BYTES)
        assert exc_info.value.code == Code.RESOURCE_EXHAUSTED

    def test_decompression_bomb(self, compression: Compression) -> None:
        data = bytes(64 * 1024 * 1024)
        compressed = compression.compress(data)
        with pytest.raises(ConnectError) as exc_info:
            compression.decompress(compressed, _READ_MAX_BYTES)
        assert exc_info.value.code == Code.RESOURCE_EXHAUSTED

    def test_no_limit(self, compression: Compression) -> None:
        data = b"a" * (_READ_MAX_BYTES + 1)
        compressed = compression.compress(data)
        assert compression.decompress(compressed) == data

    @pytest.mark.parametrize("ctor", [bytearray, memoryview])
    def test_not_bytes(
        self, compression: Compression, ctor: type[bytearray | memoryview]
    ) -> None:
        data = b"a" * _READ_MAX_BYTES
        compressed = ctor(compression.compress(data))
        assert compression.decompress(compressed, _READ_MAX_BYTES) == data


class TestGzipDecompress:
    def test_empty_with_limit(self) -> None:
        assert GzipCompression().decompress(b"", _READ_MAX_BYTES) == b""

    def test_multi_member_with_limit(self) -> None:
        compression = GzipCompression()
        compressed = compression.compress(b"a" * 60) + compression.compress(b"b" * 60)
        assert compression.decompress(compressed, 120) == b"a" * 60 + b"b" * 60

    def test_multi_member_over_limit(self) -> None:
        compression = GzipCompression()
        compressed = compression.compress(b"a" * 60) + compression.compress(b"b" * 60)
        with pytest.raises(ConnectError) as exc_info:
            compression.decompress(compressed, _READ_MAX_BYTES)
        assert exc_info.value.code == Code.RESOURCE_EXHAUSTED

    def test_truncated_with_limit(self) -> None:
        compression = GzipCompression()
        compressed = compression.compress(b"a" * 60)
        with pytest.raises(EOFError):
            compression.decompress(compressed[:-5], _READ_MAX_BYTES)


class TestZstdDecompress:
    def test_empty_with_limit(self) -> None:
        assert ZstdCompression().decompress(b"", _READ_MAX_BYTES) == b""


class TestBrotliDecompress:
    def test_truncated_with_limit(self) -> None:
        compression = BrotliCompression()
        compressed = compression.compress(b"a" * 60)
        with pytest.raises(brotli_lib.error):
            compression.decompress(compressed[: len(compressed) // 2], _READ_MAX_BYTES)

    def test_trailing_garbage_with_limit(self) -> None:
        compression = BrotliCompression()
        compressed = compression.compress(b"a" * 60)
        with pytest.raises(brotli_lib.error):
            compression.decompress(compressed + b"garbage", _READ_MAX_BYTES)

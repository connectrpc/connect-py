from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import urlencode

import brotli as brotli_lib
import pytest
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc._compression import (
    IdentityCompression,
    resolve_compressions,
    unknown_compression_error,
)
from connectrpc._protocol_connect import ConnectServerProtocol
from connectrpc._protocol_grpc import GRPCServerProtocol
from connectrpc.client import ResponseMetadata
from connectrpc.code import Code
from connectrpc.compression.brotli import BrotliCompression
from connectrpc.compression.gzip import GzipCompression
from connectrpc.compression.zstd import ZstdCompression
from connectrpc.errors import ConnectError
from connectrpc.protocol import ProtocolType
from connectrpc.request import Headers

from ._util import call, resolve_compression
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
    from collections.abc import Iterable

    from connectrpc._protocol import ServerProtocol
    from connectrpc.compression import Compression


class _BlueHaberdasher(Haberdasher):
    async def make_hat(self, request, _ctx):
        return Hat(size=request.inches, color="blue")

    async def make_similar_hats(self, request, _ctx):
        yield Hat(size=request.inches, color="blue")


class _BlueHaberdasherSync(HaberdasherSync):
    def make_hat(self, request, _ctx):
        return Hat(size=request.inches, color="blue")

    def make_similar_hats(self, request, _ctx):
        yield Hat(size=request.inches, color="blue")


@pytest.fixture(params=["async", "sync"])
def new_client(request: pytest.FixtureRequest):
    """Returns a factory for clients of a server with the given compressions."""

    def new_client(
        compressions: Iterable[Compression] | None = None,
        *,
        send_compression: Compression | None,
        accept_compression: Iterable[Compression] | None = None,
        protocol: ProtocolType = ProtocolType.CONNECT,
    ) -> HaberdasherClient | HaberdasherClientSync:
        if request.param == "async":
            app = HaberdasherASGIApplication(
                _BlueHaberdasher(), compressions=compressions
            )
            return HaberdasherClient(
                "http://localhost",
                http_client=Client(ASGITransport(app)),
                send_compression=send_compression,
                accept_compression=accept_compression,
                protocol=protocol,
            )
        app = HaberdasherWSGIApplication(
            _BlueHaberdasherSync(), compressions=compressions
        )
        return HaberdasherClientSync(
            "http://localhost",
            http_client=SyncClient(WSGITransport(app)),
            send_compression=send_compression,
            accept_compression=accept_compression,
            protocol=protocol,
        )

    return new_client


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
async def test_server_compressions(
    new_client, compressions: tuple[str], encoding: str
) -> None:
    client = new_client(
        [resolve_compression(c) for c in compressions],
        accept_compression=(ZstdCompression(), GzipCompression(), BrotliCompression()),
        send_compression=None,
    )
    with ResponseMetadata() as meta:
        res = await call(client.make_hat, Size(inches=10))
    assert res == Hat(size=10, color="blue")
    assert meta.headers.get("content-encoding") == encoding


_protocols = [ProtocolType.CONNECT, ProtocolType.GRPC, ProtocolType.GRPC_WEB]
_methods = [
    pytest.param("make_hat", id="unary"),
    pytest.param("make_similar_hats", id="stream"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("protocol", _protocols)
@pytest.mark.parametrize("method", _methods)
async def test_unknown_request_compression(
    new_client, protocol: ProtocolType, method: str
) -> None:
    # The server only supports the default gzip.
    client = new_client(protocol=protocol, send_compression=ZstdCompression())
    with pytest.raises(ConnectError) as exc_info:
        await call(getattr(client, method), Size(inches=10))
    assert exc_info.value.code == Code.UNIMPLEMENTED
    assert (
        exc_info.value.message
        == "unknown compression: 'zstd': supported encodings are gzip"
    )


@pytest.mark.parametrize(
    ("compressions", "detail"),
    [
        pytest.param(None, "supported encodings are gzip", id="default"),
        pytest.param((), "compression is not supported", id="none"),
        pytest.param(
            (ZstdCompression(), GzipCompression()),
            "supported encodings are zstd, gzip",
            id="multiple",
        ),
    ],
)
def test_unknown_compression_error(
    compressions: tuple[Compression, ...] | None, detail: str
) -> None:
    error = unknown_compression_error("foo", resolve_compressions(compressions))
    assert error.code == Code.UNIMPLEMENTED
    assert error.message == f"unknown compression: 'foo': {detail}"


class _XorCompression:
    """A toy compression registered under a name that is not all lowercase."""

    def name(self) -> str:
        return "Xor"

    def compress(self, data: bytes | bytearray | memoryview) -> bytes:
        return bytes(b ^ 0x5A for b in data)

    def decompress(
        self,
        data: bytes | bytearray | memoryview,
        read_max_bytes: int | None = None,  # noqa: ARG002
    ) -> bytes:
        return bytes(b ^ 0x5A for b in data)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", _methods)
async def test_mixed_case_request_compression(new_client, method: str) -> None:
    client = new_client([_XorCompression()], send_compression=_XorCompression())
    res = await call(getattr(client, method), Size(inches=10))
    hats = res if isinstance(res, list) else [res]
    assert hats == [Hat(size=10, color="blue")]


@pytest.mark.parametrize(
    ("protocol", "header_name"),
    [
        pytest.param(ConnectServerProtocol(), "connect-content-encoding", id="connect"),
        pytest.param(GRPCServerProtocol(), "grpc-encoding", id="grpc"),
    ],
)
@pytest.mark.parametrize(
    ("header", "expected"),
    [
        pytest.param(None, "identity", id="absent"),
        pytest.param("", "identity", id="empty"),
        pytest.param("identity", "identity", id="identity"),
        pytest.param("gzip", "gzip", id="gzip"),
        pytest.param("zstd", None, id="unknown"),
    ],
)
def test_stream_request_compression(
    protocol: ServerProtocol, header_name: str, header: str | None, expected: str | None
) -> None:
    headers = Headers()
    if header is not None:
        headers[header_name] = header
    compression, _ = protocol.negotiate_stream_compression(
        headers, resolve_compressions(None)
    )
    assert (compression.name() if compression else None) == expected


_empty_compression_requests = [
    pytest.param(
        "POST",
        "",
        {"content-type": "application/json", "content-encoding": ""},
        b'{"inches": 10}',
        id="post",
    ),
    pytest.param(
        "GET",
        "?"
        + urlencode(
            {"encoding": "json", "compression": "", "message": '{"inches": 10}'}
        ),
        {},
        b"",
        id="get",
    ),
]


@pytest.mark.parametrize(
    ("method", "query", "headers", "body"), _empty_compression_requests
)
def test_empty_request_compression_sync(method, query, headers, body) -> None:
    class SimpleHaberdasher(HaberdasherSync):
        def make_hat(self, request, _ctx):
            return Hat(size=request.inches, color="blue")

    transport = WSGITransport(HaberdasherWSGIApplication(SimpleHaberdasher()))
    res = SyncClient(transport).execute(
        method=method,
        url=f"http://localhost/connectrpc.example.Haberdasher/MakeHat{query}",
        headers=headers,
        content=body,
    )
    assert res.status == 200
    assert res.json() == {"size": 10, "color": "blue"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "query", "headers", "body"), _empty_compression_requests
)
async def test_empty_request_compression_async(method, query, headers, body) -> None:
    class SimpleHaberdasher(Haberdasher):
        async def make_hat(self, request, _ctx):
            return Hat(size=request.inches, color="blue")

    transport = ASGITransport(HaberdasherASGIApplication(SimpleHaberdasher()))
    res = await Client(transport).execute(
        method=method,
        url=f"http://localhost/connectrpc.example.Haberdasher/MakeHat{query}",
        headers=headers,
        content=body,
    )
    assert res.status == 200
    assert res.json() == {"size": 10, "color": "blue"}


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

from __future__ import annotations

from typing import TYPE_CHECKING

from pyqwest import Client, SyncClient

from connectrpc._compression import IdentityCompression
from connectrpc.compression.brotli import BrotliCompression
from connectrpc.compression.gzip import GzipCompression
from connectrpc.compression.zstd import ZstdCompression

from .connectrpc.example.haberdasher_connect import (
    HaberdasherClient,
    HaberdasherClientSync,
)

if TYPE_CHECKING:
    from pyqwest import SyncTransport, Transport

    from connectrpc.compression import Compression


def resolve_compression(encoding: str) -> Compression:
    match encoding:
        case "gzip":
            return GzipCompression()
        case "br":
            return BrotliCompression()
        case "zstd":
            return ZstdCompression()
        case "identity":
            return IdentityCompression()
        case _:
            msg = f"unknown encoding '{encoding}'"
            raise ValueError(msg)


def haberdasher_client(transport: Transport, **kwargs) -> HaberdasherClient:
    return HaberdasherClient(
        "http://localhost", http_client=Client(transport), **kwargs
    )


def haberdasher_client_sync(
    transport: SyncTransport, **kwargs
) -> HaberdasherClientSync:
    return HaberdasherClientSync(
        "http://localhost", http_client=SyncClient(transport), **kwargs
    )

"""Protocol for defining compression to use with Connect."""

from __future__ import annotations

__all__ = ["Compression"]


from typing import Protocol


class Compression(Protocol):
    """Protocol for compression methods.

    By default, gzip compression is used. Other compression methods can be
    used by specifying implementations of this protocol. We provide standard
    implementations for:

    - br ([BrotliCompression][connectrpc.compression.brotli.BrotliCompression]) -
      requires the `brotli` dependency.
    - zstd ([ZstdCompression][connectrpc.compression.zstd.ZstdCompression]) -
      requires the `zstandard` dependency.
    """

    def name(self) -> str:
        """Return the name of the compression method.

        This value is used in HTTP headers to indicate accepted and used
        compression.
        """
        ...

    def compress(self, data: bytes | bytearray | memoryview) -> bytes:
        """Compress the given data."""
        ...

    def decompress(self, data: bytes | bytearray | memoryview) -> bytes:
        """Decompress the given data."""
        ...

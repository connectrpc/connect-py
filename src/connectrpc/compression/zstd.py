"""Zstandard compression."""

from __future__ import annotations

__all__ = ["ZstdCompression"]

import zstandard

from connectrpc._shared import message_too_large_error

from . import Compression


class ZstdCompression(Compression):
    """Compression implementation using Zstandard."""

    def __init__(self, level: int = 3) -> None:
        """Create a new ZstdCompression.

        Args:
            level: Compression level to use.

        """
        self._level = level

    def name(self) -> str:
        """Return the compression name for Zstandard."""
        return "zstd"

    def compress(self, data: bytes | bytearray | memoryview) -> bytes:
        """Compress the given data using Zstandard."""
        return zstandard.ZstdCompressor(level=self._level).compress(data)

    def decompress(
        self, data: bytes | bytearray | memoryview, read_max_bytes: int | None = None
    ) -> bytes:
        """Decompress the given data using Zstandard."""
        # Support clients sending frames without length by using
        # stream API.
        with zstandard.ZstdDecompressor().stream_reader(data) as reader:
            if read_max_bytes is None:
                return reader.read()
            parts: list[bytes] = []
            remaining = read_max_bytes + 1
            while remaining > 0:
                out = reader.read(remaining)
                if not out:
                    return b"".join(parts)
                parts.append(out)
                remaining -= len(out)
            raise message_too_large_error(read_max_bytes)

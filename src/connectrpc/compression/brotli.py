"""Brotli compression."""

from __future__ import annotations

__all__ = ["BrotliCompression"]

import brotli

from connectrpc._shared import message_too_large_error

from . import Compression


class BrotliCompression(Compression):
    """Compression implementation using Brotli."""

    def __init__(self, quality: int = 3) -> None:
        """Create a new BrotliCompression.

        Args:
            quality: Compression quality to use.

        """
        self._quality = quality

    def name(self) -> str:
        """Return the compression name for Brotli."""
        return "br"

    def compress(self, data: bytes | bytearray | memoryview) -> bytes:
        """Compress the given data using Brotli."""
        return brotli.compress(data, quality=self._quality)

    def decompress(
        self, data: bytes | bytearray | memoryview, read_max_bytes: int | None = None
    ) -> bytes:
        """Decompress the given data using Brotli."""
        if read_max_bytes is None:
            return brotli.decompress(data)
        decompressor = brotli.Decompressor()
        parts: list[bytes] = []
        total = 0
        out = decompressor.process(data, output_buffer_limit=read_max_bytes + 1)
        while True:
            total += len(out)
            parts.append(out)
            if total > read_max_bytes:
                raise message_too_large_error(read_max_bytes)
            if decompressor.is_finished():
                return b"".join(parts)
            if decompressor.can_accept_more_data():
                msg = "compressed stream ended before the end-of-stream marker was reached"
                raise brotli.error(msg)
            out = decompressor.process(
                b"", output_buffer_limit=read_max_bytes + 1 - total
            )
            if not out:
                msg = "decompressor made no progress"
                raise brotli.error(msg)

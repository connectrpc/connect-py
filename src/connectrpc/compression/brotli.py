"""Brotli compression."""

from __future__ import annotations

__all__ = ["BrotliCompression"]

import brotli

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

    def decompress(self, data: bytes | bytearray | memoryview) -> bytes:
        """Decompress the given data using Brotli."""
        return brotli.decompress(data)

"""GZip compression."""

from __future__ import annotations

import gzip
import zlib

from connectrpc._shared import message_too_large_error

from . import Compression


class GzipCompression(Compression):
    """Compression implementation using Gzip."""

    def __init__(self, level: int = 6) -> None:
        """Create a new GzipCompression.

        Args:
            level: Compression level to use.

        """
        self._level = level

    def name(self) -> str:
        """Return the compression name for Gzip."""
        return "gzip"

    def compress(self, data: bytes | bytearray | memoryview) -> bytes:
        """Compress the given data using Gzip."""
        return gzip.compress(data, compresslevel=self._level)

    def decompress(
        self, data: bytes | bytearray | memoryview, read_max_bytes: int | None = None
    ) -> bytes:
        """Decompress the given data using Gzip."""
        if read_max_bytes is None:
            return gzip.decompress(data)
        if not data:
            return b""
        parts: list[bytes] = []
        total = 0
        decompressor = zlib.decompressobj(wbits=31)
        remaining_input: bytes | bytearray | memoryview = data
        while True:
            out = decompressor.decompress(remaining_input, read_max_bytes + 1 - total)
            total += len(out)
            parts.append(out)
            if total > read_max_bytes:
                raise message_too_large_error(read_max_bytes)
            if decompressor.eof:
                # In practice should never happen but match gzip.decompress above by
                # handling multi-member payloads.
                remaining_input = decompressor.unused_data
                if not remaining_input:
                    return b"".join(parts)
                decompressor = zlib.decompressobj(wbits=31)
            else:
                tail = decompressor.unconsumed_tail
                if not tail:
                    msg = "Compressed file ended before the end-of-stream marker was reached"
                    raise EOFError(msg)
                if not out and len(tail) == len(remaining_input):
                    msg = "decompressor made no progress"
                    raise zlib.error(msg)
                remaining_input = tail

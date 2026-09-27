from __future__ import annotations

from typing import TYPE_CHECKING

from connectrpc.compression.gzip import GzipCompression

from ._shared import message_too_large_error
from .code import Code
from .compression import Compression
from .errors import ConnectError

if TYPE_CHECKING:
    from collections.abc import Iterable


class IdentityCompression(Compression):
    def name(self) -> str:
        return "identity"

    def compress(self, data: bytes | bytearray | memoryview) -> bytes:
        """Return data as-is without compression."""
        return data if isinstance(data, bytes) else bytes(data)

    def decompress(
        self, data: bytes | bytearray | memoryview, read_max_bytes: int | None = None
    ) -> bytes:
        """Return data as-is without decompression."""
        if read_max_bytes is not None and len(data) > read_max_bytes:
            raise message_too_large_error(read_max_bytes)
        return data if isinstance(data, bytes) else bytes(data)


_identity = IdentityCompression()

_gzip = GzipCompression()
_default_compressions: dict[str, Compression] = {"gzip": _gzip, "identity": _identity}


def resolve_compressions(
    compressions: Iterable[Compression] | None,
) -> dict[str, Compression]:
    if compressions is None:
        return _default_compressions
    res = {comp.name(): comp for comp in compressions}
    # identity is always supported
    res["identity"] = _identity
    return res


def negotiate_compression(
    accept_encoding: str, compressions: dict[str, Compression]
) -> Compression:
    for accept in accept_encoding.split(","):
        compression = compressions.get(accept.strip())
        if compression:
            return compression
    return _identity


def resolve_request_compression(
    name: str, compressions: dict[str, Compression]
) -> Compression | None:
    """Return the compression for a request's encoding, or None if unsupported.

    Every request path resolves the name here: an empty name means identity, as
    in connect-go, and names match exactly.
    """
    return compressions.get(name or "identity")


def unknown_compression_error(
    name: str, compressions: dict[str, Compression]
) -> ConnectError:
    # identity is always accepted and is not a compression, so it is not listed.
    supported = [n for n in compressions if n != "identity"]
    detail = (
        f"supported encodings are {', '.join(supported)}"
        if supported
        else "compression is not supported"
    )
    return ConnectError(Code.UNIMPLEMENTED, f"unknown compression: '{name}': {detail}")

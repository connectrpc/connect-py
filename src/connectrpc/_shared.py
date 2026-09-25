from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from .code import Code
from .errors import ConnectError

if TYPE_CHECKING:
    from ._codec import Codec
    from .compression import Compression

_T = TypeVar("_T")


def message_too_large_error(read_max_bytes: int) -> ConnectError:
    msg = f"message is larger than configured max {read_max_bytes}"
    return ConnectError(Code.RESOURCE_EXHAUSTED, msg)


def decompress(
    compression: Compression,
    data: bytes | bytearray | memoryview,
    read_max_bytes: int | None,
) -> bytes:
    """Decompress data from the peer, reporting malformed data as invalid_argument."""
    try:
        return compression.decompress(data, read_max_bytes)
    except ConnectError:
        raise
    except Exception as e:
        raise ConnectError(Code.INVALID_ARGUMENT, f"decompress: {e}") from e


def decode(codec: Codec, data: bytes | bytearray, message_class: type[_T]) -> _T:
    """Decode a message from the peer, reporting malformed data as invalid_argument."""
    try:
        return codec.decode(data, message_class)
    except ConnectError:
        raise
    except Exception as e:
        raise ConnectError(Code.INVALID_ARGUMENT, f"unmarshal message: {e}") from e

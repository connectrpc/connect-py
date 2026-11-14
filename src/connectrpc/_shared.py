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
    *,
    code: Code = Code.INVALID_ARGUMENT,
) -> bytes:
    """Decompress data from the peer, reporting malformed data with code.

    Servers keep the default, since a malformed request is the client's fault;
    clients pass internal.
    """
    try:
        return compression.decompress(data, read_max_bytes)
    except ConnectError:
        raise
    except Exception as e:
        raise ConnectError(code, f"decompress: {e}") from e


def decode_message(
    codec: Codec,
    data: bytes | bytearray,
    message_class: type[_T],
    *,
    code: Code = Code.INVALID_ARGUMENT,
) -> _T:
    """Decode a message from the peer, reporting malformed data with code.

    Servers keep the default, since a malformed request is the client's fault;
    clients pass internal.
    """
    try:
        return codec.decode(data, message_class)
    except ConnectError:
        raise
    except Exception as e:
        raise ConnectError(code, f"unmarshal message: {e}") from e

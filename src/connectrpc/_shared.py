from __future__ import annotations

from .code import Code
from .errors import ConnectError


def message_too_large_error(read_max_bytes: int) -> ConnectError:
    msg = f"message is larger than configured max {read_max_bytes}"
    return ConnectError(Code.RESOURCE_EXHAUSTED, msg)

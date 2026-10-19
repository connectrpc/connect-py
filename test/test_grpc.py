from __future__ import annotations

import pytest

from connectrpc._protocol_grpc import _parse_timeout
from connectrpc.code import Code
from connectrpc.errors import ConnectError


def test_parse_timeout() -> None:
    assert _parse_timeout("1H") == 3600 * 1000
    assert _parse_timeout("2M") == 2 * 60 * 1000
    assert _parse_timeout("3S") == 3 * 1000
    assert _parse_timeout("4m") == 4
    # We parse gRPC timeouts with connect conventions, which means integer milliseconds
    # The below parse to 0ms.
    assert _parse_timeout("5u") == 0
    assert _parse_timeout("6n") == 0
    with pytest.raises(ConnectError) as excinfo:
        _parse_timeout("100X")
    assert excinfo.value.code == Code.INVALID_ARGUMENT
    assert excinfo.value.message == "protocol error: timeout has invalid unit 'X'"


@pytest.mark.parametrize("timeout", ["m", "-5m", "+5m", " 5m", "5_0m", "123456789m"])
def test_parse_timeout_invalid(timeout: str) -> None:
    with pytest.raises(ConnectError) as excinfo:
        _parse_timeout(timeout)
    assert excinfo.value.code == Code.INVALID_ARGUMENT

from __future__ import annotations

import pytest

from connectrpc._protocol_grpc import GRPCServerProtocol, _parse_timeout
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from connectrpc.method import IdempotencyLevel, MethodInfo, StreamType
from connectrpc.request import Headers, RequestContext

_METHOD = MethodInfo(
    name="Call",
    service_name="test.Service",
    input=bytes,
    output=bytes,
    idempotency_level=IdempotencyLevel.UNKNOWN,
    stream_type=StreamType.UNARY,
)


def _create_request_context(headers: Headers) -> RequestContext[bytes, bytes]:
    return GRPCServerProtocol().create_request_context(_METHOD, "POST", "http", headers)


@pytest.mark.parametrize(
    ("timeout", "timeout_ns"),
    [
        pytest.param("1H", 3_600_000_000_000, id="hours"),
        pytest.param("2M", 120_000_000_000, id="minutes"),
        pytest.param("3S", 3_000_000_000, id="seconds"),
        pytest.param("4m", 4_000_000, id="milliseconds"),
        pytest.param("5u", 5_000, id="microseconds"),
        pytest.param("6n", 6, id="nanoseconds"),
        pytest.param("99999999H", 359_999_996_400_000_000_000, id="max-hours"),
        pytest.param("99999999M", 5_999_999_940_000_000_000, id="max-minutes"),
        pytest.param("99999999S", 99_999_999_000_000_000, id="max-seconds"),
        pytest.param("99999999m", 99_999_999_000_000, id="max-milliseconds"),
        pytest.param("99999999u", 99_999_999_000, id="max-microseconds"),
        pytest.param("99999999n", 99_999_999, id="max-nanoseconds"),
        pytest.param("0n", 0, id="zero"),
    ],
)
def test_parse_timeout(timeout: str, timeout_ns: int) -> None:
    assert _parse_timeout(timeout) == timeout_ns


@pytest.mark.parametrize(
    "timeout",
    [
        "",
        "n",
        "m",
        "-5m",
        "+5m",
        " 5m",
        "5_0m",
        "123456789m",
        "000000001m",
        "\u0661m",
        "000000001S",
    ],
)
def test_parse_timeout_invalid(timeout: str) -> None:
    with pytest.raises(ConnectError) as excinfo:
        _create_request_context(Headers({"grpc-timeout": timeout}))
    assert excinfo.value.code == Code.INVALID_ARGUMENT


def test_parse_timeout_invalid_unit() -> None:
    with pytest.raises(ConnectError) as excinfo:
        _parse_timeout("100X")
    assert excinfo.value.code == Code.INVALID_ARGUMENT
    assert excinfo.value.message == "protocol error: timeout has invalid unit 'X'"


@pytest.mark.parametrize(("timeout", "expected_ms"), [("5u", 0.005), ("6n", 0.000006)])
def test_submillisecond_request_deadline(timeout: str, expected_ms: float, monkeypatch):
    monkeypatch.setattr("connectrpc.request.time.monotonic", lambda: 0.0)
    ctx = _create_request_context(Headers({"grpc-timeout": timeout}))
    assert ctx.timeout_ms == pytest.approx(expected_ms, abs=1e-12)


@pytest.mark.parametrize(
    ("timeout", "elapsed_ms", "remaining_ms"),
    [
        pytest.param("1500u", 0.25, 1.25, id="microseconds"),
        pytest.param("9999999n", 1.25, 8.749999, id="nanoseconds"),
    ],
)
def test_request_deadline_fractional_budget(
    timeout: str, elapsed_ms: float, remaining_ms: float, monkeypatch
) -> None:
    now = 123.456
    monkeypatch.setattr("connectrpc.request.time.monotonic", lambda: now)
    ctx = _create_request_context(Headers({"grpc-timeout": timeout}))
    now += elapsed_ms / 1000

    assert ctx.timeout_ms == pytest.approx(remaining_ms, abs=1e-9)

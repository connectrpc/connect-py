from __future__ import annotations

import pytest

from connectrpc._codec import ProtoBinaryCodec
from connectrpc._compression import GzipCompression, IdentityCompression
from connectrpc._envelope import EnvelopeReader
from connectrpc._protocol_connect import ConnectEnvelopeReader
from connectrpc._protocol_grpc import GRPCEnvelopeReader, GRPCWebEnvelopeReader
from connectrpc.code import Code
from connectrpc.errors import ConnectError

from .connectrpc.example.haberdasher_pb import Hat


@pytest.mark.parametrize(
    ("reader_type", "flags", "code"),
    [
        (EnvelopeReader, 0b10, Code.INVALID_ARGUMENT),
        (EnvelopeReader, 0b10000000, Code.INVALID_ARGUMENT),
        (GRPCEnvelopeReader, 0b10, Code.INTERNAL),
        (GRPCEnvelopeReader, 0b10000000, Code.INTERNAL),
        (ConnectEnvelopeReader, 0b100, Code.INTERNAL),
        (ConnectEnvelopeReader, 0b10000000, Code.INTERNAL),
        (GRPCWebEnvelopeReader, 0b10, Code.INTERNAL),
        (GRPCWebEnvelopeReader, 0b100, Code.INTERNAL),
    ],
)
@pytest.mark.parametrize("split", [0, 1, 4])
def test_reject_invalid_flags(reader_type, flags, code, split) -> None:
    reader = reader_type(Hat, ProtoBinaryCodec(), IdentityCompression(), None)
    payload = Hat(size=10).to_binary()
    envelope = bytes([flags]) + len(payload).to_bytes(4, "big") + payload
    assert list(reader.feed(envelope[:split])) == []
    with pytest.raises(ConnectError, match="invalid envelope flags") as excinfo:
        list(reader.feed(envelope[split:]))
    assert excinfo.value.code == code


@pytest.mark.parametrize(
    ("reader_type", "code"),
    [
        (EnvelopeReader, Code.INVALID_ARGUMENT),
        (GRPCEnvelopeReader, Code.INTERNAL),
        (ConnectEnvelopeReader, Code.INTERNAL),
        (GRPCWebEnvelopeReader, Code.INTERNAL),
    ],
)
@pytest.mark.parametrize("read_max_bytes", [None, 1])
def test_reject_invalid_flags_before_waiting_for_payload(
    reader_type, code, read_max_bytes
) -> None:
    reader = reader_type(Hat, ProtoBinaryCodec(), IdentityCompression(), read_max_bytes)
    with pytest.raises(ConnectError, match="invalid envelope flags") as excinfo:
        list(reader.feed(b"\x05\xff\xff\xff\xff"))
    assert excinfo.value.code == code


@pytest.mark.parametrize(
    "reader_type",
    [EnvelopeReader, GRPCEnvelopeReader, ConnectEnvelopeReader, GRPCWebEnvelopeReader],
)
@pytest.mark.parametrize("compressed", [False, True])
def test_read_valid_message_flags(reader_type, compressed) -> None:
    compression = GzipCompression() if compressed else IdentityCompression()
    reader = reader_type(Hat, ProtoBinaryCodec(), compression, None)
    payload = compression.compress(Hat(size=10).to_binary())
    envelope = bytes([int(compressed)]) + len(payload).to_bytes(4, "big") + payload
    assert list(reader.feed(envelope)) == [Hat(size=10)]


@pytest.mark.parametrize(
    ("reader_type", "flags", "payload"),
    [
        (ConnectEnvelopeReader, 0b10, b"{}"),
        (GRPCWebEnvelopeReader, 0b10000000, b"grpc-status: 0\r\n"),
    ],
)
@pytest.mark.parametrize("compressed", [False, True])
def test_read_valid_end_flags(reader_type, flags, payload, compressed) -> None:
    compression = GzipCompression() if compressed else IdentityCompression()
    reader = reader_type(Hat, ProtoBinaryCodec(), compression, None)
    payload = compression.compress(payload)
    envelope = (
        bytes([flags | int(compressed)]) + len(payload).to_bytes(4, "big") + payload
    )
    with reader:
        assert list(reader.feed(envelope)) == []

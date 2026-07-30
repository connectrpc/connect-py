"""The Connect protocol types."""

from __future__ import annotations

from enum import Enum


class ProtocolType(Enum):
    """A protocol supported by Connect."""

    CONNECT = 1
    """The Connect protocol."""

    GRPC = 2
    """The gRPC protocol."""

    GRPC_WEB = 3
    """The gRPC-Web protocol."""

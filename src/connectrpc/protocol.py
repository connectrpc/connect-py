"""The Connect protocol types."""

from __future__ import annotations

from enum import Enum
from typing import Literal


class ProtocolType(Enum):
    """A protocol supported by Connect."""

    CONNECT = 1
    """The Connect protocol."""

    GRPC = 2
    """The gRPC protocol."""

    GRPC_WEB = 3
    """The gRPC-Web protocol."""

    CONNECT_WEBSOCKETS = 4
    """The Connect-over-WebSocket protocol."""


HTTPProtocolType = Literal[
    ProtocolType.CONNECT, ProtocolType.GRPC, ProtocolType.GRPC_WEB
]
"""The protocols carried by HTTP requests and responses.

These are the protocols that clients and WSGI applications support.
"""

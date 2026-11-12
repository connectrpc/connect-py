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


ClientProtocolType = Literal[
    ProtocolType.CONNECT, ProtocolType.GRPC, ProtocolType.GRPC_WEB
]
"""The protocols clients support.

Connect-over-WebSocket is aimed at servers handling requests from web
browsers, so clients do not implement it.
"""

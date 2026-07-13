from __future__ import annotations

from typing import TYPE_CHECKING

from ._protocol_connect import ConnectServerProtocol
from ._protocol_grpc import (
    GRPC_CONTENT_TYPE_DEFAULT,
    GRPC_CONTENT_TYPE_PREFIX,
    GRPC_WEB_CONTENT_TYPE_DEFAULT,
    GRPC_WEB_CONTENT_TYPE_PREFIX,
    GRPCServerProtocol,
    GRPCWebServerProtocol,
)

if TYPE_CHECKING:
    from ._protocol import ServerProtocol


_CONNECT_PROTOCOL = ConnectServerProtocol()
_GRPC_PROTOCOL = GRPCServerProtocol()
_GRPC_WEB_PROTOCOL = GRPCWebServerProtocol()


def negotiate_server_protocol(content_type: str) -> ServerProtocol:
    if content_type == GRPC_CONTENT_TYPE_DEFAULT or content_type.startswith(
        GRPC_CONTENT_TYPE_PREFIX
    ):
        return _GRPC_PROTOCOL
    if content_type == GRPC_WEB_CONTENT_TYPE_DEFAULT or content_type.startswith(
        GRPC_WEB_CONTENT_TYPE_PREFIX
    ):
        return _GRPC_WEB_PROTOCOL
    return _CONNECT_PROTOCOL

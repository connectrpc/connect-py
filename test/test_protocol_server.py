from __future__ import annotations

from connectrpc._protocol_connect import ConnectServerProtocol
from connectrpc._protocol_grpc import (
    GRPCServerProtocol,
    GRPCWebServerProtocol,
)
from connectrpc._protocol_server import (
    _CONNECT_PROTOCOL,
    _GRPC_PROTOCOL,
    _GRPC_WEB_PROTOCOL,
    negotiate_server_protocol,
)


class TestNegotiateServerProtocol:
    """Tests for negotiate_server_protocol singleton reuse."""

    def test_grpc_content_type_default_returns_singleton(self):
        protocol = negotiate_server_protocol("application/grpc")
        assert protocol is _GRPC_PROTOCOL

    def test_grpc_content_type_with_suffix_returns_singleton(self):
        protocol = negotiate_server_protocol("application/grpc+proto")
        assert protocol is _GRPC_PROTOCOL

    def test_grpc_web_content_type_default_returns_singleton(self):
        protocol = negotiate_server_protocol("application/grpc-web")
        assert protocol is _GRPC_WEB_PROTOCOL

    def test_grpc_web_content_type_with_suffix_returns_singleton(self):
        protocol = negotiate_server_protocol("application/grpc-web+proto")
        assert protocol is _GRPC_WEB_PROTOCOL

    def test_connect_content_type_json_returns_singleton(self):
        protocol = negotiate_server_protocol("application/json")
        assert protocol is _CONNECT_PROTOCOL

    def test_connect_content_type_proto_returns_singleton(self):
        protocol = negotiate_server_protocol("application/proto")
        assert protocol is _CONNECT_PROTOCOL

    def test_unknown_content_type_returns_connect_singleton(self):
        protocol = negotiate_server_protocol("text/plain")
        assert protocol is _CONNECT_PROTOCOL

    def test_repeated_calls_return_same_grpc_instance(self):
        p1 = negotiate_server_protocol("application/grpc")
        p2 = negotiate_server_protocol("application/grpc+proto")
        assert p1 is p2

    def test_repeated_calls_return_same_grpc_web_instance(self):
        p1 = negotiate_server_protocol("application/grpc-web")
        p2 = negotiate_server_protocol("application/grpc-web+proto")
        assert p1 is p2

    def test_singleton_types_are_correct(self):
        assert isinstance(_CONNECT_PROTOCOL, ConnectServerProtocol)
        assert isinstance(_GRPC_PROTOCOL, GRPCServerProtocol)
        assert isinstance(_GRPC_WEB_PROTOCOL, GRPCWebServerProtocol)

    def test_singletons_are_distinct(self):
        assert _CONNECT_PROTOCOL is not _GRPC_PROTOCOL
        assert _CONNECT_PROTOCOL is not _GRPC_WEB_PROTOCOL
        assert _GRPC_PROTOCOL is not _GRPC_WEB_PROTOCOL

from __future__ import annotations

from urllib.parse import urlencode

import pytest
from pyqwest import Headers as HTTPHeaders

from connectrpc._client_shared import prepare_get_params
from connectrpc._protocol_connect import ConnectClientProtocol
from connectrpc.code import Code
from connectrpc.codec import proto_binary_codec
from connectrpc.errors import ConnectError


def test_prepare_get_params_order() -> None:
    headers = HTTPHeaders([("content-encoding", "gzip")])
    params = prepare_get_params(proto_binary_codec(), b"hello", headers)
    assert list(params.keys()) == [
        "connect",
        "base64",
        "compression",
        "encoding",
        "message",
    ]
    assert "content-encoding" not in headers
    query = urlencode(params)
    expected_prefix = "connect=v1&base64=1&compression=gzip&encoding=proto&message="
    assert query.startswith(expected_prefix)


def test_prepare_get_params_order_without_compression() -> None:
    params = prepare_get_params(proto_binary_codec(), b"hello", HTTPHeaders([]))
    assert list(params.keys()) == ["connect", "base64", "encoding", "message"]


@pytest.mark.parametrize(
    ("content_type", "codec_name", "stream"),
    [
        ("application/json", "json", False),
        ("application/json; charset=utf-8", "json", False),
        ("application/json; charset=UTF-8", "json", False),
        ('application/json; charset="utf-8"', "json", False),
        ("application/json; foo=bar; charset=utf-8", "json", False),
        ("application/json; foo=bar", "json", False),
        ("application/proto; foo=bar", "proto", False),
        ("application/proto; charset=iso-8859-1", "proto", False),
        ("application/connect+json", "json", True),
        ("application/connect+json; charset=utf-8", "json", True),
        ("application/connect+json; charset=UTF-8", "json", True),
        ('application/connect+json; charset="utf-8"', "json", True),
        ("application/connect+json; foo=bar; charset=utf-8", "json", True),
        ("application/connect+json; foo=bar", "json", True),
        ("application/connect+proto; foo=bar", "proto", True),
    ],
)
def test_validate_response_content_type(
    content_type: str, codec_name: str, *, stream: bool
) -> None:
    protocol = ConnectClientProtocol()
    if stream:
        protocol.validate_stream_response(codec_name, content_type)
    else:
        protocol.validate_response(codec_name, 200, content_type)


@pytest.mark.parametrize(
    ("content_type", "codec_name", "stream", "code"),
    [
        ("application/json; charset=iso-8859-1", "json", False, Code.INTERNAL),
        ("application/json; foo=bar; charset=latin1", "json", False, Code.INTERNAL),
        ("application/proto", "json", False, Code.INTERNAL),
        ("text/plain", "json", False, Code.UNKNOWN),
        ("application/connect+json; charset=latin1", "json", True, Code.INTERNAL),
        ("application/connect+json; a=b; charset=latin1", "json", True, Code.INTERNAL),
        ("application/connect+proto", "json", True, Code.INTERNAL),
        ("application/json", "json", True, Code.UNKNOWN),
    ],
)
def test_validate_response_content_type_invalid(
    content_type: str, codec_name: str, *, stream: bool, code: Code
) -> None:
    protocol = ConnectClientProtocol()
    with pytest.raises(ConnectError) as exc_info:
        if stream:
            protocol.validate_stream_response(codec_name, content_type)
        else:
            protocol.validate_response(codec_name, 200, content_type)
    assert exc_info.value.code == code

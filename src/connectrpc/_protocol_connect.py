from __future__ import annotations

import json
import struct
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, TypeVar

from ._codec import CODEC_NAME_JSON, Codec
from ._compression import IdentityCompression, negotiate_compression
from ._envelope import EnvelopeReader, EnvelopeWriter
from ._protocol import (
    ConnectWireError,
    HTTPException,
    host_to_server_address,
    url_to_server_address,
)
from ._response_metadata import handle_response_trailers
from ._version import __version__
from .code import Code
from .errors import ConnectError
from .method import IdempotencyLevel, MethodInfo
from .request import Headers, RequestContext

if TYPE_CHECKING:
    from collections.abc import Mapping

    import pyqwest

    from ._codec import Codec
    from ._compression import Compression

REQ = TypeVar("REQ")
RES = TypeVar("RES")

CONNECT_HEADER_PROTOCOL_VERSION = "connect-protocol-version"
CONNECT_PROTOCOL_VERSION = "1"
CONNECT_HEADER_TIMEOUT = "connect-timeout-ms"
CONNECT_UNARY_CONTENT_TYPE_PREFIX = "application/"
CONNECT_UNARY_CONTENT_TYPE_JSON = (
    f"{CONNECT_UNARY_CONTENT_TYPE_PREFIX}{CODEC_NAME_JSON}"
)
CONNECT_STREAMING_CONTENT_TYPE_PREFIX = "application/connect+"
CONNECT_UNARY_HEADER_COMPRESSION = "content-encoding"
CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION = "accept-encoding"
CONNECT_STREAMING_HEADER_COMPRESSION = "connect-content-encoding"
CONNECT_STREAMING_HEADER_ACCEPT_COMPRESSION = "connect-accept-encoding"


_DEFAULT_CONNECT_USER_AGENT = f"connectrpc/{__version__}"


def _normalize_content_type(content_type: str) -> str:
    # content-type can have parameters, most commonly charset. Our supported codecs,
    # binary and JSON are always either non-text or utf-8 and the parameters are not
    # important for matching to a codec. A custom codec could conceivably need to
    # match on parameters, but we will reconsider that if it is ever asked for.
    return content_type.partition(";")[0].strip().lower()


def codec_name_from_content_type(content_type: str, *, stream: bool) -> str:
    content_type = _normalize_content_type(content_type)
    prefix = (
        CONNECT_STREAMING_CONTENT_TYPE_PREFIX
        if stream
        else CONNECT_UNARY_CONTENT_TYPE_PREFIX
    )
    if content_type.startswith(prefix):
        return content_type[len(prefix) :]
    # Follow connect-go behavior for malformed content type. If the content type misses the prefix,
    # it will still be coincidentally handled.
    return content_type


class ConnectServerProtocol:
    def create_request_context(
        self,
        method: MethodInfo[REQ, RES],
        http_method: str,
        http_scheme: str,
        headers: Headers,
        client_address: str | None = None,
    ) -> RequestContext[REQ, RES]:
        if method.idempotency_level == IdempotencyLevel.NO_SIDE_EFFECTS:
            if http_method not in ("GET", "POST"):
                raise HTTPException(
                    HTTPStatus.METHOD_NOT_ALLOWED, [("allow", "GET, POST")]
                )
        elif http_method != "POST":
            raise HTTPException(HTTPStatus.METHOD_NOT_ALLOWED, [("allow", "POST")])

        # We don't require connect-protocol-version header. connect-go provides an option
        # to require it but it's almost never used in practice.
        connect_protocol_version = headers.get(
            CONNECT_HEADER_PROTOCOL_VERSION, CONNECT_PROTOCOL_VERSION
        )
        if connect_protocol_version != CONNECT_PROTOCOL_VERSION:
            raise ConnectError(
                Code.INVALID_ARGUMENT,
                f"connect-protocol-version must be '1': got '{connect_protocol_version}'",
            )

        timeout_header = headers.get(CONNECT_HEADER_TIMEOUT)
        if timeout_header:
            if len(timeout_header) > 10:
                raise ConnectError(
                    Code.INVALID_ARGUMENT,
                    f"Invalid timeout header: '{timeout_header} has >10 digits",
                )
            try:
                timeout_ms = int(timeout_header)
            except ValueError as e:
                raise ConnectError(
                    Code.INVALID_ARGUMENT, f"Invalid timeout header: '{timeout_header}'"
                ) from e
        else:
            timeout_ms = None

        server_address = host_to_server_address(headers.get("host"), http_scheme)

        return RequestContext(
            method=method,
            http_method=http_method,
            request_headers=headers,
            timeout_ms=timeout_ms,
            server_address=server_address,
            client_address=client_address,
        )

    def create_envelope_writer(
        self, codec: Codec[RES, Any], compression: Compression | None
    ) -> EnvelopeWriter[RES]:
        return ConnectEnvelopeWriter(codec, compression)

    def uses_trailers(self) -> bool:
        return False

    def content_type(self, codec: Codec) -> str:
        return f"{CONNECT_STREAMING_CONTENT_TYPE_PREFIX}{codec.name()}"

    def compression_header_name(self) -> str:
        return CONNECT_STREAMING_HEADER_COMPRESSION

    def codec_name_from_content_type(self, content_type: str, *, stream: bool) -> str:
        return codec_name_from_content_type(content_type, stream=stream)

    def negotiate_stream_compression(
        self, headers: Headers, compressions: dict[str, Compression]
    ) -> tuple[Compression, Compression]:
        req_compression_name = headers.get(
            CONNECT_STREAMING_HEADER_COMPRESSION, "identity"
        )
        req_compression = (
            compressions.get(req_compression_name) or IdentityCompression()
        )
        accept_compression = headers.get(
            CONNECT_STREAMING_HEADER_ACCEPT_COMPRESSION, ""
        )
        resp_compression = negotiate_compression(accept_compression, compressions)
        return req_compression, resp_compression


class ConnectEnvelopeWriter(EnvelopeWriter):
    def end(self, user_trailers: Headers, error: ConnectWireError | None) -> bytes:
        end_message = {}
        if user_trailers:
            metadata: dict[str, list[str]] = {}
            for key, value in user_trailers.allitems():
                metadata.setdefault(key, []).append(value)
            end_message["metadata"] = metadata
        if error:
            end_message["error"] = error.to_dict()
        data = json.dumps(end_message).encode()
        if self._compression:
            data = self._compression.compress(data)
        return struct.pack(">BI", self._prefix | 0b10, len(data)) + data


class ConnectClientProtocol:
    def create_request_context(
        self,
        *,
        method: MethodInfo[REQ, RES],
        url: str,
        http_method: str,
        user_headers: Headers | Mapping[str, str] | None,
        timeout_ms: int | None,
        codec: Codec,
        stream: bool,
        accept_compression: str,
        send_compression: Compression | None,
    ) -> RequestContext[REQ, RES]:
        match user_headers:
            case Headers():
                # Copy to prevent modification if user keeps reference
                # TODO: Optimize
                headers = Headers(tuple(user_headers.allitems()))
            case None:
                headers = Headers()
            case _:
                headers = Headers(user_headers)

        if "user-agent" not in headers:
            headers["user-agent"] = _DEFAULT_CONNECT_USER_AGENT
        headers["connect-protocol-version"] = CONNECT_PROTOCOL_VERSION

        compression_header = (
            CONNECT_STREAMING_HEADER_COMPRESSION
            if stream
            else CONNECT_UNARY_HEADER_COMPRESSION
        )
        accept_compression_header = (
            CONNECT_STREAMING_HEADER_ACCEPT_COMPRESSION
            if stream
            else CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION
        )

        headers[accept_compression_header] = accept_compression
        if send_compression is not None:
            headers[compression_header] = send_compression.name()
        else:
            headers.pop(compression_header, None)
        headers["content-type"] = (
            f"{CONNECT_STREAMING_CONTENT_TYPE_PREFIX if stream else CONNECT_UNARY_CONTENT_TYPE_PREFIX}{codec.name()}"
        )
        if timeout_ms is not None:
            headers["connect-timeout-ms"] = str(timeout_ms)

        server_address = url_to_server_address(url)

        return RequestContext(
            method=method,
            http_method=http_method,
            request_headers=headers,
            timeout_ms=timeout_ms,
            server_address=server_address,
        )

    def validate_response(
        self, request_codec_name: str, status_code: int, response_content_type: str
    ) -> None:
        response_content_type = _normalize_content_type(response_content_type)
        if status_code != HTTPStatus.OK:
            # Error responses must be JSON-encoded
            if response_content_type == CONNECT_UNARY_CONTENT_TYPE_JSON:
                return
            raise ConnectWireError.from_http_status(status_code).to_exception()

        if not response_content_type.startswith(CONNECT_UNARY_CONTENT_TYPE_PREFIX):
            raise ConnectError(
                Code.UNKNOWN,
                f"invalid content-type: '{response_content_type}'; expecting '{CONNECT_UNARY_CONTENT_TYPE_PREFIX}{request_codec_name}'",
            )

        response_codec_name = codec_name_from_content_type(
            response_content_type, stream=False
        )
        if response_codec_name == request_codec_name:
            return

        raise ConnectError(
            Code.INTERNAL,
            f"invalid content-type: '{response_content_type}'; expecting '{CONNECT_UNARY_CONTENT_TYPE_PREFIX}{request_codec_name}'",
        )

    def validate_stream_response(
        self, request_codec_name: str, response_content_type: str
    ) -> None:
        if not response_content_type.startswith(CONNECT_STREAMING_CONTENT_TYPE_PREFIX):
            raise ConnectError(
                Code.UNKNOWN,
                f"invalid content-type: '{response_content_type}'; expecting '{CONNECT_STREAMING_CONTENT_TYPE_PREFIX}{request_codec_name}'",
            )

        response_codec_name = response_content_type[
            len(CONNECT_STREAMING_CONTENT_TYPE_PREFIX) :
        ]
        if response_codec_name != request_codec_name:
            raise ConnectError(
                Code.INTERNAL,
                f"invalid content-type: '{response_content_type}'; expecting '{CONNECT_STREAMING_CONTENT_TYPE_PREFIX}{request_codec_name}'",
            )

    def handle_response_compression(
        self,
        headers: pyqwest.Headers,
        compressions: dict[str, Compression],
        *,
        stream: bool,
    ) -> Compression:
        compression_header = (
            CONNECT_STREAMING_HEADER_COMPRESSION
            if stream
            else CONNECT_UNARY_HEADER_COMPRESSION
        )
        encoding = headers.get(compression_header)
        if not encoding:
            return IdentityCompression()
        res = compressions.get(encoding)
        if not res:
            raise ConnectError(
                Code.INTERNAL,
                f"unknown encoding '{encoding}'; accepted encodings are {', '.join(compressions.keys())}",
            )
        return res

    def create_envelope_reader(
        self,
        message_class: type[RES],
        codec: Codec,
        compression: Compression,
        read_max_bytes: int | None,
    ) -> EnvelopeReader[RES]:
        return ConnectEnvelopeReader(message_class, codec, compression, read_max_bytes)


class ConnectEnvelopeReader(EnvelopeReader[RES]):
    def handle_end_message(
        self, prefix_byte: int, message_data: bytes | bytearray, /
    ) -> bool:
        end_stream = prefix_byte & 0b10 != 0
        if not end_stream:
            return False
        end_stream_message: dict = json.loads(message_data)
        metadata = end_stream_message.get("metadata")
        if metadata:
            handle_response_trailers(metadata)
        error = end_stream_message.get("error")
        if error:
            # Most likely a bug in the protocol, handling of unknown code is different for unary
            # and streaming.
            raise ConnectWireError.from_dict(error, 500, Code.UNKNOWN).to_exception()
        return True

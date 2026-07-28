from __future__ import annotations

import struct
import sys
import urllib.parse
from base64 import b64decode, b64encode
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, TypeVar

from pyqwest import Headers as HTTPHeaders

from ._compression import IdentityCompression, negotiate_compression
from ._envelope import EnvelopeReader, EnvelopeWriter
from ._gen.google.rpc.status_pb import Status
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
from .request import Headers, RequestContext

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pyqwest import Response, SyncResponse

    from ._codec import Codec
    from ._compression import Compression
    from .method import MethodInfo

REQ = TypeVar("REQ")
RES = TypeVar("RES")

GRPC_CONTENT_TYPE_DEFAULT = "application/grpc"
GRPC_CONTENT_TYPE_PREFIX = f"{GRPC_CONTENT_TYPE_DEFAULT}+"
GRPC_WEB_CONTENT_TYPE_DEFAULT = "application/grpc-web"
GRPC_WEB_CONTENT_TYPE_PREFIX = f"{GRPC_WEB_CONTENT_TYPE_DEFAULT}+"

GRPC_HEADER_TIMEOUT = "grpc-timeout"
GRPC_HEADER_COMPRESSION = "grpc-encoding"
GRPC_HEADER_ACCEPT_COMPRESSION = "grpc-accept-encoding"

_DEFAULT_GRPC_USER_AGENT = f"grpc-python-connect/{__version__} ({sys.version})"


class GRPCServerProtocol:
    def create_request_context(
        self,
        method: MethodInfo[REQ, RES],
        http_method: str,
        http_scheme: str,
        headers: Headers,
        client_address: str | None = None,
    ) -> RequestContext[REQ, RES]:
        if http_method != "POST":
            raise HTTPException(HTTPStatus.METHOD_NOT_ALLOWED, [("allow", "POST")])

        timeout_header = headers.get(GRPC_HEADER_TIMEOUT)
        timeout_ms = _parse_timeout(timeout_header) if timeout_header else None

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
        return GRPCEnvelopeWriter(codec, compression)

    def uses_trailers(self) -> bool:
        return True

    def content_type(self, codec: Codec) -> str:
        return f"{GRPC_CONTENT_TYPE_PREFIX}{codec.name()}"

    def compression_header_name(self) -> str:
        return GRPC_HEADER_COMPRESSION

    def codec_name_from_content_type(self, content_type: str, *, stream: bool) -> str:  # noqa: ARG002
        if content_type.startswith(GRPC_CONTENT_TYPE_PREFIX):
            return content_type[len(GRPC_CONTENT_TYPE_PREFIX) :]
        return "proto"

    def negotiate_stream_compression(
        self, headers: Headers, compressions: dict[str, Compression]
    ) -> tuple[Compression | None, Compression]:
        req_compression_name = headers.get(GRPC_HEADER_COMPRESSION, "identity")
        req_compression = compressions.get(req_compression_name)
        accept_compression = headers.get(GRPC_HEADER_ACCEPT_COMPRESSION, "")
        resp_compression = negotiate_compression(accept_compression, compressions)
        return req_compression, resp_compression


class GRPCWebServerProtocol(GRPCServerProtocol):
    def uses_trailers(self) -> bool:
        return False

    def content_type(self, codec: Codec) -> str:
        return f"{GRPC_WEB_CONTENT_TYPE_PREFIX}{codec.name()}"

    def codec_name_from_content_type(self, content_type: str, *, stream: bool) -> str:  # noqa: ARG002
        if content_type.startswith(GRPC_WEB_CONTENT_TYPE_PREFIX):
            return content_type[len(GRPC_WEB_CONTENT_TYPE_PREFIX) :]
        return "proto"

    def create_envelope_writer(
        self, codec: Codec[RES, Any], compression: Compression | None
    ) -> EnvelopeWriter[RES]:
        return GRPCWebEnvelopeWriter(codec, compression)


def _parse_timeout(timeout: str) -> int:
    # We normalize to int milliseconds matching connect's timeout header.
    value_to_ms = _lookup_timeout_unit(timeout[-1])
    try:
        value = int(timeout[:-1])
    except ValueError as e:
        msg = f"protocol error: invalid timeout '{timeout}'"
        raise ValueError(msg) from e

    # timeout must be ASCII string of at most 8 digits
    if value > 99999999:
        msg = f"protocol error: timeout '{timeout}' is too long"
        raise ValueError(msg)

    return int(value * value_to_ms)


def _lookup_timeout_unit(unit: str) -> float:
    match unit:
        case "H":
            return 60 * 60 * 1000
        case "M":
            return 60 * 1000
        case "S":
            return 1 * 1000
        case "m":
            return 1
        case "u":
            return 1 / 1000
        case "n":
            return 1 / 1000 / 1000
        case _:
            msg = f"protocol error: timeout has invalid unit '{unit}'"
            raise ValueError(msg)


class GRPCEnvelopeWriter(EnvelopeWriter):
    def end(self, user_trailers: Headers, error: ConnectWireError | None) -> Headers:
        trailers = Headers(list(user_trailers.allitems()))
        if error:
            status = _connect_status_to_grpc[error.code]
            trailers["grpc-status"] = status
            message = error.message
            if message:
                message = urllib.parse.quote(message, safe="")
                trailers["grpc-message"] = message
            if error.details:
                grpc_status = Status(
                    code=int(status),
                    message=error.message,
                    details=[d._any for d in error.details],  # noqa: SLF001
                )
                grpc_status_bin = (
                    b64encode(grpc_status.to_binary()).decode().rstrip("=")
                )
                trailers["grpc-status-details-bin"] = grpc_status_bin
        else:
            trailers["grpc-status"] = "0"
        return trailers


class GRPCWebEnvelopeWriter(GRPCEnvelopeWriter):
    def end(self, user_trailers: Headers, error: ConnectWireError | None) -> bytes:  # ty: ignore[invalid-method-override]
        trailers = super().end(user_trailers, error)
        data = "".join(f"{k}: {v}\r\n" for k, v in trailers.allitems()).encode()
        if self._compression:
            data = self._compression.compress(data)
        prefix = self._prefix | 0b10000000
        return struct.pack(">BI", prefix, len(data)) + data


class GRPCClientProtocol:
    def __init__(self) -> None:
        self._content_type = GRPC_CONTENT_TYPE_DEFAULT
        self._content_type_prefix = GRPC_CONTENT_TYPE_PREFIX

    def create_request_context(
        self,
        *,
        method: MethodInfo[REQ, RES],
        url: str,
        http_method: str,
        user_headers: Headers | Mapping[str, str] | None,
        timeout_ms: int | None,
        codec: Codec,
        stream: bool,  # noqa: ARG002
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
        headers["te"] = "trailers"
        if "user-agent" not in headers:
            headers["user-agent"] = _DEFAULT_GRPC_USER_AGENT

        headers[GRPC_HEADER_ACCEPT_COMPRESSION] = accept_compression
        if send_compression is not None:
            headers[GRPC_HEADER_COMPRESSION] = send_compression.name()
        else:
            headers.pop(GRPC_HEADER_COMPRESSION, None)
        headers["content-type"] = f"{self._content_type_prefix}{codec.name()}"
        if timeout_ms is not None:
            headers[GRPC_HEADER_TIMEOUT] = _serialize_timeout(timeout_ms)

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
        raise NotImplementedError

    def validate_stream_response(
        self, request_codec_name: str, response_content_type: str
    ) -> None:
        if not (
            response_content_type == self._content_type
            or response_content_type.startswith(self._content_type_prefix)
        ):
            raise ConnectError(
                Code.UNKNOWN,
                f"invalid content-type: '{response_content_type}'; expecting '{self._content_type_prefix}{request_codec_name}'",
            )
        if response_content_type.startswith(self._content_type_prefix):
            response_codec_name = response_content_type[
                len(self._content_type_prefix) :
            ]
        else:
            response_codec_name = "proto"
        if response_codec_name != request_codec_name:
            raise ConnectError(
                Code.INTERNAL,
                f"invalid content-type: '{response_content_type}'; expecting '{self._content_type_prefix}{request_codec_name}'",
            )

    def handle_response_compression(
        self,
        headers: HTTPHeaders,
        compressions: dict[str, Compression],
        *,
        stream: bool,  # noqa: ARG002
    ) -> Compression:
        encoding = headers.get(GRPC_HEADER_COMPRESSION)
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
        return GRPCEnvelopeReader(message_class, codec, compression, read_max_bytes)


class GRPCWebClientProtocol(GRPCClientProtocol):
    def __init__(self) -> None:
        self._content_type = GRPC_WEB_CONTENT_TYPE_DEFAULT
        self._content_type_prefix = GRPC_WEB_CONTENT_TYPE_PREFIX

    def create_envelope_reader(
        self,
        message_class: type[RES],
        codec: Codec,
        compression: Compression,
        read_max_bytes: int | None,
    ) -> EnvelopeReader[RES]:
        return GRPCWebEnvelopeReader(message_class, codec, compression, read_max_bytes)


class GRPCEnvelopeReader(EnvelopeReader[RES]):
    def __init__(
        self,
        message_class: type[RES],
        codec: Codec,
        compression: Compression,
        read_max_bytes: int | None,
    ) -> None:
        super().__init__(message_class, codec, compression, read_max_bytes)
        self._read_message = False

    def handle_end_message(
        self, _prefix_byte: int, _message_data: bytes | bytearray, /
    ) -> bool:
        # It's coincidence that this method is called when there is a body and not
        # when there isn't. Somewhat hacky, but easiest way to handle the case
        # where there is a body and no trailers, which conformance tests verify.
        self._read_message = True
        return False

    def get_response_trailers(
        self, response: Response | SyncResponse, /
    ) -> HTTPHeaders:
        return response.trailers

    def handle_response_complete(
        self, response: Response | SyncResponse, e: ConnectError | None = None
    ) -> None:
        # Get the actual HTTP trailers
        trailers = self.get_response_trailers(response)

        # gRPC trailers are either the HTTP trailers if there was body present
        # or HTTP headers if there was no body.
        grpc_status = trailers.get("grpc-status")
        if grpc_status is None:
            # If there was a body message, we do not read response headers
            if self._read_message:
                raise e or ConnectError(Code.INTERNAL, "missing grpc-status trailer")
            trailers = response.headers

        handle_response_trailers(trailers)

        grpc_status = trailers.get("grpc-status")
        if grpc_status is None:
            raise e or ConnectError(Code.INTERNAL, "missing grpc-status trailer")

        # e is present for RST_STREAM. We prioritize its code while reading message and details
        # from trailers when available.
        code = e.code if e else None
        if grpc_status != "0":
            message = trailers.get("grpc-message", "")
            if grpc_status_details := trailers.get("grpc-status-details-bin"):
                status = Status.from_binary(b64decode(grpc_status_details + "==="))
                connect_code = code or _grpc_status_to_connect.get(
                    str(status.code), Code.UNKNOWN
                )
                raise ConnectError(connect_code, status.message, details=status.details)
            connect_code = code or _grpc_status_to_connect.get(
                grpc_status, Code.UNKNOWN
            )
            raise ConnectError(connect_code, urllib.parse.unquote(message))


class GRPCWebEnvelopeReader(GRPCEnvelopeReader):
    def __init__(
        self,
        message_class: type[RES],
        codec: Codec,
        compression: Compression,
        read_max_bytes: int | None,
    ) -> None:
        super().__init__(message_class, codec, compression, read_max_bytes)
        self._trailers = HTTPHeaders()

    def handle_end_message(
        self, prefix_byte: int, message_data: bytes | bytearray
    ) -> bool:
        super().handle_end_message(prefix_byte, message_data)
        end_stream = prefix_byte & 0b10000000 != 0
        if not end_stream:
            return False
        for line in message_data.split(b"\r\n"):
            if not line:
                continue
            key, value = line.split(b":", 1)
            self._trailers.add(key.decode().strip(), value.decode().strip())
        return True

    def get_response_trailers(
        self, _response: Response | SyncResponse, /
    ) -> HTTPHeaders:
        return self._trailers


_GRPC_TIMEOUT_MAX_VALUE = 1e8


def _serialize_timeout(timeout_ms: int) -> str:
    if timeout_ms <= 0:
        return "0n"

    # The gRPC protocol limits timeouts to 8 characters (not counting the unit),
    # so timeouts must be strictly less than 1e8 of the appropriate unit.

    if timeout_ms < _GRPC_TIMEOUT_MAX_VALUE:
        size, unit = 1, "m"
    elif timeout_ms < _GRPC_TIMEOUT_MAX_VALUE * 1000:
        size, unit = 1 * 1000, "S"
    elif timeout_ms < _GRPC_TIMEOUT_MAX_VALUE * 60 * 1000:
        size, unit = 60 * 1000, "M"
    else:
        size, unit = 60 * 60 * 1000, "H"

    return f"{timeout_ms // size}{unit}"


_connect_status_to_grpc = {
    Code.CANCELED: "1",
    Code.UNKNOWN: "2",
    Code.INVALID_ARGUMENT: "3",
    Code.DEADLINE_EXCEEDED: "4",
    Code.NOT_FOUND: "5",
    Code.ALREADY_EXISTS: "6",
    Code.PERMISSION_DENIED: "7",
    Code.RESOURCE_EXHAUSTED: "8",
    Code.FAILED_PRECONDITION: "9",
    Code.ABORTED: "10",
    Code.OUT_OF_RANGE: "11",
    Code.UNIMPLEMENTED: "12",
    Code.INTERNAL: "13",
    Code.UNAVAILABLE: "14",
    Code.DATA_LOSS: "15",
    Code.UNAUTHENTICATED: "16",
}

_grpc_status_to_connect = {v: k for k, v in _connect_status_to_grpc.items()}

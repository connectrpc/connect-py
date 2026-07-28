from __future__ import annotations

import json
from base64 import b64decode, b64encode
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Protocol, TypeVar, cast

from protobuf import message_to_json_value
from protobuf.wkt import Any

from ._compression import Compression
from .code import Code
from .errors import ConnectError, ErrorDetail

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from pyqwest import FullResponse, Headers as HTTPHeaders

    from ._codec import Codec
    from ._compression import Compression
    from ._envelope import EnvelopeReader, EnvelopeWriter
    from .method import MethodInfo
    from .request import Headers, RequestContext

REQ = TypeVar("REQ")
RES = TypeVar("RES")
T = TypeVar("T")


# Define a custom class for HTTP Status to allow adding 499 status code
@dataclass(frozen=True)
class ExtendedHTTPStatus:
    code: int
    reason: str

    @staticmethod
    def from_http_status(status: HTTPStatus) -> ExtendedHTTPStatus:
        return ExtendedHTTPStatus(code=status.value, reason=status.phrase)


# Dedupe statuses that are mapped multiple times
_BAD_REQUEST = ExtendedHTTPStatus.from_http_status(HTTPStatus.BAD_REQUEST)
_CONFLICT = ExtendedHTTPStatus.from_http_status(HTTPStatus.CONFLICT)
_INTERNAL_SERVER_ERROR = ExtendedHTTPStatus.from_http_status(
    HTTPStatus.INTERNAL_SERVER_ERROR
)

_error_to_http_status = {
    Code.CANCELED: ExtendedHTTPStatus(499, "Client Closed Request"),
    Code.UNKNOWN: _INTERNAL_SERVER_ERROR,
    Code.INVALID_ARGUMENT: _BAD_REQUEST,
    Code.DEADLINE_EXCEEDED: ExtendedHTTPStatus.from_http_status(
        HTTPStatus.GATEWAY_TIMEOUT
    ),
    Code.NOT_FOUND: ExtendedHTTPStatus.from_http_status(HTTPStatus.NOT_FOUND),
    Code.ALREADY_EXISTS: _CONFLICT,
    Code.PERMISSION_DENIED: ExtendedHTTPStatus.from_http_status(HTTPStatus.FORBIDDEN),
    Code.RESOURCE_EXHAUSTED: ExtendedHTTPStatus.from_http_status(
        HTTPStatus.TOO_MANY_REQUESTS
    ),
    Code.FAILED_PRECONDITION: _BAD_REQUEST,
    Code.ABORTED: _CONFLICT,
    Code.OUT_OF_RANGE: _BAD_REQUEST,
    Code.UNIMPLEMENTED: ExtendedHTTPStatus.from_http_status(HTTPStatus.NOT_IMPLEMENTED),
    Code.INTERNAL: _INTERNAL_SERVER_ERROR,
    Code.UNAVAILABLE: ExtendedHTTPStatus.from_http_status(
        HTTPStatus.SERVICE_UNAVAILABLE
    ),
    Code.DATA_LOSS: _INTERNAL_SERVER_ERROR,
    Code.UNAUTHENTICATED: ExtendedHTTPStatus.from_http_status(HTTPStatus.UNAUTHORIZED),
}


_http_status_code_to_error = {
    400: Code.INTERNAL,
    401: Code.UNAUTHENTICATED,
    403: Code.PERMISSION_DENIED,
    404: Code.UNIMPLEMENTED,
    429: Code.UNAVAILABLE,
    502: Code.UNAVAILABLE,
    503: Code.UNAVAILABLE,
    504: Code.UNAVAILABLE,
}


@dataclass(frozen=True)
class ConnectWireError:
    code: Code
    message: str
    details: Sequence[ErrorDetail]

    @staticmethod
    def from_exception(exc: Exception) -> ConnectWireError:
        if isinstance(exc, ConnectError):
            return ConnectWireError(exc.code, exc.message, exc.details)
        return ConnectWireError(Code.UNKNOWN, str(exc), details=())

    @staticmethod
    def from_response(response: FullResponse) -> ConnectWireError:
        try:
            data = response.json()
        except Exception:  # noqa: BLE001
            data = None
        if isinstance(data, dict):
            return ConnectWireError.from_dict(data, response.status, Code.UNAVAILABLE)
        return ConnectWireError.from_http_status(response.status)

    @staticmethod
    def from_dict(
        data: dict, http_status: int, unexpected_code: Code
    ) -> ConnectWireError:
        code_str = data.get("code")
        if code_str:
            try:
                code = Code(code_str)
            except ValueError:
                code = unexpected_code
        else:
            code = _http_status_code_to_error.get(http_status, Code.UNKNOWN)
        message = data.get("message", "")
        details: Sequence[ErrorDetail] = ()
        details_json = cast("list[dict[str, str]] | None", data.get("details"))
        if details_json:
            details = []
            for detail in details_json:
                detail_type = detail.get("type")
                detail_value = detail.get("value")
                if detail_type is None or detail_value is None:
                    # Ignore malformed details
                    continue
                details.append(
                    ErrorDetail(
                        Any(
                            type_url="type.googleapis.com/" + detail_type,
                            value=b64decode(detail_value + "==="),
                        )
                    )
                )
        return ConnectWireError(code, message, details)

    @staticmethod
    def from_http_status(status_code: int) -> ConnectWireError:
        code = _http_status_code_to_error.get(status_code, Code.UNKNOWN)
        try:
            http_status = HTTPStatus(status_code)
            message = http_status.phrase
        except ValueError:
            message = "Client Closed Request" if status_code == 499 else ""
        return ConnectWireError(code, message, details=())

    def to_exception(self) -> ConnectError:
        return ConnectError(self.code, self.message, details=self.details)

    def to_http_status(self) -> ExtendedHTTPStatus:
        return _error_to_http_status.get(self.code, _INTERNAL_SERVER_ERROR)

    def to_dict(self) -> dict:
        data: dict = {"code": self.code.value, "message": self.message}
        if self.details:
            details: list[dict] = []
            for detail in self.details:
                detail_dict: dict = {
                    "type": detail.type_name,
                    # Connect requires unpadded base64
                    "value": b64encode(detail.message_bytes)
                    .decode("utf-8")
                    .rstrip("="),
                }
                # Try to produce debug info, but expect failure when we don't
                # have descriptors for the message type.
                if debug := detail.value():
                    try:
                        debug_value = message_to_json_value(debug)
                    except Exception:  # noqa: BLE001, S110
                        pass
                    else:
                        detail_dict["debug"] = debug_value
                details.append(detail_dict)
            data["details"] = details
        return data

    def to_json_bytes(self) -> bytes:
        return json.dumps(self.to_dict()).encode("utf-8")


class ServerProtocol(Protocol):
    def create_request_context(
        self,
        method: MethodInfo[REQ, RES],
        http_method: str,
        http_scheme: str,
        headers: Headers,
        client_address: str | None = None,
    ) -> RequestContext[REQ, RES]:
        """Create a RequestContext from the HTTP method and headers."""
        ...

    def create_envelope_writer(
        self, codec: Codec[T, Any], compression: Compression | None
    ) -> EnvelopeWriter[T]:
        """Create the EnvelopeWriter to write response messages."""
        ...

    def uses_trailers(self) -> bool:
        """Return whether the protocol uses trailers for status reporting."""
        ...

    def content_type(self, codec: Codec) -> str:
        """Return the content type for the given codec."""
        ...

    def compression_header_name(self) -> str:
        """Return the compression header name and value."""
        ...

    def codec_name_from_content_type(self, content_type: str, *, stream: bool) -> str:
        """Extract the codec name from the content type."""
        ...

    def negotiate_stream_compression(
        self, headers: Headers, compressions: dict[str, Compression]
    ) -> tuple[Compression | None, Compression]:
        """Negotiates request and response compression based on headers."""
        ...


class ClientProtocol(Protocol):
    def create_request_context(
        self,
        *,
        method: MethodInfo[REQ, RES],
        address: str,
        http_method: str,
        user_headers: Headers | Mapping[str, str] | None,
        timeout_ms: int | None,
        codec: Codec,
        stream: bool,
        accept_compression: str,
        send_compression: Compression | None,
    ) -> RequestContext[REQ, RES]:
        """Create a RequestContext for the given method and headers."""
        ...

    def validate_response(
        self, request_codec_name: str, status_code: int, response_content_type: str
    ) -> None:
        """Validate a unary response."""
        ...

    def validate_stream_response(
        self, request_codec_name: str, response_content_type: str
    ) -> None:
        """Validate a streaming response."""
        ...

    def handle_response_compression(
        self, headers: HTTPHeaders, *, stream: bool
    ) -> Compression:
        """Handle response compression based on the response headers."""
        ...

    def create_envelope_reader(
        self,
        message_class: type[RES],
        codec: Codec,
        compression: Compression,
        read_max_bytes: int | None,
    ) -> EnvelopeReader[RES]:
        """Create the EnvelopeReader to read response messages."""
        ...


class HTTPError(Exception):
    """An HTTP exception returned directly before starting the connect protocol."""

    def __init__(self, status: HTTPStatus, headers: list[tuple[str, str]]) -> None:
        self.status = status
        self.headers = headers


def host_to_server_address(host: str | None, http_scheme: str) -> str | None:
    if host is None:
        return None
    if ":" not in host:
        match http_scheme:
            case "https":
                host += ":443"
            case "http":
                host += ":80"
    return host


def url_to_server_address(address: str) -> str | None:
    if address.startswith("https://"):
        scheme = "https"
        address = address[len("https://") :]
    else:
        scheme = "http"
        address = address[len("http://") :]

    return host_to_server_address(address, scheme)

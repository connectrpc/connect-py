from __future__ import annotations

import base64
from typing import TYPE_CHECKING, TypeVar
from urllib.parse import urlencode

from pyqwest import FullResponse, Headers as HTTPHeaders, StreamError, StreamErrorCode

from ._codec import proto_binary_codec
from ._compression import IdentityCompression, resolve_compressions
from ._protocol import ConnectWireError
from ._protocol_connect import CONNECT_PROTOCOL_VERSION, ConnectClientProtocol
from ._protocol_grpc import GRPCClientProtocol, GRPCWebClientProtocol
from ._shared import decode_message, message_too_large_error
from .code import Code
from .errors import ConnectError
from .protocol import ProtocolType

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from ._codec import Codec
    from ._envelope import EnvelopeReader
    from .compression import Compression
    from .method import MethodInfo
    from .request import Headers, RequestContext


REQ = TypeVar("REQ")
RES = TypeVar("RES")


class ClientBase:
    """Configuration and I/O-free request handling shared by the sync and async clients."""

    def __init__(
        self,
        *,
        address: str,
        codec: Codec | None,
        protocol: ProtocolType,
        accept_compression: Iterable[Compression] | None,
        send_compression: Compression | None,
        timeout_ms: int | None,
        read_max_bytes: int | None,
    ) -> None:
        self._address = address
        self._codec = codec or proto_binary_codec()
        self._timeout_ms = timeout_ms
        self._read_max_bytes = read_max_bytes
        self._response_compressions = resolve_compressions(accept_compression)
        self._accept_compression_header = ",".join(self._response_compressions.keys())
        self._send_compression = send_compression or IdentityCompression()
        self._protocol: ConnectClientProtocol | GRPCClientProtocol
        match protocol:
            case ProtocolType.CONNECT:
                self._protocol = ConnectClientProtocol()
            case ProtocolType.GRPC:
                self._protocol = GRPCClientProtocol()
            case ProtocolType.GRPC_WEB:
                self._protocol = GRPCWebClientProtocol()

    def _new_request_context(
        self,
        method: MethodInfo[REQ, RES],
        headers: Headers | Mapping[str, str] | None,
        timeout_ms: int | None,
        *,
        stream: bool,
        use_get: bool = False,
    ) -> RequestContext[REQ, RES]:
        return self._protocol.create_request_context(
            method=method,
            url=self._address,
            http_method="GET" if use_get else "POST",
            user_headers=headers,
            timeout_ms=self._timeout_ms if timeout_ms is None else timeout_ms,
            codec=self._codec,
            stream=stream,
            accept_compression=self._accept_compression_header,
            send_compression=self._send_compression,
        )

    def _unary_uses_stream(self) -> bool:
        return isinstance(self._protocol, GRPCClientProtocol)

    def _url(self, ctx: RequestContext) -> str:
        return f"{self._address}/{ctx.method.service_name}/{ctx.method.name}"

    def _prepare_unary(
        self, request: object, ctx: RequestContext
    ) -> tuple[str, HTTPHeaders, bytes | None]:
        """Return the URL, headers, and body of a Connect unary request.

        The body is None for a GET request, whose message is in the URL.
        """
        headers = HTTPHeaders(ctx.request_headers.allitems())
        url = self._url(ctx)
        data = self._send_compression.compress(self._codec.encode(request))
        if ctx.http_method != "GET":
            return url, headers, data
        params = prepare_get_params(self._codec, data, headers)
        headers.pop("content-type", None)
        return f"{url}?{urlencode(params)}", headers, None

    def _stream_reader(
        self, headers: HTTPHeaders, ctx: RequestContext[REQ, RES]
    ) -> EnvelopeReader[RES]:
        """Return the reader for the messages of a successful streaming response."""
        self._protocol.validate_stream_response(
            self._codec.name(), headers.get("content-type", "")
        )
        compression = self._protocol.handle_response_compression(
            headers, self._response_compressions, stream=True
        )
        return self._protocol.create_envelope_reader(
            ctx.method.output, self._codec, compression, self._read_max_bytes
        )

    def _finish_unary(self, resp: FullResponse, ctx: RequestContext[REQ, RES]) -> RES:
        """Validate a Connect unary response and decode its message or error."""
        self._protocol.validate_response(
            self._codec.name(), resp.status, resp.headers.get("content-type", "")
        )
        # Decompression itself is handled by pyqwest, but we validate it
        # by resolving it.
        self._protocol.handle_response_compression(
            resp.headers, self._response_compressions, stream=False
        )
        if (
            self._read_max_bytes is not None
            and len(resp.content) > self._read_max_bytes
        ):
            raise message_too_large_error(self._read_max_bytes)
        if resp.status == 200:
            return decode_message(self._codec, resp.content, ctx.method.output)
        raise ConnectWireError.from_response(resp).to_exception()


def timeout_seconds(ctx: RequestContext) -> float | None:
    """Return the remaining timeout of ctx in seconds, raising if it has expired."""
    if (timeout_ms := ctx.timeout_ms) is None:
        return None
    if timeout_ms <= 0:
        raise ConnectError(Code.DEADLINE_EXCEEDED, "Request timed out")
    return timeout_ms / 1000.0


def stream_status_error(
    status: int, headers: HTTPHeaders, content: bytes, trailers: HTTPHeaders
) -> ConnectError:
    """Return the error for a streaming response with a non-200 status."""
    resp = FullResponse(
        status=status, headers=headers, content=content, trailers=trailers
    )
    return ConnectWireError.from_response(resp).to_exception()


def prepare_get_params(
    codec: Codec, request_data: bytes, headers: HTTPHeaders
) -> dict[str, str]:
    # Follow order in spec: https://connectrpc.com/docs/protocol/#unary-get-request
    params: dict[str, str] = {"connect": f"v{CONNECT_PROTOCOL_VERSION}", "base64": "1"}
    if "content-encoding" in headers:
        params["compression"] = headers.pop("content-encoding")
    params["encoding"] = codec.name()
    params["message"] = base64.urlsafe_b64encode(request_data).decode("ascii")
    return params


# https://github.com/connectrpc/connect-go/blob/59cc6973156cd9164d6bea493b1d106ed894f2df/error.go#L393
def maybe_map_stream_reset(
    e: Exception, ctx: RequestContext[REQ, RES]
) -> ConnectError | None:
    if not isinstance(e, StreamError):
        return None

    msg = str(e)
    match e.code:
        case (
            StreamErrorCode.NO_ERROR
            | StreamErrorCode.PROTOCOL_ERROR
            | StreamErrorCode.INTERNAL_ERROR
            | StreamErrorCode.FLOW_CONTROL_ERROR
            | StreamErrorCode.SETTINGS_TIMEOUT
            | StreamErrorCode.FRAME_SIZE_ERROR
            | StreamErrorCode.COMPRESSION_ERROR
            | StreamErrorCode.CONNECT_ERROR
        ):
            return ConnectError(Code.INTERNAL, msg)
        case StreamErrorCode.REFUSED_STREAM:
            return ConnectError(Code.UNAVAILABLE, msg)
        case StreamErrorCode.CANCEL:
            # Some servers use CANCEL when deadline expires. We can't differentiate
            # that from normal cancel without checking our own deadline.
            if (t := ctx.timeout_ms) is not None and t <= 0:
                return ConnectError(Code.DEADLINE_EXCEEDED, msg)
            return ConnectError(Code.CANCELED, msg)
        case StreamErrorCode.ENHANCE_YOUR_CALM:
            return ConnectError(Code.RESOURCE_EXHAUSTED, f"Bandwidth exhausted: {msg}")
        case StreamErrorCode.INADEQUATE_SECURITY:
            return ConnectError(
                Code.PERMISSION_DENIED, f"Transport protocol insecure: {msg}"
            )

    return None

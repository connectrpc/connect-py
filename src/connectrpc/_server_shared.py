from __future__ import annotations

import base64
from dataclasses import dataclass, replace
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Protocol, TypeVar

from ._codec import get_default_codecs
from ._compression import (
    negotiate_compression,
    resolve_compressions,
    unknown_compression_error,
)
from ._protocol import ConnectWireError, HTTPError
from ._protocol_connect import CONNECT_UNARY_CONTENT_TYPE_PREFIX
from ._protocol_grpc import GRPCServerProtocol
from ._shared import (
    chain_interceptors,
    decode_message,
    decompress,
    message_too_large_error,
)
from .code import Code
from .errors import ConnectError

if TYPE_CHECKING:
    from collections.abc import (
        AsyncIterator,
        Awaitable,
        Callable,
        Iterable,
        Iterator,
        Mapping,
        Sequence,
    )

    from ._codec import Codec
    from ._protocol import ServerProtocol
    from .compression import Compression
    from .method import MethodInfo
    from .request import Headers, RequestContext

REQ = TypeVar("REQ")
RES = TypeVar("RES")
T = TypeVar("T")
U = TypeVar("U")


class _EndpointDataclass(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Any]]

    @property
    def function(self) -> Callable[..., Any]: ...


E = TypeVar("E", bound=_EndpointDataclass)

DEFAULT_READ_MAX_BYTES = 1024 * 1024 * 4  # 4 MB
"""The default maximum size of a request message (4MB)."""


@dataclass(kw_only=True, frozen=True, slots=True)
class Endpoint(Generic[REQ, RES]):
    """Represents an endpoint in a service.

    Attributes:
        method: The method to map to the RPC function.

    """

    method: MethodInfo[REQ, RES]

    @staticmethod
    def unary(
        method: MethodInfo[T, U],
        function: Callable[[T, RequestContext[T, U]], Awaitable[U]],
    ) -> EndpointUnary[T, U]:
        return EndpointUnary(method=method, function=function)

    @staticmethod
    def client_stream(
        method: MethodInfo[T, U],
        function: Callable[[AsyncIterator[T], RequestContext[T, U]], Awaitable[U]],
    ) -> EndpointClientStream[T, U]:
        return EndpointClientStream(method=method, function=function)

    @staticmethod
    def server_stream(
        method: MethodInfo[T, U],
        function: Callable[[T, RequestContext[T, U]], AsyncIterator[U]],
    ) -> EndpointServerStream[T, U]:
        return EndpointServerStream(method=method, function=function)

    @staticmethod
    def bidi_stream(
        method: MethodInfo[T, U],
        function: Callable[[AsyncIterator[T], RequestContext[T, U]], AsyncIterator[U]],
    ) -> EndpointBidiStream[T, U]:
        return EndpointBidiStream(method=method, function=function)


@dataclass(kw_only=True, frozen=True, slots=True)
class EndpointUnary(Endpoint[REQ, RES]):
    function: Callable[[REQ, RequestContext[REQ, RES]], Awaitable[RES]]


@dataclass(kw_only=True, frozen=True, slots=True)
class EndpointClientStream(Endpoint[REQ, RES]):
    function: Callable[[AsyncIterator[REQ], RequestContext[REQ, RES]], Awaitable[RES]]


@dataclass(kw_only=True, frozen=True, slots=True)
class EndpointServerStream(Endpoint[REQ, RES]):
    function: Callable[[REQ, RequestContext[REQ, RES]], AsyncIterator[RES]]


@dataclass(kw_only=True, frozen=True, slots=True)
class EndpointBidiStream(Endpoint[REQ, RES]):
    function: Callable[
        [AsyncIterator[REQ], RequestContext[REQ, RES]], AsyncIterator[RES]
    ]


@dataclass(kw_only=True, frozen=True, slots=True)
class EndpointSync(Generic[REQ, RES]):
    """Represents a sync endpoint in a service.

    Attributes:
        method: The method to map to the RPC function.

    """

    method: MethodInfo[REQ, RES]

    @staticmethod
    def unary(
        *, method: MethodInfo[T, U], function: Callable[[T, RequestContext[T, U]], U]
    ) -> EndpointUnarySync[T, U]:
        return EndpointUnarySync(method=method, function=function)

    @staticmethod
    def client_stream(
        *,
        method: MethodInfo[T, U],
        function: Callable[[Iterator[T], RequestContext[T, U]], U],
    ) -> EndpointClientStreamSync[T, U]:
        return EndpointClientStreamSync(method=method, function=function)

    @staticmethod
    def server_stream(
        *,
        method: MethodInfo[T, U],
        function: Callable[[T, RequestContext[T, U]], Iterator[U]],
    ) -> EndpointServerStreamSync[T, U]:
        return EndpointServerStreamSync(method=method, function=function)

    @staticmethod
    def bidi_stream(
        method: MethodInfo[T, U],
        function: Callable[[Iterator[T], RequestContext[T, U]], Iterator[U]],
    ) -> EndpointBidiStreamSync[T, U]:
        return EndpointBidiStreamSync(method=method, function=function)


@dataclass(kw_only=True, frozen=True, slots=True)
class EndpointUnarySync(EndpointSync[REQ, RES]):
    function: Callable[[REQ, RequestContext[REQ, RES]], RES]


@dataclass(kw_only=True, frozen=True, slots=True)
class EndpointClientStreamSync(EndpointSync[REQ, RES]):
    function: Callable[[Iterator[REQ], RequestContext[REQ, RES]], RES]


@dataclass(kw_only=True, frozen=True, slots=True)
class EndpointServerStreamSync(EndpointSync[REQ, RES]):
    function: Callable[[REQ, RequestContext[REQ, RES]], Iterator[RES]]


@dataclass(kw_only=True, frozen=True, slots=True)
class EndpointBidiStreamSync(EndpointSync[REQ, RES]):
    function: Callable[[Iterator[REQ], RequestContext[REQ, RES]], Iterator[RES]]


class ServerBase:
    """Configuration and I/O-free request handling shared by the WSGI and ASGI applications."""

    def __init__(
        self,
        *,
        read_max_bytes: int | None,
        compressions: Iterable[Compression] | None,
        codecs: Iterable[Codec] | None,
    ) -> None:
        self._read_max_bytes = read_max_bytes
        self._compressions = resolve_compressions(compressions)
        codecs = codecs if codecs is not None else get_default_codecs()
        self._codecs = {codec.name(): codec for codec in codecs}

    def _resolve_codec(
        self,
        protocol: ServerProtocol,
        headers: Headers,
        query_params: Mapping[str, list[str]] | None,
        *,
        stream: bool,
    ) -> Codec:
        """Return the codec of the request, or raise 415 if it is not supported.

        query_params is set only for a GET request, whose codec is in the URL.
        """
        if query_params is not None:
            codec_name = query_params.get("encoding", ("",))[0]
        else:
            codec_name = protocol.codec_name_from_content_type(
                headers.get("content-type", ""), stream=stream
            )
        if codec := self._codecs.get(codec_name):
            return codec
        accept = (
            "application/connect+json, application/connect+proto"
            if stream
            else "application/json, application/proto"
        )
        raise HTTPError(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, [("Accept-Post", accept)])

    def _check_read_size(self, size: int) -> None:
        if self._read_max_bytes is not None and size > self._read_max_bytes:
            raise message_too_large_error(self._read_max_bytes)

    def _decode_unary_get(
        self, params: Mapping[str, list[str]], codec: Codec, message_class: type[T]
    ) -> T:
        """Decode the message of a Connect unary GET request from its query parameters."""
        if "message" not in params:
            raise ConnectError(
                Code.INVALID_ARGUMENT,
                "'message' parameter is required for GET requests",
            )
        message = params["message"][0]
        if params.get("base64", ("",))[0] == "1":
            try:
                data = base64.urlsafe_b64decode(message + "===")
            except Exception as e:
                raise ConnectError(
                    Code.INVALID_ARGUMENT, f"Invalid base64 encoding: {e!s}"
                ) from e
        else:
            data = message.encode("utf-8")
        compression_name = params.get("compression", ("",))[0]
        return self._decode_unary(data, compression_name, codec, message_class)

    def _decode_unary(
        self,
        data: bytes,
        compression_name: str | None,
        codec: Codec,
        message_class: type[T],
    ) -> T:
        """Decompress and decode the message of a Connect unary request."""
        compression_name = compression_name or "identity"
        compression = self._compressions.get(compression_name)
        if not compression:
            raise unknown_compression_error(compression_name, self._compressions)
        if data:
            data = decompress(compression, data, self._read_max_bytes)
        return decode_message(codec, data, message_class)

    def _unary_response_compression(self, headers: Headers) -> Compression:
        """Return the compression of a Connect unary response."""
        return negotiate_compression(
            headers.get("accept-encoding", ""), self._compressions
        )


def encode_unary(
    response: object, codec: Codec, compression: Compression, ctx: RequestContext
) -> tuple[list[tuple[str, str]], bytes]:
    """Return the headers and body of a successful Connect unary response."""
    body = compression.compress(codec.encode(response))
    headers = [
        ("content-type", f"{CONNECT_UNARY_CONTENT_TYPE_PREFIX}{codec.name()}"),
        ("content-encoding", compression.name()),
        ("vary", "Accept-Encoding"),
    ]
    headers.extend(context_headers(ctx))
    return headers, body


def error_response(
    exc: Exception, ctx: RequestContext | None, protocol: ServerProtocol | None
) -> tuple[int, str, list[tuple[str, str]], bytes]:
    """Return the status, reason, headers, and body of a response for exc."""
    if isinstance(protocol, GRPCServerProtocol) and not isinstance(exc, HTTPError):
        # gRPC clients read the status from trailers, so this is a trailers-only
        # response: HTTP 200 with the gRPC status in the headers.
        grpc_headers = protocol.trailers_only_headers(ctx, exc)
        return 200, "OK", list(grpc_headers.allitems()), b""
    if isinstance(exc, HTTPError):
        status, reason = exc.status.value, exc.status.phrase
        headers = list(exc.headers)
        body = b""
    else:
        wire_error = ConnectWireError.from_exception(exc)
        http_status = wire_error.to_http_status()
        status, reason = http_status.code, http_status.reason
        headers = [("content-type", "application/json")]
        body = wire_error.to_json_bytes()
    if ctx:
        headers.extend(context_headers(ctx))
    return status, reason, headers, body


def stream_response_headers(
    protocol: ServerProtocol,
    codec: Codec,
    compression: Compression,
    ctx: RequestContext,
) -> list[tuple[str, str]]:
    """Return the headers of a streaming response."""
    headers = [
        ("content-type", protocol.content_type(codec)),
        (protocol.compression_header_name(), compression.name()),
    ]
    headers.extend(ctx.response_headers.allitems())
    return headers


def context_headers(ctx: RequestContext) -> Iterator[tuple[str, str]]:
    """Return the response headers of ctx, with its trailers as `trailer-` headers."""
    yield from ctx.response_headers.allitems()
    for key, value in ctx.response_trailers.allitems():
        yield f"trailer-{key}", value


def apply_interceptors(
    endpoints: Mapping[str, E],
    interceptors: Sequence[object],
    kinds: Mapping[type, tuple[type, str]],
) -> Mapping[str, E]:
    """Wrap the function of each endpoint in interceptors.

    kinds maps each endpoint class to the interceptor class that applies to it and the
    name of that interceptor's method.
    """
    if not interceptors:
        return endpoints
    wrapped = {}
    for path, endpoint in endpoints.items():
        kind, method_name = kinds[type(endpoint)]
        function = chain_interceptors(
            endpoint.function, interceptors, kind, method_name
        )
        wrapped[path] = replace(endpoint, function=function)
    return wrapped

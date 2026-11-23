from __future__ import annotations

from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from protobuf.wkt.google.protobuf.descriptor_pb import MethodOptions

from ._protocol import HTTPError
from .code import Code
from .errors import ConnectError
from .method import IdempotencyLevel, MethodInfo, StreamType
from .protocol import ProtocolType

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator

    from protobuf import DescMethod

    from .request import RequestContext

REQ = TypeVar("REQ")
RES = TypeVar("RES")
T = TypeVar("T")
U = TypeVar("U")

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


def resolve_protocols(
    protocols: Iterable[ProtocolType] | None,
) -> frozenset[ProtocolType]:
    """Return the protocols to serve."""
    if protocols is None:
        return frozenset(ProtocolType) - {ProtocolType.CONNECT_WEBSOCKET}
    resolved = frozenset(protocols)
    if not resolved:
        msg = "protocols must not be empty"
        raise ValueError(msg)
    return resolved


def unserved_protocol_error(
    protocols: frozenset[ProtocolType], *, unary: bool
) -> HTTPError:
    """Return the error for a request in a protocol that is not served.

    Its Accept-Post header lists the content types of the protocols that
    are, as a hint to a client using the wrong one.
    """
    content_types: list[str] = []
    if ProtocolType.CONNECT in protocols:
        content_types += (
            ("application/json", "application/proto")
            if unary
            else ("application/connect+json", "application/connect+proto")
        )
    if ProtocolType.GRPC in protocols:
        content_types += (
            "application/grpc",
            "application/grpc+proto",
            "application/grpc+json",
        )
    if ProtocolType.GRPC_WEB in protocols:
        content_types += (
            "application/grpc-web",
            "application/grpc-web+proto",
            "application/grpc-web+json",
        )
    return HTTPError(
        HTTPStatus.UNSUPPORTED_MEDIA_TYPE, [("Accept-Post", ", ".join(content_types))]
    )


def check_deadline(ctx: RequestContext) -> None:
    """Raise deadline_exceeded if the request's deadline has already passed."""
    if (timeout_ms := ctx.timeout_ms) is not None and timeout_ms <= 0:
        raise ConnectError(Code.DEADLINE_EXCEEDED, "Request timed out")


def method_url(method: DescMethod) -> str:
    return f"/{method.parent.type_name}/{method.name}"


def method_info(method: DescMethod) -> MethodInfo[Any, Any]:
    """Return the MethodInfo for a method descriptor."""
    return MethodInfo(
        name=method.name,
        service_name=method.parent.type_name,
        input=method.input.type,
        output=method.output.type,
        idempotency_level=_idempotency_level(method),
        stream_type=_stream_type(method),
    )


def _idempotency_level(method: DescMethod) -> IdempotencyLevel:
    match method.idempotency:
        case MethodOptions.IdempotencyLevel.NO_SIDE_EFFECTS:
            return IdempotencyLevel.NO_SIDE_EFFECTS
        case MethodOptions.IdempotencyLevel.IDEMPOTENT:
            return IdempotencyLevel.IDEMPOTENT
        case _:
            return IdempotencyLevel.UNKNOWN


def _stream_type(method: DescMethod) -> StreamType:
    match method.method_kind:
        case "unary":
            return StreamType.UNARY
        case "client_streaming":
            return StreamType.CLIENT_STREAM
        case "server_streaming":
            return StreamType.SERVER_STREAM
        case "bidi_streaming":
            return StreamType.BIDI_STREAM

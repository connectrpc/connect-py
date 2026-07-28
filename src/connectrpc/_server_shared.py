from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterator

    from .method import MethodInfo
    from .request import RequestContext

REQ = TypeVar("REQ")
RES = TypeVar("RES")
T = TypeVar("T")
U = TypeVar("U")


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

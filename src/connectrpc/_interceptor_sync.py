from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Sequence

    from .request import RequestContext

REQ = TypeVar("REQ")
RES = TypeVar("RES")
T = TypeVar("T")


@runtime_checkable
class UnaryInterceptorSync(Protocol):
    """An interceptor of a synchronous unary RPC method."""

    def intercept_unary_sync(
        self,
        call_next: Callable[[REQ, RequestContext], RES],
        request: REQ,
        ctx: RequestContext,
        /,
    ) -> RES:
        """Intercept a unary RPC.

        Args:
            call_next: A callable to invoke to continue processing, either to another
                interceptor or the actual RPC. Generally will be called with the same
                request the interceptor received but the request can be replaced as
                needed. Can be skipped if returning a response from the interceptor
                directly.
            request: The request message.
            ctx: The request context.

        Returns:
            The response message.

        """
        ...


@runtime_checkable
class ClientStreamInterceptorSync(Protocol):
    """An interceptor of a synchronous client-streaming RPC method."""

    def intercept_client_stream_sync(
        self,
        call_next: Callable[[Iterator[REQ], RequestContext], RES],
        request: Iterator[REQ],
        ctx: RequestContext,
        /,
    ) -> RES:
        """Intercept a client-streaming RPC.

        Args:
            call_next: A callable to invoke to continue processing, either to another
                interceptor or the actual RPC. Generally will be called with the same
                request the interceptor received but the request can be replaced as
                needed. Can be skipped if returning a response from the interceptor
                directly.
            request: The request message iterator.
            ctx: The request context.

        Returns:
            The response message.

        """
        ...


@runtime_checkable
class ServerStreamInterceptorSync(Protocol):
    """An interceptor of a synchronous server-streaming RPC method."""

    def intercept_server_stream_sync(
        self,
        call_next: Callable[[REQ, RequestContext], Iterator[RES]],
        request: REQ,
        ctx: RequestContext,
        /,
    ) -> Iterator[RES]:
        """Intercept a server-streaming RPC.

        Args:
            call_next: A callable to invoke to continue processing, either to another
                interceptor or the actual RPC. Generally will be called with the same
                request the interceptor received but the request can be replaced as
                needed. Can be skipped if returning a response from the interceptor
                directly.
            request: The request message.
            ctx: The request context.

        Returns:
            The response message iterator.

        """
        ...


@runtime_checkable
class BidiStreamInterceptorSync(Protocol):
    """An interceptor of a synchronous bidirectional-streaming RPC method."""

    def intercept_bidi_stream_sync(
        self,
        call_next: Callable[[Iterator[REQ], RequestContext], Iterator[RES]],
        request: Iterator[REQ],
        ctx: RequestContext,
        /,
    ) -> Iterator[RES]:
        """Intercept a bidirectional-streaming RPC.

        Args:
            call_next: A callable to invoke to continue processing, either to another
                interceptor or the actual RPC. Generally will be called with the same
                request the interceptor received but the request can be replaced as
                needed. Can be skipped if returning a response from the interceptor
                directly.
            request: The request message iterator.
            ctx: The request context.

        Returns:
            The response message iterator.

        """
        ...


@runtime_checkable
class MetadataInterceptorSync(Protocol[T]):
    """An interceptor that can be applied to any type of method.

    Only metadata such as headers and trailers is accessible. To access request
    and response bodies of a method, instead use an interceptor corresponding to
    the type of method such as [UnaryInterceptorSync][].

    On servers, metadata interceptors at the front of the interceptor list are
    invoked before the request message is read or parsed, allowing logic such
    as authentication to reject a request without processing its body. A
    metadata interceptor following a message interceptor is invoked in order
    within the chain, after the request message is parsed.
    """

    def on_start_sync(self, ctx: RequestContext, /) -> T:
        """Handle the start of the RPC.

        The return value is passed to [on_end_sync][] as-is. For example, if
        measuring RPC invocation time, on_start_sync may return the current time.
        If a return value isn't needed or [on_end_sync][] won't be used, return
        None.
        """
        ...

    def on_end_sync(
        self,
        token: T,  # noqa: ARG002 # keep name clean for public API
        ctx: RequestContext,  # noqa: ARG002 # keep name clean for public API
        error: Exception | None,  # noqa: ARG002 # keep name clean for public API
        /,
    ) -> None:
        """Handle the end of the RPC."""
        return


InterceptorSync = (
    UnaryInterceptorSync
    | ClientStreamInterceptorSync
    | ServerStreamInterceptorSync
    | BidiStreamInterceptorSync
    | MetadataInterceptorSync
)
"""An interceptor to apply to a synchronous RPC server or client."""


class MetadataInterceptorInvokerSync(Generic[T]):
    _delegate: MetadataInterceptorSync[T]

    def __init__(self, delegate: MetadataInterceptorSync[T]) -> None:
        self._delegate = delegate

    def intercept_unary_sync(
        self,
        call_next: Callable[[REQ, RequestContext], RES],
        request: REQ,
        ctx: RequestContext,
    ) -> RES:
        token = self._delegate.on_start_sync(ctx)
        error: Exception | None = None
        try:
            return call_next(request, ctx)
        except Exception as e:
            error = e
            raise
        finally:
            self._delegate.on_end_sync(token, ctx, error)

    def intercept_client_stream_sync(
        self,
        call_next: Callable[[Iterator[REQ], RequestContext], RES],
        request: Iterator[REQ],
        ctx: RequestContext,
    ) -> RES:
        token = self._delegate.on_start_sync(ctx)
        error: Exception | None = None
        try:
            return call_next(request, ctx)
        except Exception as e:
            error = e
            raise
        finally:
            self._delegate.on_end_sync(token, ctx, error)

    def intercept_server_stream_sync(
        self,
        call_next: Callable[[REQ, RequestContext], Iterator[RES]],
        request: REQ,
        ctx: RequestContext,
    ) -> Iterator[RES]:
        token = self._delegate.on_start_sync(ctx)
        error: Exception | None = None
        try:
            yield from call_next(request, ctx)
        except Exception as e:
            error = e
            raise
        finally:
            self._delegate.on_end_sync(token, ctx, error)

    def intercept_bidi_stream_sync(
        self,
        call_next: Callable[[Iterator[REQ], RequestContext], Iterator[RES]],
        request: Iterator[REQ],
        ctx: RequestContext,
    ) -> Iterator[RES]:
        token = self._delegate.on_start_sync(ctx)
        error: Exception | None = None
        try:
            yield from call_next(request, ctx)
        except Exception as e:
            error = e
            raise
        finally:
            self._delegate.on_end_sync(token, ctx, error)


class MetadataInterceptorsRunSync:
    """A single request's invocation of hoisted metadata interceptors.

    Metadata interceptors at the front of the interceptor list don't have
    access to the request message, so servers invoke them before reading or
    parsing the request instead of wrapping the endpoint function.
    """

    _interceptors: Sequence[MetadataInterceptorSync[Any]]
    _ctx: RequestContext
    _started: list[tuple[MetadataInterceptorSync[Any], Any]]
    _ended: bool

    def __init__(
        self, interceptors: Sequence[MetadataInterceptorSync[Any]], ctx: RequestContext
    ) -> None:
        self._interceptors = interceptors
        self._ctx = ctx
        self._started = []
        self._ended = False

    def start(self) -> None:
        """Invoke on_start_sync for each interceptor in order.

        If an on_start_sync raises, interceptors that already started are ended
        by the following call to [end][].
        """
        for interceptor in self._interceptors:
            token = interceptor.on_start_sync(self._ctx)
            self._started.append((interceptor, token))

    def end(self, error: Exception | None) -> Exception | None:
        """Invoke on_end_sync for each started interceptor in reverse order.

        If an on_end_sync raises, the exception replaces the current error and
        is passed to the remaining interceptors, matching the behavior of
        interceptors nested around the endpoint function. Only the first call
        invokes on_end_sync; later calls return the error unchanged.
        """
        if self._ended:
            return error
        self._ended = True
        for interceptor, token in reversed(self._started):
            try:
                interceptor.on_end_sync(token, self._ctx, error)
            except Exception as e:  # noqa: BLE001, PERF203 # invoking user callback
                error = e
        return error


def split_leading_metadata_interceptors(
    interceptors: Iterable[InterceptorSync],
) -> tuple[Sequence[MetadataInterceptorSync[Any]], Sequence[InterceptorSync]]:
    """Split interceptors into leading metadata interceptors and the rest.

    Metadata interceptors at the front of the list can be invoked before the
    request message is read since they don't have access to it. A metadata
    interceptor following a message interceptor must still be invoked in order
    within the chain.
    """
    remaining = list(interceptors)
    leading: list[MetadataInterceptorSync[Any]] = []
    for i, interceptor in enumerate(remaining):
        if not isinstance(interceptor, MetadataInterceptorSync):
            return leading, remaining[i:]
        leading.append(interceptor)
    return leading, []


def resolve_interceptors(
    interceptors: Iterable[InterceptorSync],
) -> Sequence[
    UnaryInterceptorSync
    | ClientStreamInterceptorSync
    | ServerStreamInterceptorSync
    | BidiStreamInterceptorSync
]:
    return [
        MetadataInterceptorInvokerSync(interceptor)
        if isinstance(interceptor, MetadataInterceptorSync)
        else interceptor
        for interceptor in interceptors
    ]

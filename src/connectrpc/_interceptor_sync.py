from __future__ import annotations

from typing import TYPE_CHECKING, Generic, Protocol, TypeVar, runtime_checkable

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
        """Intercepts a unary RPC.

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
        """Intercepts a client-streaming RPC.

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
        """Intercepts a server-streaming RPC.

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
        """Intercepts a bidirectional-streaming RPC.

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
    """An interceptor that can be applied to any type of method, only having
    access to metadata such as headers and trailers.

    To access request and response bodies of a method, instead use an interceptor
    corresponding to the type of method such as [UnaryInterceptorSync][].
    """

    def on_start_sync(self, ctx: RequestContext, /) -> T:
        """Called when the RPC starts. The return value will be passed to [on_end_sync][] as-is.
        For example, if measuring RPC invocation time, on_start_sync may return the current
        time. If a return value isn't needed or [on_end_sync][] won't be used, return None.
        """
        ...

    def on_end_sync(
        self,
        token: T,  # noqa: ARG002 # keep name clean for public API
        ctx: RequestContext,  # noqa: ARG002 # keep name clean for public API
        error: Exception | None,  # noqa: ARG002 # keep name clean for public API
        /,
    ) -> None:
        """Called when the RPC ends."""
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

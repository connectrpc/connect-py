from __future__ import annotations

from typing import TYPE_CHECKING, Generic, Protocol, TypeVar, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Sequence

    from .request import RequestContext

REQ = TypeVar("REQ")
RES = TypeVar("RES")
T = TypeVar("T")


@runtime_checkable
class UnaryInterceptor(Protocol):
    """An interceptor of an asynchronous unary RPC method."""

    async def intercept_unary(
        self,
        call_next: Callable[[REQ, RequestContext], Awaitable[RES]],
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
class ClientStreamInterceptor(Protocol):
    """An interceptor of an asynchronous client-streaming RPC method."""

    async def intercept_client_stream(
        self,
        call_next: Callable[[AsyncIterator[REQ], RequestContext], Awaitable[RES]],
        request: AsyncIterator[REQ],
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
class ServerStreamInterceptor(Protocol):
    """An interceptor of an asynchronous server-streaming RPC method."""

    def intercept_server_stream(
        self,
        call_next: Callable[[REQ, RequestContext], AsyncIterator[RES]],
        request: REQ,
        ctx: RequestContext,
        /,
    ) -> AsyncIterator[RES]:
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
class BidiStreamInterceptor(Protocol):
    """An interceptor of an asynchronous bidirectional-streaming RPC method."""

    def intercept_bidi_stream(
        self,
        call_next: Callable[[AsyncIterator[REQ], RequestContext], AsyncIterator[RES]],
        request: AsyncIterator[REQ],
        ctx: RequestContext,
        /,
    ) -> AsyncIterator[RES]:
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
class MetadataInterceptor(Protocol[T]):
    """An interceptor that can be applied to any type of method.

    Only metadata such as headers and trailers is accessible. To access request
    and response bodies of a method, instead use an interceptor corresponding to
    the type of method such as [UnaryInterceptor][].
    """

    async def on_start(self, ctx: RequestContext) -> T:
        """Handle the start of the RPC.

        The return value is passed to [on_end][] as-is. For example, if measuring
        RPC invocation time, on_start may return the current time. If a return
        value isn't needed or [on_end][] won't be used, return None.
        """
        ...

    async def on_end(
        self,
        token: T,  # noqa: ARG002 # keep name clean for public API
        ctx: RequestContext,  # noqa: ARG002 # keep name clean for public API
        error: Exception | None,  # noqa: ARG002 # keep name clean for public API
        /,
    ) -> None:
        """Handle the end of the RPC."""
        return


Interceptor = (
    UnaryInterceptor
    | ClientStreamInterceptor
    | ServerStreamInterceptor
    | BidiStreamInterceptor
    | MetadataInterceptor
)
"""An interceptor to apply to an asynchronous RPC server or client."""


class MetadataInterceptorInvoker(Generic[T]):
    _delegate: MetadataInterceptor[T]

    def __init__(self, delegate: MetadataInterceptor[T]) -> None:
        self._delegate = delegate

    async def intercept_unary(
        self,
        call_next: Callable[[REQ, RequestContext], Awaitable[RES]],
        request: REQ,
        ctx: RequestContext,
    ) -> RES:
        token = await self._delegate.on_start(ctx)
        error: Exception | None = None
        try:
            return await call_next(request, ctx)
        except Exception as e:
            error = e
            raise
        finally:
            await self._delegate.on_end(token, ctx, error)

    async def intercept_client_stream(
        self,
        call_next: Callable[[AsyncIterator[REQ], RequestContext], Awaitable[RES]],
        request: AsyncIterator[REQ],
        ctx: RequestContext,
    ) -> RES:
        token = await self._delegate.on_start(ctx)
        error: Exception | None = None
        try:
            return await call_next(request, ctx)
        except Exception as e:
            error = e
            raise
        finally:
            await self._delegate.on_end(token, ctx, error)

    async def intercept_server_stream(
        self,
        call_next: Callable[[REQ, RequestContext], AsyncIterator[RES]],
        request: REQ,
        ctx: RequestContext,
    ) -> AsyncIterator[RES]:
        token = await self._delegate.on_start(ctx)
        error: Exception | None = None
        try:
            async for response in call_next(request, ctx):
                yield response
        except Exception as e:
            error = e
            raise
        finally:
            await self._delegate.on_end(token, ctx, error)

    async def intercept_bidi_stream(
        self,
        call_next: Callable[[AsyncIterator[REQ], RequestContext], AsyncIterator[RES]],
        request: AsyncIterator[REQ],
        ctx: RequestContext,
    ) -> AsyncIterator[RES]:
        token = await self._delegate.on_start(ctx)
        error: Exception | None = None
        try:
            async for response in call_next(request, ctx):
                yield response
        except Exception as e:
            error = e
            raise
        finally:
            await self._delegate.on_end(token, ctx, error)


def resolve_interceptors(
    interceptors: Iterable[Interceptor],
) -> Sequence[
    UnaryInterceptor
    | ClientStreamInterceptor
    | ServerStreamInterceptor
    | BidiStreamInterceptor
]:
    return [
        MetadataInterceptorInvoker(interceptor)
        if isinstance(interceptor, MetadataInterceptor)
        else interceptor
        for interceptor in interceptors
    ]

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, runtime_checkable

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

    On servers, metadata interceptors at the front of the interceptor list are
    invoked before the request message is read or parsed, allowing logic such
    as authentication to reject a request without processing its body. A
    metadata interceptor following a message interceptor is invoked in order
    within the chain, after the request message is parsed.
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


class MetadataInterceptorsRun:
    """A single request's invocation of hoisted metadata interceptors.

    Metadata interceptors at the front of the interceptor list don't have
    access to the request message, so servers invoke them before reading or
    parsing the request instead of wrapping the endpoint function.
    """

    _interceptors: Sequence[MetadataInterceptor[Any]]
    _ctx: RequestContext
    _started: list[tuple[MetadataInterceptor[Any], Any]]
    _ended: bool

    def __init__(
        self, interceptors: Sequence[MetadataInterceptor[Any]], ctx: RequestContext
    ) -> None:
        self._interceptors = interceptors
        self._ctx = ctx
        self._started = []
        self._ended = False

    async def start(self) -> None:
        """Invoke on_start for each interceptor in order.

        If an on_start raises, interceptors that already started are ended by
        the following call to [end][].
        """
        for interceptor in self._interceptors:
            token = await interceptor.on_start(self._ctx)
            self._started.append((interceptor, token))

    async def end(self, error: Exception | None) -> Exception | None:
        """Invoke on_end for each started interceptor in reverse order.

        If an on_end raises, the exception replaces the current error and is
        passed to the remaining interceptors, matching the behavior of
        interceptors nested around the endpoint function. Only the first call
        invokes on_end; later calls return the error unchanged.
        """
        if self._ended:
            return error
        self._ended = True
        for interceptor, token in reversed(self._started):
            try:
                await interceptor.on_end(token, self._ctx, error)
            except Exception as e:  # noqa: BLE001, PERF203 # invoking user callback
                error = e
        return error


def split_leading_metadata_interceptors(
    interceptors: Iterable[Interceptor],
) -> tuple[Sequence[MetadataInterceptor[Any]], Sequence[Interceptor]]:
    """Split interceptors into leading metadata interceptors and the rest.

    Metadata interceptors at the front of the list can be invoked before the
    request message is read since they don't have access to it. A metadata
    interceptor following a message interceptor must still be invoked in order
    within the chain.
    """
    remaining = list(interceptors)
    leading: list[MetadataInterceptor[Any]] = []
    for i, interceptor in enumerate(remaining):
        if not isinstance(interceptor, MetadataInterceptor):
            return leading, remaining[i:]
        leading.append(interceptor)
    return leading, []


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

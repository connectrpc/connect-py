"""Stream handling under trio, written against anyio."""

from __future__ import annotations

import contextlib
import functools
from typing import TYPE_CHECKING, TypeVar

import anyio
import anyio.lowlevel

from ._envelope import EnvelopeReader
from ._interceptor_async import MetadataInterceptorsRun, _aclose
from ._server_async import (
    _consume_single_request,
    _read_body,
    _ResponseSender,
    _yield_single_response,
)
from ._server_shared import (
    EndpointBidiStream,
    EndpointClientStream,
    EndpointServerStream,
    EndpointUnary,
)
from .code import Code
from .errors import ConnectError

if TYPE_CHECKING:
    from collections.abc import (
        AsyncGenerator,
        AsyncIterator,
        Awaitable,
        Callable,
        Sequence,
    )
    from typing import Any

    from asgiref.typing import ASGIReceiveCallable, ASGISendCallable

    from ._codec import Codec
    from ._interceptor_async import MetadataInterceptor
    from ._protocol import ServerProtocol
    from ._server_async import Endpoint
    from .compression import Compression
    from .request import Headers, RequestContext

_REQ = TypeVar("_REQ")
_RES = TypeVar("_RES")

# Seconds allowed for each cleanup step that runs shielded after a
# cancellation: closing the handler's generator, then the on_end hooks.
# Unshielded, each would be interrupted at its first await. A cancelled request
# therefore delays whoever cancelled it by at most twice this. The value is a
# guess at how long cleanup that does not wait on the client can take.
_CLEANUP_TIMEOUT = 1.0


async def handle_stream(
    *,
    compressions: dict[str, Compression],
    metadata_interceptors: Sequence[MetadataInterceptor[Any]],
    read_max_bytes: int | None,
    receive: ASGIReceiveCallable,
    send: ASGISendCallable,
    protocol: ServerProtocol,
    endpoint: Endpoint[_REQ, _RES],
    codec: Codec,
    headers: Headers,
    ctx: RequestContext,
) -> None:
    """Handle a streaming request under trio.

    Follows ConnectASGIApplication._handle_stream, except that a cancellation
    is re-raised to the scope that requested it once the interceptors that lead
    the list have been told of it, and nothing more is sent. On asyncio it
    becomes a ConnectError.
    """
    req_compression, resp_compression = protocol.negotiate_stream_compression(
        headers, compressions
    )

    sender = _ResponseSender(send, protocol, codec, resp_compression, ctx)

    metadata_run = MetadataInterceptorsRun(metadata_interceptors, ctx)
    error: Exception | None = None
    cancelled = False
    request_stream = None

    async def send_messages(
        response_stream: AsyncIterator[_RES], disconnected: anyio.Event | None = None
    ) -> None:
        try:
            async for message in response_stream:
                if disconnected is not None and disconnected.is_set():
                    raise ConnectError(Code.CANCELED, "Client disconnected")
                await sender.send_message(message)
        except BaseException as e:
            if not _has_cancellation(e):
                await _aclose(response_stream)
                raise
            with (
                anyio.move_on_after(_CLEANUP_TIMEOUT, shield=True),
                contextlib.suppress(Exception),
            ):
                await _aclose(response_stream)
            raise
        await _aclose(response_stream)

    try:
        await metadata_run.start()
        if not req_compression:
            raise ConnectError(Code.UNIMPLEMENTED, "Unrecognized request compression")
        request_stream = _request_stream(
            receive, endpoint.method.input, codec, req_compression, read_max_bytes
        )

        match endpoint:
            case EndpointUnary():
                request = await _consume_single_request(request_stream)
                response = await endpoint.function(request, ctx)
                if (end_error := await metadata_run.end(None)) is not None:
                    raise end_error
                await send_messages(_yield_single_response(response))
            case EndpointClientStream():
                response = await endpoint.function(request_stream, ctx)
                if (end_error := await metadata_run.end(None)) is not None:
                    raise end_error
                await send_messages(_yield_single_response(response))
            case EndpointServerStream():
                request = await _consume_single_request(request_stream)
                await _send_while_watching_for_disconnect(
                    receive,
                    functools.partial(send_messages, endpoint.function(request, ctx)),
                )
            case EndpointBidiStream():
                await send_messages(endpoint.function(request_stream, ctx))
    except Exception as e:  # noqa: BLE001 # invoking user callback
        error = e
    except BaseException as e:
        cancelled = _has_cancellation(e)
        raise
    finally:
        try:
            if cancelled:
                # Send nothing: the canceller owns the connection.
                with anyio.move_on_after(_CLEANUP_TIMEOUT, shield=True):
                    await metadata_run.end(
                        ConnectError(Code.CANCELED, "Request was cancelled")
                    )
            else:
                error = await metadata_run.end(error)
                await sender.end(error)
        finally:
            if request_stream is not None:
                # Left open, trio closes it from the garbage collector, in a
                # cancelled scope and with a ResourceWarning.
                await request_stream.aclose()
    if error and not isinstance(error, ConnectError):
        raise error


def _has_cancellation(error: BaseException) -> bool:
    if isinstance(error, anyio.get_cancelled_exc_class()):
        return True
    # A cancelled trio nursery raises its cancellation in an exception group,
    # beside any error its tasks raised while they were being cancelled. The
    # group is not a builtin before Python 3.11.
    exceptions = getattr(error, "exceptions", None)
    return isinstance(exceptions, tuple) and any(
        _has_cancellation(e) for e in exceptions
    )


async def _send_while_watching_for_disconnect(
    receive: ASGIReceiveCallable,
    send_messages: Callable[[anyio.Event], Awaitable[None]],
) -> None:
    disconnected = anyio.Event()

    async def watch() -> None:
        # An error raised in a task group cancels the rest of it, so an error
        # from receive() would end the response.
        with contextlib.suppress(Exception):
            while True:
                msg = await receive()
                if msg["type"] == "http.disconnect":
                    disconnected.set()
                    return

    error: BaseException | None = None
    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(watch)
            try:
                await send_messages(disconnected)
            except anyio.get_cancelled_exc_class():
                raise
            except BaseException as e:  # noqa: BLE001
                # Raised after the task group exits; raised inside, it would be
                # wrapped in an ExceptionGroup.
                error = e
            finally:
                tg.cancel_scope.cancel()
    except anyio.get_cancelled_exc_class():
        # A cancelled task group raises a cancellation of its own, which would
        # drop an error that the handler raised beside the cancellation.
        if error is not None and _has_cancellation(error):
            raise error from None
        raise
    if error is not None:
        raise error


async def _request_stream(
    receive: ASGIReceiveCallable,
    request_class: type[_REQ],
    codec: Codec,
    compression: Compression,
    read_max_bytes: int | None = None,
) -> AsyncGenerator[_REQ]:
    reader = EnvelopeReader(request_class, codec, compression, read_max_bytes)
    body = _read_body(receive)
    try:
        async for chunk in body:
            for message in reader.feed(chunk):
                yield message
                # The conformance tests require a cancellation check after
                # each message.
                await anyio.lowlevel.checkpoint()
    finally:
        await _aclose(body)

from __future__ import annotations

import base64
import contextlib
import functools
import inspect
from abc import ABC
from dataclasses import dataclass, replace
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, cast, overload
from urllib.parse import parse_qs

from typing_extensions import deprecated

from ._codec import Codec, get_default_codecs
from ._codegen import method_local_name
from ._compression import (
    negotiate_compression,
    resolve_compressions,
    unknown_compression_error,
)
from ._envelope import EnvelopeReader, EnvelopeWriter
from ._interceptor_async import (
    BidiStreamInterceptor,
    ClientStreamInterceptor,
    Interceptor,
    MetadataInterceptorsRun,
    ServerStreamInterceptor,
    UnaryInterceptor,
    _aclose,
    _throw_into,
    resolve_interceptors,
    split_leading_metadata_interceptors,
)
from ._loop import AsyncioLoop, current_loop
from ._protocol import ConnectWireError, HTTPError, ServerProtocol
from ._protocol_connect import CONNECT_UNARY_CONTENT_TYPE_PREFIX, ConnectServerProtocol
from ._protocol_grpc import GRPCServerProtocol
from ._protocol_server import negotiate_server_protocol
from ._server_shared import (
    DEFAULT_READ_MAX_BYTES,
    EndpointBidiStream,
    EndpointClientStream,
    EndpointServerStream,
    EndpointUnary,
    method_info,
    method_url,
)
from ._shared import decode_message, decompress
from .code import Code
from .errors import ConnectError
from .request import Headers, RequestContext

if TYPE_CHECKING:
    # We don't use asgiref code so only import from it for type checking
    from asyncio import Timeout
    from collections.abc import (
        AsyncGenerator,
        AsyncIterator,
        Awaitable,
        Callable,
        Iterable,
        Mapping,
        Sequence,
    )

    from asgiref.typing import (
        ASGIReceiveCallable,
        ASGIReceiveEvent,
        ASGISendCallable,
        ASGISendEvent,
        HTTPScope,
        Scope,
    )
    from protobuf import DescService

    from . import _server_shared
    from ._server_trio import TrioLoop, TrioTimeout
    from .compression import Compression
else:
    ASGIReceiveCallable = "asgiref.typing.ASGIReceiveCallable"
    ASGISendCallable = "asgiref.typing.ASGISendCallable"
    HTTPScope = "asgiref.typing.HTTPScope"
    Scope = "asgiref.typing.Scope"


_SVC = TypeVar("_SVC")
_REQ = TypeVar("_REQ")
_RES = TypeVar("_RES")

# We don't mutate query params so use a singleton for when they're not set.
_UNSET_QUERY_PARAMS: dict[str, list[str]] = {}

# While _server_shared.Endpoint is a closed type, we can't indicate that to Python so define
# a more precise type here.
Endpoint = (
    EndpointBidiStream[_REQ, _RES]
    | EndpointClientStream[_REQ, _RES]
    | EndpointServerStream[_REQ, _RES]
    | EndpointUnary[_REQ, _RES]
)


class _Event(Protocol):
    """The loop's event: asyncio.Event or trio.Event."""

    def set(self) -> None: ...

    def wait(self) -> Awaitable[object]: ...


class _ResponseSender:
    """The request's send(), tracking whether the response started or failed.

    A failed send means the server rejected the message or lost the connection,
    so the response can't be completed and sending more would only raise again,
    hiding the first error.
    """

    __slots__ = ("_send", "failed", "started")

    def __init__(self, send: ASGISendCallable) -> None:
        self._send = send
        self.started = False
        self.failed = False

    async def __call__(self, message: ASGISendEvent) -> None:
        try:
            await self._send(message)
        except Exception:
            self.failed = True
            raise
        if message["type"] == "http.response.start":
            self.started = True


class _RequestReceiver:
    """The request's receive(), tracking the end of the body and a disconnect.

    A disconnect is only delivered by receive(), which also delivers the
    request body, so a watch for it can only start reading once body_done is
    set.
    """

    __slots__ = ("_receive", "body_done", "disconnected")

    def __init__(self, receive: ASGIReceiveCallable, body_done: _Event) -> None:
        self._receive = receive
        self.body_done = body_done
        self.disconnected = False

    async def __call__(self) -> ASGIReceiveEvent:
        message = await self._receive()
        match message["type"]:
            case "http.request":
                if not message.get("more_body", False):
                    self.body_done.set()
            case "http.disconnect":
                self.disconnected = True
        return message


@dataclass(slots=True)
class _UnaryResponse:
    """How a Connect unary request's response is written."""

    codec: Codec
    compression: Compression

    async def send(
        self, send: _ResponseSender, response_data: object, ctx: RequestContext
    ) -> None:
        res_bytes = self.compression.compress(self.codec.encode(response_data))
        headers: list[tuple[bytes, bytes]] = [
            (
                b"content-type",
                f"{CONNECT_UNARY_CONTENT_TYPE_PREFIX}{self.codec.name()}".encode(),
            ),
            (b"content-encoding", self.compression.name().encode()),
            (b"vary", b"Accept-Encoding"),
        ]
        _add_context_headers(headers, ctx)
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": headers,
                "trailers": False,
            }
        )
        await send(
            {"type": "http.response.body", "body": res_bytes, "more_body": False}
        )


@dataclass(slots=True)
class _StreamResponse:
    """How a stream's response is written."""

    protocol: ServerProtocol
    codec: Codec
    compression: Compression
    writer: EnvelopeWriter
    request_stream: AsyncGenerator[Any] | None = None

    async def send_headers(self, send: _ResponseSender, ctx: RequestContext) -> None:
        headers = [
            (b"content-type", self.protocol.content_type(self.codec).encode()),
            (
                self.protocol.compression_header_name().encode(),
                self.compression.name().encode(),
            ),
        ]
        headers.extend(
            (key.encode(), value.encode())
            for key, value in ctx.response_headers.allitems()
        )
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": headers,
                "trailers": self.protocol.uses_trailers(),
            }
        )

    async def end(
        self, send: _ResponseSender, error: Exception | None, ctx: RequestContext
    ) -> bool:
        """End the stream, unless a failed send means it can't be."""
        if send.failed:
            return False
        if not send.started:
            # Exception before any response message is returned
            await self.send_headers(send, ctx)
        end_message = self.writer.end(
            ctx.response_trailers,
            ConnectWireError.from_exception(error) if error else None,
        )
        if isinstance(end_message, bytes):
            await send(
                {"type": "http.response.body", "body": end_message, "more_body": False}
            )
            return True
        await send({"type": "http.response.body", "body": b"", "more_body": False})
        await send(
            {
                "type": "http.response.trailers",
                "headers": [
                    (k.encode(), v.encode()) for k, v in end_message.allitems()
                ],
                "more_trailers": False,
            }
        )
        return True


class ConnectASGIApplication(ABC, Generic[_SVC]):
    """An ASGI application for the Connect protocol."""

    _resolved_endpoints: Mapping[str, Endpoint] | None

    @overload
    def __init__(
        self,
        *,
        service: _SVC | AsyncGenerator[_SVC],
        desc: DescService,
        interceptors: Iterable[Interceptor] = (),
        read_max_bytes: int | None = DEFAULT_READ_MAX_BYTES,
        compressions: Iterable[Compression] | None = None,
        codecs: Iterable[Codec] | None = None,
    ) -> None: ...

    @overload
    @deprecated(
        "Pass desc instead of endpoints. Regenerate code with the current version of "
        "protoc-gen-connectrpc."
    )
    def __init__(
        self,
        *,
        service: _SVC | AsyncGenerator[_SVC],
        endpoints: Callable[[_SVC], Mapping[str, Endpoint]],
        interceptors: Iterable[Interceptor] = (),
        read_max_bytes: int | None = DEFAULT_READ_MAX_BYTES,
        compressions: Iterable[Compression] | None = None,
        codecs: Iterable[Codec] | None = None,
    ) -> None: ...

    def __init__(
        self,
        *,
        service: _SVC | AsyncGenerator[_SVC],
        desc: DescService | None = None,
        endpoints: Callable[[_SVC], Mapping[str, Endpoint]] | None = None,
        interceptors: Iterable[Interceptor] = (),
        read_max_bytes: int | None = DEFAULT_READ_MAX_BYTES,
        compressions: Iterable[Compression] | None = None,
        codecs: Iterable[Codec] | None = None,
    ) -> None:
        """Initialize the ASGI application.

        Args:
            service: The service instance or async generator that yields the service during lifespan.
            desc: The descriptor of the service. Method implementations are looked up on the
                service instance by name using the same conventions as generated code.
            endpoints: Deprecated, pass desc instead. A callable that takes the service instance
                and returns a mapping of URL paths to endpoints.
            interceptors: A sequence of interceptors to apply to the endpoints.
            read_max_bytes: Maximum size of request messages.
            compressions: Supported compression algorithms. If unset, defaults to gzip.
                          If set to empty, disables compression.
            codecs: The codecs supported by the server. If unset, defaults to Protocol Buffers
                    binary and JSON codecs.

        """
        super().__init__()
        self._service = service
        self._desc: DescService | None
        self._endpoints: Callable[[_SVC], Mapping[str, Endpoint]] | None
        self._path: str | None
        if desc is not None:
            if endpoints is not None:
                msg = "only one of desc or endpoints may be provided"
                raise ValueError(msg)
            self._desc = desc
            self._endpoints = None
            self._path = f"/{desc.type_name}"
        elif endpoints is not None:
            self._desc = None
            self._endpoints = endpoints
            self._path = None
        else:
            msg = "one of desc or endpoints must be provided"
            raise ValueError(msg)
        self._metadata_interceptors, self._interceptors = (
            split_leading_metadata_interceptors(interceptors)
        )
        self._resolved_endpoints = None
        self._read_max_bytes = read_max_bytes
        self._compressions = resolve_compressions(compressions)
        codecs = codecs if codecs is not None else get_default_codecs()
        self._codecs = {codec.name(): codec for codec in codecs}

    @property
    def path(self) -> str:
        """Returns the URL path to mount the application to when serving multiple applications."""
        if self._path is None:
            msg = "path must be overridden when constructed with endpoints"
            raise NotImplementedError(msg)
        return self._path

    async def __call__(
        self, scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable
    ) -> None:
        if scope["type"] == "websocket":
            msg = "connect does not support websockets"
            raise RuntimeError(msg)

        if scope["type"] == "lifespan":
            await self._handle_lifespan(receive, send)
            return

        if self._resolved_endpoints is None:
            if inspect.isasyncgen(self._service):
                msg = "ASGI server does not support lifespan but async generator passed for service. Enable lifespan support."
                raise RuntimeError(msg)

            self._resolved_endpoints = self._resolve_endpoints(
                cast("_SVC", self._service)
            )
        await self._handle_request(scope, receive, send, self._resolved_endpoints)

    async def _handle_lifespan(
        self, receive: ASGIReceiveCallable, send: ASGISendCallable
    ) -> None:
        service_iter = None
        while True:
            msg = await receive()
            match msg["type"]:
                case "lifespan.startup":
                    # Need to cast since type checking doesn't seem to narrow well with isasyncgen
                    if inspect.isasyncgen(self._service):
                        service_iter = cast("AsyncGenerator[_SVC, None]", self._service)
                        try:
                            service = await anext(service_iter)
                        except Exception as e:  # noqa: BLE001 # invoking user callback
                            await send(
                                {"type": "lifespan.startup.failed", "message": str(e)}
                            )
                            return
                    else:
                        service = cast("_SVC", self._service)
                    self._resolved_endpoints = self._resolve_endpoints(service)
                    await send({"type": "lifespan.startup.complete"})
                case "lifespan.shutdown":
                    if service_iter is not None:
                        try:
                            await service_iter.aclose()
                        except Exception as e:  # noqa: BLE001 # invoking user callback
                            await send(
                                {"type": "lifespan.shutdown.failed", "message": str(e)}
                            )
                            return
                    await send({"type": "lifespan.shutdown.complete"})
                    return

    async def _handle_request(
        self,
        scope: HTTPScope,
        receive: ASGIReceiveCallable,
        send: ASGISendCallable,
        endpoints: Mapping[str, Endpoint],
    ) -> None:
        loop = current_loop()
        receiver = _RequestReceiver(receive, loop.event())
        sender = _ResponseSender(send)
        ctx: RequestContext | None = None
        protocol: ServerProtocol | None = None
        metadata_run: MetadataInterceptorsRun | None = None
        unary: _UnaryResponse | None = None
        stream: _StreamResponse | None = None
        response_data: object = None
        deadline: float | None = None
        error: Exception | None = None
        cancelled = False
        try:
            # The request's deadline, once known, covers everything up to its
            # status, including writing the full unary response of end-of-stream
            # message. A timeout or a disconnect before then becomes the status,
            # which is written without a deadline. A timeout after that is left
            # to the server as it is too late for us to be able to do anything.
            async with (timeout := loop.timeout_at(None)):
                try:
                    path = scope["path"]
                    endpoint = endpoints.get(path)
                    if not endpoint and scope["root_path"]:
                        # The application was mounted at some root so try stripping the prefix.
                        path = path.removeprefix(scope["root_path"])
                        endpoint = endpoints.get(path)
                    if not endpoint:
                        raise HTTPError(HTTPStatus.NOT_FOUND, [])

                    http_method = scope["method"]
                    http_scheme = scope.get("scheme", "http")
                    headers = _process_headers(scope.get("headers", ()))
                    client_address = (
                        f"{ca[0]}:{ca[1]}" if (ca := scope.get("client")) else None
                    )

                    content_type = headers.get("content-type", "")
                    protocol = negotiate_server_protocol(content_type)
                    if (
                        protocol.uses_trailers()
                        and "http.response.trailers"
                        not in cast("dict", scope.get("extensions", {}))
                    ):
                        msg = f"ASGI server does not support ASGI trailers extension but protocol for content-type '{content_type}' requires trailers"
                        raise RuntimeError(msg)

                    ctx = protocol.create_request_context(
                        endpoint.method,
                        http_method,
                        http_scheme,
                        headers,
                        client_address,
                    )
                    if (timeout_ms := ctx.timeout_ms) is not None:
                        deadline = loop.time() + timeout_ms / 1000
                        timeout.reschedule(deadline)

                    if http_method == "GET":
                        query_string = scope.get("query_string", b"").decode("utf-8")
                        query_params = parse_qs(query_string, keep_blank_values=True)
                        codec_name = query_params.get("encoding", ("",))[0]
                    else:
                        query_params = _UNSET_QUERY_PARAMS
                        codec_name = protocol.codec_name_from_content_type(
                            headers.get("content-type", ""),
                            stream=not isinstance(endpoint, EndpointUnary),
                        )
                    codec = self._codecs.get(codec_name)
                    if not codec:
                        accept_post = (
                            "application/json, application/proto"
                            if isinstance(endpoint, EndpointUnary)
                            else "application/connect+json, application/connect+proto"
                        )
                        raise HTTPError(
                            HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                            [("Accept-Post", accept_post)],
                        )

                    metadata_run = MetadataInterceptorsRun(
                        self._metadata_interceptors, ctx
                    )
                    # We resolve the response protocol before any fallible logic to
                    # be able to return errors properly.
                    if isinstance(endpoint, EndpointUnary) and isinstance(
                        protocol, ConnectServerProtocol
                    ):
                        unary = _UnaryResponse(
                            codec,
                            negotiate_compression(
                                headers.get("accept-encoding", ""), self._compressions
                            ),
                        )
                        await metadata_run.start()
                        response_data = await self._call_unary(
                            endpoint,
                            http_method,
                            headers,
                            codec,
                            query_params,
                            receiver,
                            loop,
                            timeout,
                            ctx,
                        )
                    else:
                        req_compression, resp_compression = (
                            protocol.negotiate_stream_compression(
                                headers, self._compressions
                            )
                        )
                        stream = _StreamResponse(
                            protocol,
                            codec,
                            resp_compression,
                            protocol.create_envelope_writer(codec, resp_compression),
                        )
                        await metadata_run.start()
                        if not req_compression:
                            raise unknown_compression_error(
                                headers.get(protocol.compression_header_name(), ""),
                                self._compressions,
                            )
                        await self._call_stream(
                            endpoint,
                            stream,
                            req_compression,
                            metadata_run,
                            receiver,
                            sender,
                            loop,
                            timeout,
                            ctx,
                        )
                except Exception as e:  # noqa: BLE001 # invoking user callback
                    error = e
                if metadata_run is not None:
                    # End the run before sending the response so on_end can still
                    # modify response metadata. A no-op if the run already ended.
                    error = await metadata_run.end(error)
                if error is None and unary is not None:
                    assert ctx is not None  # noqa: S101 # set before unary
                    try:
                        await unary.send(sender, response_data, ctx)
                    except Exception as e:  # noqa: BLE001 # encoding the handler's response
                        error = e
        except TimeoutError as e:
            # A timeout while on_end runs after an error leaves that error.
            if error is None:
                error = _timeout_error(timeout, receiver, deadline, e)
        except BaseException as e:
            # Save the error for sending to the client, then let it reach the
            # app server.
            if loop.is_cancellation(e):
                error = ConnectError(Code.CANCELED, "Request was cancelled")
                cancelled = True
            else:
                error = ConnectError(Code.UNKNOWN, "Request was interrupted")
                error.__cause__ = e
            raise
        finally:
            try:
                with loop.async_cleanup() if cancelled else contextlib.nullcontext():
                    if metadata_run is not None:
                        # A no-op unless the deadline or a cancellation prevented
                        # any from running.
                        error = await metadata_run.end(error)
                    # The status is written without a deadline. After a
                    # cancellation it is best effort: the connection may be gone,
                    # and the cancellation matters more than the send's error.
                    with (
                        contextlib.suppress(Exception)
                        if cancelled
                        else contextlib.nullcontext()
                    ):
                        if stream is not None:
                            assert ctx is not None  # noqa: S101 # set before stream
                            responded = await stream.end(sender, error, ctx)
                        elif error is not None:
                            responded = await self._handle_error(
                                error, ctx, protocol, sender
                            )
                        else:
                            responded = True
            finally:
                if stream is not None and stream.request_stream is not None:
                    await stream.request_stream.aclose()
        # Outside the finally, so that it does not replace a cancellation in flight.
        if error is not None and (
            not responded or not isinstance(error, (ConnectError, HTTPError))
        ):
            raise error

    async def _call_unary(
        self,
        endpoint: EndpointUnary[_REQ, _RES],
        http_method: str,
        headers: Headers,
        codec: Codec,
        query_params: dict[str, list[str]],
        receive: _RequestReceiver,
        loop: AsyncioLoop | TrioLoop,
        scope: Timeout | TrioTimeout,
        ctx: RequestContext,
    ) -> _RES:
        """Read a Connect unary request and invoke the handler."""
        if http_method == "GET":
            request = await self._read_get_request(endpoint, codec, query_params)
            # There is no body to read before watching for a disconnect.
            receive.body_done.set()
        else:
            request = await self._read_post_request(endpoint, receive, codec, headers)
        # There is nothing here to poll a flag, so the watch must cancel.
        async with loop.watch_for_disconnect(receive, scope, cancel=True):
            return await endpoint.function(request, ctx)

    async def _read_get_request(
        self,
        endpoint: EndpointUnary[_REQ, _RES],
        codec: Codec,
        params: dict[str, list[str]],
    ) -> _REQ:
        """Handle GET request with query parameters."""
        # Validation
        if "message" not in params:
            raise ConnectError(
                Code.INVALID_ARGUMENT,
                "'message' parameter is required for GET requests",
            )

        # Get and decode message
        message = params["message"][0]
        is_base64 = "base64" in params and params["base64"][0] == "1"

        if is_base64:
            try:
                message = base64.urlsafe_b64decode(message + "===")
            except Exception as e:
                raise ConnectError(
                    Code.INVALID_ARGUMENT, f"Invalid base64 encoding: {e!s}"
                ) from e
        else:
            message = message.encode("utf-8")

        # Handle compression
        compression_name = params.get("compression", [""])[0] or "identity"
        compression = self._compressions.get(compression_name)
        if not compression:
            raise unknown_compression_error(compression_name, self._compressions)

        # Decompress and decode message
        if message:  # Don't decompress empty messages
            message = decompress(compression, message, self._read_max_bytes)

        # Get the appropriate decoder for the endpoint
        return decode_message(codec, message, endpoint.method.input)

    async def _read_post_request(
        self,
        endpoint: Endpoint[_REQ, _RES],
        receive: _RequestReceiver,
        codec: Codec,
        headers: Headers,
    ) -> _REQ:
        """Handle POST request with body."""
        # Get request body
        chunks: list[bytes] = []
        read_bytes = 0
        async for chunk in _read_body(receive):
            read_bytes += len(chunk)
            if self._read_max_bytes is not None and read_bytes > self._read_max_bytes:
                raise ConnectError(
                    Code.RESOURCE_EXHAUSTED,
                    f"message is larger than configured max {self._read_max_bytes}",
                )
            chunks.append(chunk)
        req_body = b"".join(chunks)

        # Handle compression if specified
        compression_name = headers.get("content-encoding") or "identity"
        compression = self._compressions.get(compression_name)
        if not compression:
            raise unknown_compression_error(compression_name, self._compressions)

        if req_body:  # Don't decompress empty body
            req_body = decompress(compression, req_body, self._read_max_bytes)

        return decode_message(codec, req_body, endpoint.method.input)

    async def _call_stream(
        self,
        endpoint: Endpoint[_REQ, _RES],
        stream: _StreamResponse,
        req_compression: Compression,
        metadata_run: MetadataInterceptorsRun,
        receive: _RequestReceiver,
        send: _ResponseSender,
        loop: AsyncioLoop | TrioLoop,
        scope: Timeout | TrioTimeout,
        ctx: RequestContext,
    ) -> None:
        """Invoke the handler on the request stream and send its response messages."""
        request_stream = _request_stream(
            receive,
            endpoint.method.input,
            stream.codec,
            req_compression,
            self._read_max_bytes,
            loop,
        )
        # Closed by the request's end, since the handler may not consume it.
        stream.request_stream = request_stream
        # Once the request has been fully read, watch receive() for a client
        # disconnect so a handler that is blocked or still streaming is stopped
        # promptly. The handler's generator is closed within the watch's scope.
        async with loop.watch_for_disconnect(receive, scope) as disconnected:
            await self._stream_responses(
                endpoint,
                request_stream,
                stream,
                metadata_run,
                disconnected,
                send,
                loop,
                ctx,
            )

    async def _stream_responses(
        self,
        endpoint: Endpoint[_REQ, _RES],
        request_stream: AsyncGenerator[_REQ],
        stream: _StreamResponse,
        metadata_run: MetadataInterceptorsRun,
        disconnected: Callable[[], bool] | None,
        send: _ResponseSender,
        loop: AsyncioLoop | TrioLoop,
        ctx: RequestContext,
    ) -> None:
        """Invoke the handler and send its response messages."""
        match endpoint:
            case EndpointUnary():
                request = await _consume_single_request(request_stream)
                response = await endpoint.function(request, ctx)
                # End the run before sending the response so on_end can
                # still modify response metadata.
                if (end_error := await metadata_run.end(None)) is not None:
                    raise end_error
                response_stream = _yield_single_response(response)
            case EndpointClientStream():
                response = await endpoint.function(request_stream, ctx)
                if (end_error := await metadata_run.end(None)) is not None:
                    raise end_error
                response_stream = _yield_single_response(response)
            case EndpointServerStream():
                request = await _consume_single_request(request_stream)
                response_stream = endpoint.function(request, ctx)
            case EndpointBidiStream():
                response_stream = endpoint.function(request_stream, ctx)

        try:
            async for message in response_stream:
                # Only a watch that can't cancel the handler yields a
                # flag to poll.
                if disconnected is not None and disconnected():
                    raise ConnectError(Code.CANCELED, "Client disconnected")
                # Don't send headers until the first message to allow logic a
                # chance to add response headers.
                if not send.started:
                    await stream.send_headers(send, ctx)

                body = stream.writer.write(message)
                await send(
                    {"type": "http.response.body", "body": body, "more_body": True}
                )
        except BaseException as e:
            await _close_response_stream(response_stream, loop, e)
            raise
        await _close_response_stream(response_stream, loop, None)

    async def _handle_error(
        self,
        exc: Exception,
        ctx: RequestContext | None,
        protocol: ServerProtocol | None,
        send: _ResponseSender,
    ) -> bool:
        """Respond with the error, unless the response can no longer be replaced."""
        if send.started or send.failed:
            return False
        headers: list[tuple[bytes, bytes]]
        body: bytes
        status: int
        if isinstance(protocol, GRPCServerProtocol) and not isinstance(exc, HTTPError):
            # gRPC clients read the status from trailers, so this is a trailers-only
            # response: HTTP 200 with the gRPC status in the headers.
            grpc_headers = protocol.trailers_only_headers(ctx, exc)
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (k.encode(), v.encode()) for k, v in grpc_headers.allitems()
                    ],
                    "trailers": False,
                }
            )
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            return True
        if isinstance(exc, HTTPError):
            status = exc.status.value
            headers = [(k.encode("utf-8"), v.encode("utf-8")) for k, v in exc.headers]
            body = b""
        else:
            wire_error = ConnectWireError.from_exception(exc)
            status = wire_error.to_http_status().code
            headers = [(b"content-type", b"application/json")]
            body = wire_error.to_json_bytes()

        if ctx:
            _add_context_headers(headers, ctx)

        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": headers,
                "trailers": False,
            }
        )
        await send({"type": "http.response.body", "body": body, "more_body": False})
        return True

    def _build_endpoints(self, service: _SVC) -> Mapping[str, _server_shared.Endpoint]:
        """Build the endpoints for the service.

        This method is overridden by generated code in protobuf=google mode so must have
        a stable API.
        """
        if self._desc is None:
            msg = "endpoints cannot be built without desc"
            raise RuntimeError(msg)
        endpoints: dict[str, _server_shared.Endpoint] = {}
        for method in self._desc.methods:
            info = method_info(method)
            function = getattr(service, method_local_name(method))
            endpoint: Endpoint
            match method.method_kind:
                case "unary":
                    endpoint = EndpointUnary(method=info, function=function)
                case "client_streaming":
                    endpoint = EndpointClientStream(method=info, function=function)
                case "server_streaming":
                    endpoint = EndpointServerStream(method=info, function=function)
                case "bidi_streaming":
                    endpoint = EndpointBidiStream(method=info, function=function)
            endpoints[method_url(method)] = endpoint
        return endpoints

    def _resolve_endpoints(self, service: _SVC) -> Mapping[str, Endpoint]:
        resolved_endpoints = (
            self._endpoints(service)
            if self._endpoints is not None
            else cast("Mapping[str, Endpoint]", self._build_endpoints(service))
        )
        if self._interceptors:
            resolved_endpoints = {
                path: _apply_interceptors(
                    endpoint, resolve_interceptors(self._interceptors)
                )
                for path, endpoint in resolved_endpoints.items()
            }
        return resolved_endpoints


def _timeout_error(
    timeout: Timeout | TrioTimeout,
    receive: _RequestReceiver,
    deadline: float | None,
    error: TimeoutError,
) -> Exception:
    """Return the error for a TimeoutError raised out of the request's scope.

    The scope is cut short by the deadline or, before it, by a client
    disconnect, which the watch reports by moving the scope's deadline up to
    the disconnect. The run catches its hooks' exceptions, so any other
    TimeoutError was raised by a send and is reported as is.
    """
    if not timeout.expired():
        return error
    when = timeout.when()
    if receive.disconnected and (deadline is None or when is None or when < deadline):
        result = ConnectError(Code.CANCELED, "Client disconnected")
    else:
        result = ConnectError(Code.DEADLINE_EXCEEDED, "Request timed out")
    result.__cause__ = error
    return result


async def _close_response_stream(
    stream: AsyncIterator[object],
    loop: AsyncioLoop | TrioLoop,
    error: BaseException | None,
) -> None:
    """Close the handler's generator, giving it the error that ended the request.

    While the server awaits a send, the generator is suspended at a yield, so
    a cancellation or a failed send is raised in the server rather than in
    the handler. Throwing the error into the generator lets the handler, and
    the interceptors wrapping it, see why the request ended.
    """
    if error is None:
        await _aclose(stream)
    elif loop.is_cancellation(error):
        with loop.async_cleanup(), contextlib.suppress(Exception):
            await _throw_into(stream, error)
    else:
        await _throw_into(stream, error)


async def _request_stream(
    receive: _RequestReceiver,
    request_class: type[_REQ],
    codec: Codec,
    compression: Compression,
    read_max_bytes: int | None,
    loop: AsyncioLoop | TrioLoop,
) -> AsyncGenerator[_REQ]:
    body = _read_body(receive)
    try:
        with EnvelopeReader(
            request_class, codec, compression, read_max_bytes
        ) as reader:
            async for chunk in body:
                for message in reader.feed(chunk):
                    yield message
                    # Check for cancellation each message. While this seems heavyweight,
                    # conformance tests require it.
                    await loop.checkpoint()
    finally:
        await _aclose(body)


async def _read_body(receive: _RequestReceiver) -> AsyncIterator[bytes]:
    """Read the body of the request."""
    while True:
        message = await receive()
        match message["type"]:
            case "http.request":
                body = message.get("body", b"")
                yield body
                if not message.get("more_body", False):
                    return
            case "http.disconnect":
                raise ConnectError(
                    Code.CANCELED, "Client disconnected before request completion"
                )
            case _:
                raise ConnectError(Code.UNKNOWN, "Unexpected message type")


async def _consume_single_request(stream: AsyncIterator[_REQ]) -> _REQ:
    req = None
    async for message in stream:
        if req is not None:
            raise ConnectError(
                Code.UNIMPLEMENTED, "unary request has multiple messages"
            )
        req = message
    if req is None:
        raise ConnectError(Code.UNIMPLEMENTED, "unary request has zero messages")
    return req


async def _yield_single_response(response: _RES) -> AsyncIterator[_RES]:
    yield response


def _process_headers(iterable: Iterable[tuple[bytes, bytes]]) -> Headers:
    result = Headers()
    for key, value in iterable:
        result.add(key.decode(), value.decode())
    return result


def _add_context_headers(
    headers: list[tuple[bytes, bytes]], ctx: RequestContext
) -> None:
    headers.extend(
        (key.encode(), value.encode()) for key, value in ctx.response_headers.allitems()
    )
    headers.extend(
        (f"trailer-{key}".encode(), value.encode())
        for key, value in ctx.response_trailers.allitems()
    )


def _apply_interceptors(
    endpoint: Endpoint[_REQ, _RES], interceptors: Sequence[Interceptor]
) -> Endpoint[_REQ, _RES]:
    match endpoint:
        case EndpointUnary():
            func = endpoint.function
            for interceptor in reversed(interceptors):
                if not isinstance(interceptor, UnaryInterceptor):
                    continue
                func = functools.partial(interceptor.intercept_unary, func)
            return replace(endpoint, function=func)
        case EndpointClientStream():
            func = endpoint.function
            for interceptor in reversed(interceptors):
                if not isinstance(interceptor, ClientStreamInterceptor):
                    continue
                func = functools.partial(interceptor.intercept_client_stream, func)
            return replace(endpoint, function=func)
        case EndpointServerStream():
            func = endpoint.function
            for interceptor in reversed(interceptors):
                if not isinstance(interceptor, ServerStreamInterceptor):
                    continue
                func = functools.partial(interceptor.intercept_server_stream, func)
            return replace(endpoint, function=func)
        case EndpointBidiStream():
            func = endpoint.function
            for interceptor in reversed(interceptors):
                if not isinstance(interceptor, BidiStreamInterceptor):
                    continue
                func = functools.partial(interceptor.intercept_bidi_stream, func)
            return replace(endpoint, function=func)

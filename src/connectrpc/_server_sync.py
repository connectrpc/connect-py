from __future__ import annotations

import base64
import contextvars
import functools
import traceback
from abc import ABC
from dataclasses import dataclass, replace
from http import HTTPStatus
from typing import TYPE_CHECKING, Generic, TypeVar, cast, overload
from urllib.parse import parse_qs

from typing_extensions import deprecated

from . import _server_shared
from ._codec import Codec, get_default_codecs
from ._codegen import method_local_name
from ._compression import (
    negotiate_compression,
    resolve_compressions,
    unknown_compression_error,
)
from ._envelope import EnvelopeReader, EnvelopeWriter
from ._interceptor_sync import (
    BidiStreamInterceptorSync,
    ClientStreamInterceptorSync,
    InterceptorSync,
    MetadataInterceptorInvokerSync,
    MetadataInterceptorsRunSync,
    MetadataInterceptorSync,
    ServerStreamInterceptorSync,
    UnaryInterceptorSync,
    split_leading_metadata_interceptors,
)
from ._protocol import ConnectWireError, HTTPError, ServerProtocol
from ._protocol_connect import (
    CONNECT_UNARY_CONTENT_TYPE_PREFIX,
    ConnectServerProtocol,
    codec_name_from_content_type,
)
from ._protocol_grpc import GRPCServerProtocol
from ._protocol_server import negotiate_server_protocol
from ._server_shared import (
    DEFAULT_READ_MAX_BYTES,
    EndpointBidiStreamSync,
    EndpointClientStreamSync,
    EndpointServerStreamSync,
    EndpointUnarySync,
    check_deadline,
    method_info,
    method_url,
)
from ._shared import decode_message, decompress
from .code import Code
from .errors import ConnectError
from .request import Headers, RequestContext

if TYPE_CHECKING:
    from collections.abc import (
        Callable,
        Generator,
        Iterable,
        Iterator,
        Mapping,
        Sequence,
    )
    from io import BytesIO
    from wsgiref.types import ErrorStream, StartResponse, WSGIEnvironment

    from protobuf import DescService

    from .compression import Compression
else:
    StartResponse = "wsgiref.types.StartResponse"
    WSGIEnvironment = "wsgiref.types.WSGIEnvironment"


_REQ = TypeVar("_REQ")
_RES = TypeVar("_RES")
_SVC = TypeVar("_SVC")

_BODY_CHUNK_SIZE = 4096

# We don't mutate query params so use a singleton for when they're not set.
_UNSET_QUERY_PARAMS: dict[str, list[str]] = {}

# While _server_shared.EndpointSync is a closed type, we can't indicate that to Python so define
# a more precise type here.
EndpointSync = (
    EndpointBidiStreamSync[_REQ, _RES]
    | EndpointClientStreamSync[_REQ, _RES]
    | EndpointServerStreamSync[_REQ, _RES]
    | EndpointUnarySync[_REQ, _RES]
)


def _process_headers(environ: WSGIEnvironment) -> Headers:
    headers = Headers()
    if "CONTENT_TYPE" in environ:
        headers["content-type"] = environ["CONTENT_TYPE"]
    if "CONTENT_LENGTH" in environ:
        headers["content-length"] = environ["CONTENT_LENGTH"]

    for key, value in environ.items():
        if key.startswith("HTTP_"):
            header = key[5:].replace("_", "-")
            headers[header] = value
    return headers


def prepare_response_headers(
    base_headers: dict[str, list[str]], selected_encoding: str
) -> dict[str, list[str]]:
    """Prepare response headers with the selected compression encoding.

    Args:
        base_headers: Base response headers.
        selected_encoding: Selected compression encoding.

    Returns:
        The final response headers with content-encoding set.

    """
    headers = base_headers.copy()

    if "content-type" not in headers:
        headers["content-type"] = ["application/proto"]

    headers["content-encoding"] = [selected_encoding]
    headers["vary"] = ["Accept-Encoding"]
    return headers


class _RequestBody:
    """The request body, capped at content-length."""

    __slots__ = ("_remaining", "_stream")

    @classmethod
    def from_environ(cls, environ: WSGIEnvironment) -> _RequestBody:
        """Read the body of the request described by environ, bounded by CONTENT_LENGTH."""
        content_length = environ.get("CONTENT_LENGTH")
        remaining: int | None = None
        if content_length:
            try:
                remaining = int(content_length)
            except ValueError:
                remaining = None
        return cls(environ["wsgi.input"], remaining)

    def __init__(self, stream: BytesIO, remaining: int | None) -> None:
        self._stream = stream
        self._remaining = remaining

    def read(self, size: int = -1) -> bytes:
        if self._remaining is None:
            return self._stream.read(size)
        if self._remaining <= 0:
            return b""
        if size < 0:
            size = self._remaining
        chunk = self._stream.read(min(size, self._remaining))
        self._remaining -= len(chunk)
        return chunk


def _read_body_with_content_length(
    request_body: _RequestBody, content_length: int
) -> bytes:
    # Many app servers buffer the entire request before executing the app
    # so do an optimistic read before looping.
    chunk = request_body.read(content_length)
    if len(chunk) == content_length:
        return chunk

    bytes_read = len(chunk)
    chunks = [chunk]
    while bytes_read < content_length:
        to_read = content_length - bytes_read
        chunk = request_body.read(to_read)
        if not chunk:
            break
        chunks.append(chunk)
        bytes_read += len(chunk)
    if bytes_read < content_length:
        raise ConnectError(
            Code.INVALID_ARGUMENT,
            f"request truncated, expected {content_length} bytes but only received {bytes_read} bytes",
        )
    return b"".join(chunks)


def _read_body(request_body: _RequestBody) -> Iterator[bytes]:
    while True:
        chunk = request_body.read(_BODY_CHUNK_SIZE)
        if not chunk:
            return
        yield chunk


@dataclass(slots=True)
class _StreamResponse:
    """How a stream's response is written."""

    protocol: ServerProtocol
    codec: Codec
    compression: Compression
    writer: EnvelopeWriter

    def send_headers(
        self, start_response: StartResponse, ctx: RequestContext | None
    ) -> None:
        headers = [
            ("content-type", self.protocol.content_type(self.codec)),
            (self.protocol.compression_header_name(), self.compression.name()),
        ]
        if ctx is not None:
            headers.extend(ctx.response_headers.allitems())
        start_response("200 OK", headers)

    def end(
        self,
        start_response: StartResponse,
        send_trailers: Callable[[list[tuple[str, str]]], None] | None,
        ctx: RequestContext | None,
        error: Exception | None,
    ) -> list[bytes]:
        """Respond with only the end of the stream.

        ctx is None when the request failed before its context was created.
        """
        self.send_headers(start_response, ctx)
        end_message = self.writer.end(
            ctx.response_trailers if ctx is not None else Headers(),
            ConnectWireError.from_exception(error) if error else None,
        )
        return [_end_response(end_message, send_trailers)]


class ConnectWSGIApplication(ABC, Generic[_SVC]):
    """A WSGI application for the Connect protocol."""

    @overload
    def __init__(
        self,
        *,
        service: _SVC,
        desc: DescService,
        interceptors: Iterable[InterceptorSync] = (),
        read_max_bytes: int | None = DEFAULT_READ_MAX_BYTES,
        compressions: Iterable[Compression] | None = None,
        codecs: Iterable[Codec] | None = None,
    ) -> None: ...

    @overload
    @deprecated(
        "Pass service and desc instead of endpoints. Regenerate code with the current "
        "version of protoc-gen-connectrpc."
    )
    def __init__(
        self,
        *,
        endpoints: Mapping[str, EndpointSync],
        interceptors: Iterable[InterceptorSync] = (),
        read_max_bytes: int | None = DEFAULT_READ_MAX_BYTES,
        compressions: Iterable[Compression] | None = None,
        codecs: Iterable[Codec] | None = None,
    ) -> None: ...

    def __init__(
        self,
        *,
        service: _SVC | None = None,
        desc: DescService | None = None,
        endpoints: Mapping[str, EndpointSync] | None = None,
        interceptors: Iterable[InterceptorSync] = (),
        read_max_bytes: int | None = DEFAULT_READ_MAX_BYTES,
        compressions: Iterable[Compression] | None = None,
        codecs: Iterable[Codec] | None = None,
    ) -> None:
        """Initialize the WSGI application.

        Args:
            service: The service instance.
            desc: The descriptor of the service. Method implementations are looked up on the
                service instance by name using the same conventions as generated code.
            endpoints: Deprecated, pass service and desc instead. A mapping of URL paths to
                endpoints.
            interceptors: A sequence of interceptors to apply to the endpoints.
            read_max_bytes: Maximum size of request messages.
            compressions: Supported compression algorithms. If unset, defaults to gzip.
                          If set to empty, disables compression.
            codecs: The codecs supported by the server. If unset, defaults to Protocol Buffers
                    binary and JSON codecs.

        """
        super().__init__()
        self._desc: DescService | None
        self._path: str | None
        if desc is not None:
            if endpoints is not None:
                msg = "only one of desc or endpoints may be provided"
                raise ValueError(msg)
            if service is None:
                msg = "service must be provided with desc"
                raise ValueError(msg)
            self._desc = desc
            self._path = f"/{desc.type_name}"
            endpoints = cast(
                "Mapping[str, EndpointSync]", self._build_endpoints(service)
            )
        elif endpoints is not None:
            self._desc = None
            self._path = None
        else:
            msg = "one of desc or endpoints must be provided"
            raise ValueError(msg)
        self._metadata_interceptors, interceptors = split_leading_metadata_interceptors(
            interceptors
        )
        if interceptors:
            interceptors = [
                MetadataInterceptorInvokerSync(interceptor)
                if isinstance(interceptor, MetadataInterceptorSync)
                else interceptor
                for interceptor in interceptors
            ]
            endpoints = {
                path: _apply_interceptors(endpoint, interceptors)
                for path, endpoint in endpoints.items()
            }
        self._endpoints = endpoints
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

    def _build_endpoints(
        self, service: _SVC
    ) -> Mapping[str, _server_shared.EndpointSync]:
        """Build the endpoints for the service.

        This method is overridden by generated code in protobuf=google mode so must have
        a stable API.
        """
        if self._desc is None:
            msg = "endpoints cannot be built without desc"
            raise RuntimeError(msg)
        endpoints: dict[str, _server_shared.EndpointSync] = {}
        for method in self._desc.methods:
            info = method_info(method)
            function = getattr(service, method_local_name(method))
            endpoint: EndpointSync
            match method.method_kind:
                case "unary":
                    endpoint = EndpointUnarySync(method=info, function=function)
                case "client_streaming":
                    endpoint = EndpointClientStreamSync(method=info, function=function)
                case "server_streaming":
                    endpoint = EndpointServerStreamSync(method=info, function=function)
                case "bidi_streaming":
                    endpoint = EndpointBidiStreamSync(method=info, function=function)
            endpoints[method_url(method)] = endpoint
        return endpoints

    def __call__(
        self, environ: WSGIEnvironment, start_response: StartResponse
    ) -> Iterable[bytes]:
        ctx: RequestContext | None = None
        protocol: ServerProtocol | None = None
        stream: _StreamResponse | None = None
        send_trailers: Callable[[list[tuple[str, str]]], None] | None = None
        request_body = _RequestBody.from_environ(environ)
        try:
            path = environ["PATH_INFO"]
            if not path:
                path = "/"
            endpoint = self._endpoints.get(path)
            if not endpoint and environ["SCRIPT_NAME"] == self.path:
                # The application was mounted at the service's path so we reconstruct
                # the full URL.
                endpoint = self._endpoints.get(self.path + path)

            if not endpoint:
                raise HTTPError(HTTPStatus.NOT_FOUND, [])

            http_method = environ["REQUEST_METHOD"]
            http_scheme = environ.get("wsgi.url_scheme", "http")
            headers = _process_headers(environ)
            if ra := environ.get("REMOTE_ADDR"):
                port = environ.get("REMOTE_PORT", "0")
                client_address = f"{ra}:{port}"
            else:
                client_address = None

            content_type = headers.get("content-type", "")
            protocol = negotiate_server_protocol(content_type)

            if protocol.uses_trailers():
                send_trailers = environ.get("wsgi.ext.http.send_trailers")
                if not send_trailers:
                    msg = f"WSGI server does not support WSGI trailers extension but protocol for content-type '{content_type}' requires trailers"
                    raise RuntimeError(msg)

            stream_response: _StreamResponse | None = None
            req_compression: Compression | None = None
            if not (
                isinstance(endpoint, EndpointUnarySync)
                and isinstance(protocol, ConnectServerProtocol)
            ) and (
                codec := self._codecs.get(
                    protocol.codec_name_from_content_type(content_type, stream=True)
                )
            ):
                req_compression, resp_compression = (
                    protocol.negotiate_stream_compression(headers, self._compressions)
                )
                stream_response = _StreamResponse(
                    protocol,
                    codec,
                    resp_compression,
                    protocol.create_envelope_writer(codec, resp_compression),
                )
                if isinstance(protocol, ConnectServerProtocol):
                    # Connect reports a stream's errors in its end-stream message,
                    # including errors in the request headers validated below, so
                    # it needs the response first.
                    stream = stream_response

            ctx = protocol.create_request_context(
                endpoint.method, http_method, http_scheme, headers, client_address
            )
            check_deadline(ctx)

            if isinstance(endpoint, EndpointUnarySync) and isinstance(
                protocol, ConnectServerProtocol
            ):
                return self._handle_unary(
                    environ,
                    request_body,
                    start_response,
                    http_method,
                    endpoint,
                    ctx,
                    headers,
                )
            if stream_response is None:
                raise HTTPError(
                    HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                    [
                        (
                            "Accept-Post",
                            "application/connect+json, application/connect+proto",
                        )
                    ],
                )
            context = contextvars.copy_context()
            return context.run(
                self._handle_stream,
                environ,
                request_body,
                start_response,
                send_trailers,
                stream_response,
                req_compression,
                headers,
                endpoint,
                ctx,
                context,
            )

        except Exception as e:  # noqa: BLE001 # invoking user callback
            _drain_request_body(environ, request_body)
            _maybe_log_exception(environ, e)
            # An HTTP error is always a plain HTTP response.
            if stream is not None and not isinstance(e, HTTPError):
                return stream.end(start_response, send_trailers, ctx, e)
            return self._handle_error(e, ctx, protocol, start_response)

    def _handle_unary(
        self,
        environ: WSGIEnvironment,
        request_body: _RequestBody,
        start_response: StartResponse,
        http_method: str,
        endpoint: EndpointUnarySync[_REQ, _RES],
        ctx: RequestContext[_REQ, _RES],
        headers: Headers,
    ) -> Iterable[bytes]:
        if http_method == "GET":
            query_params = parse_qs(
                environ.get("QUERY_STRING", ""), keep_blank_values=True
            )
            codec_name = query_params.get("encoding", ("",))[0]
        else:
            query_params = _UNSET_QUERY_PARAMS
            codec_name = codec_name_from_content_type(
                headers.get("content-type", ""), stream=False
            )
        codec = self._codecs.get(codec_name)
        if not codec:
            raise HTTPError(
                HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                [("Accept-Post", "application/json, application/proto")],
            )

        metadata_run = MetadataInterceptorsRunSync(self._metadata_interceptors, ctx)
        response: _RES | None = None
        error: Exception | None = None
        try:
            metadata_run.start()
            # Handle request based on method
            if http_method == "GET":
                request = self._handle_get_request(query_params, endpoint, codec)
            else:
                request = self._handle_post_request(
                    environ, request_body, endpoint, codec
                )

            # Process request
            response = endpoint.function(request, ctx)
        except Exception as e:  # noqa: BLE001 # re-raised after ending the run
            error = e
        except BaseException as e:
            # Save the error for on_end, then let the exception reach the app server.
            error = ConnectError(Code.UNKNOWN, "Request was interrupted")
            error.__cause__ = e
            raise
        finally:
            # End the run before sending the response so on_end can still modify
            # response metadata.
            error = metadata_run.end(error)
        if error is not None:
            raise error
        assert response is not None  # noqa: S101 # no error means function returned

        # Encode response
        res_bytes = codec.encode(response)
        base_headers = {
            "content-type": [f"{CONNECT_UNARY_CONTENT_TYPE_PREFIX}{codec.name()}"]
        }

        # Handle compression if accepted
        accept_encoding = headers.get("accept-encoding", "identity")
        compression = negotiate_compression(accept_encoding, self._compressions)
        res_bytes = compression.compress(res_bytes)
        response_headers = prepare_response_headers(base_headers, compression.name())

        # Convert headers to WSGI format
        wsgi_headers: list[tuple[str, str]] = []
        for key, values in response_headers.items():
            normalized_key = key.lower()
            wsgi_headers.extend((normalized_key, value) for value in values)
        _add_context_headers(wsgi_headers, ctx)

        start_response("200 OK", wsgi_headers)
        return [res_bytes]

    def _handle_post_request(
        self,
        environ: WSGIEnvironment,
        request_body: _RequestBody,
        endpoint: _server_shared.EndpointSync[_REQ, _RES],
        codec: Codec,
    ) -> _REQ:
        """Handle POST request with body."""
        try:
            content_length = environ.get("CONTENT_LENGTH")
            content_length = 0 if not content_length else int(content_length)
            if content_length > 0:
                if (
                    self._read_max_bytes is not None
                    and content_length > self._read_max_bytes
                ):
                    raise ConnectError(
                        Code.RESOURCE_EXHAUSTED,
                        f"message is larger than configured max {self._read_max_bytes}",
                    )
                req_body = _read_body_with_content_length(request_body, content_length)
            else:
                chunks: list[bytes] = []
                read_bytes = 0
                for chunk in _read_body(request_body):
                    read_bytes += len(chunk)
                    if (
                        self._read_max_bytes is not None
                        and read_bytes > self._read_max_bytes
                    ):
                        raise ConnectError(
                            Code.RESOURCE_EXHAUSTED,
                            f"message is larger than configured max {self._read_max_bytes}",
                        )
                    chunks.append(chunk)
                req_body = b"".join(chunks)

            # Handle compression if specified
            compression_name = environ.get("HTTP_CONTENT_ENCODING") or "identity"
            compression = self._compressions.get(compression_name)
            if not compression:
                raise unknown_compression_error(compression_name, self._compressions)
            if req_body:  # Don't decompress empty body
                req_body = decompress(compression, req_body, self._read_max_bytes)
            return decode_message(codec, req_body, endpoint.method.input)

        except Exception as e:
            if not isinstance(e, ConnectError):
                raise ConnectError(
                    Code.INTERNAL,
                    str(e),  # TODO
                ) from e
            raise

    def _handle_get_request(
        self,
        params: dict[str, list[str]],
        endpoint: EndpointUnarySync[_REQ, _RES],
        codec: Codec,
    ) -> _REQ:
        """Handle GET request with query parameters."""
        try:
            if "message" not in params:
                raise ConnectError(
                    Code.INVALID_ARGUMENT,
                    "'message' parameter is required for GET requests",
                )

            message = params["message"][0]

            if "base64" in params and params["base64"][0] == "1":
                try:
                    message = base64.urlsafe_b64decode(message + "===")
                except Exception as e:
                    raise ConnectError(
                        Code.INVALID_ARGUMENT, f"Invalid base64 encoding: {e!s}"
                    ) from e
            else:
                message = message.encode("utf-8")

            # Handle compression if specified
            compression_name = params.get("compression", [""])[0] or "identity"
            compression = self._compressions.get(compression_name)
            if not compression:
                raise unknown_compression_error(compression_name, self._compressions)
            if message:  # Don't decompress empty message
                message = decompress(compression, message, self._read_max_bytes)

            return decode_message(codec, message, endpoint.method.input)

        except Exception as e:
            if not isinstance(e, ConnectError):
                raise ConnectError(Code.INTERNAL, str(e)) from e
            raise

    def _handle_stream(
        self,
        environ: WSGIEnvironment,
        request_body: _RequestBody,
        start_response: StartResponse,
        send_trailers: Callable[[list[tuple[str, str]]], None] | None,
        stream: _StreamResponse,
        req_compression: Compression | None,
        headers: Headers,
        endpoint: EndpointSync[_REQ, _RES],
        ctx: RequestContext[_REQ, _RES],
        context: contextvars.Context,
    ) -> Iterable[bytes]:
        metadata_run = MetadataInterceptorsRunSync(self._metadata_interceptors, ctx)
        try:
            metadata_run.start()
            if not req_compression:
                raise unknown_compression_error(
                    headers.get(stream.protocol.compression_header_name(), ""),
                    self._compressions,
                )
            request_stream = _request_stream(
                request_body,
                endpoint.method.input,
                stream.codec,
                req_compression,
                self._read_max_bytes,
            )
            match endpoint:
                case _server_shared.EndpointUnarySync():
                    request = _consume_single_request(request_stream)
                    response = endpoint.function(request, ctx)
                    # End the run before sending the response so on_end can still
                    # modify response metadata.
                    if (end_error := metadata_run.end(None)) is not None:
                        raise end_error
                    response_stream = iter([response])
                case _server_shared.EndpointClientStreamSync():
                    response = endpoint.function(request_stream, ctx)
                    if (end_error := metadata_run.end(None)) is not None:
                        raise end_error
                    response_stream = iter([response])
                case _server_shared.EndpointServerStreamSync():
                    request = _consume_single_request(request_stream)
                    response_stream = endpoint.function(request, ctx)
                case _server_shared.EndpointBidiStreamSync():
                    response_stream = endpoint.function(request_stream, ctx)

            # Trigger service logic by consuming the first (possibly only) response message.
            first_response = next(response_stream, None)
            if first_response is None:
                # It's valid for a service method to return no messages, finish the response
                # without error.
                error = metadata_run.end(None)
                return stream.end(start_response, send_trailers, ctx, error)
            # Response headers set before the first message should be set to the context and
            # we can send them.
            stream.send_headers(start_response, ctx)

            # WSGI requires start_response to be called before returning the body iterator.
            # This means we cannot call yield in this function since the function would not
            # run at all until the iterator is consumed, meaning start_response wouldn't have
            # been called in time. So we return the response stream as a separate generator
            # function. This means some duplication of error handling.
            return _iterate_in_context(
                context,
                _response_stream(
                    first_response,
                    environ,
                    request_body,
                    response_stream,
                    stream.writer,
                    send_trailers,
                    ctx,
                    metadata_run,
                ),
            )
        except Exception as e:  # noqa: BLE001 # invoking user callback
            # Exception before any response message was returned. An error after the first
            # response message will be handled by _response_stream, so here we have a
            # full error-only response.
            error = metadata_run.end(e)
            assert error is not None  # noqa: S101 # end never discards an error
            _drain_request_body(environ, request_body)
            _maybe_log_exception(environ, error)
            return stream.end(start_response, send_trailers, ctx, error)
        except BaseException as e:
            # Report the error to on_end, then let the exception reach the app server.
            error = ConnectError(Code.UNKNOWN, "Request was interrupted")
            error.__cause__ = e
            metadata_run.end(error)
            raise

    def _handle_error(
        self,
        exc: Exception,
        ctx: RequestContext | None,
        protocol: ServerProtocol | None,
        start_response: StartResponse,
    ) -> Iterable[bytes]:
        """Handle and log errors with detailed information."""
        headers: list[tuple[str, str]]
        body: list[bytes]
        status: str
        if isinstance(protocol, GRPCServerProtocol) and not isinstance(exc, HTTPError):
            # gRPC clients read the status from trailers, so this is a trailers-only
            # response: HTTP 200 with the gRPC status in the headers.
            grpc_headers = protocol.trailers_only_headers(ctx, exc)
            start_response("200 OK", list(grpc_headers.allitems()))
            return []
        if isinstance(exc, HTTPError):
            headers = exc.headers
            body = []
            status = f"{exc.status.value} {exc.status.phrase}"
        else:
            wire_error = ConnectWireError.from_exception(exc)
            http_status = wire_error.to_http_status()
            headers = [("Content-Type", "application/json")]
            body = [wire_error.to_json_bytes()]
            status = f"{http_status.code} {http_status.reason}"

        if ctx:
            _add_context_headers(headers, ctx)

        start_response(status, headers)
        return body


def _end_response(
    message: bytes | Headers,
    send_trailers: Callable[[list[tuple[str, str]]], None] | None,
) -> bytes:
    if isinstance(message, bytes):
        return message
    assert send_trailers is not None  # noqa: S101
    send_trailers(list(message.allitems()))
    return b""


def _add_context_headers(headers: list[tuple[str, str]], ctx: RequestContext) -> None:
    headers.extend((key, value) for key, value in ctx.response_headers.allitems())
    headers.extend(
        (f"trailer-{key}", value) for key, value in ctx.response_trailers.allitems()
    )


def _request_stream(
    request_body: _RequestBody,
    request_class: type[_REQ],
    codec: Codec,
    compression: Compression,
    read_max_bytes: int | None = None,
) -> Iterator[_REQ]:
    with EnvelopeReader(request_class, codec, compression, read_max_bytes) as reader:
        for chunk in _read_body(request_body):
            yield from reader.feed(chunk)


def _end_stream(stream: Iterator[object], error: Exception | None) -> None:
    """End the handler's stream, throwing in the error that ended the response.

    The handler, and the interceptors wrapping it, then see the error at the
    yield they are suspended at.
    """
    throw = getattr(stream, "throw", None)
    if error is not None and throw is not None:
        try:
            throw(error)
        except StopIteration:
            return
        except Exception as e:
            if e is not error:
                raise
            return
    if (close := getattr(stream, "close", None)) is not None:
        close()


def _iterate_in_context(
    context: contextvars.Context, stream: Generator[bytes]
) -> Generator[bytes]:
    """Iterate stream, and close it, in context.

    The app server iterates the response after the application returns, outside
    any context middleware set up around the call.
    """
    try:
        while True:
            try:
                chunk = context.run(stream.__next__)
            except StopIteration:
                return
            yield chunk
    finally:
        context.run(stream.close)


def _response_stream(
    first_response: _RES,
    environ: WSGIEnvironment,
    request_body: _RequestBody,
    response_stream: Iterator[_RES],
    writer: EnvelopeWriter,
    send_trailers: Callable[[list[tuple[str, str]]], None] | None,
    ctx: RequestContext,
    metadata_run: MetadataInterceptorsRunSync,
) -> Generator[bytes]:
    error: Exception | None = None
    try:
        body = writer.write(first_response)
        yield body
        for message in response_stream:
            body = writer.write(message)
            yield body
    except Exception as e:  # noqa: BLE001 # invoking user callback
        error = e
        _drain_request_body(environ, request_body)
    except BaseException as e:
        # Save the error for on_end, then let the exception reach the app server.
        if isinstance(e, GeneratorExit):
            # The app server closed the response before its end, such as when the
            # client disconnected.
            error = ConnectError(Code.CANCELED, "Client disconnected")
        else:
            error = ConnectError(Code.UNKNOWN, "Request was interrupted")
        error.__cause__ = e
        raise
    finally:
        # End the handler's stream so its cleanup runs before on_end, giving
        # it the error that ended the response.
        _end_stream(response_stream, error)
        # End the run before ending the response so on_end can still modify
        # response trailers. This is a no-op if the run already ended.
        error = metadata_run.end(error)

    yield _end_response(
        writer.end(
            ctx.response_trailers,
            ConnectWireError.from_exception(error) if error else None,
        ),
        send_trailers,
    )


def _consume_single_request(stream: Iterator[_REQ]) -> _REQ:
    req = None
    for message in stream:
        if req is not None:
            raise ConnectError(
                Code.UNIMPLEMENTED, "unary request has multiple messages"
            )
        req = message
    if req is None:
        raise ConnectError(Code.UNIMPLEMENTED, "unary request has zero messages")
    return req


def _apply_interceptors(
    endpoint: EndpointSync[_REQ, _RES], interceptors: Sequence[InterceptorSync]
) -> EndpointSync:
    match endpoint:
        case EndpointUnarySync():
            func = endpoint.function
            for interceptor in reversed(interceptors):
                if not isinstance(interceptor, UnaryInterceptorSync):
                    continue
                func = functools.partial(interceptor.intercept_unary_sync, func)
            return replace(endpoint, function=func)
        case EndpointClientStreamSync():
            func = endpoint.function
            for interceptor in reversed(interceptors):
                if not isinstance(interceptor, ClientStreamInterceptorSync):
                    continue
                func = functools.partial(interceptor.intercept_client_stream_sync, func)
            return replace(endpoint, function=func)
        case EndpointServerStreamSync():
            func = endpoint.function
            for interceptor in reversed(interceptors):
                if not isinstance(interceptor, ServerStreamInterceptorSync):
                    continue
                func = functools.partial(interceptor.intercept_server_stream_sync, func)
            return replace(endpoint, function=func)
        case EndpointBidiStreamSync():
            func = endpoint.function
            for interceptor in reversed(interceptors):
                if not isinstance(interceptor, BidiStreamInterceptorSync):
                    continue
                func = functools.partial(interceptor.intercept_bidi_stream_sync, func)
            return replace(endpoint, function=func)


def _drain_request_body(environ: WSGIEnvironment, request_body: _RequestBody) -> None:
    if environ.get("SERVER_PROTOCOL", "").startswith("HTTP/1"):
        # In HTTP/1, the request body should be drained before returning. Generally it's
        # best for the application server to handle this, but gunicorn is a famous
        # server that doesn't do so, so we go ahead and do it ourselves.
        for _ in _read_body(request_body):
            pass


def _maybe_log_exception(environ: WSGIEnvironment, exc: Exception) -> None:
    if isinstance(exc, (ConnectError, HTTPError)):
        return
    errors: ErrorStream = environ["wsgi.errors"]
    errors.write(
        f"Exception in WSGI application\n{''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))}"
    )

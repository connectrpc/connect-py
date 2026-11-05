from __future__ import annotations

import traceback
from abc import ABC, abstractmethod
from http import HTTPStatus
from typing import TYPE_CHECKING, TypeVar
from urllib.parse import parse_qs

from . import _server_shared
from ._compression import unknown_compression_error
from ._envelope import EnvelopeReader, EnvelopeWriter
from ._interceptor_sync import (
    BidiStreamInterceptorSync,
    ClientStreamInterceptorSync,
    InterceptorSync,
    MetadataInterceptorsRunSync,
    ServerStreamInterceptorSync,
    UnaryInterceptorSync,
    resolve_interceptors,
    split_leading_metadata_interceptors,
)
from ._protocol import ConnectWireError, HTTPError, ServerProtocol
from ._protocol_connect import ConnectServerProtocol
from ._protocol_server import negotiate_server_protocol
from ._server_shared import (
    DEFAULT_READ_MAX_BYTES,
    EndpointBidiStreamSync,
    EndpointClientStreamSync,
    EndpointServerStreamSync,
    EndpointUnarySync,
)
from .code import Code
from .errors import ConnectError
from .request import Headers, RequestContext

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping
    from io import BytesIO
    from wsgiref.types import ErrorStream, StartResponse, WSGIEnvironment

    from ._codec import Codec
    from .compression import Compression
else:
    StartResponse = "wsgiref.types.StartResponse"
    WSGIEnvironment = "wsgiref.types.WSGIEnvironment"


_REQ = TypeVar("_REQ")
_RES = TypeVar("_RES")

_BODY_CHUNK_SIZE = 4096

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


class ConnectWSGIApplication(_server_shared.ServerBase, ABC):
    """A WSGI application for the Connect protocol."""

    @property
    @abstractmethod
    def path(self) -> str: ...

    def __init__(
        self,
        *,
        endpoints: Mapping[str, EndpointSync],
        interceptors: Iterable[InterceptorSync] = (),
        read_max_bytes: int | None = DEFAULT_READ_MAX_BYTES,
        compressions: Iterable[Compression] | None = None,
        codecs: Iterable[Codec] | None = None,
    ) -> None:
        """Initialize the WSGI application.

        Args:
            endpoints: A mapping of URL paths to endpoints. Typically provided directly
                by generated code from the Connect Python plugin.
            interceptors: A sequence of interceptors to apply to the endpoints.
            read_max_bytes: Maximum size of request messages.
            compressions: Supported compression algorithms. If unset, defaults to gzip.
                          If set to empty, disables compression.
            codecs: The codecs supported by the server. If unset, defaults to Protocol Buffers
                    binary and JSON codecs.

        """
        super().__init__(
            read_max_bytes=read_max_bytes, compressions=compressions, codecs=codecs
        )
        self._metadata_interceptors, interceptors = split_leading_metadata_interceptors(
            interceptors
        )
        self._endpoints = _server_shared.apply_interceptors(
            endpoints, resolve_interceptors(interceptors), _INTERCEPTOR_KINDS
        )

    def __call__(
        self, environ: WSGIEnvironment, start_response: StartResponse
    ) -> Iterable[bytes]:
        ctx: RequestContext | None = None
        protocol: ServerProtocol | None = None
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
            send_trailers: Callable[[list[tuple[str, str]]], None] | None = None

            if protocol.uses_trailers():
                send_trailers = environ.get("wsgi.ext.http.send_trailers")
                if not send_trailers:
                    msg = f"WSGI server does not support WSGI trailers extension but protocol for content-type '{content_type}' requires trailers"
                    raise RuntimeError(msg)
            ctx = protocol.create_request_context(
                endpoint.method, http_method, http_scheme, headers, client_address
            )

            if isinstance(endpoint, EndpointUnarySync) and isinstance(
                protocol, ConnectServerProtocol
            ):
                return self._handle_unary(
                    environ,
                    request_body,
                    start_response,
                    http_method,
                    protocol,
                    endpoint,
                    ctx,
                    headers,
                )
            return self._handle_stream(
                environ,
                request_body,
                start_response,
                send_trailers,
                protocol,
                headers,
                endpoint,
                ctx,
            )

        except Exception as e:  # noqa: BLE001 # invoking user callback
            _drain_request_body(environ, request_body)
            _maybe_log_exception(environ, e)
            return self._handle_error(e, ctx, protocol, start_response)

    def _handle_unary(
        self,
        environ: WSGIEnvironment,
        request_body: _RequestBody,
        start_response: StartResponse,
        http_method: str,
        protocol: ServerProtocol,
        endpoint: EndpointUnarySync[_REQ, _RES],
        ctx: RequestContext[_REQ, _RES],
        headers: Headers,
    ) -> Iterable[bytes]:
        query_params = (
            parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
            if http_method == "GET"
            else None
        )
        codec = self._resolve_codec(protocol, headers, query_params, stream=False)

        metadata_run = MetadataInterceptorsRunSync(self._metadata_interceptors, ctx)
        response: _RES | None = None
        error: Exception | None = None
        try:
            metadata_run.start()
            if query_params is not None:
                request = self._decode_unary_get(
                    query_params, codec, endpoint.method.input
                )
            else:
                request = self._decode_unary(
                    self._read_unary_body(environ, request_body),
                    headers.get("content-encoding"),
                    codec,
                    endpoint.method.input,
                )
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

        response_headers, body = _server_shared.encode_unary(
            response, codec, self._unary_response_compression(headers), ctx
        )
        start_response("200 OK", response_headers)
        return [body]

    def _read_unary_body(
        self, environ: WSGIEnvironment, request_body: _RequestBody
    ) -> bytes:
        try:
            content_length = environ.get("CONTENT_LENGTH")
            content_length = 0 if not content_length else int(content_length)
            if content_length > 0:
                self._check_read_size(content_length)
                return _read_body_with_content_length(request_body, content_length)
            chunks: list[bytes] = []
            read_bytes = 0
            for chunk in _read_body(request_body):
                read_bytes += len(chunk)
                self._check_read_size(read_bytes)
                chunks.append(chunk)
            return b"".join(chunks)
        except ConnectError:
            raise
        except Exception as e:
            raise ConnectError(Code.INTERNAL, str(e)) from e

    def _handle_stream(
        self,
        environ: WSGIEnvironment,
        request_body: _RequestBody,
        start_response: StartResponse,
        send_trailers: Callable[[list[tuple[str, str]]], None] | None,
        protocol: ServerProtocol,
        headers: Headers,
        endpoint: EndpointSync[_REQ, _RES],
        ctx: RequestContext[_REQ, _RES],
    ) -> Iterable[bytes]:
        req_compression, resp_compression = protocol.negotiate_stream_compression(
            headers, self._compressions
        )

        codec = self._resolve_codec(protocol, headers, None, stream=True)
        writer = protocol.create_envelope_writer(codec, resp_compression)
        metadata_run = MetadataInterceptorsRunSync(self._metadata_interceptors, ctx)
        try:
            metadata_run.start()
            if not req_compression:
                raise unknown_compression_error(
                    headers.get(protocol.compression_header_name(), ""),
                    self._compressions,
                )
            request_stream = _request_stream(
                request_body,
                endpoint.method.input,
                codec,
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
            # Response headers set before the first message should be set to the context and
            # we can send them.
            start_response(
                "200 OK",
                _server_shared.stream_response_headers(
                    protocol, codec, resp_compression, ctx
                ),
            )
            if first_response is None:
                # It's valid for a service method to return no messages, finish the response
                # without error.
                error = metadata_run.end(None)
                return [
                    _end_response(
                        writer.end(
                            ctx.response_trailers,
                            ConnectWireError.from_exception(error) if error else None,
                        ),
                        send_trailers,
                    )
                ]

            # WSGI requires start_response to be called before returning the body iterator.
            # This means we cannot call yield in this function since the function would not
            # run at all until the iterator is consumed, meaning start_response wouldn't have
            # been called in time. So we return the response stream as a separate generator
            # function. This means some duplication of error handling.
            return _response_stream(
                first_response,
                environ,
                request_body,
                response_stream,
                writer,
                send_trailers,
                ctx,
                metadata_run,
            )
        except Exception as e:  # noqa: BLE001 # invoking user callback
            # Exception before any response message was returned. An error after the first
            # response message will be handled by _response_stream, so here we have a
            # full error-only response.
            error = metadata_run.end(e)
            assert error is not None  # noqa: S101 # end never discards an error
            _drain_request_body(environ, request_body)
            _maybe_log_exception(environ, error)
            start_response(
                "200 OK",
                _server_shared.stream_response_headers(
                    protocol, codec, resp_compression, ctx
                ),
            )
            return [
                _end_response(
                    writer.end(
                        ctx.response_trailers, ConnectWireError.from_exception(error)
                    ),
                    send_trailers,
                )
            ]
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
        status, reason, headers, body = _server_shared.error_response(
            exc, ctx, protocol
        )
        start_response(f"{status} {reason}", headers)
        return [body] if body else []


def _end_response(
    message: bytes | Headers,
    send_trailers: Callable[[list[tuple[str, str]]], None] | None,
) -> bytes:
    if isinstance(message, bytes):
        return message
    assert send_trailers is not None  # noqa: S101
    send_trailers(list(message.allitems()))
    return b""


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


def _response_stream(
    first_response: _RES,
    environ: WSGIEnvironment,
    request_body: _RequestBody,
    response_stream: Iterator[_RES],
    writer: EnvelopeWriter,
    send_trailers: Callable[[list[tuple[str, str]]], None] | None,
    ctx: RequestContext,
    metadata_run: MetadataInterceptorsRunSync,
) -> Iterable[bytes]:
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


_INTERCEPTOR_KINDS: dict[type, tuple[type, str]] = {
    EndpointUnarySync: (UnaryInterceptorSync, "intercept_unary_sync"),
    EndpointClientStreamSync: (
        ClientStreamInterceptorSync,
        "intercept_client_stream_sync",
    ),
    EndpointServerStreamSync: (
        ServerStreamInterceptorSync,
        "intercept_server_stream_sync",
    ),
    EndpointBidiStreamSync: (BidiStreamInterceptorSync, "intercept_bidi_stream_sync"),
}


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

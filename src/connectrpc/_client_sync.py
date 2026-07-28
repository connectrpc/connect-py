from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any, Protocol, TypeVar
from urllib.parse import urlencode

from pyqwest import FullResponse, Headers as HTTPHeaders, SyncClient, SyncResponse

from connectrpc._protocol_grpc import GRPCClientProtocol, GRPCWebClientProtocol

from . import _client_shared
from ._codec import proto_binary_codec
from ._compression import IdentityCompression, _gzip, resolve_compressions
from ._interceptor_sync import (
    BidiStreamInterceptorSync,
    ClientStreamInterceptorSync,
    InterceptorSync,
    ServerStreamInterceptorSync,
    UnaryInterceptorSync,
    resolve_interceptors,
)
from ._protocol import ConnectWireError
from ._protocol_connect import ConnectClientProtocol, ConnectEnvelopeWriter
from ._response_metadata import handle_response_headers
from .code import Code
from .errors import ConnectError
from .protocol import ProtocolType

if TYPE_CHECKING:
    import sys
    from collections.abc import Iterable, Iterator, Mapping
    from types import TracebackType

    from ._envelope import EnvelopeReader
    from .codec import Codec
    from .compression import Compression
    from .method import MethodInfo
    from .request import Headers, RequestContext

    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self
else:
    Self = "Self"

REQ = TypeVar("REQ")
RES = TypeVar("RES")


class _ExecuteUnary(Protocol[REQ, RES]):
    def __call__(self, request: REQ, ctx: RequestContext[REQ, RES], /) -> RES: ...


class _ExecuteClientStream(Protocol[REQ, RES]):
    def __call__(
        self, request: Iterator[REQ], ctx: RequestContext[REQ, RES], /
    ) -> RES: ...


class _ExecuteServerStream(Protocol[REQ, RES]):
    def __call__(
        self, request: REQ, ctx: RequestContext[REQ, RES], /
    ) -> Iterator[RES]: ...


class _ExecuteBidiStream(Protocol[REQ, RES]):
    def __call__(
        self, request: Iterator[REQ], ctx: RequestContext[REQ, RES], /
    ) -> Iterator[RES]: ...


class ConnectClientSync:
    """A synchronous client for the Connect protocol."""

    _execute_unary: _ExecuteUnary
    _execute_client_stream: _ExecuteClientStream
    _execute_server_stream: _ExecuteServerStream
    _execute_bidi_stream: _ExecuteBidiStream

    def __init__(
        self,
        address: str,
        *,
        codec: Codec | None = None,
        protocol: ProtocolType = ProtocolType.CONNECT,
        accept_compression: Iterable[Compression] | None = None,
        send_compression: Compression | None = _gzip,
        timeout_ms: int | None = None,
        read_max_bytes: int | None = None,
        interceptors: Iterable[InterceptorSync] = (),
        http_client: SyncClient | None = None,
    ) -> None:
        """Create a new synchronous Connect client.

        When providing an HTTP client, for example to configure TLS settings,
        it is the caller's responsibility to close it.

        Examples:
            ```python
            from pyqwest import SyncClient
            from my_service import MyServiceClientSync

            with (
                SyncClient() as http_client,
                MyServiceClientSync("http://localhost:8000", http_client=http_client) as client,
            ):
                # Use the client!
            ```

        Args:
            address: The address of the server to connect to, including scheme.
            codec: The [Codec][] to use for requests. If unset, defaults to binary protobuf.
                   For JSON encoding, use [proto_json_codec][connectrpc.codec.proto_json_codec].
            protocol: The [ProtocolType][] to use for requests.
            accept_compression: Compression algorithms to accept from the server. If unset,
                                defaults to gzip. If set to empty, disables response compression.
            send_compression: Compression algorithm to use for sending requests. If unset,
                              defaults to gzip. If set to None, disables request compression.
            timeout_ms: The timeout for requests in milliseconds.
            read_max_bytes: The maximum number of bytes to read from the response.
            interceptors: A list of interceptors to apply to requests.
            http_client: A pyqwest SyncClient to use for requests.

        """
        self._address = address
        self._codec = codec or proto_binary_codec()
        self._timeout_ms = timeout_ms
        self._read_max_bytes = read_max_bytes
        self._response_compressions = resolve_compressions(accept_compression)
        self._accept_compression_header = ",".join(self._response_compressions.keys())
        self._send_compression = send_compression or IdentityCompression()
        if http_client:
            self._http_client = http_client
        else:
            # Use shared default transport if not specified
            self._http_client = SyncClient()
        self._closed = False

        match protocol:
            case ProtocolType.CONNECT:
                self._protocol = ConnectClientProtocol()
            case ProtocolType.GRPC:
                self._protocol = GRPCClientProtocol()
            case ProtocolType.GRPC_WEB:
                self._protocol = GRPCWebClientProtocol()

        interceptors = resolve_interceptors(interceptors)
        execute_unary = self._send_request_unary
        for interceptor in (
            i for i in reversed(interceptors) if isinstance(i, UnaryInterceptorSync)
        ):
            execute_unary = functools.partial(
                interceptor.intercept_unary_sync, execute_unary
            )
        self._execute_unary = execute_unary

        execute_client_stream = self._send_request_client_stream
        for interceptor in (
            i
            for i in reversed(interceptors)
            if isinstance(i, ClientStreamInterceptorSync)
        ):
            execute_client_stream = functools.partial(
                interceptor.intercept_client_stream_sync, execute_client_stream
            )
        self._execute_client_stream = execute_client_stream

        execute_server_stream: _ExecuteServerStream = self._send_request_server_stream
        for interceptor in (
            i
            for i in reversed(interceptors)
            if isinstance(i, ServerStreamInterceptorSync)
        ):
            execute_server_stream = functools.partial(
                interceptor.intercept_server_stream_sync, execute_server_stream
            )
        self._execute_server_stream = execute_server_stream

        execute_bidi_stream = self._send_request_bidi_stream
        for interceptor in (
            i
            for i in reversed(interceptors)
            if isinstance(i, BidiStreamInterceptorSync)
        ):
            execute_bidi_stream = functools.partial(
                interceptor.intercept_bidi_stream_sync, execute_bidi_stream
            )
        self._execute_bidi_stream = execute_bidi_stream

    def close(self) -> None:
        """Close the client. After closing, the client cannot be used to make requests."""
        if not self._closed:
            self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        self.close()

    def execute_unary(
        self,
        *,
        request: REQ,
        method: MethodInfo[REQ, RES],
        headers: Headers | Mapping[str, str] | None = None,
        timeout_ms: int | None = None,
        use_get: bool = False,
    ) -> RES:
        ctx = self._protocol.create_request_context(
            method=method,
            url=self._address,
            http_method="GET" if use_get else "POST",
            user_headers=headers,
            timeout_ms=timeout_ms or self._timeout_ms,
            codec=self._codec,
            stream=False,
            accept_compression=self._accept_compression_header,
            send_compression=self._send_compression,
        )
        return self._execute_unary(request, ctx)

    def execute_client_stream(
        self,
        *,
        request: Iterator[REQ],
        method: MethodInfo[REQ, RES],
        headers: Headers | Mapping[str, str] | None = None,
        timeout_ms: int | None = None,
    ) -> RES:
        ctx = self._protocol.create_request_context(
            method=method,
            url=self._address,
            http_method="POST",
            user_headers=headers,
            timeout_ms=timeout_ms or self._timeout_ms,
            codec=self._codec,
            stream=True,
            accept_compression=self._accept_compression_header,
            send_compression=self._send_compression,
        )
        return self._execute_client_stream(request, ctx)

    def execute_server_stream(
        self,
        *,
        request: REQ,
        method: MethodInfo[REQ, RES],
        headers: Headers | Mapping[str, str] | None = None,
        timeout_ms: int | None = None,
    ) -> Iterator[RES]:
        ctx = self._protocol.create_request_context(
            method=method,
            url=self._address,
            http_method="POST",
            user_headers=headers,
            timeout_ms=timeout_ms or self._timeout_ms,
            codec=self._codec,
            stream=True,
            accept_compression=self._accept_compression_header,
            send_compression=self._send_compression,
        )
        return self._execute_server_stream(request, ctx)

    def execute_bidi_stream(
        self,
        *,
        request: Iterator[REQ],
        method: MethodInfo[REQ, RES],
        headers: Headers | Mapping[str, str] | None = None,
        timeout_ms: int | None = None,
    ) -> Iterator[RES]:
        ctx = self._protocol.create_request_context(
            method=method,
            url=self._address,
            http_method="POST",
            user_headers=headers,
            timeout_ms=timeout_ms or self._timeout_ms,
            codec=self._codec,
            stream=True,
            accept_compression=self._accept_compression_header,
            send_compression=self._send_compression,
        )
        return self._execute_bidi_stream(request, ctx)

    def _send_request_unary(self, request: REQ, ctx: RequestContext[REQ, RES]) -> RES:
        if isinstance(self._protocol, GRPCClientProtocol):
            return _consume_single_response(
                self._send_request_bidi_stream(iter([request]), ctx)
            )

        request_headers = HTTPHeaders(ctx.request_headers.allitems())
        url = f"{self._address}/{ctx.method.service_name}/{ctx.method.name}"
        if (timeout_ms := ctx.timeout_ms) is not None:
            timeout_s = timeout_ms / 1000.0
        else:
            timeout_s = None

        try:
            request_data = self._codec.encode(request)
            if self._send_compression:
                request_data = self._send_compression.compress(request_data)

            if ctx.http_method == "GET":
                params = _client_shared.prepare_get_params(
                    self._codec, request_data, request_headers
                )
                params_str = urlencode(params)
                url = f"{url}?{params_str}"
                request_headers.pop("content-type", None)
                resp = self._http_client.get(
                    url=url, headers=request_headers, timeout=timeout_s
                )
            else:
                resp = self._http_client.post(
                    url=url,
                    headers=request_headers,
                    content=request_data,
                    timeout=timeout_s,
                )
            handle_response_headers(resp.status, resp.headers)

            self._protocol.validate_response(
                self._codec.name(), resp.status, resp.headers.get("content-type", "")
            )
            # Decompression itself is handled by pyqwest, but we validate it
            # by resolving it.
            self._protocol.handle_response_compression(
                resp.headers, self._response_compressions, stream=False
            )

            if resp.status == 200:
                if (
                    self._read_max_bytes is not None
                    and len(resp.content) > self._read_max_bytes
                ):
                    raise ConnectError(
                        Code.RESOURCE_EXHAUSTED,
                        f"message is larger than configured max {self._read_max_bytes}",
                    )

                return self._codec.decode(resp.content, ctx.method.output)
            raise ConnectWireError.from_response(resp).to_exception()
        except TimeoutError as e:
            raise ConnectError(Code.DEADLINE_EXCEEDED, "Request timed out") from e
        except ConnectError:
            raise
        except Exception as e:
            raise ConnectError(Code.UNAVAILABLE, str(e)) from e

    def _send_request_client_stream(
        self, request: Iterator[REQ], ctx: RequestContext[REQ, RES], /
    ) -> RES:
        return _consume_single_response(self._send_request_bidi_stream(request, ctx))

    def _send_request_server_stream(
        self, request: REQ, ctx: RequestContext[REQ, RES], /
    ) -> Iterator[RES]:
        return self._send_request_bidi_stream(iter([request]), ctx)

    def _send_request_bidi_stream(
        self, request: Iterator[REQ], ctx: RequestContext[REQ, RES], /
    ) -> Iterator[RES]:
        request_headers = HTTPHeaders(ctx.request_headers.allitems())
        url = f"{self._address}/{ctx.method.service_name}/{ctx.method.name}"
        if (timeout_ms := ctx.timeout_ms) is not None:
            timeout_s = timeout_ms / 1000.0
        else:
            timeout_s = None

        stream_error: Exception | None = None
        reader: EnvelopeReader | None = None
        resp: SyncResponse | None = None
        try:
            request_data = _streaming_request_content(
                request, self._codec, self._send_compression
            )

            with self._http_client.stream(
                method="POST",
                url=url,
                headers=request_headers,
                content=request_data,
                timeout=timeout_s,
            ) as resp:
                handle_response_headers(resp.status, resp.headers)

                if resp.status == 200:
                    self._protocol.validate_stream_response(
                        self._codec.name(), resp.headers.get("content-type", "")
                    )
                    compression = self._protocol.handle_response_compression(
                        resp.headers, self._response_compressions, stream=True
                    )
                    reader = self._protocol.create_envelope_reader(
                        ctx.method.output,
                        self._codec,
                        compression,
                        self._read_max_bytes,
                    )
                    try:
                        for chunk in resp.content:
                            yield from reader.feed(chunk)
                    except ConnectError as e:
                        stream_error = e
                        raise
                    # For sync, we rely on the HTTP client to handle timeout, but
                    # currently the one we use for gRPC does not propagate RST_STREAM
                    # correctly which is used for server timeouts. We go ahead and check
                    # the timeout ourselves too.
                    # https://github.com/hyperium/hyper/issues/3681#issuecomment-3734084436
                    if (t := ctx.timeout_ms) is not None and t <= 0:
                        raise TimeoutError

                    reader.handle_response_complete(resp)
                else:
                    content = bytearray()
                    for chunk in resp.content:
                        content.extend(chunk)
                    fres = FullResponse(
                        status=resp.status,
                        headers=resp.headers,
                        content=bytes(content),
                        trailers=resp.trailers,
                    )
                    raise ConnectWireError.from_response(fres).to_exception()
        except TimeoutError as e:
            raise ConnectError(Code.DEADLINE_EXCEEDED, "Request timed out") from e
        except ConnectError:
            raise
        except Exception as e:
            # If a context manager's exit raises an exception, it overwrites any raised
            # by our own stream handling. This seems to happen when we end the response without
            # fully consuming it due to message limits. It should always be fine to prioritize
            # the stream error here.
            if stream_error is not None:
                raise stream_error from None

            if rst_err := _client_shared.maybe_map_stream_reset(e, ctx):
                # It is possible for a reset to come with trailers which should
                # be used.
                if reader and resp:
                    reader.handle_response_complete(resp, rst_err)
                raise rst_err from e
            raise ConnectError(Code.UNAVAILABLE, str(e)) from e


def _streaming_request_content(
    msgs: Iterator[Any], codec: Codec, compression: Compression | None
) -> Iterator[bytes]:
    writer = ConnectEnvelopeWriter(codec, compression)
    for msg in msgs:
        yield writer.write(msg)


def _consume_single_response(stream: Iterator[RES]) -> RES:
    res = None
    for message in stream:
        if res is not None:
            raise ConnectError(
                Code.UNIMPLEMENTED, "unary response has multiple messages"
            )
        res = message
    if res is None:
        raise ConnectError(Code.UNIMPLEMENTED, "unary response has zero messages")
    return res

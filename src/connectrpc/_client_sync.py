from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from pyqwest import Headers as HTTPHeaders, SyncClient, SyncResponse

from . import _client_shared
from ._compression import _gzip
from ._interceptor_sync import (
    BidiStreamInterceptorSync,
    ClientStreamInterceptorSync,
    InterceptorSync,
    ServerStreamInterceptorSync,
    UnaryInterceptorSync,
    resolve_interceptors,
)
from ._protocol_connect import ConnectEnvelopeWriter
from ._response_metadata import handle_response_headers
from ._shared import chain_interceptors
from .code import Code
from .errors import ConnectError
from .protocol import ProtocolType

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping
    from types import TracebackType
    from typing import Self

    from ._envelope import EnvelopeReader
    from .codec import Codec
    from .compression import Compression
    from .method import MethodInfo
    from .request import Headers, RequestContext
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


class ConnectClientSync(_client_shared.ClientBase):
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
        super().__init__(
            address=address,
            codec=codec,
            protocol=protocol,
            accept_compression=accept_compression,
            send_compression=send_compression,
            timeout_ms=timeout_ms,
            read_max_bytes=read_max_bytes,
        )
        self._http_client = http_client or SyncClient()
        self._closed = False

        interceptors = resolve_interceptors(interceptors)
        self._execute_unary = chain_interceptors(
            self._send_request_unary,
            interceptors,
            UnaryInterceptorSync,
            "intercept_unary_sync",
        )
        self._execute_client_stream = chain_interceptors(
            self._send_request_client_stream,
            interceptors,
            ClientStreamInterceptorSync,
            "intercept_client_stream_sync",
        )
        self._execute_server_stream = chain_interceptors(
            self._send_request_server_stream,
            interceptors,
            ServerStreamInterceptorSync,
            "intercept_server_stream_sync",
        )
        self._execute_bidi_stream = chain_interceptors(
            self._send_request_bidi_stream,
            interceptors,
            BidiStreamInterceptorSync,
            "intercept_bidi_stream_sync",
        )

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
        ctx = self._new_request_context(
            method, headers, timeout_ms, stream=False, use_get=use_get
        )
        return self._execute_unary(request, ctx)

    def execute_client_stream(
        self,
        *,
        # This should not be changed to accept a list unless reworking logic that would
        # concatenate them into a single payload that may cause backpressure issues.
        request: Iterator[REQ],
        method: MethodInfo[REQ, RES],
        headers: Headers | Mapping[str, str] | None = None,
        timeout_ms: int | None = None,
    ) -> RES:
        ctx = self._new_request_context(method, headers, timeout_ms, stream=True)
        return self._execute_client_stream(request, ctx)

    def execute_server_stream(
        self,
        *,
        request: REQ,
        method: MethodInfo[REQ, RES],
        headers: Headers | Mapping[str, str] | None = None,
        timeout_ms: int | None = None,
    ) -> Iterator[RES]:
        ctx = self._new_request_context(method, headers, timeout_ms, stream=True)
        return self._execute_server_stream(request, ctx)

    def execute_bidi_stream(
        self,
        *,
        # This should not be changed to accept a list unless reworking logic that would
        # concatenate them into a single payload that may cause backpressure issues.
        request: Iterator[REQ],
        method: MethodInfo[REQ, RES],
        headers: Headers | Mapping[str, str] | None = None,
        timeout_ms: int | None = None,
    ) -> Iterator[RES]:
        ctx = self._new_request_context(method, headers, timeout_ms, stream=True)
        return self._execute_bidi_stream(request, ctx)

    def _send_request_unary(self, request: REQ, ctx: RequestContext[REQ, RES]) -> RES:
        if self._unary_uses_stream():
            return _consume_single_response(
                self._send_request_bidi_stream([request], ctx)
            )

        timeout_s = _client_shared.timeout_seconds(ctx)
        try:
            url, headers, content = self._prepare_unary(request, ctx)
            resp = self._http_client.execute(
                ctx.http_method, url, headers, content, timeout=timeout_s
            )
            handle_response_headers(resp.status, resp.headers)
            return self._finish_unary(resp, ctx)
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
        return self._send_request_bidi_stream([request], ctx)

    def _send_request_bidi_stream(
        self, request: Iterator[REQ] | list[REQ], ctx: RequestContext[REQ, RES], /
    ) -> Iterator[RES]:
        request_headers = HTTPHeaders(ctx.request_headers.allitems())
        timeout_s = _client_shared.timeout_seconds(ctx)
        stream_error: Exception | None = None
        reader: EnvelopeReader | None = None
        resp: SyncResponse | None = None
        try:
            request_data = _request_content(
                request, self._codec, self._send_compression
            )

            with self._http_client.stream(
                method="POST",
                url=self._url(ctx),
                headers=request_headers,
                content=request_data,
                timeout=timeout_s,
            ) as resp:
                handle_response_headers(resp.status, resp.headers)

                if resp.status == 200:
                    with self._stream_reader(resp.headers, ctx) as reader:
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
                    raise _client_shared.stream_status_error(
                        resp.status, resp.headers, bytes(content), resp.trailers
                    )
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


def _request_content(
    msgs: Iterator[Any] | list[Any], codec: Codec, compression: Compression | None
) -> bytes | Iterator[bytes]:
    if isinstance(msgs, list):
        writer = ConnectEnvelopeWriter(codec, compression)
        return b"".join(writer.write(msg) for msg in msgs)
    return _streaming_request_content(msgs, codec, compression)


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

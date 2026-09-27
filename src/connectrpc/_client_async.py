from __future__ import annotations

import asyncio
import contextlib
from asyncio import CancelledError, sleep, timeout_at as asyncio_timeout_at, wait_for
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from pyqwest import Client as HTTPClient, Headers as HTTPHeaders, Response

from . import _client_shared
from ._compression import _gzip
from ._interceptor_async import (
    BidiStreamInterceptor,
    ClientStreamInterceptor,
    Interceptor,
    ServerStreamInterceptor,
    UnaryInterceptor,
    resolve_interceptors,
)
from ._protocol_connect import ConnectEnvelopeWriter
from ._response_metadata import handle_response_headers
from ._shared import chain_interceptors
from .code import Code
from .errors import ConnectError
from .protocol import ProtocolType

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Mapping
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
    async def __call__(self, request: REQ, ctx: RequestContext[REQ, RES], /) -> RES: ...


class _ExecuteClientStream(Protocol[REQ, RES]):
    async def __call__(
        self, request: AsyncIterator[REQ], ctx: RequestContext[REQ, RES], /
    ) -> RES: ...


class _ExecuteServerStream(Protocol[REQ, RES]):
    def __call__(
        self, request: REQ, ctx: RequestContext[REQ, RES], /
    ) -> AsyncIterator[RES]: ...


class _ExecuteBidiStream(Protocol[REQ, RES]):
    def __call__(
        self, request: AsyncIterator[REQ], ctx: RequestContext[REQ, RES], /
    ) -> AsyncIterator[RES]: ...


class ConnectClient(_client_shared.ClientBase):
    """An asynchronous client for the Connect protocol."""

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
        interceptors: Iterable[Interceptor] = (),
        http_client: HTTPClient | None = None,
    ) -> None:
        """Create a new asynchronous Connect client.

        When providing an HTTP client, for example to configure TLS settings,
        it is the caller's responsibility to close it.

        Examples:
            ```python
            from pyqwest import Client
            from my_service import MyServiceClient

            async with (
                Client() as http_client,
                MyServiceClient("http://localhost:8000", http_client=http_client) as client,
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
            http_client: A pyqwest Client to use for requests.

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
        self._http_client = http_client or HTTPClient()
        self._closed = False

        interceptors = resolve_interceptors(interceptors)
        self._execute_unary = chain_interceptors(
            self._send_request_unary, interceptors, UnaryInterceptor, "intercept_unary"
        )
        self._execute_client_stream = chain_interceptors(
            self._send_request_client_stream,
            interceptors,
            ClientStreamInterceptor,
            "intercept_client_stream",
        )
        self._execute_server_stream = chain_interceptors(
            self._send_request_server_stream,
            interceptors,
            ServerStreamInterceptor,
            "intercept_server_stream",
        )
        self._execute_bidi_stream = chain_interceptors(
            self._send_request_bidi_stream,
            interceptors,
            BidiStreamInterceptor,
            "intercept_bidi_stream",
        )

    async def close(self) -> None:
        """Close the client. After closing, the client cannot be used to make requests."""
        if not self._closed:
            self._closed = True

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def execute_unary(
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
        return await self._execute_unary(request, ctx)

    async def execute_client_stream(
        self,
        *,
        # This should not be changed to accept a list unless reworking logic that would
        # concatenate them into a single payload that may cause backpressure issues.
        request: AsyncIterator[REQ],
        method: MethodInfo[REQ, RES],
        headers: Headers | Mapping[str, str] | None = None,
        timeout_ms: int | None = None,
    ) -> RES:
        ctx = self._new_request_context(method, headers, timeout_ms, stream=True)
        return await self._execute_client_stream(request, ctx)

    def execute_server_stream(
        self,
        *,
        request: REQ,
        method: MethodInfo[REQ, RES],
        headers: Headers | Mapping[str, str] | None = None,
        timeout_ms: int | None = None,
    ) -> AsyncIterator[RES]:
        ctx = self._new_request_context(method, headers, timeout_ms, stream=True)
        return self._execute_server_stream(request, ctx)

    def execute_bidi_stream(
        self,
        *,
        # This should not be changed to accept a list unless reworking logic that would
        # concatenate them into a single payload that may cause backpressure issues.
        request: AsyncIterator[REQ],
        method: MethodInfo[REQ, RES],
        headers: Headers | Mapping[str, str] | None = None,
        timeout_ms: int | None = None,
    ) -> AsyncIterator[RES]:
        ctx = self._new_request_context(method, headers, timeout_ms, stream=True)
        return self._execute_bidi_stream(request, ctx)

    async def _send_request_unary(
        self, request: REQ, ctx: RequestContext[REQ, RES], /
    ) -> RES:
        if self._unary_uses_stream():
            return await _consume_single_response(
                self._send_request_bidi_stream([request], ctx)
            )

        timeout_s = _client_shared.timeout_seconds(ctx)
        try:
            url, headers, content = self._prepare_unary(request, ctx)
            resp = await wait_for(
                self._http_client.execute(ctx.http_method, url, headers, content),
                timeout_s,
            )
            handle_response_headers(resp.status, resp.headers)
            return self._finish_unary(resp, ctx)
        except TimeoutError as e:
            raise ConnectError(Code.DEADLINE_EXCEEDED, "Request timed out") from e
        except ConnectError:
            raise
        except CancelledError as e:
            raise ConnectError(Code.CANCELED, "Request was cancelled") from e
        except Exception as e:
            raise ConnectError(Code.UNAVAILABLE, str(e)) from e

    async def _send_request_client_stream(
        self, request: AsyncIterator[REQ], ctx: RequestContext[REQ, RES], /
    ) -> RES:
        return await _consume_single_response(
            self._send_request_bidi_stream(request, ctx)
        )

    def _send_request_server_stream(
        self, request: REQ, ctx: RequestContext[REQ, RES], /
    ) -> AsyncIterator[RES]:
        return self._send_request_bidi_stream([request], ctx)

    async def _send_request_bidi_stream(
        self, request: AsyncIterator[REQ] | list[REQ], ctx: RequestContext[REQ, RES], /
    ) -> AsyncIterator[RES]:
        request_headers = HTTPHeaders(ctx.request_headers.allitems())
        loop = asyncio.get_running_loop()
        timeout_s = _client_shared.timeout_seconds(ctx)
        deadline = None if timeout_s is None else loop.time() + timeout_s

        reader: EnvelopeReader | None = None
        resp: Response | None = None
        try:
            request_data = _request_content(
                request, self._codec, self._send_compression
            )

            # The deadline is applied to each network wait rather than the whole
            # block, so it is never in effect while suspended at a yield, where it
            # would cancel the caller's task instead of the request.
            async with contextlib.AsyncExitStack() as stack:
                async with asyncio_timeout_at(deadline):
                    resp = await stack.enter_async_context(
                        self._http_client.stream(
                            "POST",
                            self._url(ctx),
                            headers=request_headers,
                            content=request_data,
                        )
                    )
                handle_response_headers(resp.status, resp.headers)
                if resp.status == 200:
                    with self._stream_reader(resp.headers, ctx) as reader:
                        it = aiter(resp.content)
                        while True:
                            # A read of buffered data doesn't suspend, so the timeout
                            # alone wouldn't fire once the deadline has passed.
                            if deadline is not None and loop.time() >= deadline:
                                raise TimeoutError
                            async with asyncio_timeout_at(deadline):
                                try:
                                    chunk = await anext(it)
                                except StopAsyncIteration:
                                    break
                            for message in reader.feed(bytes(chunk)):
                                yield message
                                # Check for cancellation each message. While this seems heavyweight,
                                # conformance tests require it.
                                await sleep(0)
                    reader.handle_response_complete(resp)
                else:
                    content = bytearray()
                    async with asyncio_timeout_at(deadline):
                        async for chunk in resp.content:
                            content.extend(chunk)
                    raise _client_shared.stream_status_error(
                        resp.status, resp.headers, bytes(content), resp.trailers
                    )
        except TimeoutError as e:
            raise ConnectError(Code.DEADLINE_EXCEEDED, "Request timed out") from e
        except ConnectError:
            raise
        except CancelledError as e:
            raise ConnectError(Code.CANCELED, "Request was cancelled") from e
        except Exception as e:
            if rst_err := _client_shared.maybe_map_stream_reset(e, ctx):
                # It is possible for a reset to come with trailers which should
                # be used.
                if reader and resp:
                    reader.handle_response_complete(resp, rst_err)
                raise rst_err from e
            raise ConnectError(Code.UNAVAILABLE, str(e)) from e


def _request_content(
    msgs: AsyncIterator[Any] | list[Any], codec: Codec, compression: Compression | None
) -> bytes | AsyncIterator[bytes]:
    if isinstance(msgs, list):
        writer = ConnectEnvelopeWriter(codec, compression)
        return b"".join(writer.write(msg) for msg in msgs)
    return _streaming_request_content(msgs, codec, compression)


async def _streaming_request_content(
    msgs: AsyncIterator[Any], codec: Codec, compression: Compression | None
) -> AsyncIterator[bytes]:
    writer = ConnectEnvelopeWriter(codec, compression)
    async for msg in msgs:
        yield writer.write(msg)


async def _consume_single_response(stream: AsyncIterator[RES]) -> RES:
    res = None
    async for message in stream:
        if res is not None:
            raise ConnectError(
                Code.UNIMPLEMENTED, "unary response has multiple messages"
            )
        res = message
    if res is None:
        raise ConnectError(Code.UNIMPLEMENTED, "unary response has zero messages")
    return res

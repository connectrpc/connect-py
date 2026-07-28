from __future__ import annotations

import argparse
import asyncio
import contextlib
import multiprocessing
import queue
import sys
import time
import traceback
from typing import TYPE_CHECKING, Literal, TypeVar, get_args

import _cov_embed  # noqa: F401
from _util import create_standard_streams
from gen.connectrpc.conformance.v1 import client_compat_pb, service_pb
from gen.connectrpc.conformance.v1.client_compat_pb import (
    ClientCompatRequest,
    ClientCompatResponse,
    ClientErrorResult,
    ClientResponseResult,
)
from gen.connectrpc.conformance.v1.config_pb import (
    Code as ConformanceCode,
    Codec,
    Compression as ConformanceCompression,
    HTTPVersion,
    Protocol,
    StreamType,
)
from gen.connectrpc.conformance.v1.service_connect import (
    ConformanceServiceClient,
    ConformanceServiceClientSync,
)
from gen.connectrpc.conformance.v1.service_pb import (
    BidiStreamRequest,
    ClientStreamRequest,
    ConformancePayload,
    Error as ConformanceError,
    Header as ConformanceHeader,
    IdempotentUnaryRequest,
    ServerStreamRequest,
    UnaryRequest,
    UnimplementedRequest,
)
from protobuf import Message, Oneof, Registry
from pyqwest import (
    Client,
    HTTPTransport,
    HTTPVersion as PyQwestHTTPVersion,
    SyncClient,
    SyncHTTPTransport,
)

from connectrpc.client import ResponseMetadata
from connectrpc.code import Code
from connectrpc.codec import proto_json_codec
from connectrpc.compression.brotli import BrotliCompression
from connectrpc.compression.gzip import GzipCompression
from connectrpc.compression.zstd import ZstdCompression
from connectrpc.errors import ConnectError
from connectrpc.protocol import ProtocolType
from connectrpc.request import Headers

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from protobuf.wkt import Any

    from connectrpc.compression import Compression

_REGISTRY = Registry(client_compat_pb.desc(), service_pb.desc())


def _convert_code(error: Code) -> ConformanceCode:
    match error:
        case Code.CANCELED:
            return ConformanceCode.CANCELED
        case Code.UNKNOWN:
            return ConformanceCode.UNKNOWN
        case Code.INVALID_ARGUMENT:
            return ConformanceCode.INVALID_ARGUMENT
        case Code.DEADLINE_EXCEEDED:
            return ConformanceCode.DEADLINE_EXCEEDED
        case Code.NOT_FOUND:
            return ConformanceCode.NOT_FOUND
        case Code.ALREADY_EXISTS:
            return ConformanceCode.ALREADY_EXISTS
        case Code.PERMISSION_DENIED:
            return ConformanceCode.PERMISSION_DENIED
        case Code.RESOURCE_EXHAUSTED:
            return ConformanceCode.RESOURCE_EXHAUSTED
        case Code.FAILED_PRECONDITION:
            return ConformanceCode.FAILED_PRECONDITION
        case Code.ABORTED:
            return ConformanceCode.ABORTED
        case Code.OUT_OF_RANGE:
            return ConformanceCode.OUT_OF_RANGE
        case Code.UNIMPLEMENTED:
            return ConformanceCode.UNIMPLEMENTED
        case Code.INTERNAL:
            return ConformanceCode.INTERNAL
        case Code.UNAVAILABLE:
            return ConformanceCode.UNAVAILABLE
        case Code.DATA_LOSS:
            return ConformanceCode.DATA_LOSS
        case Code.UNAUTHENTICATED:
            return ConformanceCode.UNAUTHENTICATED


def _convert_compression(compression: ConformanceCompression) -> Compression | None:
    match compression:
        case ConformanceCompression.IDENTITY:
            return None
        case ConformanceCompression.GZIP:
            return GzipCompression()
        case ConformanceCompression.BR:
            return BrotliCompression()
        case ConformanceCompression.ZSTD:
            return ZstdCompression()
        case _:
            msg = f"Unsupported compression: {compression}"
            raise ValueError(msg)


T = TypeVar("T", bound=Message)


def _unpack_request(message: Any, request: type[T]) -> T:
    res = message.unpack(request)
    assert res is not None
    return res


def pyqwest_client_kwargs(test_request: ClientCompatRequest) -> dict:
    kwargs: dict = {}
    match test_request.http_version:
        case HTTPVersion.HTTP_VERSION_1:
            kwargs["http_version"] = PyQwestHTTPVersion.HTTP1
        case HTTPVersion.HTTP_VERSION_2:
            kwargs["http_version"] = PyQwestHTTPVersion.HTTP2
        case HTTPVersion.HTTP_VERSION_3:
            kwargs["http_version"] = PyQwestHTTPVersion.HTTP3
    if test_request.server_tls_cert:
        kwargs["tls_ca_cert"] = test_request.server_tls_cert
        if test_request.client_tls_creds:
            kwargs["tls_key"] = test_request.client_tls_creds.key
            kwargs["tls_cert"] = test_request.client_tls_creds.cert

    return kwargs


@contextlib.asynccontextmanager
async def client_sync(
    test_request: ClientCompatRequest,
) -> AsyncIterator[ConformanceServiceClientSync]:
    read_max_bytes = None
    if test_request.message_receive_limit:
        read_max_bytes = test_request.message_receive_limit
    scheme = "https" if test_request.server_tls_cert else "http"
    args = pyqwest_client_kwargs(test_request)

    cleanup = contextlib.ExitStack()
    if args:
        transport = cleanup.enter_context(SyncHTTPTransport(**args))
        http_client = SyncClient(transport)
    else:
        http_client = None

    match test_request.protocol:
        case Protocol.CONNECT:
            protocol = ProtocolType.CONNECT
        case Protocol.GRPC:
            protocol = ProtocolType.GRPC
        case Protocol.GRPC_WEB:
            protocol = ProtocolType.GRPC_WEB

    with (
        cleanup,
        ConformanceServiceClientSync(
            f"{scheme}://{test_request.host}:{test_request.port}",
            http_client=http_client,
            accept_compression=[
                GzipCompression(),
                BrotliCompression(),
                ZstdCompression(),
            ],
            send_compression=_convert_compression(test_request.compression),
            codec=proto_json_codec(_REGISTRY)
            if test_request.codec == Codec.JSON
            else None,
            protocol=protocol,
            read_max_bytes=read_max_bytes,
        ) as client,
    ):
        yield client


@contextlib.asynccontextmanager
async def client_async(
    test_request: ClientCompatRequest,
) -> AsyncIterator[ConformanceServiceClient]:
    read_max_bytes = None
    if test_request.message_receive_limit:
        read_max_bytes = test_request.message_receive_limit
    scheme = "https" if test_request.server_tls_cert else "http"
    args = pyqwest_client_kwargs(test_request)

    cleanup = contextlib.AsyncExitStack()
    if args:
        transport = HTTPTransport(**args)
        # Type parameter for enter_async_context requires coroutine even though
        # implementation doesn't. We can directly push aexit to work around it.
        cleanup.push_async_exit(transport.__aexit__)
        http_client = Client(transport)
    else:
        http_client = None

    match test_request.protocol:
        case Protocol.CONNECT:
            protocol = ProtocolType.CONNECT
        case Protocol.GRPC:
            protocol = ProtocolType.GRPC
        case Protocol.GRPC_WEB:
            protocol = ProtocolType.GRPC_WEB

    async with (
        cleanup,
        ConformanceServiceClient(
            f"{scheme}://{test_request.host}:{test_request.port}",
            http_client=http_client,
            accept_compression=[
                GzipCompression(),
                BrotliCompression(),
                ZstdCompression(),
            ],
            send_compression=_convert_compression(test_request.compression),
            codec=proto_json_codec(_REGISTRY)
            if test_request.codec == Codec.JSON
            else None,
            protocol=protocol,
            read_max_bytes=read_max_bytes,
        ) as client,
    ):
        yield client


async def _run_test(
    mode: Mode, test_request: ClientCompatRequest
) -> ClientCompatResponse:
    test_response = ClientCompatResponse()
    test_response.test_name = test_request.test_name
    client_response_result = ClientResponseResult()

    timeout_ms = None
    if test_request.timeout_ms:
        timeout_ms = test_request.timeout_ms

    request_headers = Headers()
    for header in test_request.request_headers:
        for value in header.value:
            request_headers.add(header.name, value)

    payloads: list[ConformancePayload] = []

    with ResponseMetadata() as meta:
        try:
            task: asyncio.Task
            request_closed = asyncio.Event()
            match mode:
                case "sync":
                    async with client_sync(test_request) as client:
                        match test_request.method:
                            case "BidiStream":
                                request_queue = queue.Queue()

                                def send_bidi_stream_request_sync(
                                    client: ConformanceServiceClientSync,
                                    request: Iterator[BidiStreamRequest],
                                ) -> None:
                                    responses = client.bidi_stream(
                                        request,
                                        headers=request_headers,
                                        timeout_ms=timeout_ms,
                                    )
                                    for message in test_request.request_messages:
                                        if test_request.request_delay_ms:
                                            time.sleep(
                                                test_request.request_delay_ms / 1000.0
                                            )
                                        request_queue.put(
                                            _unpack_request(message, BidiStreamRequest)
                                        )

                                        if (
                                            test_request.stream_type
                                            != StreamType.FULL_DUPLEX_BIDI_STREAM
                                        ):
                                            continue

                                        response = next(responses, None)
                                        if response is not None:
                                            payloads.append(
                                                response.payload or ConformancePayload()
                                            )
                                            if test_request.cancel:
                                                match test_request.cancel.cancel_timing:
                                                    case Oneof(
                                                        "after_num_responses", num
                                                    ):
                                                        if len(payloads) >= num:
                                                            task.cancel()

                                    if test_request.cancel:
                                        match test_request.cancel.cancel_timing:
                                            case Oneof("before_close_send", _):
                                                task.cancel()

                                    request_queue.put(None)

                                    request_closed.set()

                                    for response in responses:
                                        payloads.append(
                                            response.payload or ConformancePayload()
                                        )
                                        if test_request.cancel:
                                            match test_request.cancel.cancel_timing:
                                                case Oneof("after_num_responses", num):
                                                    if len(payloads) >= num:
                                                        task.cancel()

                                def bidi_stream_request_sync():
                                    while True:
                                        request = request_queue.get()
                                        if request is None:
                                            return
                                        yield request

                                task = asyncio.create_task(
                                    asyncio.to_thread(
                                        send_bidi_stream_request_sync,
                                        client,
                                        bidi_stream_request_sync(),
                                    )
                                )

                            case "ClientStream":

                                def send_client_stream_request_sync(
                                    client: ConformanceServiceClientSync,
                                    request: Iterator[ClientStreamRequest],
                                ) -> None:
                                    res = client.client_stream(
                                        request,
                                        headers=request_headers,
                                        timeout_ms=timeout_ms,
                                    )
                                    payloads.append(res.payload or ConformancePayload())

                                def request_stream_sync():
                                    for message in test_request.request_messages:
                                        if test_request.request_delay_ms:
                                            time.sleep(
                                                test_request.request_delay_ms / 1000.0
                                            )
                                        yield _unpack_request(
                                            message, ClientStreamRequest
                                        )
                                    if test_request.cancel:
                                        match test_request.cancel.cancel_timing:
                                            case Oneof("before_close_send", _):
                                                task.cancel()
                                    request_closed.set()

                                task = asyncio.create_task(
                                    asyncio.to_thread(
                                        send_client_stream_request_sync,
                                        client,
                                        request_stream_sync(),
                                    )
                                )
                            case "IdempotentUnary":

                                def send_idempotent_unary_request_sync(
                                    client: ConformanceServiceClientSync,
                                    request: IdempotentUnaryRequest,
                                ) -> None:
                                    res = client.idempotent_unary(
                                        request,
                                        headers=request_headers,
                                        use_get=test_request.use_get_http_method,
                                        timeout_ms=timeout_ms,
                                    )
                                    payloads.append(res.payload or ConformancePayload())

                                task = asyncio.create_task(
                                    asyncio.to_thread(
                                        send_idempotent_unary_request_sync,
                                        client,
                                        _unpack_request(
                                            test_request.request_messages[0],
                                            IdempotentUnaryRequest,
                                        ),
                                    )
                                )
                                request_closed.set()
                            case "ServerStream":

                                def send_server_stream_request_sync(
                                    client: ConformanceServiceClientSync,
                                    request: ServerStreamRequest,
                                ) -> None:
                                    for message in client.server_stream(
                                        request,
                                        headers=request_headers,
                                        timeout_ms=timeout_ms,
                                    ):
                                        payloads.append(
                                            message.payload or ConformancePayload()
                                        )
                                        if test_request.cancel:
                                            match test_request.cancel.cancel_timing:
                                                case Oneof("after_num_responses", num):
                                                    if len(payloads) >= num:
                                                        task.cancel()

                                task = asyncio.create_task(
                                    asyncio.to_thread(
                                        send_server_stream_request_sync,
                                        client,
                                        _unpack_request(
                                            test_request.request_messages[0],
                                            ServerStreamRequest,
                                        ),
                                    )
                                )
                                request_closed.set()
                            case "Unary":

                                def send_unary_request_sync(
                                    client: ConformanceServiceClientSync,
                                    request: UnaryRequest,
                                ) -> None:
                                    res = client.unary(
                                        request,
                                        headers=request_headers,
                                        timeout_ms=timeout_ms,
                                    )
                                    payloads.append(res.payload or ConformancePayload())

                                task = asyncio.create_task(
                                    asyncio.to_thread(
                                        send_unary_request_sync,
                                        client,
                                        _unpack_request(
                                            test_request.request_messages[0],
                                            UnaryRequest,
                                        ),
                                    )
                                )
                                request_closed.set()
                            case "Unimplemented":
                                task = asyncio.create_task(
                                    asyncio.to_thread(
                                        client.unimplemented,
                                        _unpack_request(
                                            test_request.request_messages[0],
                                            UnimplementedRequest,
                                        ),
                                        headers=request_headers,
                                        timeout_ms=timeout_ms,
                                    )
                                )
                                request_closed.set()
                            case _:
                                msg = f"Unrecognized method: {test_request.method}"
                                raise ValueError(msg)
                        if test_request.cancel:
                            match test_request.cancel.cancel_timing:
                                case Oneof("after_close_send_ms", after_close_send_ms):
                                    await request_closed.wait()
                                    await asyncio.sleep(after_close_send_ms / 1000.0)
                                    task.cancel()
                        await task
                case "async":
                    async with client_async(test_request) as client:
                        match test_request.method:
                            case "BidiStream":
                                request_queue = asyncio.Queue()

                                async def send_bidi_stream_request(
                                    client: ConformanceServiceClient,
                                    request: AsyncIterator[BidiStreamRequest],
                                ) -> None:
                                    responses = client.bidi_stream(
                                        request,
                                        headers=request_headers,
                                        timeout_ms=timeout_ms,
                                    )
                                    for message in test_request.request_messages:
                                        if test_request.request_delay_ms:
                                            await asyncio.sleep(
                                                test_request.request_delay_ms / 1000.0
                                            )
                                        await request_queue.put(
                                            _unpack_request(message, BidiStreamRequest)
                                        )

                                        if (
                                            test_request.stream_type
                                            != StreamType.FULL_DUPLEX_BIDI_STREAM
                                        ):
                                            continue

                                        response = await anext(responses, None)
                                        if response is not None:
                                            payloads.append(
                                                response.payload or ConformancePayload()
                                            )
                                            if test_request.cancel:
                                                match test_request.cancel.cancel_timing:
                                                    case Oneof(
                                                        "after_num_responses", num
                                                    ):
                                                        if len(payloads) >= num:
                                                            task.cancel()

                                    if test_request.cancel:
                                        match test_request.cancel.cancel_timing:
                                            case Oneof("before_close_send", _):
                                                task.cancel()

                                    await request_queue.put(None)

                                    request_closed.set()

                                    async for response in responses:
                                        payloads.append(
                                            response.payload or ConformancePayload()
                                        )
                                        if test_request.cancel:
                                            match test_request.cancel.cancel_timing:
                                                case Oneof("after_num_responses", num):
                                                    if len(payloads) >= num:
                                                        task.cancel()

                                async def bidi_stream_request():
                                    while True:
                                        request = await request_queue.get()
                                        if request is None:
                                            return
                                        yield request

                                task = asyncio.create_task(
                                    send_bidi_stream_request(
                                        client, bidi_stream_request()
                                    )
                                )

                            case "ClientStream":

                                async def send_client_stream_request(
                                    client: ConformanceServiceClient,
                                    request: AsyncIterator[ClientStreamRequest],
                                ) -> None:
                                    res = await client.client_stream(
                                        request,
                                        headers=request_headers,
                                        timeout_ms=timeout_ms,
                                    )
                                    payloads.append(res.payload or ConformancePayload())

                                async def client_stream_request():
                                    for message in test_request.request_messages:
                                        if test_request.request_delay_ms:
                                            await asyncio.sleep(
                                                test_request.request_delay_ms / 1000.0
                                            )
                                        yield _unpack_request(
                                            message, ClientStreamRequest
                                        )
                                    if test_request.cancel:
                                        match test_request.cancel.cancel_timing:
                                            case Oneof("before_close_send", _):
                                                task.cancel()
                                    request_closed.set()

                                task = asyncio.create_task(
                                    send_client_stream_request(
                                        client, client_stream_request()
                                    )
                                )
                            case "IdempotentUnary":

                                async def send_idempotent_unary_request(
                                    client: ConformanceServiceClient,
                                    request: IdempotentUnaryRequest,
                                ) -> None:
                                    res = await client.idempotent_unary(
                                        request,
                                        headers=request_headers,
                                        use_get=test_request.use_get_http_method,
                                        timeout_ms=timeout_ms,
                                    )
                                    payloads.append(res.payload or ConformancePayload())

                                task = asyncio.create_task(
                                    send_idempotent_unary_request(
                                        client,
                                        _unpack_request(
                                            test_request.request_messages[0],
                                            IdempotentUnaryRequest,
                                        ),
                                    )
                                )
                                request_closed.set()
                            case "ServerStream":

                                async def send_server_stream_request(
                                    client: ConformanceServiceClient,
                                    request: ServerStreamRequest,
                                ) -> None:
                                    async for message in client.server_stream(
                                        request,
                                        headers=request_headers,
                                        timeout_ms=timeout_ms,
                                    ):
                                        payloads.append(
                                            message.payload or ConformancePayload()
                                        )
                                        if test_request.cancel:
                                            match test_request.cancel.cancel_timing:
                                                case Oneof("after_num_responses", num):
                                                    if len(payloads) >= num:
                                                        task.cancel()

                                task = asyncio.create_task(
                                    send_server_stream_request(
                                        client,
                                        _unpack_request(
                                            test_request.request_messages[0],
                                            ServerStreamRequest,
                                        ),
                                    )
                                )
                                request_closed.set()
                            case "Unary":

                                async def send_unary_request(
                                    client: ConformanceServiceClient,
                                    request: UnaryRequest,
                                ) -> None:
                                    res = await client.unary(
                                        request,
                                        headers=request_headers,
                                        timeout_ms=timeout_ms,
                                    )
                                    payloads.append(res.payload or ConformancePayload())

                                task = asyncio.create_task(
                                    send_unary_request(
                                        client,
                                        _unpack_request(
                                            test_request.request_messages[0],
                                            UnaryRequest,
                                        ),
                                    )
                                )
                                request_closed.set()
                            case "Unimplemented":
                                task = asyncio.create_task(
                                    client.unimplemented(
                                        _unpack_request(
                                            test_request.request_messages[0],
                                            UnimplementedRequest,
                                        ),
                                        headers=request_headers,
                                        timeout_ms=timeout_ms,
                                    )
                                )
                                request_closed.set()
                            case _:
                                msg = f"Unrecognized method: {test_request.method}"
                                raise ValueError(msg)
                        if test_request.cancel:
                            match test_request.cancel.cancel_timing:
                                case Oneof("after_close_send_ms", after_close_send_ms):
                                    await request_closed.wait()
                                    await asyncio.sleep(after_close_send_ms / 1000.0)
                                    task.cancel()
                        await task
        except ConnectError as e:
            client_response_result.error = ConformanceError(
                code=_convert_code(e.code),
                message=e.message,
                details=[d._any for d in e.details],
            )
        except (asyncio.CancelledError, Exception) as e:
            traceback.print_tb(e.__traceback__, file=sys.stderr)
            test_response.result = Oneof("error", ClientErrorResult(message=str(e)))
            return test_response

        client_response_result.payloads.extend(payloads)

        for name in meta.headers:
            client_response_result.response_headers.append(
                ConformanceHeader(name=name, value=list(meta.headers.getall(name)))
            )
        for name in meta.trailers:
            client_response_result.response_trailers.append(
                ConformanceHeader(name=name, value=list(meta.trailers.getall(name)))
            )
        test_response.result = Oneof("response", client_response_result)

    return test_response


Mode = Literal["sync", "async"]


class Args(argparse.Namespace):
    mode: Mode
    parallel: int


async def main() -> None:
    parser = argparse.ArgumentParser(description="Conformance client")
    parser.add_argument("--mode", choices=get_args(Mode))
    parser.add_argument("--parallel", type=int, default=multiprocessing.cpu_count() * 4)
    args = parser.parse_args(namespace=Args())

    stdin, stdout = await create_standard_streams()
    sema = asyncio.Semaphore(args.parallel)
    tasks: list[asyncio.Task] = []
    try:
        while True:
            try:
                size_buf = await stdin.readexactly(4)
            except asyncio.IncompleteReadError:
                return
            size = int.from_bytes(size_buf, byteorder="big")
            # Allow to raise even on EOF since we always should have a message
            request_buf = await stdin.readexactly(size)
            request = ClientCompatRequest.from_binary(request_buf)

            async def task(request: ClientCompatRequest) -> None:
                async with sema:
                    response = await _run_test(args.mode, request)

                    response_buf = response.to_binary()
                    size_buf = len(response_buf).to_bytes(4, byteorder="big")
                    stdout.write(size_buf + response_buf)
                    await stdout.drain()

            tasks.append(asyncio.create_task(task(request)))
    finally:
        asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())

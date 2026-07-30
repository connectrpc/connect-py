from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import re
import signal
import socket
import ssl
import sys
import time
from contextlib import ExitStack, closing
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Literal, TypeVar, get_args

# Needs to run before importing from connectrpc
import _cov_embed  # noqa: F401
from _util import create_standard_streams
from gen.connectrpc.conformance.v1 import service_pb
from gen.connectrpc.conformance.v1.config_pb import Code as ConformanceCode, HTTPVersion
from gen.connectrpc.conformance.v1.server_compat_pb import (
    ServerCompatRequest,
    ServerCompatResponse,
)
from gen.connectrpc.conformance.v1.service_connect import (
    ConformanceService,
    ConformanceServiceASGIApplication,
    ConformanceServiceSync,
    ConformanceServiceWSGIApplication,
)
from gen.connectrpc.conformance.v1.service_pb import (
    BidiStreamRequest,
    BidiStreamResponse,
    ClientStreamRequest,
    ClientStreamResponse,
    ConformancePayload,
    Header as ConformanceHeader,
    IdempotentUnaryRequest,
    IdempotentUnaryResponse,
    ServerStreamRequest,
    ServerStreamResponse,
    StreamResponseDefinition,
    UnaryRequest,
    UnaryResponse,
    UnaryResponseDefinition,
)
from protobuf import Oneof, Registry
from protobuf.wkt import Any
from pyvoy import PyvoyServer

from connectrpc.code import Code
from connectrpc.codec import proto_binary_codec, proto_json_codec
from connectrpc.compression.brotli import BrotliCompression
from connectrpc.compression.gzip import GzipCompression
from connectrpc.compression.zstd import ZstdCompression
from connectrpc.errors import ConnectError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from protobuf import Message

    from connectrpc.request import RequestContext


_REGISTRY = Registry(service_pb.desc())


def _convert_code(conformance_code: ConformanceCode) -> Code:
    match conformance_code:
        case ConformanceCode.CANCELED:
            return Code.CANCELED
        case ConformanceCode.UNKNOWN:
            return Code.UNKNOWN
        case ConformanceCode.INVALID_ARGUMENT:
            return Code.INVALID_ARGUMENT
        case ConformanceCode.DEADLINE_EXCEEDED:
            return Code.DEADLINE_EXCEEDED
        case ConformanceCode.NOT_FOUND:
            return Code.NOT_FOUND
        case ConformanceCode.ALREADY_EXISTS:
            return Code.ALREADY_EXISTS
        case ConformanceCode.PERMISSION_DENIED:
            return Code.PERMISSION_DENIED
        case ConformanceCode.RESOURCE_EXHAUSTED:
            return Code.RESOURCE_EXHAUSTED
        case ConformanceCode.FAILED_PRECONDITION:
            return Code.FAILED_PRECONDITION
        case ConformanceCode.ABORTED:
            return Code.ABORTED
        case ConformanceCode.OUT_OF_RANGE:
            return Code.OUT_OF_RANGE
        case ConformanceCode.UNIMPLEMENTED:
            return Code.UNIMPLEMENTED
        case ConformanceCode.INTERNAL:
            return Code.INTERNAL
        case ConformanceCode.UNAVAILABLE:
            return Code.UNAVAILABLE
        case ConformanceCode.DATA_LOSS:
            return Code.DATA_LOSS
        case ConformanceCode.UNAUTHENTICATED:
            return Code.UNAUTHENTICATED
    msg = f"Unknown ConformanceCode: {conformance_code}"
    raise ValueError(msg)


RES = TypeVar(
    "RES",
    bound=UnaryResponse
    | IdempotentUnaryResponse
    | ClientStreamResponse
    | ServerStreamResponse
    | BidiStreamResponse,
)


def _send_headers(
    ctx: RequestContext, definition: UnaryResponseDefinition | StreamResponseDefinition
) -> None:
    for header in definition.response_headers:
        for value in header.value:
            ctx.response_headers.add(header.name, value)
    for trailer in definition.response_trailers:
        for value in trailer.value:
            ctx.response_trailers.add(trailer.name, value)


def _create_request_info(
    ctx: RequestContext, reqs: list[Any]
) -> ConformancePayload.RequestInfo:
    request_info = ConformancePayload.RequestInfo(requests=reqs)
    timeout_ms = ctx.timeout_ms
    if timeout_ms is not None:
        request_info.timeout_ms = int(timeout_ms)
    for key in ctx.request_headers:
        request_info.request_headers.append(
            ConformanceHeader(name=key, value=list(ctx.request_headers.getall(key)))
        )
    return request_info


async def _handle_unary_response(
    definition: UnaryResponseDefinition, reqs: list[Any], res: RES, ctx: RequestContext
) -> RES:
    _send_headers(ctx, definition)
    request_info = _create_request_info(ctx, reqs)

    match definition.response:
        case Oneof("error", error):
            raise ConnectError(
                code=_convert_code(error.code),
                message=error.message,
                details=[*error.details, request_info],
            )
        case Oneof("response_data", response_data):
            pass
        case _:
            response_data = b""
    if definition.response_delay_ms:
        await asyncio.sleep(definition.response_delay_ms / 1000.0)

    res.payload = ConformancePayload(request_info=request_info, data=response_data)
    return res


class TestService(ConformanceService):
    async def unary(self, request: UnaryRequest, ctx: RequestContext) -> UnaryResponse:
        return await _handle_unary_response(
            request.response_definition or UnaryResponseDefinition(),
            [Any.pack(request)],
            UnaryResponse(),
            ctx,
        )

    async def idempotent_unary(
        self, request: IdempotentUnaryRequest, ctx: RequestContext
    ) -> IdempotentUnaryResponse:
        return await _handle_unary_response(
            request.response_definition or UnaryResponseDefinition(),
            [Any.pack(request)],
            IdempotentUnaryResponse(),
            ctx,
        )

    async def client_stream(
        self, request: AsyncIterator[ClientStreamRequest], ctx: RequestContext
    ) -> ClientStreamResponse:
        requests: list[Any] = []
        definition: UnaryResponseDefinition | None = None
        async for message in request:
            requests.append(Any.pack(message))
            if not definition:
                definition = message.response_definition or UnaryResponseDefinition()

        if not definition:
            msg = "ClientStream must have a response definition"
            raise ValueError(msg)
        return await _handle_unary_response(
            definition, requests, ClientStreamResponse(), ctx
        )

    async def server_stream(
        self, request: ServerStreamRequest, ctx: RequestContext
    ) -> AsyncIterator[ServerStreamResponse]:
        definition = request.response_definition or StreamResponseDefinition()
        _send_headers(ctx, definition)
        request_info = _create_request_info(ctx, [Any.pack(request)])
        sent_message = False
        for res_data in definition.response_data:
            res = ServerStreamResponse()
            res.payload = ConformancePayload()
            if not sent_message:
                res.payload.request_info = request_info
            res.payload.data = res_data
            if definition.response_delay_ms:
                await asyncio.sleep(definition.response_delay_ms / 1000.0)
            sent_message = True
            yield res

        if definition.error:
            details: list[Message] = [*definition.error.details]
            if not sent_message:
                details.append(request_info)
            raise ConnectError(
                code=_convert_code(definition.error.code),
                message=definition.error.message,
                details=details,
            )

    async def bidi_stream(
        self, request: AsyncIterator[BidiStreamRequest], ctx: RequestContext
    ) -> AsyncIterator[BidiStreamResponse]:
        definition: StreamResponseDefinition | None = None
        full_duplex = False
        requests: list[Any] = []
        res_idx = 0
        async for message in request:
            if not definition:
                definition = message.response_definition or StreamResponseDefinition()
                _send_headers(ctx, definition)
                full_duplex = message.full_duplex
            requests.append(Any.pack(message))
            if not full_duplex:
                continue
            if not definition or res_idx >= len(definition.response_data):
                break
            if definition.response_delay_ms:
                await asyncio.sleep(definition.response_delay_ms / 1000.0)
            res = BidiStreamResponse()
            res.payload = ConformancePayload()
            res.payload.data = definition.response_data[res_idx]
            res.payload.request_info = _create_request_info(ctx, [Any.pack(message)])
            yield res
            res_idx += 1
            requests = []

        if not definition:
            return

        request_info = _create_request_info(ctx, requests)
        for i in range(res_idx, len(definition.response_data)):
            if definition.response_delay_ms:
                await asyncio.sleep(definition.response_delay_ms / 1000.0)
            res = BidiStreamResponse()
            res.payload = ConformancePayload()
            res.payload.data = definition.response_data[i]
            if i == 0:
                res.payload.request_info = request_info
            yield res

        if definition.error:
            details: list[Message] = [*definition.error.details]
            if len(definition.response_data) == 0:
                details.append(request_info)
            raise ConnectError(
                code=_convert_code(definition.error.code),
                message=definition.error.message,
                details=details,
            )


def _handle_unary_response_sync(
    definition: UnaryResponseDefinition, reqs: list[Any], res: RES, ctx: RequestContext
) -> RES:
    _send_headers(ctx, definition)
    request_info = _create_request_info(ctx, reqs)

    match definition.response:
        case Oneof("error", error):
            raise ConnectError(
                code=_convert_code(error.code),
                message=error.message,
                details=[*error.details, request_info],
            )
        case Oneof("response_data", response_data):
            pass
        case _:
            response_data = b""
    if definition.response_delay_ms:
        time.sleep(definition.response_delay_ms / 1000.0)

    res.payload = ConformancePayload(request_info=request_info, data=response_data)
    return res


class TestServiceSync(ConformanceServiceSync):
    def unary(self, request: UnaryRequest, ctx: RequestContext) -> UnaryResponse:
        return _handle_unary_response_sync(
            request.response_definition or UnaryResponseDefinition(),
            [Any.pack(request)],
            UnaryResponse(),
            ctx,
        )

    def idempotent_unary(
        self, request: IdempotentUnaryRequest, ctx: RequestContext
    ) -> IdempotentUnaryResponse:
        return _handle_unary_response_sync(
            request.response_definition or UnaryResponseDefinition(),
            [Any.pack(request)],
            IdempotentUnaryResponse(),
            ctx,
        )

    def client_stream(
        self, request: Iterator[ClientStreamRequest], ctx: RequestContext
    ) -> ClientStreamResponse:
        requests: list[Any] = []
        definition: UnaryResponseDefinition | None = None
        for message in request:
            requests.append(Any.pack(message))
            if not definition:
                definition = message.response_definition or UnaryResponseDefinition()

        if not definition:
            msg = "ClientStream must have a response definition"
            raise ValueError(msg)
        return _handle_unary_response_sync(
            definition, requests, ClientStreamResponse(), ctx
        )

    def server_stream(
        self, request: ServerStreamRequest, ctx: RequestContext
    ) -> Iterator[ServerStreamResponse]:
        definition = request.response_definition or StreamResponseDefinition()
        _send_headers(ctx, definition)
        request_info = _create_request_info(ctx, [Any.pack(request)])
        sent_message = False
        for res_data in definition.response_data:
            res = ServerStreamResponse()
            res.payload = ConformancePayload()
            if not sent_message:
                res.payload.request_info = request_info
            res.payload.data = res_data
            if definition.response_delay_ms:
                time.sleep(definition.response_delay_ms / 1000.0)
            sent_message = True
            yield res

        if definition.error:
            details: list[Message] = [*definition.error.details]
            if not sent_message:
                details.append(request_info)
            raise ConnectError(
                code=_convert_code(definition.error.code),
                message=definition.error.message,
                details=details,
            )

    def bidi_stream(
        self, request: Iterator[BidiStreamRequest], ctx: RequestContext
    ) -> Iterator[BidiStreamResponse]:
        definition: StreamResponseDefinition | None = None
        full_duplex = False
        requests: list[Any] = []
        res_idx = 0
        for message in request:
            if not definition:
                definition = message.response_definition or StreamResponseDefinition()
                _send_headers(ctx, definition)
                full_duplex = message.full_duplex
            requests.append(Any.pack(message))
            if not full_duplex:
                continue
            if not definition or res_idx >= len(definition.response_data):
                break
            if definition.response_delay_ms:
                time.sleep(definition.response_delay_ms / 1000.0)
            res = BidiStreamResponse()
            res.payload = ConformancePayload()
            res.payload.data = definition.response_data[res_idx]
            res.payload.request_info = _create_request_info(ctx, [Any.pack(message)])
            yield res
            res_idx += 1
            requests = []

        if not definition:
            return

        request_info = _create_request_info(ctx, requests)
        for i in range(res_idx, len(definition.response_data)):
            if definition.response_delay_ms:
                time.sleep(definition.response_delay_ms / 1000.0)
            res = BidiStreamResponse()
            res.payload = ConformancePayload()
            res.payload.data = definition.response_data[i]
            if i == 0:
                res.payload.request_info = request_info
            yield res

        if definition.error:
            details: list[Message] = [*definition.error.details]
            if len(definition.response_data) == 0:
                details.append(request_info)
            raise ConnectError(
                code=_convert_code(definition.error.code),
                message=definition.error.message,
                details=details,
            )


read_max_bytes = os.getenv("READ_MAX_BYTES")
if read_max_bytes is not None:
    read_max_bytes = int(read_max_bytes)

asgi_app = ConformanceServiceASGIApplication(
    TestService(),
    read_max_bytes=read_max_bytes,
    compressions=(GzipCompression(), ZstdCompression(), BrotliCompression()),
    codecs=(proto_binary_codec(), proto_json_codec(_REGISTRY)),
)
wsgi_app = ConformanceServiceWSGIApplication(
    TestServiceSync(),
    read_max_bytes=read_max_bytes,
    compressions=(GzipCompression(), ZstdCompression(), BrotliCompression()),
    codecs=(proto_binary_codec(), proto_json_codec(_REGISTRY)),
)


def _server_env(request: ServerCompatRequest) -> dict[str, str]:
    pythonpath = os.pathsep.join(sys.path)
    env = {
        **os.environ,
        "PYTHONPATH": pythonpath,
        "PYTHONHOME": f"{sys.prefix}:{sys.exec_prefix}",
    }
    if request.message_receive_limit:
        env["READ_MAX_BYTES"] = str(request.message_receive_limit)
    return env


_port_regex = re.compile(r".*://[^:]+:(\d+).*")


async def _tee_to_stderr(stream: asyncio.StreamReader) -> AsyncIterator[bytes]:
    while True:
        line = await stream.readline()
        if not line:
            break
        print(line.decode("utf-8"), end="", file=sys.stderr)  # noqa: T201
        yield line


async def _consume_log(stream: AsyncIterator[bytes]) -> None:
    async for _ in stream:
        pass


async def serve_granian(
    request: ServerCompatRequest,
    mode: Literal["sync", "async"],
    certfile: str | None,
    keyfile: str | None,
    cafile: str | None,
    port_future: asyncio.Future[int],
):
    # Granian seems to have a bug that it prints out 0 rather than the resolved port,
    # so we need to determine it ourselves. If we see race conditions because of it,
    # we can set max-servers=1 in the runner.
    # ref: https://github.com/emmett-framework/granian/issues/711
    port = _find_free_port()
    args = [f"--port={port}", "--workers=8"]
    if certfile:
        args.append(f"--ssl-certificate={certfile}")
    if keyfile:
        args.append(f"--ssl-keyfile={keyfile}")
    if cafile:
        args.append(f"--ssl-ca={cafile}")
        args.append("--ssl-client-verify")

    if mode == "sync":
        args.append("--interface=wsgi")
        args.append("server:wsgi_app")
    else:
        args.append("--interface=asgi")
        args.append("server:asgi_app")

    proc = await asyncio.create_subprocess_exec(
        "granian",
        *args,
        stderr=asyncio.subprocess.STDOUT,
        stdout=asyncio.subprocess.PIPE,
        env=_server_env(request),
    )
    stdout = proc.stdout
    assert stdout is not None
    stdout = _tee_to_stderr(stdout)
    try:
        async for line in stdout:
            if b"Started worker-8 runtime-1" in line:
                break
        port_future.set_result(port)
        await _consume_log(stdout)
        await proc.wait()
    except asyncio.CancelledError:
        proc.terminate()
        await proc.wait()


async def serve_gunicorn(
    request: ServerCompatRequest,
    certfile: str | None,
    keyfile: str | None,
    cafile: str | None,
    port_future: asyncio.Future[int],
):
    args = [
        "--bind=127.0.0.1:0",
        "--workers=4",
        "--worker-class=gthread",
        "--threads=16",
        "--keep-alive=0",
    ]
    if certfile:
        args.append(f"--certfile={certfile}")
    if keyfile:
        args.append(f"--keyfile={keyfile}")
    if cafile:
        args.append(f"--ca-certs={cafile}")
        args.append(f"--cert-reqs={ssl.CERT_REQUIRED}")

    args.append("server:wsgi_app")

    proc = await asyncio.create_subprocess_exec(
        "gunicorn",
        *args,
        stderr=asyncio.subprocess.STDOUT,
        stdout=asyncio.subprocess.PIPE,
        env=_server_env(request),
    )
    stdout = proc.stdout
    assert stdout is not None
    stdout = _tee_to_stderr(stdout)
    try:
        async for line in stdout:
            match = _port_regex.match(line.decode("utf-8"))
            if match:
                port_future.set_result(int(match.group(1)))
                break
        await _consume_log(stdout)
    except asyncio.CancelledError:
        proc.terminate()
        await proc.wait()


async def serve_hypercorn(
    request: ServerCompatRequest,
    mode: Literal["sync", "async"],
    certfile: str | None,
    keyfile: str | None,
    cafile: str | None,
    port_future: asyncio.Future[int],
):
    args = ["--bind=localhost:0"]
    if certfile:
        args.append(f"--certfile={certfile}")
    if keyfile:
        args.append(f"--keyfile={keyfile}")
    if cafile:
        args.append(f"--ca-certs={cafile}")
        args.append("--verify-mode=CERT_REQUIRED")

    if mode == "sync":
        args.append("server:wsgi_app")
    else:
        args.append("server:asgi_app")

    proc = await asyncio.create_subprocess_exec(
        "hypercorn",
        *args,
        stderr=asyncio.subprocess.STDOUT,
        stdout=asyncio.subprocess.PIPE,
        env=_server_env(request),
    )
    stdout = proc.stdout
    assert stdout is not None
    stdout = _tee_to_stderr(stdout)
    try:
        async for line in stdout:
            match = _port_regex.match(line.decode("utf-8"))
            if match:
                port_future.set_result(int(match.group(1)))
                break
        await _consume_log(stdout)
    except asyncio.CancelledError:
        proc.terminate()
        await proc.wait()


async def serve_pyvoy(
    request: ServerCompatRequest,
    mode: Literal["sync", "async"],
    certfile: str | None,
    keyfile: str | None,
    cafile: str | None,
    port_future: asyncio.Future[int],
):
    tls_cert = Path(certfile) if certfile else None
    tls_key = Path(keyfile) if keyfile else None
    tls_ca_cert = Path(cafile) if cafile else None
    tls_port = 0 if tls_key else None

    if mode == "sync":
        interface = "wsgi"
        app = "server:wsgi_app"
    else:
        interface = "asgi"
        app = "server:asgi_app"

    async def start():
        # Currently explicit env not accepted but it's safe to set it here.
        os.environ["READ_MAX_BYTES"] = str(request.message_receive_limit)
        async with PyvoyServer(
            app,
            port=0,
            tls_port=tls_port,
            interface=interface,
            tls_key=tls_key,
            tls_cert=tls_cert,
            tls_ca_cert=tls_ca_cert,
        ) as server:
            if (
                request.http_version == HTTPVersion.HTTP_VERSION_3
                and server.listener_port_quic
            ):
                port = server.listener_port_quic
            else:
                port = server.listener_port_tls or server.listener_port
            port_future.set_result(port)
            await asyncio.Future()  # run until cancelled

    task = asyncio.create_task(start())
    try:
        await task
    except asyncio.CancelledError:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def serve_uvicorn(
    request: ServerCompatRequest,
    certfile: str | None,
    keyfile: str | None,
    cafile: str | None,
    port_future: asyncio.Future[int],
):
    args = ["--port=0", "--no-access-log"]
    if certfile:
        args.append(f"--ssl-certfile={certfile}")
    if keyfile:
        args.append(f"--ssl-keyfile={keyfile}")
    if cafile:
        args.append(f"--ssl-ca-certs={cafile}")
        args.append(f"--ssl-cert-reqs={ssl.CERT_REQUIRED}")

    args.append("server:asgi_app")

    proc = await asyncio.create_subprocess_exec(
        "uvicorn",
        *args,
        stderr=asyncio.subprocess.STDOUT,
        stdout=asyncio.subprocess.PIPE,
        env=_server_env(request),
    )
    stdout = proc.stdout
    assert stdout is not None
    stdout = _tee_to_stderr(stdout)
    try:
        async for line in stdout:
            match = _port_regex.match(line.decode("utf-8"))
            if match:
                port_future.set_result(int(match.group(1)))
                break
        await _consume_log(stdout)
    except asyncio.CancelledError:
        proc.terminate()
        await proc.wait()


def _find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


Mode = Literal["sync", "async"]
Server = Literal["granian", "gunicorn", "hypercorn", "pyvoy", "uvicorn"]


class Args(argparse.Namespace):
    mode: Mode
    server: Server


async def main() -> None:
    parser = argparse.ArgumentParser(description="Conformance server")
    parser.add_argument("--mode", choices=get_args(Mode))
    parser.add_argument("--server", choices=get_args(Server))
    args = parser.parse_args(namespace=Args())

    stdin, stdout = await create_standard_streams()
    try:
        size_buf = await stdin.readexactly(4)
    except asyncio.IncompleteReadError:
        return
    size = int.from_bytes(size_buf, byteorder="big")
    # Allow to raise even on EOF since we always should have a message
    request_buf = await stdin.readexactly(size)
    request = ServerCompatRequest.from_binary(request_buf)

    cleanup = ExitStack()
    certfile = None
    keyfile = None
    cafile = None
    if request.use_tls:
        cert_file = cleanup.enter_context(NamedTemporaryFile())
        key_file = cleanup.enter_context(NamedTemporaryFile())
        if request.server_creds:
            cert_file.write(request.server_creds.cert)
            cert_file.flush()
            key_file.write(request.server_creds.key)
            key_file.flush()
        certfile = cert_file.name
        keyfile = key_file.name
        if request.client_tls_cert:
            ca_cert_file = cleanup.enter_context(NamedTemporaryFile())
            ca_cert_file.write(request.client_tls_cert)
            ca_cert_file.flush()
            cafile = ca_cert_file.name

    with cleanup:
        port_future: asyncio.Future[int] = asyncio.get_event_loop().create_future()
        match args.server:
            case "granian":
                serve_task = asyncio.create_task(
                    serve_granian(
                        request, args.mode, certfile, keyfile, cafile, port_future
                    )
                )
            case "gunicorn":
                if args.mode == "async":
                    msg = "gunicorn does not support async mode"
                    raise ValueError(msg)
                serve_task = asyncio.create_task(
                    serve_gunicorn(request, certfile, keyfile, cafile, port_future)
                )
            case "hypercorn":
                serve_task = asyncio.create_task(
                    serve_hypercorn(
                        request, args.mode, certfile, keyfile, cafile, port_future
                    )
                )
            case "pyvoy":
                serve_task = asyncio.create_task(
                    serve_pyvoy(
                        request, args.mode, certfile, keyfile, cafile, port_future
                    )
                )
            case "uvicorn":
                if args.mode == "sync":
                    msg = "uvicorn does not support sync mode"
                    raise ValueError(msg)
                serve_task = asyncio.create_task(
                    serve_uvicorn(request, certfile, keyfile, cafile, port_future)
                )

        asyncio.get_event_loop().add_signal_handler(signal.SIGTERM, serve_task.cancel)

        port = await port_future
        response = ServerCompatResponse()
        response.host = "127.0.0.1"
        response.port = port
        if request.use_tls and request.server_creds:
            response.pem_cert = request.server_creds.cert
        response_buf = response.to_binary()
        size_buf = len(response_buf).to_bytes(4, byteorder="big")
        stdout.write(size_buf)
        stdout.write(response_buf)
        await stdout.drain()
        await serve_task


if __name__ == "__main__":
    asyncio.run(main())

from __future__ import annotations

import asyncio
import threading
from http import HTTPStatus
from typing import TYPE_CHECKING, NoReturn

import pytest
from pyqwest import (
    Client,
    Headers,
    Request,
    Response,
    SyncClient,
    SyncRequest,
    SyncResponse,
    SyncTransport,
    Transport,
)
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc._protocol import HTTPException
from connectrpc.client import ResponseMetadata
from connectrpc.code import Code
from connectrpc.errors import ConnectError

from .connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClient,
    HaberdasherClientSync,
    HaberdasherSync,
    HaberdasherWSGIApplication,
)
from .connectrpc.example.haberdasher_pb import Hat, Size

if TYPE_CHECKING:
    from connectrpc.request import RequestContext

_errors = [
    (Code.CANCELED, "Operation was cancelled", 499),
    (Code.UNKNOWN, "An unknown error occurred", 500),
    (Code.INVALID_ARGUMENT, "That's not right", 400),
    (Code.DEADLINE_EXCEEDED, "Deadline exceeded", 504),
    (Code.NOT_FOUND, "Resource not found", 404),
    (Code.ALREADY_EXISTS, "Resource already exists", 409),
    (Code.PERMISSION_DENIED, "Permission denied", 403),
    (Code.RESOURCE_EXHAUSTED, "Resource exhausted", 429),
    (Code.FAILED_PRECONDITION, "Failed precondition", 400),
    (Code.ABORTED, "Operation aborted", 409),
    (Code.OUT_OF_RANGE, "Out of range", 400),
    (Code.UNIMPLEMENTED, "Method not implemented", 501),
    (Code.INTERNAL, "Internal server error", 500),
    (Code.UNAVAILABLE, "Service unavailable", 503),
    (Code.DATA_LOSS, "Data loss occurred", 500),
    (Code.UNAUTHENTICATED, "Unauthenticated access", 401),
]


@pytest.mark.parametrize(("code", "message", "http_status"), _errors)
def test_sync_errors(code: Code, message: str, http_status: int) -> None:
    class ErrorHaberdasherSync(HaberdasherSync):
        def __init__(self, exception: ConnectError) -> None:
            self._exception = exception

        def make_hat(self, request, ctx) -> NoReturn:
            raise self._exception

    haberdasher = ErrorHaberdasherSync(ConnectError(code, message))
    app = HaberdasherWSGIApplication(haberdasher)
    transport = WSGITransport(app)

    recorded_response: SyncResponse | None = SyncResponse(status=100)

    class ResponseRecorder(SyncTransport):
        def __init__(self, transport: SyncTransport) -> None:
            self._transport = transport

        def execute_sync(self, request: SyncRequest) -> SyncResponse:
            nonlocal recorded_response
            response = self._transport.execute_sync(request)
            recorded_response = response
            return response

    http_client = SyncClient(transport=ResponseRecorder(transport))

    with (
        HaberdasherClientSync("http://localhost", http_client=http_client) as client,
        pytest.raises(ConnectError) as exc_info,
        ResponseMetadata() as resp,
    ):
        client.make_hat(request=Size(inches=10))

    assert exc_info.value.code == code
    assert exc_info.value.message == message
    assert recorded_response is not None
    assert recorded_response.status == http_status
    assert resp.http_status == http_status


@pytest.mark.asyncio
@pytest.mark.parametrize(("code", "message", "http_status"), _errors)
async def test_async_errors(code: Code, message: str, http_status: int) -> None:
    class ErrorHaberdasher(Haberdasher):
        def __init__(self, exception: ConnectError) -> None:
            self._exception = exception

        async def make_hat(self, request, ctx) -> NoReturn:
            raise self._exception

    haberdasher = ErrorHaberdasher(ConnectError(code, message))
    app = HaberdasherASGIApplication(haberdasher)
    transport = ASGITransport(app)

    recorded_response: Response | None = Response(status=100)

    class ResponseRecorder(Transport):
        def __init__(self, transport: Transport) -> None:
            self._transport = transport

        async def execute(self, request: Request) -> Response:
            nonlocal recorded_response
            response = await self._transport.execute(request)
            recorded_response = response
            return response

    http_client = Client(transport=ResponseRecorder(transport))
    async with HaberdasherClient("http://localhost", http_client=http_client) as client:
        with pytest.raises(ConnectError) as exc_info, ResponseMetadata() as resp:
            await client.make_hat(request=Size(inches=10))

    assert exc_info.value.code == code
    assert exc_info.value.message == message
    assert recorded_response is not None
    assert recorded_response.status == http_status
    assert resp.http_status == http_status


_http_errors = [
    pytest.param(400, b"", {}, Code.INTERNAL, "Bad Request", id="400"),
    pytest.param(401, b"", {}, Code.UNAUTHENTICATED, "Unauthorized", id="401"),
    pytest.param(403, b"", {}, Code.PERMISSION_DENIED, "Forbidden", id="403"),
    pytest.param(404, b"", {}, Code.UNIMPLEMENTED, "Not Found", id="404"),
    pytest.param(429, b"", {}, Code.UNAVAILABLE, "Too Many Requests", id="429"),
    pytest.param(499, b"", {}, Code.UNKNOWN, "Client Closed Request", id="499"),
    pytest.param(502, b"", {}, Code.UNAVAILABLE, "Bad Gateway", id="502"),
    pytest.param(503, b"", {}, Code.UNAVAILABLE, "Service Unavailable", id="503"),
    pytest.param(504, b"", {}, Code.UNAVAILABLE, "Gateway Timeout", id="504"),
    pytest.param(
        400,
        b'{"code": "invalid_argument", "message": "Bad parameter"}',
        {"content-type": "application/json"},
        Code.INVALID_ARGUMENT,
        "Bad parameter",
        id="connect error",
    ),
    pytest.param(
        400,
        b'{"message": "Bad parameter"}',
        {"content-type": "application/json"},
        Code.INTERNAL,
        "Bad parameter",
        id="connect error without code",
    ),
    pytest.param(
        404,
        b'{"code": "not_found"}',
        {"content-type": "application/json"},
        Code.NOT_FOUND,
        "",
        id="connect error without message",
    ),
    pytest.param(
        502, b'"{bad_json', {}, Code.UNAVAILABLE, "Bad Gateway", id="bad json"
    ),
    pytest.param(
        200,
        b"weird encoding",
        {"content-type": "application/proto", "content-encoding": "weird"},
        Code.INTERNAL,
        "unknown encoding 'weird'; accepted encodings are gzip, identity",
        id="bad encoding",
    ),
]


@pytest.mark.parametrize(
    ("response_status", "content", "response_headers", "code", "message"), _http_errors
)
def test_sync_http_errors(
    response_status, content, response_headers, code, message
) -> None:
    class MockTransport(SyncTransport):
        def execute_sync(self, request: SyncRequest) -> SyncResponse:
            return SyncResponse(
                status=response_status,
                content=content,
                headers=Headers(response_headers),
            )

    with (
        HaberdasherClientSync(
            "http://localhost", http_client=SyncClient(transport=MockTransport())
        ) as client,
        pytest.raises(ConnectError) as exc_info,
    ):
        client.make_hat(request=Size(inches=10))
    assert exc_info.value.code == code
    assert exc_info.value.message == message


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response_status", "content", "response_headers", "code", "message"), _http_errors
)
async def test_async_http_errors(
    response_status, content, response_headers, code, message
) -> None:
    class MockTransport(Transport):
        async def execute(self, request: Request) -> Response:
            return Response(
                status=response_status,
                content=content,
                headers=Headers(response_headers),
            )

    async with HaberdasherClient(
        "http://localhost", http_client=Client(transport=MockTransport())
    ) as client:
        with pytest.raises(ConnectError) as exc_info:
            await client.make_hat(request=Size(inches=10))
    assert exc_info.value.code == code
    assert exc_info.value.message == message


_client_errors = [
    pytest.param(
        "PUT",
        "/connectrpc.example.Haberdasher/MakeHat",
        {"Content-Type": "application/proto"},
        Size(inches=10).to_binary(),
        HTTPStatus.METHOD_NOT_ALLOWED,
        {"Allow": "GET, POST"},
        id="bad method",
    ),
    pytest.param(
        "POST",
        "/notservicemethod",
        {"Content-Type": "application/proto"},
        Size(inches=10).to_binary(),
        HTTPStatus.NOT_FOUND,
        {},
        id="not found",
    ),
    pytest.param(
        "POST",
        "/notservice/method",
        {"Content-Type": "application/proto"},
        Size(inches=10).to_binary(),
        HTTPStatus.NOT_FOUND,
        {},
        id="not present service",
    ),
    pytest.param(
        "POST",
        "/connectrpc.example.Haberdasher/notmethod",
        {"Content-Type": "application/proto"},
        Size(inches=10).to_binary(),
        HTTPStatus.NOT_FOUND,
        {},
        id="not present method",
    ),
    pytest.param(
        "POST",
        "/connectrpc.example.Haberdasher/MakeHat",
        {"Content-Type": "text/html"},
        Size(inches=10).to_binary(),
        HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
        {"Accept-Post": "application/json, application/proto"},
        id="bad content type",
    ),
    pytest.param(
        "POST",
        "/connectrpc.example.Haberdasher/MakeHat",
        {"Content-Type": "application/proto", "connect-protocol-version": "2"},
        Size(inches=10).to_binary(),
        HTTPStatus.BAD_REQUEST,
        {"content-type": "application/json"},
        id="bad connect protocol version",
    ),
    pytest.param(
        "POST",
        "/connectrpc.example.Haberdasher/MakeHat",
        {"Content-Type": "application/proto", "connect-timeout-ms": "10000000000"},
        Size(inches=10).to_binary(),
        HTTPStatus.BAD_REQUEST,
        {"content-type": "application/json"},
        id="connect timeout header too long",
    ),
    pytest.param(
        "POST",
        "/connectrpc.example.Haberdasher/MakeHat",
        {"Content-Type": "application/proto", "connect-timeout-ms": "goodbeer"},
        Size(inches=10).to_binary(),
        HTTPStatus.BAD_REQUEST,
        {"content-type": "application/json"},
        id="connect timeout header invalid",
    ),
]


@pytest.mark.parametrize(
    ("method", "path", "headers", "body", "response_status", "response_headers"),
    _client_errors,
)
def test_sync_client_errors(
    method, path, headers, body, response_status, response_headers
) -> None:
    class ValidHaberdasherSync(HaberdasherSync):
        def make_hat(self, request, ctx):
            return Hat()

    app = HaberdasherWSGIApplication(ValidHaberdasherSync())
    transport = WSGITransport(app)

    client = SyncClient(transport)
    response = client.execute(
        method=method, url=f"http://localhost{path}", content=body, headers=headers
    )

    assert response.status == response_status
    assert response.headers == response_headers


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path", "headers", "body", "response_status", "response_headers"),
    _client_errors,
)
async def test_async_client_errors(
    method, path, headers, body, response_status, response_headers
) -> None:
    class ValidHaberdasher(Haberdasher):
        async def make_hat(self, request, ctx):
            return Hat()

    haberdasher = ValidHaberdasher()
    app = HaberdasherASGIApplication(haberdasher)
    transport = ASGITransport(app)

    client = Client(transport)
    response = await client.execute(
        method=method, url=f"http://localhost{path}", content=body, headers=headers
    )

    assert response.status == response_status
    assert response.headers == response_headers


@pytest.mark.parametrize(
    ("client_timeout_ms", "call_timeout_ms"), [(200, None), (None, 200)]
)
def test_sync_client_timeout(client_timeout_ms, call_timeout_ms) -> None:
    recorded_timeout_header = ""

    class ModifyTimeout(SyncTransport):
        def __init__(self, transport: SyncTransport) -> None:
            self._transport = transport

        def execute_sync(self, request: SyncRequest) -> SyncResponse:
            nonlocal recorded_timeout_header
            recorded_timeout_header = request.headers.get("connect-timeout-ms")
            # Make sure server doesn't timeout since we are verifying client timeout
            request.headers["connect-timeout-ms"] = "10000"
            return self._transport.execute_sync(request)

    timed_out = threading.Event()

    class SleepingHaberdasher(HaberdasherSync):
        def make_hat(self, request, ctx) -> NoReturn:
            timed_out.wait()
            raise AssertionError("Timedout already")

    app = HaberdasherWSGIApplication(SleepingHaberdasher())
    http_client = SyncClient(ModifyTimeout(WSGITransport(app)))
    with (
        HaberdasherClientSync(
            "http://localhost", timeout_ms=client_timeout_ms, http_client=http_client
        ) as client,
        pytest.raises(ConnectError) as exc_info,
    ):
        client.make_hat(request=Size(inches=10), timeout_ms=call_timeout_ms)

    assert exc_info.value.code == Code.DEADLINE_EXCEEDED
    assert exc_info.value.message == "Request timed out"
    assert recorded_timeout_header == "200"
    timed_out.set()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("client_timeout_ms", "call_timeout_ms"), [(200, None), (None, 200)]
)
async def test_async_client_timeout(client_timeout_ms, call_timeout_ms) -> None:
    recorded_timeout_header = ""

    class ModifyTimeout(Transport):
        def __init__(self, transport: Transport) -> None:
            self._transport = transport

        async def execute(self, request: Request) -> Response:
            nonlocal recorded_timeout_header
            recorded_timeout_header = request.headers.get("connect-timeout-ms")
            # Make sure server doesn't timeout since we are verifying client timeout
            request.headers["connect-timeout-ms"] = "10000"
            return await self._transport.execute(request)

    class SleepingHaberdasher(Haberdasher):
        async def make_hat(self, request, ctx) -> NoReturn:
            await asyncio.sleep(10)
            raise AssertionError("Should be timedout already")

    app = HaberdasherASGIApplication(SleepingHaberdasher())
    http_client = Client(ModifyTimeout(ASGITransport(app)))

    async with HaberdasherClient(
        "http://localhost", timeout_ms=client_timeout_ms, http_client=http_client
    ) as client:
        with pytest.raises(ConnectError) as exc_info:
            await client.make_hat(request=Size(inches=10), timeout_ms=call_timeout_ms)

    assert exc_info.value.code == Code.DEADLINE_EXCEEDED
    assert exc_info.value.message == "Request timed out"
    assert recorded_timeout_header == "200"


@pytest.mark.asyncio
async def test_async_unhandled_exception_reraised() -> None:
    class RaisingHaberdasher(Haberdasher):
        async def make_hat(self, request, ctx) -> NoReturn:
            raise TypeError("Something went wrong")

    app = HaberdasherASGIApplication(RaisingHaberdasher())
    transport = ASGITransport(app)
    http_client = Client(transport)

    async with HaberdasherClient(
        "http://localhost", timeout_ms=200, http_client=http_client
    ) as client:
        with pytest.raises(ConnectError, match="Something went wrong"):
            await client.make_hat(request=Size(inches=10))

    assert isinstance(transport.app_exception, TypeError)
    assert str(transport.app_exception) == "Something went wrong"


@pytest.mark.asyncio
async def test_async_unhandled_exception_reraised_stream() -> None:
    class RaisingHaberdasher(Haberdasher):
        def make_similar_hats(self, request: Size, ctx: RequestContext) -> NoReturn:
            raise TypeError("Something went wrong")

    app = HaberdasherASGIApplication(RaisingHaberdasher())
    transport = ASGITransport(app)
    http_client = Client(transport)

    async with HaberdasherClient(
        "http://localhost", timeout_ms=200, http_client=http_client
    ) as client:
        with pytest.raises(ConnectError, match="Something went wrong"):
            async for _ in client.make_similar_hats(request=Size(inches=10)):
                pass

    assert isinstance(transport.app_exception, TypeError)
    assert str(transport.app_exception) == "Something went wrong"


@pytest.mark.asyncio
async def test_async_connect_exception_not_reraised() -> None:
    class RaisingHaberdasher(Haberdasher):
        async def make_hat(self, request, ctx) -> NoReturn:
            raise ConnectError(Code.INTERNAL, "We're broken")

    app = HaberdasherASGIApplication(RaisingHaberdasher())
    transport = ASGITransport(app)
    http_client = Client(transport)

    async with HaberdasherClient(
        "http://localhost", timeout_ms=200, http_client=http_client
    ) as client:
        with pytest.raises(ConnectError, match="We're broken"):
            await client.make_hat(request=Size(inches=10))

    assert transport.app_exception is None


@pytest.mark.asyncio
async def test_async_connect_exception_not_reraised_stream() -> None:
    class RaisingHaberdasher(Haberdasher):
        def make_similar_hats(self, request: Size, ctx: RequestContext) -> NoReturn:
            raise ConnectError(Code.INTERNAL, "We're broken")

    app = HaberdasherASGIApplication(RaisingHaberdasher())
    transport = ASGITransport(app)
    http_client = Client(transport)

    async with HaberdasherClient(
        "http://localhost", timeout_ms=200, http_client=http_client
    ) as client:
        with pytest.raises(ConnectError, match="We're broken"):
            async for _ in client.make_similar_hats(request=Size(inches=10)):
                pass

    assert transport.app_exception is None


@pytest.mark.asyncio
async def test_async_http_exception_not_reraised() -> None:
    class RaisingHaberdasher(Haberdasher):
        async def make_hat(self, request, ctx) -> NoReturn:
            raise HTTPException(status=HTTPStatus.INTERNAL_SERVER_ERROR, headers=[])

    app = HaberdasherASGIApplication(RaisingHaberdasher())
    transport = ASGITransport(app)
    http_client = Client(transport)

    async with HaberdasherClient(
        "http://localhost", timeout_ms=200, http_client=http_client
    ) as client:
        with pytest.raises(ConnectError, match="Internal Server Error"):
            await client.make_hat(request=Size(inches=10))

    assert transport.app_exception is None


def test_sync_unhandled_exception_logged() -> None:
    class RaisingHaberdasher(HaberdasherSync):
        def make_hat(self, request, ctx) -> NoReturn:
            raise TypeError("Something went wrong")

    app = HaberdasherWSGIApplication(RaisingHaberdasher())
    transport = WSGITransport(app)
    http_client = SyncClient(transport)

    with (
        HaberdasherClientSync(
            "http://localhost", timeout_ms=200, http_client=http_client
        ) as client,
        pytest.raises(ConnectError, match="Something went wrong"),
    ):
        client.make_hat(request=Size(inches=10))

    logged_error = transport.error_stream.getvalue()
    assert "Exception in WSGI application" in logged_error
    assert "TypeError: Something went wrong" in logged_error
    assert "Traceback" in logged_error


def test_sync_unhandled_exception_logged_stream() -> None:
    class RaisingHaberdasher(HaberdasherSync):
        def make_similar_hats(self, request, ctx) -> NoReturn:
            raise TypeError("Something went wrong")

    app = HaberdasherWSGIApplication(RaisingHaberdasher())
    transport = WSGITransport(app)
    http_client = SyncClient(transport)

    with (
        HaberdasherClientSync(
            "http://localhost", timeout_ms=200, http_client=http_client
        ) as client,
        pytest.raises(ConnectError, match="Something went wrong"),
    ):
        next(client.make_similar_hats(request=Size(inches=10)))

    logged_error = transport.error_stream.getvalue()
    assert "Exception in WSGI application" in logged_error
    assert "TypeError: Something went wrong" in logged_error
    assert "Traceback" in logged_error

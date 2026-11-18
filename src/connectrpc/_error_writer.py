from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING

from ._codec import proto_binary_codec
from ._protocol import ConnectWireError, ExtendedHTTPStatus
from ._protocol_connect import (
    CONNECT_STREAMING_CONTENT_TYPE_PREFIX,
    ConnectEnvelopeWriter,
)
from ._protocol_grpc import GRPCServerProtocol
from ._protocol_server import negotiate_server_protocol
from ._server_async import _process_headers as _process_asgi_headers
from ._server_sync import (
    _drain_request_body,
    _process_headers as _process_wsgi_headers,
    _RequestBody,
)
from .request import Headers

if TYPE_CHECKING:
    from collections.abc import Iterable
    from wsgiref.types import StartResponse, WSGIEnvironment

    from asgiref.typing import ASGISendCallable, HTTPScope

    from .errors import ConnectError

_OK = ExtendedHTTPStatus.from_http_status(HTTPStatus.OK)


class ErrorWriter:
    """Writes an error in the format of the request's protocol.

    ASGI and WSGI middleware can use it to reject a request with an error that
    Connect, gRPC and gRPC-Web clients read with its code, message and details.
    gRPC and gRPC-Web requests get a trailers-only response, Connect streaming
    requests an end-of-stream message, and all others a Connect unary error.

    Examples:
        ```python
        async def middleware(scope, receive, send):
            if scope["type"] == "http" and not allow_request(scope):
                error = ConnectError(Code.RESOURCE_EXHAUSTED, "rate limit exceeded")
                await ErrorWriter().write_asgi(scope, send, error)
                return
            await app(scope, receive, send)
        ```

    """

    async def write_asgi(
        self, scope: HTTPScope, send: ASGISendCallable, error: ConnectError
    ) -> None:
        """Write the error as the whole response, in place of the ASGI application."""
        headers = _process_asgi_headers(scope.get("headers", ()))
        status, response_headers, body = _error_response(
            scope["method"], headers, error
        )
        await send(
            {
                "type": "http.response.start",
                "status": status.code,
                "headers": [(k.encode(), v.encode()) for k, v in response_headers],
                "trailers": False,
            }
        )
        await send({"type": "http.response.body", "body": body, "more_body": False})

    def write_wsgi(
        self,
        environ: WSGIEnvironment,
        start_response: StartResponse,
        error: ConnectError,
    ) -> Iterable[bytes]:
        """Write the error as the whole response, in place of the WSGI application.

        Return its result from the middleware. On HTTP/1 it first reads the rest
        of the request body, as the WSGI application does.
        """
        headers = _process_wsgi_headers(environ)
        status, response_headers, body = _error_response(
            environ["REQUEST_METHOD"], headers, error
        )
        _drain_request_body(environ, _RequestBody.from_environ(environ))
        start_response(f"{status.code} {status.reason}", response_headers)
        return [body] if body else []


def _error_response(
    http_method: str, headers: Headers, error: ConnectError
) -> tuple[ExtendedHTTPStatus, list[tuple[str, str]], bytes]:
    content_type = headers.get("content-type", "")
    protocol = negotiate_server_protocol(content_type)
    if http_method == "POST" and isinstance(protocol, GRPCServerProtocol):
        # A trailers-only response, as the applications write it, but with the
        # request's content type so that clients accept its codec.
        grpc_headers = protocol.trailers_only_headers(None, error)
        grpc_headers["content-type"] = content_type
        return _OK, list(grpc_headers.allitems()), b""
    wire_error = ConnectWireError.from_exception(error)
    if http_method == "POST" and content_type.startswith(
        CONNECT_STREAMING_CONTENT_TYPE_PREFIX
    ):
        writer = ConnectEnvelopeWriter(proto_binary_codec(), None)
        body = writer.end(Headers(), wire_error)
        return _OK, [("content-type", content_type)], body
    status = wire_error.to_http_status()
    return status, [("content-type", "application/json")], wire_error.to_json_bytes()

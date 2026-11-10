from __future__ import annotations

import inspect
import re
import sys
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from connectrpc._codegen import method_local_name
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from connectrpc.server import ConnectASGIApplication
from fastapi import APIRouter, Depends, Request, Response
from protobuf.wkt.google.protobuf.descriptor_pb import MethodOptions
from starlette.requests import ClientDisconnect

if TYPE_CHECKING:
    from collections.abc import (
        AsyncIterator,
        Awaitable,
        Callable,
        Coroutine,
        Iterator,
        Sequence,
    )

    from fastapi.routing import APIRoute
    from protobuf import DescMessage, DescMethod, DescService
    from starlette.types import Message, Receive, Scope, Send


class ServiceType(Protocol):
    """A generated service protocol class."""

    @classmethod
    def desc(cls) -> DescService: ...


_SVC = TypeVar("_SVC", bound=ServiceType)

MESSAGE_MARKER = "x-connect-message"
"""Key of a placeholder schema holding the type name of the message it stands for."""

METHOD_ATTRIBUTE = "__connect_method__"
"""Attribute of a route's endpoint holding the descriptor of the method it serves."""

_STREAM_TYPES = {
    "unary": "Unary RPC.",
    "client_streaming": "Client streaming RPC.",
    "server_streaming": "Server streaming RPC.",
    "bidi_streaming": "Bidirectional streaming RPC.",
}


def _error_response(*, streaming: bool) -> dict[str, Any]:
    if streaming:
        return {
            "description": "Connect errors are sent in the end of the stream, with "
            "HTTP status 200. Other errors are a Connect error in JSON."
        }
    return {
        "description": "Connect error",
        "content": {
            "application/json": {
                "schema": {
                    "title": "ConnectError",
                    "type": "object",
                    "required": ["code"],
                    "properties": {
                        "code": {
                            "type": "string",
                            "enum": [code.value for code in Code],
                        },
                        "message": {"type": "string"},
                        "details": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["type", "value"],
                                "properties": {
                                    "type": {"type": "string"},
                                    "value": {"type": "string", "format": "byte"},
                                    "debug": {},
                                },
                            },
                        },
                    },
                }
            }
        },
    }


def _get_parameters() -> list[dict[str, Any]]:
    return [
        {
            "name": "message",
            "in": "query",
            "required": True,
            "description": "The request message, encoded as `encoding`.",
            "schema": {"type": "string"},
        },
        {
            "name": "encoding",
            "in": "query",
            "required": True,
            "description": "The encoding of `message`, `json` or `proto`.",
            "schema": {"type": "string", "enum": ["json", "proto"]},
        },
        {
            "name": "base64",
            "in": "query",
            "required": False,
            "description": "Set to `1` if `message` is base64 encoded.",
            "schema": {"type": "string", "enum": ["1"]},
        },
        {
            "name": "compression",
            "in": "query",
            "required": False,
            "description": "The compression of `message`.",
            "schema": {"type": "string"},
        },
        {
            "name": "connect",
            "in": "query",
            "required": False,
            "description": "The Connect protocol version.",
            "schema": {"type": "string", "enum": ["v1"]},
        },
    ]


class _ConnectResponse(Response):
    """Hands the request to a Connect application instead of writing a body."""

    def __init__(
        self, app: ConnectASGIApplication[Any], path: str, request: Request
    ) -> None:
        super().__init__()
        self._app = app
        self._path = path
        self._request = request

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                connect_headers = list(message.get("headers", []))
                connect_names = {name.lower() for name, _ in connect_headers}
                connect_names.add(b"content-length")
                message = {
                    **message,
                    "headers": [
                        *connect_headers,
                        *(
                            (name, value)
                            for name, value in self.raw_headers
                            if name == b"set-cookie" or name not in connect_names
                        ),
                    ],
                }
            await send(message)

        await self._app(
            {**scope, "path": self._path, "root_path": ""},
            _receive_body(self._request, receive),
            send_with_headers,
        )
        if self.background is not None:
            await self.background()


def _receive_body(request: Request, receive: Receive) -> Receive:
    """Return a receive callable that reads the request body through the request."""
    chunks = request.stream()
    streaming = True

    async def receive_body() -> Message:
        nonlocal streaming
        if not streaming:
            return await receive()
        try:
            chunk = await anext(chunks)
        except StopAsyncIteration:
            streaming = False
            return {"type": "http.request", "body": b"", "more_body": False}
        except ClientDisconnect:
            streaming = False
            return {"type": "http.disconnect"}
        except RuntimeError as e:
            if str(e) != "Stream consumed":
                raise
            msg = (
                "the request body was consumed by request.stream() before the RPC, "
                "read it with request.body() instead"
            )
            raise ConnectError(Code.INTERNAL, msg) from e
        return {"type": "http.request", "body": chunk, "more_body": True}

    return receive_body


def connect_router(
    service: type[_SVC],
    get_service: Callable[
        ..., _SVC | Awaitable[_SVC] | AsyncIterator[_SVC] | Iterator[_SVC]
    ],
    *,
    application: Callable[[_SVC], ConnectASGIApplication[_SVC]] | None = None,
    tags: Sequence[str] | None = None,
) -> APIRouter:
    """Create a FastAPI router serving a Connect service.

    Each RPC becomes a route. FastAPI's dependency injection provides the service
    instance for each request. The RPC runs while the response is sent, after
    request-scoped dependencies are resolved and before they are cleaned up.

    The routes document their request and response messages with placeholder
    schemas. [add_connect_schemas][connectrpc_fastapi.add_connect_schemas] replaces
    them with the messages' schemas for docs.

    Args:
        service: The generated service protocol class.
        get_service: A FastAPI dependency providing the service instance.
        application: Creates the Connect application for a service instance. Defaults
            to the application class generated next to the service, or a
            ConnectASGIApplication for the service if there is none. Use this to pass
            options such as interceptors.
        tags: OpenAPI tags for the routes. Defaults to the service's type name.

    """
    desc = service.desc()
    router = APIRouter(
        tags=list(tags) if tags is not None else [desc.type_name],
        generate_unique_id_function=_operation_id,
    )
    create_application = application or _default_application(service, desc)
    for method in desc.methods:
        _add_method(router, service, method, get_service, create_application)
    return router


def _default_application(
    service: type[_SVC], desc: DescService
) -> Callable[[_SVC], ConnectASGIApplication[_SVC]]:
    """Return the default factory of the Connect application for a service."""
    for cls in service.__mro__:
        generated = getattr(
            sys.modules.get(cls.__module__), f"{cls.__name__}ASGIApplication", None
        )
        if isinstance(generated, type) and issubclass(
            generated, ConnectASGIApplication
        ):
            return generated

    def create(service: _SVC) -> ConnectASGIApplication[_SVC]:
        return ConnectASGIApplication(service=service, desc=desc)

    return create


def _add_method(
    router: APIRouter,
    service: type[ServiceType],
    method: DescMethod,
    get_service: Callable[..., Any],
    create_application: Callable[[Any], ConnectASGIApplication[Any]],
) -> None:
    path = f"/{method.parent.type_name}/{method.name}"
    streaming_request = method.method_kind in ("client_streaming", "bidi_streaming")
    streaming_response = method.method_kind in ("server_streaming", "bidi_streaming")
    deprecated = (
        method.deprecated
        or method.parent.deprecated
        or method.parent.file.deprecated
        or None
    )
    name = f"{method.parent.type_name}.{method.name}"
    description = (
        inspect.getdoc(getattr(service, method_local_name(method), None))
        or _STREAM_TYPES[method.method_kind]
    )
    endpoint = _endpoint(get_service, create_application, path)
    setattr(endpoint, METHOD_ATTRIBUTE, method)

    request_body: dict[str, Any] = {
        "required": True,
        "content": _content(method.input, streaming=streaming_request),
    }
    if streaming_request:
        request_body["description"] = (
            f"A Connect enveloped stream of {method.input.type_name} messages"
        )
    response: dict[str, Any] = {
        "content": _content(method.output, streaming=streaming_response)
    }
    if streaming_response:
        response["description"] = (
            f"A Connect enveloped stream of {method.output.type_name} messages"
        )
    router.add_api_route(
        path,
        endpoint,
        methods=["POST"],
        name=name,
        summary=method.name,
        description=description,
        deprecated=deprecated,
        response_class=Response,
        responses={"default": _error_response(streaming=streaming_response)},
        openapi_extra={"requestBody": request_body, "responses": {"200": response}},
    )

    if (
        method.method_kind == "unary"
        and method.idempotency == MethodOptions.IdempotencyLevel.NO_SIDE_EFFECTS
    ):
        router.add_api_route(
            path,
            endpoint,
            methods=["GET"],
            name=f"{name}.GET",
            summary=method.name,
            description=description,
            deprecated=deprecated,
            response_class=Response,
            responses={"default": _error_response(streaming=False)},
            openapi_extra={
                "parameters": _get_parameters(),
                "responses": {"200": response},
            },
        )


def _operation_id(route: APIRoute) -> str:
    """Return the operation ID of a route, from its full path and HTTP method."""
    operation_id = re.sub(r"[^\w.]", "", route.path_format.strip("/").replace("/", "."))
    if "GET" in route.methods:
        operation_id += ".GET"
    return operation_id


def _content(message: DescMessage, *, streaming: bool) -> dict[str, Any]:
    json_media_type = "application/connect+json" if streaming else "application/json"
    binary_media_type = (
        "application/connect+proto" if streaming else "application/proto"
    )
    return {
        json_media_type: {
            "schema": {"type": "object", MESSAGE_MARKER: message.type_name}
        },
        binary_media_type: {"schema": {"type": "string", "format": "binary"}},
    }


def _endpoint(
    get_service: Callable[..., Any],
    create_application: Callable[[Any], ConnectASGIApplication[Any]],
    path: str,
) -> Callable[..., Coroutine[Any, Any, Response]]:
    async def serve(
        request: Request,
        response: Response,
        service: Any = Depends(get_service),  # noqa: B008
    ) -> Response:
        """Serve the request with the Connect application."""
        connect_response = _ConnectResponse(create_application(service), path, request)
        connect_response.raw_headers.extend(response.raw_headers)
        return connect_response

    return serve

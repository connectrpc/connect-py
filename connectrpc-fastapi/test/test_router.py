from __future__ import annotations

import functools
import re
from typing import TYPE_CHECKING, Annotated, Any

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from connectrpc.protocol import ProtocolType
from example.eliza_service import DemoElizaService
from example.gen.connectrpc.eliza.v1 import eliza_pb
from example.gen.connectrpc.eliza.v1.eliza_connect import (
    ElizaService,
    ElizaServiceASGIApplication,
    ElizaServiceClient,
    ElizaServiceSync,
)
from example.gen.connectrpc.eliza.v1.eliza_pb import (
    ConverseRequest,
    IntroduceRequest,
    IntroduceResponse,
    SayRequest,
    SayResponse,
)
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    FastAPI,
    HTTPException,
    Request,
    Response as FastAPIResponse,
)
from protobuf import DescService
from protobuf.wkt import FileDescriptorSet
from protobuf.wkt.google.protobuf.descriptor_pb import (
    DescriptorProto,
    EnumDescriptorProto,
    EnumValueDescriptorProto,
    FieldDescriptorProto,
    FileDescriptorProto,
    FileOptions,
    MessageOptions,
    MethodDescriptorProto,
    MethodOptions,
    ServiceDescriptorProto,
    ServiceOptions,
)
from pyqwest import Client
from pyqwest.testing import ASGITransport

from connectrpc_fastapi import add_connect_schemas, connect_router
from connectrpc_fastapi._schema import message_schemas

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from connectrpc.request import RequestContext
    from connectrpc.server import ConnectASGIApplication
    from protobuf import DescService

_SERVICE = "connectrpc.eliza.v1.ElizaService"


class RecordingElizaService(DemoElizaService):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self.events = events

    async def say(self, request: SayRequest, _ctx: RequestContext) -> SayResponse:
        self.events.append("say")
        return await super().say(request, _ctx)

    async def introduce(
        self, request: IntroduceRequest, _ctx: RequestContext
    ) -> AsyncIterator[IntroduceResponse]:
        async for response in super().introduce(request, _ctx):
            self.events.append("item")
            yield response


def get_events(request: Request) -> list[str]:
    return request.app.state.events


async def get_eliza(
    events: Annotated[list[str], Depends(get_events)],
) -> AsyncIterator[RecordingElizaService]:
    events.append("open")
    try:
        yield RecordingElizaService(events)
    finally:
        events.append("close")


def create_app(**kwargs: Any) -> FastAPI:
    app = FastAPI()
    app.state.events = []
    app.include_router(connect_router(ElizaService, get_eliza, **kwargs))
    return app


def create_client(
    app: FastAPI, base_url: str = "http://localhost"
) -> ElizaServiceClient:
    return ElizaServiceClient(base_url, http_client=Client(ASGITransport(app)))


async def post(
    app: FastAPI, path: str, body: bytes, headers: dict[str, str] | None = None
) -> tuple[int, list[tuple[str, str]], bytes]:
    """Send a unary JSON request and return the status, headers, and body."""
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [
            (b"content-type", b"application/json"),
            (b"connect-protocol-version", b"1"),
            *((k.encode(), v.encode()) for k, v in (headers or {}).items()),
        ],
        "client": ("127.0.0.1", 1234),
        "server": ("127.0.0.1", 80),
        "app": app,
    }
    received = False
    status = 0
    response_headers: list[tuple[str, str]] = []
    chunks: list[bytes] = []

    async def receive() -> dict[str, Any]:
        nonlocal received
        if received:
            return {"type": "http.disconnect"}
        received = True
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        nonlocal status
        if message["type"] == "http.response.start":
            status = message["status"]
            response_headers.extend(
                (name.decode(), value.decode()) for name, value in message["headers"]
            )
        elif message["type"] == "http.response.body":
            chunks.append(message.get("body", b""))

    await app(scope, receive, send)
    return status, response_headers, b"".join(chunks)


@pytest.mark.asyncio
async def test_unary() -> None:
    app = create_app()
    async with create_client(app) as client:
        response = await client.say(SayRequest(sentence="Hello"))
        assert response.sentence
        response = await client.say(SayRequest(sentence="Hello"), use_get=True)
        assert response.sentence
    assert app.state.events == ["open", "say", "close", "open", "say", "close"]


@pytest.mark.asyncio
async def test_streaming() -> None:
    app = create_app()
    async with create_client(app) as client:
        introductions = [
            response.sentence
            async for response in client.introduce(IntroduceRequest(name="Test"))
        ]
        assert len(introductions) > 1
        events = app.state.events
        assert events[0] == "open"
        assert events[-1] == "close"
        assert events[1:-1] == ["item"] * len(introductions)

        async def requests() -> AsyncIterator[ConverseRequest]:
            yield ConverseRequest(sentence="Hello")
            yield ConverseRequest(sentence="How are you?")

        replies = [response.sentence async for response in client.converse(requests())]
        assert len(replies) == 2
    assert app.state.events[-2:] == ["open", "close"]


@pytest.mark.asyncio
async def test_prefix() -> None:
    app = FastAPI()
    app.state.events = []
    app.include_router(connect_router(ElizaService, get_eliza), prefix="/api")
    nested = APIRouter(prefix="/v2")
    nested.include_router(connect_router(ElizaService, get_eliza))
    app.include_router(nested, prefix="/nested")
    for base_url in ("http://localhost/api", "http://localhost/nested/v2"):
        async with create_client(app, base_url) as client:
            assert (await client.say(SayRequest(sentence="Hello"))).sentence
            assert (
                await client.say(SayRequest(sentence="Hello"), use_get=True)
            ).sentence
            assert [r async for r in client.introduce(IntroduceRequest(name="Test"))]
    add_connect_schemas(app)
    schema = app.openapi()
    assert_refs_resolve(schema)
    assert schema["paths"][f"/nested/v2/{_SERVICE}/Say"]["post"]["requestBody"][
        "content"
    ]["application/json"]["schema"] == _ref("connectrpc.eliza.v1.SayRequest")
    assert schema["paths"][f"/api/{_SERVICE}/Say"]["post"]["operationId"] == (
        f"api.{_SERVICE}.Say"
    )
    assert schema["paths"][f"/api/{_SERVICE}/Say"]["get"]["operationId"] == (
        f"api.{_SERVICE}.Say.GET"
    )
    assert set(schema["paths"]) == {
        f"/api/{_SERVICE}/Say",
        f"/api/{_SERVICE}/Converse",
        f"/api/{_SERVICE}/Introduce",
        f"/nested/v2/{_SERVICE}/Say",
        f"/nested/v2/{_SERVICE}/Converse",
        f"/nested/v2/{_SERVICE}/Introduce",
    }


@pytest.mark.asyncio
async def test_dependency_overrides() -> None:
    class FakeElizaService(ElizaService):
        async def say(self, request: SayRequest, _ctx: Any, /) -> SayResponse:
            return SayResponse(sentence=f"fake: {request.sentence}")

    def fake_eliza() -> FakeElizaService:
        return FakeElizaService()

    app = create_app()
    app.dependency_overrides[get_eliza] = fake_eliza
    async with create_client(app) as client:
        response = await client.say(SayRequest(sentence="Hello"))
    assert response.sentence == "fake: Hello"
    assert app.state.events == []


@pytest.mark.asyncio
async def test_include_router_dependencies() -> None:
    def require_auth(request: Request) -> None:
        if request.headers.get("authorization") != "Bearer secret":
            raise HTTPException(status_code=401, detail="unauthenticated")

    app = FastAPI()
    app.state.events = []
    app.include_router(
        connect_router(ElizaService, get_eliza), dependencies=[Depends(require_auth)]
    )
    async with create_client(app) as client:
        with pytest.raises(ConnectError) as exc_info:
            await client.say(SayRequest(sentence="Hello"))
        assert exc_info.value.code == Code.UNAUTHENTICATED
        response = await client.say(
            SayRequest(sentence="Hello"), headers={"authorization": "Bearer secret"}
        )
        assert response.sentence
    assert app.state.events == ["open", "say", "close"]


@pytest.mark.asyncio
async def test_background_tasks_and_headers() -> None:
    ran: list[str] = []

    async def get_eliza_with_extras(
        background_tasks: BackgroundTasks, response: FastAPIResponse
    ) -> AsyncIterator[DemoElizaService]:
        background_tasks.add_task(ran.append, "task")
        response.headers["x-from-dependency"] = "yes"
        response.set_cookie("session", "abc")
        ran.append("open")
        try:
            yield DemoElizaService()
        finally:
            ran.append("close")

    app = FastAPI()
    app.include_router(connect_router(ElizaService, get_eliza_with_extras))
    status, headers, body = await post(
        app, f"/{_SERVICE}/Say", SayRequest(sentence="Hello").to_json().encode()
    )
    assert status == 200
    assert SayResponse.from_json(body.decode()).sentence
    by_name: dict[str, list[str]] = {}
    for name, value in headers:
        by_name.setdefault(name, []).append(value)
    assert by_name["x-from-dependency"] == ["yes"]
    assert len(by_name["set-cookie"]) == 1
    assert by_name["set-cookie"][0].startswith("session=abc")
    assert by_name["content-type"] == ["application/json"]
    assert ("content-length", "0") not in headers
    assert ran == ["open", "task", "close"]


@pytest.mark.asyncio
async def test_application() -> None:
    created: list[ElizaService] = []

    def application(service: ElizaService) -> ConnectASGIApplication[ElizaService]:
        created.append(service)
        return ElizaServiceASGIApplication(service)

    app = create_app(application=application)
    async with create_client(app) as client:
        await client.say(SayRequest(sentence="Hello"))
        await client.say(SayRequest(sentence="Hello"))
    assert len(created) == 2
    assert all(isinstance(service, RecordingElizaService) for service in created)

    partial: Callable[..., Any] = functools.partial(
        ElizaServiceASGIApplication, interceptors=[]
    )
    app = create_app(application=partial)
    async with create_client(app) as client:
        assert (await client.say(SayRequest(sentence="Hello"))).sentence


def _ref(type_name: str) -> dict[str, str]:
    return {"$ref": f"#/components/schemas/{type_name}"}


_BINARY = {"schema": {"type": "string", "format": "binary"}}


def assert_refs_resolve(schema: dict[str, Any]) -> None:
    """Assert that every reference in the OpenAPI schema resolves to a component."""
    components = schema.get("components", {}).get("schemas", {})

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            ref = node.get("$ref")
            if isinstance(ref, str):
                prefix = "#/components/schemas/"
                assert ref.startswith(prefix), ref
                assert ref.removeprefix(prefix) in components, ref
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(schema)


def test_openapi() -> None:
    app = create_app()
    add_connect_schemas(app)
    schema = app.openapi()
    assert_refs_resolve(schema)
    paths = schema["paths"]
    assert set(paths) == {
        f"/{_SERVICE}/Say",
        f"/{_SERVICE}/Converse",
        f"/{_SERVICE}/Introduce",
    }
    components = schema["components"]["schemas"]
    assert {
        "connectrpc.eliza.v1.SayRequest",
        "connectrpc.eliza.v1.SayResponse",
        "connectrpc.eliza.v1.ConverseRequest",
        "connectrpc.eliza.v1.ConverseResponse",
        "connectrpc.eliza.v1.IntroduceRequest",
        "connectrpc.eliza.v1.IntroduceResponse",
    } <= set(components)
    say_request = components["connectrpc.eliza.v1.SayRequest"]
    assert say_request["type"] == "object"
    assert say_request["description"] == "SayRequest is a single-sentence request."
    assert say_request["properties"]["sentence"]["type"] == "string"

    say = paths[f"/{_SERVICE}/Say"]
    assert set(say) == {"post", "get"}
    post = say["post"]
    assert post["operationId"] == f"{_SERVICE}.Say"
    assert post["tags"] == [_SERVICE]
    assert post["summary"] == "Say"
    assert post["description"].startswith("Say is a unary RPC.")
    assert "deprecated" not in post
    assert post["requestBody"] == {
        "required": True,
        "content": {
            "application/json": {"schema": _ref("connectrpc.eliza.v1.SayRequest")},
            "application/proto": _BINARY,
        },
    }
    assert set(post["responses"]) == {"200", "default"}
    assert post["responses"]["default"]["description"] == "Connect error"
    assert post["responses"]["200"]["content"] == {
        "application/json": {"schema": _ref("connectrpc.eliza.v1.SayResponse")},
        "application/proto": _BINARY,
    }
    assert post["responses"]["default"]["content"]["application/json"]["schema"][
        "properties"
    ]["code"]["enum"] == [code.value for code in Code]

    get = say["get"]
    assert get["operationId"] == f"{_SERVICE}.Say.GET"
    assert "requestBody" not in get
    assert {p["name"]: p["required"] for p in get["parameters"]} == {
        "message": True,
        "encoding": True,
        "base64": False,
        "compression": False,
        "connect": False,
    }
    assert get["responses"]["200"] == post["responses"]["200"]

    introduce = paths[f"/{_SERVICE}/Introduce"]["post"]
    assert introduce["requestBody"]["content"] == {
        "application/json": {"schema": _ref("connectrpc.eliza.v1.IntroduceRequest")},
        "application/proto": _BINARY,
    }
    assert introduce["responses"]["200"] == {
        "description": "A Connect enveloped stream of "
        "connectrpc.eliza.v1.IntroduceResponse messages",
        "content": {
            "application/connect+json": {
                "schema": _ref("connectrpc.eliza.v1.IntroduceResponse")
            },
            "application/connect+proto": _BINARY,
        },
    }

    assert "content" not in introduce["responses"]["default"]
    assert introduce["responses"]["default"]["description"].startswith(
        "Connect errors are sent in the end of the stream"
    )
    assert app.url_path_for(f"{_SERVICE}.Introduce") == f"/{_SERVICE}/Introduce"
    assert app.url_path_for(f"{_SERVICE}.Say.GET") == f"/{_SERVICE}/Say"

    converse = paths[f"/{_SERVICE}/Converse"]["post"]
    assert converse["requestBody"] == {
        "required": True,
        "description": "A Connect enveloped stream of "
        "connectrpc.eliza.v1.ConverseRequest messages",
        "content": {
            "application/connect+json": {
                "schema": _ref("connectrpc.eliza.v1.ConverseRequest")
            },
            "application/connect+proto": _BINARY,
        },
    }

    assert app.openapi() is schema


def test_openapi_without_schemas() -> None:
    """Messages are documented as objects without fields."""
    schema = create_app().openapi()
    assert_refs_resolve(schema)
    body = schema["paths"][f"/{_SERVICE}/Say"]["post"]["requestBody"]
    assert body["content"]["application/json"]["schema"] == {
        "type": "object",
        "x-connect-message": "connectrpc.eliza.v1.SayRequest",
    }
    assert "connectrpc.eliza.v1.SayRequest" not in schema.get("components", {}).get(
        "schemas", {}
    )


def test_openapi_excluded() -> None:
    app = FastAPI()
    app.include_router(connect_router(ElizaService, get_eliza), include_in_schema=False)
    add_connect_schemas(app)
    assert not app.openapi().get("paths")


def test_add_connect_schemas_repeated() -> None:
    app = FastAPI()
    app.include_router(connect_router(ElizaService, get_eliza), prefix="/a")
    app.include_router(connect_router(ElizaService, get_eliza), prefix="/b")
    add_connect_schemas(app)
    add_connect_schemas(app)
    schema = app.openapi()
    assert_refs_resolve(schema)
    assert "connectrpc.eliza.v1.SayRequest" in schema["components"]["schemas"]
    assert app.openapi() is schema


def test_add_connect_schemas_conflict() -> None:
    app = create_app()
    original = app.openapi

    def openapi_with_conflict() -> dict[str, Any]:
        schema = original()
        schema.setdefault("components", {}).setdefault("schemas", {})[
            "connectrpc.eliza.v1.SayRequest"
        ] = {"type": "string"}
        return schema

    app.openapi = openapi_with_conflict  # type: ignore[method-assign]
    add_connect_schemas(app)
    for _ in range(2):
        with pytest.raises(
            ValueError, match=re.escape("connectrpc.eliza.v1.SayRequest")
        ):
            app.openapi()


def test_message_schemas_nested() -> None:
    """Messages and enums contained in other messages are named by type name too."""
    schemas = message_schemas([FileDescriptorProto.desc()])
    assert "google.protobuf.FileDescriptorProto" in schemas
    assert "google.protobuf.DescriptorProto" in schemas
    file_schema = schemas["google.protobuf.FileDescriptorProto"]
    assert file_schema["properties"]["messageType"]["items"] == _ref(
        "google.protobuf.DescriptorProto"
    )
    # DescriptorProto contains itself.
    assert schemas["google.protobuf.DescriptorProto"]["properties"]["nestedType"][
        "items"
    ] == _ref("google.protobuf.DescriptorProto")
    field_schema = schemas["google.protobuf.FieldDescriptorProto"]
    assert field_schema["properties"]["type"] == _ref(
        "google.protobuf.FieldDescriptorProto.Type"
    )
    assert schemas["google.protobuf.FieldDescriptorProto.Type"]["type"] == "string"
    assert_refs_resolve({"components": {"schemas": schemas}, "paths": schemas})
    for name in schemas:
        assert "." in name, name


@pytest.mark.asyncio
async def test_mount_and_root_path() -> None:
    sub = FastAPI()
    sub.state.events = []
    sub.include_router(connect_router(ElizaService, get_eliza))
    app = FastAPI()
    app.mount("/sub", sub)
    async with create_client(app, "http://localhost/sub") as client:
        assert (await client.say(SayRequest(sentence="Hello"))).sentence
        assert [r async for r in client.introduce(IntroduceRequest(name="Test"))]

    rooted = FastAPI()
    rooted.state.events = []
    rooted.include_router(connect_router(ElizaService, get_eliza))

    async def server_with_root_path(scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            scope = {**scope, "root_path": "/root"}
        await rooted(scope, receive, send)

    async with ElizaServiceClient(
        "http://localhost/root",
        http_client=Client(ASGITransport(server_with_root_path)),
    ) as client:
        assert (await client.say(SayRequest(sentence="Hello"))).sentence
        assert (await client.say(SayRequest(sentence="Hello"), use_get=True)).sentence


@pytest.mark.asyncio
async def test_dependency_reads_body() -> None:
    bodies: list[bytes] = []

    async def get_eliza_reading_body(request: Request) -> DemoElizaService:
        bodies.append(await request.body())
        return DemoElizaService()

    app = FastAPI()
    app.include_router(connect_router(ElizaService, get_eliza_reading_body))
    async with create_client(app) as client:
        assert (await client.say(SayRequest(sentence="Hello"))).sentence
        assert [r async for r in client.introduce(IntroduceRequest(name="Test"))]

        async def requests() -> AsyncIterator[ConverseRequest]:
            yield ConverseRequest(sentence="Hello")

        assert [r async for r in client.converse(requests())]
    assert len(bodies) == 3
    assert all(bodies)


def test_sync_service_rejected() -> None:
    with pytest.raises(
        TypeError, match=re.escape("ElizaServiceSync.say is not an async method")
    ):
        connect_router(ElizaServiceSync, get_eliza)  # type: ignore[invalid-argument-type]


def test_tags() -> None:
    app = FastAPI()
    app.include_router(connect_router(ElizaService, get_eliza, tags=()))
    app.include_router(connect_router(ElizaService, get_eliza), prefix="/x", tags=["x"])
    paths = app.openapi()["paths"]
    assert "tags" not in paths[f"/{_SERVICE}/Say"]["post"]
    assert paths[f"/x/{_SERVICE}/Say"]["post"]["tags"] == ["x", _SERVICE]


def test_dynamic_message_types() -> None:
    """Services whose messages have no generated class are documented too."""
    file = eliza_pb.desc()
    registry = FileDescriptorSet(
        file=[dependency.proto for dependency in file.dependencies] + [file.proto]
    ).to_registry()
    dynamic = registry.service(_SERVICE)
    assert dynamic is not None
    assert dynamic.methods[0].input.type is not SayRequest

    class DynamicElizaService:
        @classmethod
        def desc(cls) -> DescService:
            return dynamic

        async def say(self, request: Any, _ctx: Any, /) -> Any:
            raise NotImplementedError

        async def converse(self, _request: Any, _ctx: Any, /) -> AsyncIterator[Any]:
            raise NotImplementedError
            yield

        async def introduce(self, _request: Any, _ctx: Any, /) -> AsyncIterator[Any]:
            raise NotImplementedError
            yield

    def get_dynamic() -> DynamicElizaService:
        return DynamicElizaService()

    app = FastAPI()
    app.include_router(connect_router(DynamicElizaService, get_dynamic), prefix="/dyn")
    add_connect_schemas(app)
    schema = app.openapi()
    assert_refs_resolve(schema)
    assert schema["components"]["schemas"]["connectrpc.eliza.v1.SayRequest"][
        "properties"
    ] == {"sentence": {"type": "string", "title": "sentence"}}


@pytest.mark.asyncio
async def test_client_disconnect_during_request() -> None:
    """A client that disconnects while sending is handled and cleaned up after."""
    app = create_app()
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": f"/{_SERVICE}/Converse",
        "raw_path": f"/{_SERVICE}/Converse".encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [
            (b"content-type", b"application/connect+json"),
            (b"connect-protocol-version", b"1"),
        ],
        "client": ("127.0.0.1", 1234),
        "server": ("127.0.0.1", 80),
        "app": app,
    }
    message = ConverseRequest(sentence="Hello").to_json().encode()
    envelope = b"\x00" + len(message).to_bytes(4, "big") + message
    messages = iter(
        [
            {"type": "http.request", "body": envelope, "more_body": True},
            {"type": "http.disconnect"},
        ]
    )

    async def receive() -> dict[str, Any]:
        return next(messages)

    async def send(_message: dict[str, Any]) -> None:
        pass

    await app(scope, receive, send)
    assert app.state.events[0] == "open"
    assert app.state.events[-1] == "close"


def _dynamic_file() -> DescService:
    """A service with a deprecated method, in a file with a map of enum values."""
    proto = FileDescriptorProto(
        name="test/dynamic.proto",
        package="test.dynamic",
        syntax="proto3",
        options=FileOptions(deprecated=True),
        enum_type=[
            EnumDescriptorProto(
                name="Kind",
                value=[
                    EnumValueDescriptorProto(name="KIND_UNSPECIFIED", number=0),
                    EnumValueDescriptorProto(name="KIND_ONE", number=1),
                ],
            )
        ],
        message_type=[
            DescriptorProto(
                name="Request",
                field=[
                    FieldDescriptorProto(
                        name="kinds",
                        number=1,
                        label=FieldDescriptorProto.Label.REPEATED,
                        type=FieldDescriptorProto.Type.MESSAGE,
                        type_name=".test.dynamic.Request.KindsEntry",
                        json_name="kinds",
                    )
                ],
                nested_type=[
                    DescriptorProto(
                        name="KindsEntry",
                        options=MessageOptions(map_entry=True),
                        field=[
                            FieldDescriptorProto(
                                name="key",
                                number=1,
                                label=FieldDescriptorProto.Label.OPTIONAL,
                                type=FieldDescriptorProto.Type.STRING,
                                json_name="key",
                            ),
                            FieldDescriptorProto(
                                name="value",
                                number=2,
                                label=FieldDescriptorProto.Label.OPTIONAL,
                                type=FieldDescriptorProto.Type.ENUM,
                                type_name=".test.dynamic.Kind",
                                json_name="value",
                            ),
                        ],
                    )
                ],
            ),
            DescriptorProto(name="Response"),
        ],
        service=[
            ServiceDescriptorProto(
                name="Dynamic",
                method=[
                    MethodDescriptorProto(
                        name="Old",
                        input_type=".test.dynamic.Request",
                        output_type=".test.dynamic.Response",
                        options=MethodOptions(deprecated=True),
                    ),
                    MethodDescriptorProto(
                        name="Current",
                        input_type=".test.dynamic.Request",
                        output_type=".test.dynamic.Response",
                    ),
                ],
            ),
            ServiceDescriptorProto(
                name="Retired",
                method=[
                    MethodDescriptorProto(
                        name="Current",
                        input_type=".test.dynamic.Request",
                        output_type=".test.dynamic.Response",
                    )
                ],
                options=ServiceOptions(deprecated=True),
            ),
        ],
    )
    service = (
        FileDescriptorSet(file=[proto]).to_registry().service("test.dynamic.Dynamic")
    )
    assert service is not None
    return service


def test_deprecated_and_map_of_enum() -> None:
    """Deprecation of a method, a service, or the file deprecates the operations."""
    dynamic = _dynamic_file()
    retired = next(s for s in dynamic.file.services if s.name == "Retired")

    class DynamicService:
        @classmethod
        def desc(cls) -> DescService:
            return dynamic

        async def old(self, request: Any, _ctx: Any, /) -> Any:
            raise NotImplementedError

        async def current(self, request: Any, _ctx: Any, /) -> Any:
            raise NotImplementedError

    class RetiredService:
        @classmethod
        def desc(cls) -> DescService:
            return retired

        async def current(self, request: Any, _ctx: Any, /) -> Any:
            raise NotImplementedError

    def get_dynamic() -> DynamicService:
        return DynamicService()

    def get_retired() -> RetiredService:
        return RetiredService()

    app = FastAPI()
    app.include_router(connect_router(DynamicService, get_dynamic))
    app.include_router(connect_router(RetiredService, get_retired))
    add_connect_schemas(app)
    schema = app.openapi()
    assert_refs_resolve(schema)
    paths = schema["paths"]
    assert paths["/test.dynamic.Dynamic/Old"]["post"]["deprecated"] is True
    assert paths["/test.dynamic.Dynamic/Current"]["post"]["deprecated"] is True
    assert paths["/test.dynamic.Retired/Current"]["post"]["deprecated"] is True
    request = schema["components"]["schemas"]["test.dynamic.Request"]
    assert request["properties"]["kinds"]["additionalProperties"] == _ref(
        "test.dynamic.Kind"
    )
    assert schema["components"]["schemas"]["test.dynamic.Kind"]["enum"] == [
        "KIND_UNSPECIFIED",
        "KIND_ONE",
    ]


def test_other_definition_of_message_does_not_leak() -> None:
    """Applications document only the definitions their own routes serve.

    The other definition is the same file with an extra field on SayRequest. A
    router for it is created without being included, and included in another
    application.
    """
    app = create_app()
    add_connect_schemas(app)

    proto = eliza_pb.desc().proto
    changed = FileDescriptorProto.from_binary(proto.to_binary())
    say_request = next(m for m in changed.message_type if m.name == "SayRequest")
    say_request.field.append(
        FieldDescriptorProto(
            name="extra",
            number=99,
            label=FieldDescriptorProto.Label.OPTIONAL,
            type=FieldDescriptorProto.Type.STRING,
            json_name="extra",
        )
    )
    registry = FileDescriptorSet(
        file=[d.proto for d in eliza_pb.desc().dependencies] + [changed]
    ).to_registry()
    other = registry.service(_SERVICE)
    assert other is not None

    class OtherElizaService:
        @classmethod
        def desc(cls) -> DescService:
            return other

        async def say(self, _request: Any, _ctx: Any, /) -> Any:
            raise NotImplementedError

        async def converse(self, _request: Any, _ctx: Any, /) -> AsyncIterator[Any]:
            raise NotImplementedError
            yield

        async def introduce(self, _request: Any, _ctx: Any, /) -> AsyncIterator[Any]:
            raise NotImplementedError
            yield

    def get_other() -> OtherElizaService:
        return OtherElizaService()

    connect_router(OtherElizaService, get_other)
    properties = app.openapi()["components"]["schemas"][
        "connectrpc.eliza.v1.SayRequest"
    ]["properties"]
    assert set(properties) == {"sentence"}

    other_app = FastAPI()
    other_app.include_router(connect_router(OtherElizaService, get_other))
    add_connect_schemas(other_app)
    other_properties = other_app.openapi()["components"]["schemas"][
        "connectrpc.eliza.v1.SayRequest"
    ]["properties"]
    assert set(other_properties) == {"sentence", "extra"}


def test_async_detection_accepts_async_forms() -> None:
    """Streaming methods are rejected only when they are known to be sync."""
    desc = ElizaService.desc()

    class Unannotated:
        @classmethod
        def desc(cls) -> DescService:
            return desc

        async def say(self, _request: Any, _ctx: Any, /) -> Any:
            raise NotImplementedError

        async def converse(self, _request, _ctx, /):
            raise NotImplementedError
            yield

        def introduce(self, _request: Any, _ctx: Any, /) -> AsyncIterator[Any]:
            raise NotImplementedError

    class Generator:
        @classmethod
        def desc(cls) -> DescService:
            return desc

        async def say(self, _request: Any, _ctx: Any, /) -> Any:
            raise NotImplementedError

        async def converse(self, _request: Any, _ctx: Any, /) -> AsyncIterator[Any]:
            raise NotImplementedError
            yield

        def introduce(self, _request: Any, _ctx: Any, /) -> Any:
            yield

    connect_router(Unannotated, get_eliza)  # type: ignore[invalid-argument-type]
    with pytest.raises(TypeError, match=re.escape("Generator.introduce")):
        connect_router(Generator, get_eliza)  # type: ignore[invalid-argument-type]


@pytest.mark.asyncio
async def test_connect_error_does_not_reach_dependency() -> None:
    """A Connect error is a response, so a yield dependency sees no exception."""
    outcomes: list[str] = []

    class FailingElizaService(ElizaService):
        async def say(self, _request: SayRequest, _ctx: Any, /) -> SayResponse:
            raise ConnectError(Code.INVALID_ARGUMENT, "bad sentence")

    async def get_failing() -> AsyncIterator[FailingElizaService]:
        try:
            yield FailingElizaService()
        except Exception:
            outcomes.append("rollback")
            raise
        else:
            outcomes.append("commit")

    app = FastAPI()
    app.include_router(connect_router(ElizaService, get_failing))
    async with create_client(app) as client:
        with pytest.raises(ConnectError) as exc_info:
            await client.say(SayRequest(sentence="Hello"))
    assert exc_info.value.code == Code.INVALID_ARGUMENT
    assert outcomes == ["commit"]


def _importing_service(*, extra_field: bool) -> DescService:
    """A service whose request message is defined in an imported file.

    The importing file is the same either way. Only the imported file differs,
    by an extra field on the message.
    """
    fields = [
        FieldDescriptorProto(
            name="a",
            number=1,
            label=FieldDescriptorProto.Label.OPTIONAL,
            type=FieldDescriptorProto.Type.STRING,
            json_name="a",
        )
    ]
    if extra_field:
        fields.append(
            FieldDescriptorProto(
                name="b",
                number=2,
                label=FieldDescriptorProto.Label.OPTIONAL,
                type=FieldDescriptorProto.Type.STRING,
                json_name="b",
            )
        )
    imported = FileDescriptorProto(
        name="test/imported.proto",
        package="test.imported",
        syntax="proto3",
        message_type=[DescriptorProto(name="Inner", field=fields)],
    )
    importing = FileDescriptorProto(
        name="test/importing.proto",
        package="test.importing",
        syntax="proto3",
        dependency=["test/imported.proto"],
        message_type=[
            DescriptorProto(
                name="Request",
                field=[
                    FieldDescriptorProto(
                        name="inner",
                        number=1,
                        label=FieldDescriptorProto.Label.OPTIONAL,
                        type=FieldDescriptorProto.Type.MESSAGE,
                        type_name=".test.imported.Inner",
                        json_name="inner",
                    )
                ],
            )
        ],
        service=[
            ServiceDescriptorProto(
                name="Importing",
                method=[
                    MethodDescriptorProto(
                        name="Call",
                        input_type=".test.importing.Request",
                        output_type=".test.imported.Inner",
                    )
                ],
            )
        ],
    )
    registry = FileDescriptorSet(file=[imported, importing]).to_registry()
    service = registry.service("test.importing.Importing")
    assert service is not None
    return service


def _importing_service_class(desc: DescService) -> type[Any]:
    class ImportingService:
        @classmethod
        def desc(cls) -> DescService:
            return desc

        async def call(self, _request: Any, _ctx: Any, /) -> Any:
            raise NotImplementedError

    return ImportingService


def test_changed_imported_file_is_another_definition() -> None:
    """A different imported file is a different definition, with the same own file.

    Applications document their own definition, and one application with both
    definitions is an error.
    """
    first = _importing_service(extra_field=False)
    second = _importing_service(extra_field=True)
    assert first.file.proto == second.file.proto
    first_class = _importing_service_class(first)
    second_class = _importing_service_class(second)

    app = FastAPI()
    app.include_router(connect_router(first_class, first_class))
    add_connect_schemas(app)
    inner = app.openapi()["components"]["schemas"]["test.imported.Inner"]
    assert set(inner["properties"]) == {"a"}

    connect_router(second_class, second_class)
    assert app.openapi()["components"]["schemas"]["test.imported.Inner"] == inner

    other_app = FastAPI()
    other_app.include_router(connect_router(second_class, second_class))
    add_connect_schemas(other_app)
    other_inner = other_app.openapi()["components"]["schemas"]["test.imported.Inner"]
    assert set(other_inner["properties"]) == {"a", "b"}

    both = FastAPI()
    both.include_router(connect_router(first_class, first_class), prefix="/v1")
    both.include_router(connect_router(second_class, second_class), prefix="/v2")
    add_connect_schemas(both)
    with pytest.raises(ValueError, match="two different definitions"):
        both.openapi()


def test_two_definitions_in_one_app_raise() -> None:
    proto = FileDescriptorProto.from_binary(eliza_pb.desc().proto.to_binary())
    say_request = next(m for m in proto.message_type if m.name == "SayRequest")
    say_request.field.append(
        FieldDescriptorProto(
            name="extra",
            number=99,
            label=FieldDescriptorProto.Label.OPTIONAL,
            type=FieldDescriptorProto.Type.STRING,
            json_name="extra",
        )
    )
    registry = FileDescriptorSet(
        file=[d.proto for d in eliza_pb.desc().dependencies] + [proto]
    ).to_registry()
    other = registry.service(_SERVICE)
    assert other is not None

    class OtherElizaService:
        @classmethod
        def desc(cls) -> DescService:
            return other

        async def say(self, _request: Any, _ctx: Any, /) -> Any:
            raise NotImplementedError

        async def converse(self, _request: Any, _ctx: Any, /) -> AsyncIterator[Any]:
            raise NotImplementedError
            yield

        async def introduce(self, _request: Any, _ctx: Any, /) -> AsyncIterator[Any]:
            raise NotImplementedError
            yield

    def get_other() -> OtherElizaService:
        return OtherElizaService()

    app = FastAPI()
    app.state.events = []
    app.include_router(connect_router(ElizaService, get_eliza), prefix="/v1")
    app.include_router(connect_router(OtherElizaService, get_other), prefix="/v2")
    add_connect_schemas(app)
    with pytest.raises(ValueError, match="two different definitions"):
        app.openapi()


def test_openapi_regenerated() -> None:
    """A new schema is processed after FastAPI's cache is cleared.

    FastAPI's cache keeps the placeholders.
    """
    app = create_app()
    add_connect_schemas(app)
    first = app.openapi()
    assert_refs_resolve(first)
    app.openapi_schema = None
    second = app.openapi()
    assert second is not first
    assert second == first
    assert_refs_resolve(second)
    assert "x-connect-message" in str(app.openapi_schema)


@pytest.mark.asyncio
async def test_grpc_protocols() -> None:
    class StrictElizaService(DemoElizaService):
        async def say(self, request: SayRequest, _ctx: RequestContext) -> SayResponse:
            if not request.sentence:
                raise ConnectError(Code.INVALID_ARGUMENT, "empty sentence")
            return await super().say(request, _ctx)

    def get_strict() -> StrictElizaService:
        return StrictElizaService()

    app = FastAPI()
    app.include_router(connect_router(ElizaService, get_strict))
    for protocol in (ProtocolType.GRPC, ProtocolType.GRPC_WEB):
        async with ElizaServiceClient(
            "http://localhost",
            http_client=Client(ASGITransport(app)),
            protocol=protocol,
        ) as client:
            assert (await client.say(SayRequest(sentence="Hello"))).sentence
            assert [r async for r in client.introduce(IntroduceRequest(name="Test"))]
            with pytest.raises(ConnectError) as exc_info:
                await client.say(SayRequest(sentence=""))
            assert exc_info.value.code == Code.INVALID_ARGUMENT


def test_tags_must_be_a_sequence() -> None:
    with pytest.raises(TypeError, match="sequence of strings"):
        connect_router(ElizaService, get_eliza, tags="eliza")  # type: ignore[invalid-argument-type]


def test_unknown_placeholder_stays() -> None:
    """A placeholder for a message no route of the application serves is kept."""
    app = FastAPI()

    async def other() -> None:
        return None

    app.add_api_route(
        "/other",
        other,
        methods=["POST"],
        openapi_extra={
            "requestBody": {
                "content": {
                    "application/json": {
                        "schema": {"type": "object", "x-connect-message": "x.Unknown"}
                    }
                }
            }
        },
    )
    app.include_router(connect_router(ElizaService, get_eliza))
    add_connect_schemas(app)
    schema = app.openapi()
    assert_refs_resolve(schema)
    assert schema["paths"]["/other"]["post"]["requestBody"]["content"][
        "application/json"
    ]["schema"] == {"type": "object", "x-connect-message": "x.Unknown"}
    assert "x.Unknown" not in schema["components"]["schemas"]


def test_add_connect_schemas_before_include() -> None:
    app = FastAPI()
    app.state.events = []
    add_connect_schemas(app)
    app.include_router(connect_router(ElizaService, get_eliza))
    schema = app.openapi()
    assert_refs_resolve(schema)
    assert "connectrpc.eliza.v1.SayRequest" in schema["components"]["schemas"]

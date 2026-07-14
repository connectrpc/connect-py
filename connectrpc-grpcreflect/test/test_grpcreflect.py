from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import pytest_asyncio
from connectrpc.errors import ConnectError
from example.gen.connectrpc.eliza.v1 import eliza_pb
from protobuf import DescFile, Oneof
from protobuf.wkt import (
    DescriptorProto,
    EnumDescriptorProto,
    EnumValueDescriptorProto,
    FieldDescriptorProto,
    FileDescriptorProto,
    FileDescriptorSet,
)
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc_grpcreflect import (
    ServerReflectionAlphaASGIApplication,
    ServerReflectionAlphaService,
    ServerReflectionAlphaServiceSync,
    ServerReflectionAlphaWSGIApplication,
    ServerReflectionASGIApplication,
    ServerReflectionService,
    ServerReflectionServiceSync,
    ServerReflectionWSGIApplication,
)
from connectrpc_grpcreflect._gen.grpc.reflection.v1.reflection_connect import (
    ServerReflectionClient,
    ServerReflectionClientSync,
)
from connectrpc_grpcreflect._gen.grpc.reflection.v1.reflection_pb import (
    ExtensionRequest,
    ServerReflectionRequest,
    ServerReflectionResponse,
)
from connectrpc_grpcreflect._gen.grpc.reflection.v1alpha.reflection_connect import (
    ServerReflectionClient as ServerReflectionAlphaClient,
)
from connectrpc_grpcreflect._gen.grpc.reflection.v1alpha.reflection_connect import (
    ServerReflectionClientSync as ServerReflectionAlphaClientSync,
)
from connectrpc_grpcreflect._gen.grpc.reflection.v1alpha.reflection_pb import (
    ServerReflectionRequest as ServerReflectionAlphaRequest,
)

from .connectrpc.example import haberdasher_pb

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


ReflectionClient = ServerReflectionClient | ServerReflectionClientSync


@pytest_asyncio.fixture(params=["sync", "async"])
async def reflection_client(
    request: pytest.FixtureRequest,
) -> AsyncIterator[ReflectionClient]:
    match request.param:
        case "sync":
            app = ServerReflectionWSGIApplication(
                ServerReflectionServiceSync(haberdasher_pb.desc(), eliza_pb.desc())
            )
            with ServerReflectionClientSync(
                "http://localhost", http_client=SyncClient(WSGITransport(app=app))
            ) as client:
                yield client
        case "async":
            app = ServerReflectionASGIApplication(
                ServerReflectionService(haberdasher_pb.desc(), eliza_pb.desc())
            )
            async with ServerReflectionClient(
                "http://localhost", http_client=Client(ASGITransport(app))
            ) as client:
                yield client


async def _responses(
    client: ReflectionClient, requests: list[ServerReflectionRequest]
) -> list[ServerReflectionResponse]:
    if isinstance(client, ServerReflectionClientSync):
        return list(client.server_reflection_info(iter(requests)))

    async def request_iter() -> AsyncIterator[ServerReflectionRequest]:
        for request in requests:
            yield request

    return [
        response async for response in client.server_reflection_info(request_iter())
    ]


def _file_names(res: ServerReflectionResponse) -> list[str]:
    assert res.message_response is not None
    assert res.message_response.field == "file_descriptor_response"
    return [
        FileDescriptorProto.from_binary(data).name
        for data in res.message_response.value.file_descriptor_proto
    ]


@pytest.mark.asyncio
async def test_list_services(reflection_client: ReflectionClient) -> None:
    req = ServerReflectionRequest(
        host="example.com", message_request=Oneof("list_services", "")
    )

    [res] = await _responses(reflection_client, [req])

    assert res.valid_host == "example.com"
    assert res.original_request == req
    assert res.message_response is not None
    assert res.message_response.field == "list_services_response"
    assert [svc.name for svc in res.message_response.value.service] == [
        "connectrpc.eliza.v1.ElizaService",
        "connectrpc.example.Haberdasher",
    ]


def test_list_services_service_descriptor() -> None:
    app = ServerReflectionWSGIApplication(
        ServerReflectionServiceSync(haberdasher_pb.desc().services[0])
    )

    with ServerReflectionClientSync(
        "http://localhost", http_client=SyncClient(WSGITransport(app=app))
    ) as client:
        [res] = client.server_reflection_info(
            iter([ServerReflectionRequest(message_request=Oneof("list_services", ""))])
        )

    assert res.message_response is not None
    assert res.message_response.field == "list_services_response"
    assert [svc.name for svc in res.message_response.value.service] == [
        "connectrpc.example.Haberdasher"
    ]


@pytest.mark.asyncio
async def test_file_by_filename(reflection_client: ReflectionClient) -> None:
    [res] = await _responses(
        reflection_client,
        [
            ServerReflectionRequest(
                message_request=Oneof(
                    "file_by_filename", "connectrpc/example/haberdasher.proto"
                )
            )
        ],
    )

    assert _file_names(res) == [
        "connectrpc/example/haberdasher.proto",
        "google/protobuf/empty.proto",
    ]


@pytest.mark.asyncio
async def test_file_dependencies(reflection_client: ReflectionClient) -> None:
    file_res, symbol_res = await _responses(
        reflection_client,
        [
            ServerReflectionRequest(
                message_request=Oneof("file_by_filename", "google/protobuf/empty.proto")
            ),
            ServerReflectionRequest(
                message_request=Oneof("file_containing_symbol", "google.protobuf.Empty")
            ),
        ],
    )

    assert _file_names(file_res) == ["google/protobuf/empty.proto"]
    assert _file_names(symbol_res) == ["google/protobuf/empty.proto"]


@pytest.mark.asyncio
async def test_file_containing_symbol(reflection_client: ReflectionClient) -> None:
    [res] = await _responses(
        reflection_client,
        [
            ServerReflectionRequest(
                message_request=Oneof(
                    "file_containing_symbol", "connectrpc.example.Hat.Part"
                )
            )
        ],
    )

    assert _file_names(res) == [
        "connectrpc/example/haberdasher.proto",
        "google/protobuf/empty.proto",
    ]


@pytest.mark.asyncio
async def test_file_containing_method(reflection_client: ReflectionClient) -> None:
    [res] = await _responses(
        reflection_client,
        [
            ServerReflectionRequest(
                message_request=Oneof(
                    "file_containing_symbol", "connectrpc.eliza.v1.ElizaService.Say"
                )
            )
        ],
    )

    assert _file_names(res) == ["connectrpc/eliza/v1/eliza.proto"]


@pytest.mark.asyncio
async def test_dependencies_sent_once(reflection_client: ReflectionClient) -> None:
    first, second = await _responses(
        reflection_client,
        [
            ServerReflectionRequest(
                message_request=Oneof(
                    "file_by_filename", "connectrpc/example/haberdasher.proto"
                )
            ),
            ServerReflectionRequest(
                message_request=Oneof(
                    "file_by_filename", "connectrpc/example/haberdasher.proto"
                )
            ),
        ],
    )

    assert _file_names(first) == [
        "connectrpc/example/haberdasher.proto",
        "google/protobuf/empty.proto",
    ]
    assert _file_names(second) == ["connectrpc/example/haberdasher.proto"]


@pytest.mark.asyncio
async def test_not_found(reflection_client: ReflectionClient) -> None:
    responses = await _responses(
        reflection_client,
        [
            ServerReflectionRequest(
                message_request=Oneof("file_by_filename", "missing.proto")
            ),
            ServerReflectionRequest(
                message_request=Oneof("file_containing_symbol", "missing.Message")
            ),
            ServerReflectionRequest(
                message_request=Oneof(
                    "all_extension_numbers_of_type", "missing.Message"
                )
            ),
        ],
    )

    assert len(responses) == 3
    for res in responses:
        assert res.message_response is not None
        assert res.message_response.field == "error_response"
        assert res.message_response.value.error_code == 5
        assert res.message_response.value.error_message


@pytest.mark.asyncio
async def test_invalid_request(reflection_client: ReflectionClient) -> None:
    with pytest.raises(ConnectError, match="invalid request"):
        await _responses(reflection_client, [ServerReflectionRequest()])


# Define an extension inline to avoid needing a proto for it.
def _extension_file() -> DescFile:
    base = FileDescriptorProto(
        name="reflect/base.proto",
        package="reflect.test",
        message_type=[
            DescriptorProto(
                name="Extendable",
                extension_range=[DescriptorProto.ExtensionRange(start=100, end=200)],
            )
        ],
        syntax="proto2",
    )
    ext = FileDescriptorProto(
        name="reflect/ext.proto",
        package="reflect.test",
        dependency=["reflect/base.proto"],
        extension=[
            FieldDescriptorProto(
                name="nickname",
                number=100,
                label=FieldDescriptorProto.Label.OPTIONAL,
                type=FieldDescriptorProto.Type.STRING,
                extendee=".reflect.test.Extendable",
            ),
            FieldDescriptorProto(
                name="score",
                number=101,
                label=FieldDescriptorProto.Label.OPTIONAL,
                type=FieldDescriptorProto.Type.INT32,
                extendee=".reflect.test.Extendable",
            ),
        ],
        syntax="proto2",
    )
    desc = FileDescriptorSet(file=[base, ext]).to_registry().file("reflect/ext.proto")
    assert desc is not None
    return desc


def _enum_file() -> DescFile:
    proto = FileDescriptorProto(
        name="reflect/enum.proto",
        package="reflect.test",
        enum_type=[
            EnumDescriptorProto(
                name="Mood",
                value=[
                    EnumValueDescriptorProto(name="MOOD_UNKNOWN", number=0),
                    EnumValueDescriptorProto(name="MOOD_HAPPY", number=1),
                ],
            )
        ],
        message_type=[
            DescriptorProto(
                name="Container",
                enum_type=[
                    EnumDescriptorProto(
                        name="State",
                        value=[
                            EnumValueDescriptorProto(name="STATE_UNKNOWN", number=0),
                            EnumValueDescriptorProto(name="STATE_ON", number=1),
                        ],
                    )
                ],
            )
        ],
        syntax="proto3",
    )
    desc = FileDescriptorSet(file=[proto]).to_registry().file("reflect/enum.proto")
    assert desc is not None
    return desc


def test_file_containing_enum_values() -> None:
    app = ServerReflectionWSGIApplication(ServerReflectionServiceSync(_enum_file()))
    reqs = [
        ServerReflectionRequest(
            message_request=Oneof("file_containing_symbol", "reflect.test.MOOD_HAPPY")
        ),
        ServerReflectionRequest(
            message_request=Oneof(
                "file_containing_symbol", "reflect.test.Container.STATE_ON"
            )
        ),
    ]

    with ServerReflectionClientSync(
        "http://localhost", http_client=SyncClient(WSGITransport(app=app))
    ) as client:
        top_level_res, nested_res = client.server_reflection_info(iter(reqs))

    assert _file_names(top_level_res) == ["reflect/enum.proto"]
    assert _file_names(nested_res) == ["reflect/enum.proto"]


def test_extensions() -> None:
    app = ServerReflectionWSGIApplication(
        ServerReflectionServiceSync(_extension_file())
    )
    reqs = [
        ServerReflectionRequest(
            message_request=Oneof(
                "file_containing_extension",
                ExtensionRequest(
                    containing_type="reflect.test.Extendable", extension_number=100
                ),
            )
        ),
        ServerReflectionRequest(
            message_request=Oneof(
                "all_extension_numbers_of_type", "reflect.test.Extendable"
            )
        ),
    ]

    with ServerReflectionClientSync(
        "http://localhost", http_client=SyncClient(WSGITransport(app=app))
    ) as client:
        file_res, numbers_res = client.server_reflection_info(iter(reqs))

    assert _file_names(file_res) == ["reflect/ext.proto", "reflect/base.proto"]
    assert numbers_res.message_response is not None
    assert numbers_res.message_response.field == "all_extension_numbers_response"
    assert (
        numbers_res.message_response.value.base_type_name == "reflect.test.Extendable"
    )
    assert sorted(numbers_res.message_response.value.extension_number) == [100, 101]


@pytest.mark.asyncio
async def test_v1alpha() -> None:
    app = ServerReflectionAlphaASGIApplication(
        ServerReflectionAlphaService(eliza_pb.desc())
    )
    req = ServerReflectionAlphaRequest(
        host="alpha.example.com", message_request=Oneof("list_services", "")
    )

    async with ServerReflectionAlphaClient(
        "http://localhost", http_client=Client(ASGITransport(app=app))
    ) as client:

        async def request_iter() -> AsyncIterator[ServerReflectionAlphaRequest]:
            yield req

        res = await anext(client.server_reflection_info(request_iter()))

    assert res.valid_host == "alpha.example.com"
    assert res.original_request == req
    assert res.message_response is not None
    assert res.message_response.field == "list_services_response"
    assert [svc.name for svc in res.message_response.value.service] == [
        "connectrpc.eliza.v1.ElizaService"
    ]


def test_v1alpha_sync() -> None:
    app = ServerReflectionAlphaWSGIApplication(
        ServerReflectionAlphaServiceSync(eliza_pb.desc())
    )
    req = ServerReflectionAlphaRequest(
        host="alpha.example.com", message_request=Oneof("list_services", "")
    )

    with ServerReflectionAlphaClientSync(
        "http://localhost", http_client=SyncClient(WSGITransport(app=app))
    ) as client:
        res = next(client.server_reflection_info(iter([req])))

    assert res.valid_host == "alpha.example.com"
    assert res.original_request == req
    assert res.message_response is not None
    assert res.message_response.field == "list_services_response"
    assert [svc.name for svc in res.message_response.value.service] == [
        "connectrpc.eliza.v1.ElizaService"
    ]

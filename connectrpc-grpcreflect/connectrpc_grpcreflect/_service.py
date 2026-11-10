from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, Literal

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from protobuf import (
    DescEnum,
    DescExtension,
    DescFile,
    DescMessage,
    DescService,
    Oneof,
    Registry,
)

from ._gen.grpc.reflection.v1.reflection_connect import (
    ServerReflection,
    ServerReflectionSync,
)
from ._gen.grpc.reflection.v1.reflection_pb import (
    ErrorResponse,
    ExtensionNumberResponse,
    FileDescriptorResponse,
    ListServiceResponse,
    ServerReflectionRequest,
    ServerReflectionResponse,
    ServiceResponse,
)
from ._gen.grpc.reflection.v1alpha.reflection_connect import (
    ServerReflection as ServerReflectionAlpha,
    ServerReflectionSync as ServerReflectionAlphaSync,
)
from ._gen.grpc.reflection.v1alpha.reflection_pb import (
    ServerReflectionRequest as ServerReflectionAlphaRequest,
    ServerReflectionResponse as ServerReflectionAlphaResponse,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from connectrpc.request import RequestContext

# gRPC int error code, not used elsewhere in connect so redefined here
_CODE_NOT_FOUND = 5


class ServerReflectionService(ServerReflection):
    """Asynchronous service implementation for gRPC reflection."""

    def __init__(self, *descs: DescFile | DescService) -> None:
        """Create a service for gRPC reflection.

        The services in the passed in descriptors will be made available for reflection,
        along with the v1 and v1alpha reflection services. For
        [DescFile][protobuf.DescFile], this means all the services declared in that
        file. Note that file dependencies are also automatically added and available
        for resolution by name, but the service list will only reflect the reflection
        services and the descriptors passed in.

        Serve it with [ServerReflectionASGIApplication][connectrpc_grpcreflect.ServerReflectionASGIApplication].

        Args:
            *descs: The descriptors to make available for reflection.

        """
        self._index = _Index(descs)

    async def server_reflection_info(
        self,
        request: AsyncIterator[ServerReflectionRequest],
        _ctx: RequestContext[ServerReflectionRequest, ServerReflectionResponse],
    ) -> AsyncIterator[ServerReflectionResponse]:
        seen: set[str] = set()
        async for req in request:
            yield _handle_request(req, seen, self._index)


class ServerReflectionServiceSync(ServerReflectionSync):
    """Synchronous service implementation for gRPC reflection."""

    def __init__(self, *descs: DescFile | DescService) -> None:
        """Create a service for gRPC reflection.

        The services in the passed in descriptors will be made available for reflection,
        along with the v1 and v1alpha reflection services. For
        [DescFile][protobuf.DescFile], this means all the services declared in that
        file. Note that file dependencies are also automatically added and available
        for resolution by name, but the service list will only reflect the reflection
        services and the descriptors passed in.

        Serve it with [ServerReflectionWSGIApplication][connectrpc_grpcreflect.ServerReflectionWSGIApplication].

        Args:
            *descs: The descriptors to make available for reflection.

        """
        self._index = _Index(descs)

    def server_reflection_info(
        self,
        request: Iterator[ServerReflectionRequest],
        _ctx: RequestContext[ServerReflectionRequest, ServerReflectionResponse],
    ) -> Iterator[ServerReflectionResponse]:
        seen: set[str] = set()
        for req in request:
            yield _handle_request(req, seen, self._index)


# v1 and v1alpha are binary compatible, to keep the implementation simple but still
# fully typed, we use a simple binary shim.


class ServerReflectionAlphaService(ServerReflectionAlpha):
    """Asynchronous service implementation for gRPC reflection (v1alpha)."""

    def __init__(self, *descs: DescFile | DescService) -> None:
        """Create a service for gRPC reflection.

        The services in the passed in descriptors will be made available for reflection,
        along with the v1 and v1alpha reflection services. For
        [DescFile][protobuf.DescFile], this means all the services declared in that
        file. Note that file dependencies are also automatically added and available
        for resolution by name, but the service list will only reflect the reflection
        services and the descriptors passed in.

        Serve it with [ServerReflectionAlphaASGIApplication][connectrpc_grpcreflect.ServerReflectionAlphaASGIApplication].

        Args:
            *descs: The descriptors to make available for reflection.

        """
        self._index = _Index(descs)

    async def server_reflection_info(
        self,
        request: AsyncIterator[ServerReflectionAlphaRequest],
        _ctx: RequestContext[
            ServerReflectionAlphaRequest, ServerReflectionAlphaResponse
        ],
    ) -> AsyncIterator[ServerReflectionAlphaResponse]:
        seen: set[str] = set()
        async for req in request:
            yield ServerReflectionAlphaResponse.from_binary(
                _handle_request(
                    ServerReflectionRequest.from_binary(req.to_binary()),
                    seen,
                    self._index,
                ).to_binary()
            )


class ServerReflectionAlphaServiceSync(ServerReflectionAlphaSync):
    """Synchronous service implementation for gRPC reflection (v1alpha)."""

    def __init__(self, *descs: DescFile | DescService) -> None:
        """Create a service for gRPC reflection.

        The services in the passed in descriptors will be made available for reflection,
        along with the v1 and v1alpha reflection services. For
        [DescFile][protobuf.DescFile], this means all the services declared in that
        file. Note that file dependencies are also automatically added and available
        for resolution by name, but the service list will only reflect the reflection
        services and the descriptors passed in.

        Serve it with [ServerReflectionAlphaWSGIApplication][connectrpc_grpcreflect.ServerReflectionAlphaWSGIApplication].

        Args:
            *descs: The descriptors to make available for reflection.

        """
        self._index = _Index(descs)

    def server_reflection_info(
        self,
        request: Iterator[ServerReflectionAlphaRequest],
        _ctx: RequestContext[
            ServerReflectionAlphaRequest, ServerReflectionAlphaResponse
        ],
    ) -> Iterator[ServerReflectionAlphaResponse]:
        seen: set[str] = set()
        for req in request:
            yield ServerReflectionAlphaResponse.from_binary(
                _handle_request(
                    ServerReflectionRequest.from_binary(req.to_binary()),
                    seen,
                    self._index,
                ).to_binary()
            )


class _Index:
    """The files available for reflection, and lookups into them."""

    def __init__(self, descs: tuple[DescFile | DescService, ...]) -> None:
        files: list[DescFile] = []
        seen: set[str] = set()
        service_names: set[str] = set()
        for desc in (*descs, ServerReflection.desc(), ServerReflectionAlpha.desc()):
            if isinstance(desc, DescFile):
                files.extend(_file_descriptor_with_dependencies(desc, seen))
                service_names.update(d.type_name for d in desc.services)
            else:
                files.extend(_file_descriptor_with_dependencies(desc.file, seen))
                service_names.add(desc.type_name)
        self.registry = Registry(*files)
        self.service_names = sorted(service_names)
        # Every symbol that C++ protobuf's
        # DescriptorPool::FindFileContainingSymbol resolves, mapped to the file
        # declaring it.
        self.symbols: dict[str, DescFile] = {}
        # Fully qualified names of messages, mapped to their extension numbers.
        self.extension_numbers: dict[str, list[int]] = {}
        for desc in self.registry:
            if isinstance(desc, DescFile):
                continue
            self.symbols[desc.type_name] = desc.file
            match desc:
                case DescMessage():
                    # The descriptor's oneof_decl includes synthetic oneofs for
                    # proto3 optional fields, which desc.oneofs omits.
                    for name in (
                        *(f.name for f in desc.fields),
                        *(o.name for o in desc.proto.oneof_decl),
                    ):
                        self.symbols[f"{desc.type_name}.{name}"] = desc.file
                case DescEnum():
                    # Enum values are siblings of their enum, not children.
                    scope = desc.type_name.removesuffix(desc.name)
                    for value in desc.values:
                        self.symbols[scope + value.name] = desc.file
                case DescService():
                    for method in desc.methods:
                        self.symbols[f"{desc.type_name}.{method.name}"] = desc.file
                case DescExtension():
                    self.extension_numbers.setdefault(
                        desc.extendee.type_name, []
                    ).append(desc.number)


def _handle_request(
    req: ServerReflectionRequest, seen: set[str], index: _Index
) -> ServerReflectionResponse:
    res = ServerReflectionResponse(valid_host=req.host, original_request=req)
    match req.message_request:
        case Oneof("file_by_filename", filename):
            res.message_response = _create_file_response(
                index.registry.file(filename), seen, "file not found"
            )
        case Oneof("file_containing_symbol", symbol):
            res.message_response = _create_file_response(
                index.symbols.get(symbol), seen, "symbol not found"
            )
        case Oneof("file_containing_extension", extension_request):
            ext = index.registry.extension_for(
                extension_request.containing_type, extension_request.extension_number
            )
            res.message_response = _create_file_response(
                ext.file if ext else None, seen, "extension not found"
            )
        case Oneof("all_extension_numbers_of_type", containing_type):
            if not index.registry.message(containing_type):
                res.message_response = _not_found("type not found")
            else:
                res.message_response = Oneof(
                    "all_extension_numbers_response",
                    ExtensionNumberResponse(
                        base_type_name=containing_type,
                        extension_number=index.extension_numbers.get(
                            containing_type, []
                        ),
                    ),
                )
        case Oneof("list_services", _):
            services = [ServiceResponse(name=name) for name in index.service_names]
            res.message_response = Oneof(
                "list_services_response", ListServiceResponse(service=services)
            )
        case _:
            raise ConnectError(Code.INVALID_ARGUMENT, "invalid request")
    return res


def _not_found(message: str) -> Oneof[Literal["error_response"], ErrorResponse]:
    return Oneof(
        "error_response",
        ErrorResponse(error_code=_CODE_NOT_FOUND, error_message=message),
    )


def _create_file_response(
    desc: DescFile | None, seen: set[str], not_found_message: str
) -> (
    Oneof[Literal["file_descriptor_response"], FileDescriptorResponse]
    | Oneof[Literal["error_response"], ErrorResponse]
):
    if not desc:
        return _not_found(not_found_message)
    fds = [
        fd.proto.to_binary() for fd in _file_descriptor_with_dependencies(desc, seen)
    ]
    return Oneof(
        "file_descriptor_response", FileDescriptorResponse(file_descriptor_proto=fds)
    )


def _file_descriptor_with_dependencies(
    root: DescFile, seen: set[str]
) -> Iterator[DescFile]:
    seen.add(root.name)
    yield root
    # Avoid recursion since not difficult
    queue: deque[DescFile] = deque()
    queue.extend(root.dependencies)
    while queue:
        file = queue.popleft()
        if file.name in seen:
            continue
        seen.add(file.name)
        yield file
        queue.extend(file.dependencies)

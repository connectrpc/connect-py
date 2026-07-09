from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, Literal

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from protobuf import DescEnum, DescExtension, DescFile, DescService, Oneof, Registry

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
)
from ._gen.grpc.reflection.v1alpha.reflection_connect import (
    ServerReflectionSync as ServerReflectionAlphaSync,
)
from ._gen.grpc.reflection.v1alpha.reflection_pb import (
    ServerReflectionRequest as ServerReflectionAlphaRequest,
)
from ._gen.grpc.reflection.v1alpha.reflection_pb import (
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
        """Creates a service for gRPC reflection.

        The services in the passed in descriptors will be made available for reflection.
        For [DescFile][protobuf.DescFile], this means all the services declared in that file. Note that file
        dependencies are also automatically added and available for resolution by name,
        but the service list will only reflect the descriptors passed in.

        Args:
            *descs: The descriptors to make available for reflection.

        Returns:
            A new instance of [ServerReflectionService][connectrpc.grpcreflect.ServerReflectionService],
                for use with [ServerReflectionASGIApplication][connectrpc.grpcreflect.ServerReflectionASGIApplication].
        """
        registry, service_names = _resolve_registry(descs)
        self._registry = registry
        self._service_names = service_names

    async def server_reflection_info(
        self,
        request: AsyncIterator[ServerReflectionRequest],
        ctx: RequestContext[ServerReflectionRequest, ServerReflectionResponse],
    ) -> AsyncIterator[ServerReflectionResponse]:
        seen: set[str] = set()
        async for req in request:
            yield _handle_request(req, seen, self._registry, self._service_names)


class ServerReflectionServiceSync(ServerReflectionSync):
    """Synchronous service implementation for gRPC reflection."""

    def __init__(self, *descs: DescFile | DescService) -> None:
        """Creates a service for gRPC reflection.

        The services in the passed in descriptors will be made available for reflection.
        For [DescFile][protobuf.DescFile], this means all the services declared in that file. Note that file
        dependencies are also automatically added and available for resolution by name,
        but the service list will only reflect the descriptors passed in.

        Args:
            *descs: The descriptors to make available for reflection.

        Returns:
            A new instance of [ServerReflectionServiceSync][connectrpc.grpcreflect.ServerReflectionServiceSync],
                for use with [ServerReflectionWSGIApplication][connectrpc.grpcreflect.ServerReflectionWSGIApplication].
        """
        registry, service_names = _resolve_registry(descs)
        self._registry = registry
        self._service_names = service_names

    def server_reflection_info(
        self,
        request: Iterator[ServerReflectionRequest],
        ctx: RequestContext[ServerReflectionRequest, ServerReflectionResponse],
    ) -> Iterator[ServerReflectionResponse]:
        seen: set[str] = set()
        for req in request:
            yield _handle_request(req, seen, self._registry, self._service_names)


# v1 and v1alpha are binary compatible, to keep the implementation simple but still
# fully typed, we use a simple binary shim.


class ServerReflectionAlphaService(ServerReflectionAlpha):
    """Asynchronous service implementation for gRPC reflection (v1alpha)."""

    def __init__(self, *descs: DescFile | DescService) -> None:
        """Creates a service for gRPC reflection.

        The services in the passed in descriptors will be made available for reflection.
        For [DescFile][protobuf.DescFile], this means all the services declared in that file. Note that file
        dependencies are also automatically added and available for resolution by name,
        but the service list will only reflect the descriptors passed in.

        Args:
            *descs: The descriptors to make available for reflection.

        Returns:
            A new instance of [ServerReflectionAlphaService][connectrpc.grpcreflect.ServerReflectionAlphaService],
                for use with [ServerReflectionAlphaASGIApplication][connectrpc.grpcreflect.ServerReflectionAlphaASGIApplication].
        """
        registry, service_names = _resolve_registry(descs)
        self._registry = registry
        self._service_names = service_names

    async def server_reflection_info(
        self,
        request: AsyncIterator[ServerReflectionAlphaRequest],
        ctx: RequestContext[
            ServerReflectionAlphaRequest, ServerReflectionAlphaResponse
        ],
    ) -> AsyncIterator[ServerReflectionAlphaResponse]:
        seen: set[str] = set()
        async for req in request:
            yield ServerReflectionAlphaResponse.from_binary(
                _handle_request(
                    ServerReflectionRequest.from_binary(req.to_binary()),
                    seen,
                    self._registry,
                    self._service_names,
                ).to_binary()
            )


class ServerReflectionAlphaServiceSync(ServerReflectionAlphaSync):
    """Synchronous service implementation for gRPC reflection (v1alpha)."""

    def __init__(self, *descs: DescFile | DescService) -> None:
        """Creates a service for gRPC reflection.

        The services in the passed in descriptors will be made available for reflection.
        For [DescFile][protobuf.DescFile], this means all the services declared in that file. Note that file
        dependencies are also automatically added and available for resolution by name,
        but the service list will only reflect the descriptors passed in.

        Args:
            *descs: The descriptors to make available for reflection.

        Returns:
            A new instance of [ServerReflectionAlphaServiceSync][connectrpc.grpcreflect.ServerReflectionAlphaServiceSync],
                for use with [ServerReflectionAlphaWSGIApplication][connectrpc.grpcreflect.ServerReflectionAlphaWSGIApplication].
        """
        registry, service_names = _resolve_registry(descs)
        self._registry = registry
        self._service_names = service_names

    def server_reflection_info(
        self,
        request: Iterator[ServerReflectionAlphaRequest],
        ctx: RequestContext[
            ServerReflectionAlphaRequest, ServerReflectionAlphaResponse
        ],
    ) -> Iterator[ServerReflectionAlphaResponse]:
        seen: set[str] = set()
        for req in request:
            yield ServerReflectionAlphaResponse.from_binary(
                _handle_request(
                    ServerReflectionRequest.from_binary(req.to_binary()),
                    seen,
                    self._registry,
                    self._service_names,
                ).to_binary()
            )


def _resolve_registry(
    descs: tuple[DescFile | DescService, ...],
) -> tuple[Registry, list[str]]:
    fds: list[DescFile] = []
    seen: set[str] = set()
    service_names: set[str] = set()
    for desc in descs:
        if isinstance(desc, DescFile):
            fds.extend(_file_descriptor_with_dependencies(desc, seen))
            service_names.update(d.type_name for d in desc.services)
        else:
            fds.extend(_file_descriptor_with_dependencies(desc.file, seen))
            service_names.add(desc.type_name)
    return Registry(*fds), sorted(service_names)


def _handle_request(
    req: ServerReflectionRequest,
    seen: set[str],
    registry: Registry,
    service_names: list[str],
) -> ServerReflectionResponse:
    res = ServerReflectionResponse(valid_host=req.host, original_request=req)
    match req.message_request:
        case Oneof("file_by_filename", filename):
            desc = registry.file(filename)
            res.message_response = _create_file_response(desc, seen)
        case Oneof("file_containing_symbol", symbol):
            desc = _find_file_for_symbol(registry, symbol)
            res.message_response = _create_file_response(desc, seen)
        case Oneof("file_containing_extension", extension_request):
            desc: DescFile | None = None
            ext = registry.extension_for(
                extension_request.containing_type, extension_request.extension_number
            )
            if ext:
                desc = ext.file
            res.message_response = _create_file_response(desc, seen)
        case Oneof("all_extension_numbers_of_type", containing_type):
            # We should return not found if the message itself isn't registered
            if not registry.message(containing_type):
                res.message_response = _create_file_response(None, seen)
            else:
                nums: list[int] = [
                    d.number
                    for d in registry
                    if isinstance(d, DescExtension)
                    and d.extendee.type_name == containing_type
                ]
                res.message_response = Oneof(
                    "all_extension_numbers_response",
                    ExtensionNumberResponse(
                        base_type_name=containing_type, extension_number=nums
                    ),
                )
        case Oneof("list_services", _):
            services = [ServiceResponse(name=name) for name in service_names]
            res.message_response = Oneof(
                "list_services_response", ListServiceResponse(service=services)
            )
        case _:
            raise ConnectError(Code.INVALID_ARGUMENT, "invalid request")
    return res


def _create_file_response(
    desc: DescFile | None, seen: set[str]
) -> (
    Oneof[Literal["file_descriptor_response"], FileDescriptorResponse]
    | Oneof[Literal["error_response"], ErrorResponse]
):
    if not desc:
        return Oneof(
            "error_response",
            ErrorResponse(error_code=_CODE_NOT_FOUND, error_message="symbol not found"),
        )
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


# Finds a fully-qualified symbol name, (e.g. <package>.<service>[.<method>] or <package>.<type>).
def _find_file_for_symbol(registry: Registry, symbol: str) -> DescFile | None:
    desc = (
        registry.message(symbol)
        or registry.enum(symbol)
        or registry.service(symbol)
        or registry.extension(symbol)
    )
    if desc:
        return desc.file
    # May be a fully qualified method or enum value, split off the parent and find it.
    parent, _, member = symbol.rpartition(".")
    if not member:
        return None
    if svc := registry.service(parent):
        if any(m.name == member for m in svc.methods):
            return svc.file
        return None
    # An enum value of a message-nested enum also has a message as its parent
    # scope, so always finish with the enum value scan.
    for d in registry:
        if (
            isinstance(d, DescEnum)
            and d.type_name.rpartition(".")[0] == parent
            and any(v.name == member for v in d.values)
        ):
            return d.file
    return None

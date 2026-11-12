from __future__ import annotations

from typing import TYPE_CHECKING

from ._gen.grpc.reflection.v1.reflection_connect import (
    ServerReflectionASGIApplication,
    ServerReflectionWSGIApplication,
)
from ._gen.grpc.reflection.v1alpha.reflection_connect import (
    ServerReflectionASGIApplication as ServerReflectionAlphaASGIApplication,
    ServerReflectionWSGIApplication as ServerReflectionAlphaWSGIApplication,
)
from ._service import (
    ServerReflectionAlphaService,
    ServerReflectionAlphaServiceSync,
    ServerReflectionService,
    ServerReflectionServiceSync,
)

if TYPE_CHECKING:
    from protobuf import DescFile, DescService


def reflection_apps(
    *descs: DescFile | DescService,
) -> tuple[ServerReflectionASGIApplication, ServerReflectionAlphaASGIApplication]:
    """Create ASGI applications for the v1 and v1alpha gRPC reflection services.

    Mount both applications, each at its `path`: even if only one is mounted,
    both services are returned in `ListServices`, and a client that calls the
    unmounted service gets a 404.

    Args:
        *descs: The descriptors to make available for reflection, as accepted by
            [ServerReflectionService][connectrpc_grpcreflect.ServerReflectionService].

    """
    return (
        ServerReflectionASGIApplication(ServerReflectionService(*descs)),
        ServerReflectionAlphaASGIApplication(ServerReflectionAlphaService(*descs)),
    )


def reflection_apps_sync(
    *descs: DescFile | DescService,
) -> tuple[ServerReflectionWSGIApplication, ServerReflectionAlphaWSGIApplication]:
    """Create WSGI applications for the v1 and v1alpha gRPC reflection services.

    Mount both applications, each at its `path`: even if only one is mounted,
    both services are returned in `ListServices`, and a client that calls the
    unmounted service gets a 404.

    Args:
        *descs: The descriptors to make available for reflection, as accepted by
            [ServerReflectionServiceSync][connectrpc_grpcreflect.ServerReflectionServiceSync].

    """
    return (
        ServerReflectionWSGIApplication(ServerReflectionServiceSync(*descs)),
        ServerReflectionAlphaWSGIApplication(ServerReflectionAlphaServiceSync(*descs)),
    )

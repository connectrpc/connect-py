"""Service implementations for the gRPC reflection service."""

from __future__ import annotations

__all__ = [
    "ServerReflectionASGIApplication",
    "ServerReflectionAlphaASGIApplication",
    "ServerReflectionAlphaService",
    "ServerReflectionAlphaServiceSync",
    "ServerReflectionAlphaWSGIApplication",
    "ServerReflectionService",
    "ServerReflectionServiceSync",
    "ServerReflectionWSGIApplication",
]

from connectrpc._gen.grpc.reflection.v1.reflection_connect import (
    ServerReflectionASGIApplication,
    ServerReflectionWSGIApplication,
)
from connectrpc._gen.grpc.reflection.v1alpha.reflection_connect import (
    ServerReflectionASGIApplication as ServerReflectionAlphaASGIApplication,
)
from connectrpc._gen.grpc.reflection.v1alpha.reflection_connect import (
    ServerReflectionWSGIApplication as ServerReflectionAlphaWSGIApplication,
)

from ._service import (
    ServerReflectionAlphaService,
    ServerReflectionAlphaServiceSync,
    ServerReflectionService,
    ServerReflectionServiceSync,
)

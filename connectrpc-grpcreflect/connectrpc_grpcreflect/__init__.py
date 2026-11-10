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
    "reflection_apps",
    "reflection_apps_sync",
]

from ._apps import reflection_apps, reflection_apps_sync
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

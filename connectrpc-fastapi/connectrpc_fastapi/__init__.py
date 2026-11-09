"""FastAPI integration for Connect."""

from __future__ import annotations

__all__ = ["ServiceType", "add_connect_schemas", "connect_router"]

from ._router import ServiceType, connect_router
from ._schema import add_connect_schemas

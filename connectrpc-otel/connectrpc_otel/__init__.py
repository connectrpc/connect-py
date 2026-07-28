"""OpenTelemetry instrumentation for Connect."""

from __future__ import annotations

__all__ = ["ConnectInstrumentor", "OpenTelemetryInterceptor"]

from ._instrumentor import ConnectInstrumentor
from ._interceptor import OpenTelemetryInterceptor

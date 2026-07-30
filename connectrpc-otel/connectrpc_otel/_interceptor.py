from __future__ import annotations

import time
from contextlib import AbstractContextManager, contextmanager
from typing import TYPE_CHECKING, TypeAlias, TypeVar, cast

from connectrpc.errors import ConnectError
from opentelemetry.metrics import MeterProvider, get_meter_provider
from opentelemetry.propagate import get_global_textmap
from opentelemetry.propagators.textmap import Setter, TextMapPropagator, default_setter
from opentelemetry.trace import (
    Span,
    SpanKind,
    TracerProvider,
    get_current_span,
    get_tracer_provider,
)
from opentelemetry.util.types import AttributeValue

from ._semconv import (
    CLIENT_ADDRESS,
    CLIENT_PORT,
    ERROR_TYPE,
    RPC_CLIENT_CALL_DURATION,
    RPC_METHOD,
    RPC_RESPONSE_STATUS_CODE,
    RPC_SERVER_CALL_DURATION,
    RPC_SYSTEM_NAME,
    SERVER_ADDRESS,
    SERVER_PORT,
    RpcSystemNameValues,
)
from ._version import __version__

if TYPE_CHECKING:
    from collections.abc import Iterator, MutableMapping

    from connectrpc.request import RequestContext

REQ = TypeVar("REQ")
RES = TypeVar("RES")

Token: TypeAlias = tuple[AbstractContextManager, Span, float, dict[str, AttributeValue]]

# Workaround bad typing
_DEFAULT_TEXTMAP_SETTER = cast("Setter[MutableMapping[str, str]]", default_setter)


class OpenTelemetryInterceptor:
    """Interceptor to generate telemetry for RPC server and client requests."""

    def __init__(
        self,
        *,
        propagator: TextMapPropagator | None = None,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        client: bool = False,
    ) -> None:
        """Create a new OpenTelemetry interceptor.

        Args:
            propagator: The OpenTelemetry TextMapPropagator to use. If not
                provided, the global default will be used.
            tracer_provider: The OpenTelemetry TracerProvider to use. If not
                provided, the global default will be used.
            meter_provider: The OpenTelemetry MeterProvider to use. If not
                provided, the global default will be used.
            client: Whether this interceptor is for a client or server.

        """
        self._client = client
        tracer_provider = tracer_provider or get_tracer_provider()
        self._tracer = tracer_provider.get_tracer("connectrpc-otel", __version__)
        self._propagator = propagator or get_global_textmap()

        meter_provider = meter_provider or get_meter_provider()
        meter = meter_provider.get_meter("connectrpc-otel", __version__)

        self._call_duration = meter.create_histogram(
            name=(RPC_CLIENT_CALL_DURATION if client else RPC_SERVER_CALL_DURATION),
            description=f"Measures the duration of an {'outgoing' if client else 'incoming'} Remote Procedure Call (RPC)",
            unit="s",
            explicit_bucket_boundaries_advisory=[
                0.005,
                0.01,
                0.025,
                0.05,
                0.075,
                0.1,
                0.25,
                0.5,
                0.75,
                1,
                2.5,
                5,
                7.5,
                10,
            ],
        )

    async def on_start(self, ctx: RequestContext) -> Token:
        return self.on_start_sync(ctx)

    def on_start_sync(self, ctx: RequestContext) -> Token:
        start_time = time.perf_counter()

        rpc_method = f"{ctx.method.service_name}/{ctx.method.name}"
        shared_attrs: dict[str, AttributeValue] = {
            RPC_SYSTEM_NAME: RpcSystemNameValues.CONNECTRPC.value,
            RPC_METHOD: rpc_method,
        }

        if sa := ctx.server_address:
            addr, port = sa.rsplit(":", 1)
            shared_attrs[SERVER_ADDRESS] = addr
            shared_attrs[SERVER_PORT] = int(port)

        cm = self._start_span(ctx, rpc_method, shared_attrs)
        span = cm.__enter__()
        return cm, span, start_time, shared_attrs

    async def on_end(
        self, token: Token, ctx: RequestContext, error: Exception | None
    ) -> None:
        self.on_end_sync(token, ctx, error)

    def on_end_sync(
        self, token: Token, _ctx: RequestContext, error: Exception | None
    ) -> None:
        cm, span, start_time, shared_attrs = token
        end_time = time.perf_counter()
        error_attrs = self._get_error_attributes(error)
        if error_attrs:
            span.set_attributes(error_attrs)
        # Won't use shared_attrs anymore, no need to copy.
        metric_attrs = shared_attrs
        if error_attrs:
            metric_attrs.update(error_attrs)
        self._call_duration.record(end_time - start_time, metric_attrs)
        if error:
            cm.__exit__(type(error), error, error.__traceback__)
        else:
            cm.__exit__(None, None, None)

    @contextmanager
    def _start_span(
        self,
        ctx: RequestContext,
        span_name: str,
        shared_attrs: dict[str, AttributeValue],
    ) -> Iterator[Span]:
        parent_otel_ctx = None
        if self._client:
            span_kind = SpanKind.CLIENT
            carrier = ctx.request_headers
            self._propagator.inject(carrier, setter=_DEFAULT_TEXTMAP_SETTER)
        else:
            span_kind = SpanKind.SERVER
            parent_span = get_current_span()
            if not parent_span.get_span_context().is_valid:
                carrier = ctx.request_headers
                parent_otel_ctx = self._propagator.extract(carrier)

        attrs: dict[str, AttributeValue] = shared_attrs.copy()

        if ca := ctx.client_address:
            addr, port = ca.rsplit(":", 1)
            attrs[CLIENT_ADDRESS] = addr
            attrs[CLIENT_PORT] = int(port)

        with self._tracer.start_as_current_span(
            span_name, kind=span_kind, attributes=attrs, context=parent_otel_ctx
        ) as span:
            yield span

    def _get_error_attributes(
        self, error: Exception | None
    ) -> dict[str, AttributeValue] | None:
        if not error:
            return None

        return {
            ERROR_TYPE: type(error).__qualname__,
            RPC_RESPONSE_STATUS_CODE: error.code.value
            if isinstance(error, ConnectError)
            else "unknown",
        }

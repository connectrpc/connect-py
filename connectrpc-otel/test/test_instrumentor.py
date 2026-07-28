from __future__ import annotations

import asyncio
import subprocess
import sys
import textwrap
from typing import TYPE_CHECKING, Literal

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from example.gen.connectrpc.eliza.v1.eliza_connect import (
    ElizaService,
    ElizaServiceASGIApplication,
    ElizaServiceClient,
    ElizaServiceClientSync,
    ElizaServiceSync,
    ElizaServiceWSGIApplication,
)
from example.gen.connectrpc.eliza.v1.eliza_pb import SayRequest, SayResponse
from opentelemetry.trace import SpanKind
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc_otel import ConnectInstrumentor, OpenTelemetryInterceptor

if TYPE_CHECKING:
    from connectrpc.request import RequestContext
    from opentelemetry.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )


class ElizaServiceTest(ElizaService):
    async def say(self, _request: SayRequest, _ctx: RequestContext) -> SayResponse:
        return SayResponse(sentence="Hello")


class ElizaServiceTestSync(ElizaServiceSync):
    def say(self, request: SayRequest, _ctx: RequestContext) -> SayResponse:
        if request.sentence == "connect error":
            raise ConnectError(Code.FAILED_PRECONDITION, "connect error")
        if request.sentence == "unknown error":
            msg = "unknown error"
            raise ValueError(msg)
        return SayResponse(sentence="Hello")


@pytest.fixture(params=["async", "sync"])
def client_type(request: pytest.FixtureRequest) -> Literal["async", "sync"]:
    return request.param


@pytest.mark.asyncio
async def test_instruments(
    client_type: Literal["async", "sync"],
    tracer_provider: TracerProvider,
    meter_provider: MeterProvider,
    span_exporter: InMemorySpanExporter,
    metric_reader: InMemoryMetricReader,
) -> None:
    async def send_request() -> None:
        app = ElizaServiceASGIApplication(ElizaServiceTest())
        client = ElizaServiceClient(
            "http://localhost", http_client=Client(transport=ASGITransport(app))
        )
        await client.say(SayRequest(sentence="Hi"))

    def send_request_sync() -> None:
        app = ElizaServiceWSGIApplication(ElizaServiceTestSync())
        client = ElizaServiceClientSync(
            "http://localhost", http_client=SyncClient(transport=WSGITransport(app))
        )
        client.say(SayRequest(sentence="Hi"))

    instrumentor = ConnectInstrumentor()
    instrumentor.instrument(
        tracer_provider=tracer_provider, meter_provider=meter_provider
    )

    match client_type:
        case "async":
            await send_request()
        case "sync":
            await asyncio.to_thread(send_request_sync)

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 2
    assert spans[0].kind == SpanKind.SERVER
    assert spans[1].kind == SpanKind.CLIENT
    metrics_data = metric_reader.get_metrics_data()
    assert metrics_data is not None
    assert len(metrics_data.resource_metrics) > 0

    span_exporter.clear()
    instrumentor.uninstrument()

    match client_type:
        case "async":
            await send_request()
        case "sync":
            await asyncio.to_thread(send_request_sync)

    # Harder to clear metrics, but it should be enough to confirm there weren't
    # traces after uninstrumenting.
    spans = span_exporter.get_finished_spans()
    assert len(spans) == 0


@pytest.mark.asyncio
async def test_noop_if_manually_instrumented(
    client_type: Literal["async", "sync"],
    tracer_provider: TracerProvider,
    meter_provider: MeterProvider,
    span_exporter: InMemorySpanExporter,
) -> None:

    async def send_request() -> None:
        app = ElizaServiceASGIApplication(
            ElizaServiceTest(),
            interceptors=[OpenTelemetryInterceptor(tracer_provider=tracer_provider)],
        )
        client = ElizaServiceClient(
            "http://localhost",
            http_client=Client(transport=ASGITransport(app)),
            interceptors=[
                OpenTelemetryInterceptor(tracer_provider=tracer_provider, client=True)
            ],
        )
        await client.say(SayRequest(sentence="Hi"))

    def send_request_sync() -> None:
        app = ElizaServiceWSGIApplication(
            ElizaServiceTestSync(),
            interceptors=[OpenTelemetryInterceptor(tracer_provider=tracer_provider)],
        )
        client = ElizaServiceClientSync(
            "http://localhost",
            http_client=SyncClient(transport=WSGITransport(app)),
            interceptors=[
                OpenTelemetryInterceptor(tracer_provider=tracer_provider, client=True)
            ],
        )
        client.say(SayRequest(sentence="Hi"))

    instrumentor = ConnectInstrumentor()
    instrumentor.instrument(
        tracer_provider=tracer_provider, meter_provider=meter_provider
    )

    match client_type:
        case "async":
            await send_request()
        case "sync":
            await asyncio.to_thread(send_request_sync)

    spans = span_exporter.get_finished_spans()
    # Not 4
    assert len(spans) == 2
    assert spans[0].kind == SpanKind.SERVER
    assert spans[1].kind == SpanKind.CLIENT

    instrumentor.uninstrument()


# Make sure complete auto instrumentation doesn't conflict with other tests by running
# in a subprocess. Notably, there isn't any feature to uninstrument when using it.
def test_entrypoint_auto_instruments() -> None:
    script = textwrap.dedent("""
        from __future__ import annotations

        from importlib.metadata import entry_points

        from example.gen.connectrpc.eliza.v1.eliza_connect import (
            ElizaServiceClientSync,
            ElizaServiceSync,
            ElizaServiceWSGIApplication,
        )
        from example.gen.connectrpc.eliza.v1.eliza_pb import SayRequest, SayResponse
        from opentelemetry import trace
        from opentelemetry.instrumentation.auto_instrumentation._load import _load_instrumentors
        from opentelemetry.instrumentation.distro import DefaultDistro
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )
        from pyqwest import SyncClient
        from pyqwest.testing import WSGITransport


        class ElizaServiceTestSync(ElizaServiceSync):
            def say(self, request: SayRequest, ctx: object) -> SayResponse:
                return SayResponse(sentence="Hello")


        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        trace.set_tracer_provider(provider)

        distro = DefaultDistro()
        _load_instrumentors(distro)

        app = ElizaServiceWSGIApplication(ElizaServiceTestSync())
        client = ElizaServiceClientSync(
            "http://localhost",
            http_client=SyncClient(transport=WSGITransport(app)),
        )
        client.say(SayRequest(sentence="Hi"))

        spans = exporter.get_finished_spans()
        assert len(spans) == 2
        assert all(s.instrumentation_scope.name == "connectrpc-otel" for s in spans)
    """)
    result = subprocess.run(
        [sys.executable, "-c", script], check=False, capture_output=True, text=True
    )

    assert result.returncode == 0, result.stdout + result.stderr

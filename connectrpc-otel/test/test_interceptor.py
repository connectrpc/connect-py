from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast

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
from opentelemetry.instrumentation.asgi import (
    OpenTelemetryMiddleware as ASGIOpenTelemetryMiddleware,
)
from opentelemetry.instrumentation.wsgi import (
    OpenTelemetryMiddleware as WSGIOpenTelemetryMiddleware,
)
from opentelemetry.trace import SpanKind, StatusCode
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc_otel import OpenTelemetryInterceptor

if TYPE_CHECKING:
    from asgiref.typing import ASGIApplication
    from connectrpc.request import RequestContext
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import Histogram, InMemoryMetricReader, Metric
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )


class ElizaServiceTest(ElizaService):
    async def say(self, request: SayRequest, _ctx: RequestContext) -> SayResponse:
        if request.sentence == "connect error":
            raise ConnectError(Code.FAILED_PRECONDITION, "connect error")
        if request.sentence == "unknown error":
            raise ValueError("unknown error")
        return SayResponse(sentence="Hello")


class ElizaServiceTestSync(ElizaServiceSync):
    def say(self, request: SayRequest, _ctx: RequestContext) -> SayResponse:
        if request.sentence == "connect error":
            raise ConnectError(Code.FAILED_PRECONDITION, "connect error")
        if request.sentence == "unknown error":
            raise ValueError("unknown error")
        return SayResponse(sentence="Hello")


@pytest.fixture
def app_async(
    tracer_provider: TracerProvider, meter_provider: MeterProvider
) -> ElizaServiceASGIApplication:
    return ElizaServiceASGIApplication(
        ElizaServiceTest(),
        interceptors=[
            OpenTelemetryInterceptor(
                tracer_provider=tracer_provider, meter_provider=meter_provider
            )
        ],
    )


@pytest.fixture
def client_async(
    app_async: ElizaServiceASGIApplication,
    tracer_provider: TracerProvider,
    meter_provider: MeterProvider,
) -> ElizaServiceClient:
    transport = ASGITransport(app_async, client=("123.456.7.89", 143))
    return ElizaServiceClient(
        "http://localhost",
        http_client=Client(transport=transport),
        interceptors=[
            OpenTelemetryInterceptor(
                tracer_provider=tracer_provider,
                meter_provider=meter_provider,
                client=True,
            )
        ],
    )


@pytest.fixture
def app_sync(
    tracer_provider: TracerProvider, meter_provider: MeterProvider
) -> ElizaServiceWSGIApplication:
    return ElizaServiceWSGIApplication(
        ElizaServiceTestSync(),
        interceptors=[
            OpenTelemetryInterceptor(
                tracer_provider=tracer_provider, meter_provider=meter_provider
            )
        ],
    )


@pytest.fixture
def client_sync(
    app_sync: ElizaServiceWSGIApplication,
    tracer_provider: TracerProvider,
    meter_provider: MeterProvider,
) -> ElizaServiceClientSync:
    transport = WSGITransport(app_sync, client=("123.456.7.89", 143))
    return ElizaServiceClientSync(
        "http://localhost",
        http_client=SyncClient(transport=transport),
        interceptors=[
            OpenTelemetryInterceptor(
                tracer_provider=tracer_provider,
                meter_provider=meter_provider,
                client=True,
            )
        ],
    )


@pytest.fixture(params=["async", "sync"])
def client(
    request: pytest.FixtureRequest,
    client_async: ElizaServiceClient,
    client_sync: ElizaServiceClientSync,
) -> ElizaServiceClient | ElizaServiceClientSync:
    match request.param:
        case "async":
            return client_async
        case "sync":
            return client_sync
        case _:
            raise ValueError(f"invalid client type {request.param}")


@pytest.fixture(params=["async", "sync"])
def app(
    request: pytest.FixtureRequest,
    app_async: ElizaServiceASGIApplication,
    app_sync: ElizaServiceWSGIApplication,
) -> ElizaServiceASGIApplication | ElizaServiceWSGIApplication:
    match request.param:
        case "async":
            return app_async
        case "sync":
            return app_sync
        case _:
            raise ValueError(f"invalid app type {request.param}")


def get_metric_data(metric_reader: InMemoryMetricReader) -> list[Metric]:
    metrics: list[Metric] = []
    data = metric_reader.get_metrics_data()
    assert data is not None
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            if scope_metrics.scope.name == "connectrpc-otel":
                metrics.extend(scope_metrics.metrics)
    assert len(metrics) > 0
    metrics.sort(key=lambda m: m.name)
    return metrics


@pytest.mark.asyncio
async def test_basic(
    client: ElizaServiceClient | ElizaServiceClientSync,
    span_exporter: InMemorySpanExporter,
    metric_reader: InMemoryMetricReader,
) -> None:
    if isinstance(client, ElizaServiceClient):
        await client.say(SayRequest(sentence="Hi"))
    else:
        await asyncio.to_thread(client.say, SayRequest(sentence="Hi"))

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 2
    assert spans[0].kind == SpanKind.SERVER
    assert spans[1].kind == SpanKind.CLIENT
    server_trace_context = spans[0].get_span_context()
    assert server_trace_context is not None
    server_parent_context = spans[0].parent
    assert server_parent_context is not None
    client_trace_context = spans[1].get_span_context()
    assert client_trace_context is not None
    assert client_trace_context.trace_id == server_trace_context.trace_id
    assert server_parent_context.span_id == client_trace_context.span_id

    for span in spans:
        assert span.status.is_unset
        attrs = span.attributes
        assert attrs is not None
        assert attrs["rpc.system.name"] == "connectrpc"
        assert attrs["rpc.method"] == "connectrpc.eliza.v1.ElizaService/Say"
        assert "rpc.response.status_code" not in attrs
        assert "error.type" not in attrs
        assert attrs["server.address"] == "localhost"
        assert attrs["server.port"] == 80

    server_attrs = spans[0].attributes
    assert server_attrs is not None
    assert server_attrs["client.address"] == "123.456.7.89"
    assert server_attrs["client.port"] == 143

    metrics = get_metric_data(metric_reader)
    client_metric = metrics[0]
    assert client_metric.name == "rpc.client.call.duration"
    assert (
        client_metric.description
        == "Measures the duration of an outgoing Remote Procedure Call (RPC)"
    )
    assert client_metric.unit == "s"
    client_histogram = cast("Histogram", client_metric.data)
    assert len(client_histogram.data_points) == 1
    assert client_histogram.data_points[0].count == 1
    assert client_histogram.data_points[0].sum > 0
    assert client_histogram.data_points[0].attributes == {
        "rpc.system.name": "connectrpc",
        "rpc.method": "connectrpc.eliza.v1.ElizaService/Say",
        "server.address": "localhost",
        "server.port": 80,
    }

    server_metric = metrics[1]
    assert server_metric.name == "rpc.server.call.duration"
    assert (
        server_metric.description
        == "Measures the duration of an incoming Remote Procedure Call (RPC)"
    )
    assert server_metric.unit == "s"
    server_histogram = cast("Histogram", server_metric.data)
    assert len(server_histogram.data_points) == 1
    assert server_histogram.data_points[0].count == 1
    assert server_histogram.data_points[0].sum > 0
    assert server_histogram.data_points[0].attributes == {
        "rpc.system.name": "connectrpc",
        "rpc.method": "connectrpc.eliza.v1.ElizaService/Say",
        "server.address": "localhost",
        "server.port": 80,
    }


@pytest.mark.asyncio
async def test_connect_error(
    client: ElizaServiceClient | ElizaServiceClientSync,
    span_exporter: InMemorySpanExporter,
    metric_reader: InMemoryMetricReader,
) -> None:
    with pytest.raises(ConnectError):
        if isinstance(client, ElizaServiceClient):
            await client.say(SayRequest(sentence="connect error"))
        else:
            await asyncio.to_thread(client.say, SayRequest(sentence="connect error"))

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 2
    assert spans[0].kind == SpanKind.SERVER
    assert spans[1].kind == SpanKind.CLIENT
    server_trace_context = spans[0].get_span_context()
    assert server_trace_context is not None
    server_parent_context = spans[0].parent
    assert server_parent_context is not None
    client_trace_context = spans[1].get_span_context()
    assert client_trace_context is not None
    assert client_trace_context.trace_id == server_trace_context.trace_id
    assert server_parent_context.span_id == client_trace_context.span_id

    for span in spans:
        assert span.status.status_code == StatusCode.ERROR
        attrs = span.attributes
        assert attrs is not None
        assert attrs["rpc.system.name"] == "connectrpc"
        assert attrs["rpc.method"] == "connectrpc.eliza.v1.ElizaService/Say"
        assert attrs["rpc.response.status_code"] == "failed_precondition"
        assert attrs["error.type"] == "ConnectError"
        assert attrs["server.address"] == "localhost"
        assert attrs["server.port"] == 80

    server_attrs = spans[0].attributes
    assert server_attrs is not None
    assert server_attrs["client.address"] == "123.456.7.89"
    assert server_attrs["client.port"] == 143

    metrics = get_metric_data(metric_reader)
    client_metric = metrics[0]
    assert client_metric.name == "rpc.client.call.duration"
    assert (
        client_metric.description
        == "Measures the duration of an outgoing Remote Procedure Call (RPC)"
    )
    assert client_metric.unit == "s"
    client_histogram = cast("Histogram", client_metric.data)
    assert len(client_histogram.data_points) == 1
    assert client_histogram.data_points[0].count == 1
    assert client_histogram.data_points[0].sum > 0
    assert client_histogram.data_points[0].attributes == {
        "rpc.system.name": "connectrpc",
        "rpc.method": "connectrpc.eliza.v1.ElizaService/Say",
        "server.address": "localhost",
        "server.port": 80,
        "error.type": "ConnectError",
        "rpc.response.status_code": "failed_precondition",
    }

    server_metric = metrics[1]
    assert server_metric.name == "rpc.server.call.duration"
    assert (
        server_metric.description
        == "Measures the duration of an incoming Remote Procedure Call (RPC)"
    )
    assert server_metric.unit == "s"
    server_histogram = cast("Histogram", server_metric.data)
    assert len(server_histogram.data_points) == 1
    assert server_histogram.data_points[0].count == 1
    assert server_histogram.data_points[0].sum > 0
    assert server_histogram.data_points[0].attributes == {
        "rpc.system.name": "connectrpc",
        "rpc.method": "connectrpc.eliza.v1.ElizaService/Say",
        "server.address": "localhost",
        "server.port": 80,
        "error.type": "ConnectError",
        "rpc.response.status_code": "failed_precondition",
    }


@pytest.mark.asyncio
async def test_unknown_error(
    client: ElizaServiceClient | ElizaServiceClientSync,
    span_exporter: InMemorySpanExporter,
    metric_reader: InMemoryMetricReader,
) -> None:
    with pytest.raises(ConnectError):
        if isinstance(client, ElizaServiceClient):
            await client.say(SayRequest(sentence="unknown error"))
        else:
            await asyncio.to_thread(client.say, SayRequest(sentence="unknown error"))

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 2
    assert spans[0].kind == SpanKind.SERVER
    assert spans[1].kind == SpanKind.CLIENT
    server_trace_context = spans[0].get_span_context()
    assert server_trace_context is not None
    server_parent_context = spans[0].parent
    assert server_parent_context is not None
    client_trace_context = spans[1].get_span_context()
    assert client_trace_context is not None
    assert client_trace_context.trace_id == server_trace_context.trace_id
    assert server_parent_context.span_id == client_trace_context.span_id

    for span in spans:
        assert span.status.status_code == StatusCode.ERROR
        attrs = span.attributes
        assert attrs is not None
        assert attrs["rpc.system.name"] == "connectrpc"
        assert attrs["rpc.method"] == "connectrpc.eliza.v1.ElizaService/Say"
        assert attrs["rpc.response.status_code"] == "unknown"
        assert attrs["server.address"] == "localhost"
        assert attrs["server.port"] == 80

    server_attrs = spans[0].attributes
    assert server_attrs is not None
    # Server sees the ValueError itself
    assert server_attrs["error.type"] == "ValueError"
    assert server_attrs["client.address"] == "123.456.7.89"
    assert server_attrs["client.port"] == 143

    client_attrs = spans[1].attributes
    assert client_attrs is not None
    # Client just sees a ConnectError
    assert client_attrs["error.type"] == "ConnectError"

    metrics = get_metric_data(metric_reader)
    client_metric = metrics[0]
    assert client_metric.name == "rpc.client.call.duration"
    assert (
        client_metric.description
        == "Measures the duration of an outgoing Remote Procedure Call (RPC)"
    )
    assert client_metric.unit == "s"
    client_histogram = cast("Histogram", client_metric.data)
    assert len(client_histogram.data_points) == 1
    assert client_histogram.data_points[0].count == 1
    assert client_histogram.data_points[0].sum > 0
    assert client_histogram.data_points[0].attributes == {
        "rpc.system.name": "connectrpc",
        "rpc.method": "connectrpc.eliza.v1.ElizaService/Say",
        "server.address": "localhost",
        "server.port": 80,
        "error.type": "ConnectError",
        "rpc.response.status_code": "unknown",
    }

    server_metric = metrics[1]
    assert server_metric.name == "rpc.server.call.duration"
    assert (
        server_metric.description
        == "Measures the duration of an incoming Remote Procedure Call (RPC)"
    )
    assert server_metric.unit == "s"
    server_histogram = cast("Histogram", server_metric.data)
    assert len(server_histogram.data_points) == 1
    assert server_histogram.data_points[0].count == 1
    assert server_histogram.data_points[0].sum > 0
    assert server_histogram.data_points[0].attributes == {
        "rpc.system.name": "connectrpc",
        "rpc.method": "connectrpc.eliza.v1.ElizaService/Say",
        "server.address": "localhost",
        "server.port": 80,
        "rpc.response.status_code": "unknown",
        "error.type": "ValueError",
    }


@pytest.mark.asyncio
async def test_http_server_parent(
    app: ElizaServiceASGIApplication | ElizaServiceWSGIApplication,
    tracer_provider: TracerProvider,
    span_exporter: InMemorySpanExporter,
) -> None:
    if isinstance(app, ElizaServiceASGIApplication):
        transport = ASGITransport(
            cast(
                "ASGIApplication",
                ASGIOpenTelemetryMiddleware(app, tracer_provider=tracer_provider),
            ),
            client=("123.456.7.89", 143),
        )
        client = ElizaServiceClient(
            "http://localhost",
            http_client=Client(transport=transport),
            interceptors=[
                OpenTelemetryInterceptor(tracer_provider=tracer_provider, client=True)
            ],
        )
        await client.say(SayRequest(sentence="Hi"))
    else:
        transport = WSGITransport(
            WSGIOpenTelemetryMiddleware(app, tracer_provider=tracer_provider),
            client=("123.456.7.89", 143),
        )
        client = ElizaServiceClientSync(
            "http://localhost",
            http_client=SyncClient(transport=transport),
            interceptors=[
                OpenTelemetryInterceptor(tracer_provider=tracer_provider, client=True)
            ],
        )
        await asyncio.to_thread(client.say, SayRequest(sentence="Hi"))

    spans = [
        s
        for s in span_exporter.get_finished_spans()
        if s.instrumentation_scope and s.instrumentation_scope.name == "connectrpc-otel"
    ]
    assert len(spans) == 2
    assert spans[0].kind == SpanKind.SERVER
    assert spans[1].kind == SpanKind.CLIENT
    server_trace_context = spans[0].get_span_context()
    assert server_trace_context is not None
    server_parent_context = spans[0].parent
    assert server_parent_context is not None
    client_trace_context = spans[1].get_span_context()
    assert client_trace_context is not None
    assert client_trace_context.trace_id == server_trace_context.trace_id
    # Parent is the server middleware span, not client span.
    assert server_parent_context.span_id != client_trace_context.span_id

    for span in spans:
        assert span.status.is_unset
        attrs = span.attributes
        assert attrs is not None
        assert attrs["rpc.system.name"] == "connectrpc"
        assert attrs["rpc.method"] == "connectrpc.eliza.v1.ElizaService/Say"
        assert "rpc.response.status_code" not in attrs
        assert "error.type" not in attrs
        assert attrs["server.address"] == "localhost"
        assert attrs["server.port"] == 80

    server_attrs = spans[0].attributes
    assert server_attrs is not None
    assert server_attrs["client.address"] == "123.456.7.89"
    assert server_attrs["client.port"] == 143


@pytest.mark.asyncio
async def test_non_standard_port(
    app: ElizaServiceASGIApplication | ElizaServiceWSGIApplication,
    tracer_provider: TracerProvider,
    span_exporter: InMemorySpanExporter,
) -> None:
    if isinstance(app, ElizaServiceASGIApplication):
        transport = ASGITransport(app, client=("123.456.7.89", 143))
        client = ElizaServiceClient(
            "http://localhost:9123",
            http_client=Client(transport=transport),
            interceptors=[
                OpenTelemetryInterceptor(tracer_provider=tracer_provider, client=True)
            ],
        )
        await client.say(SayRequest(sentence="Hi"))
    else:
        transport = WSGITransport(app, client=("123.456.7.89", 143))
        client = ElizaServiceClientSync(
            "http://localhost:9123",
            http_client=SyncClient(transport=transport),
            interceptors=[
                OpenTelemetryInterceptor(tracer_provider=tracer_provider, client=True)
            ],
        )
        await asyncio.to_thread(client.say, SayRequest(sentence="Hi"))

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 2
    assert spans[0].kind == SpanKind.SERVER
    assert spans[1].kind == SpanKind.CLIENT
    server_trace_context = spans[0].get_span_context()
    assert server_trace_context is not None
    server_parent_context = spans[0].parent
    assert server_parent_context is not None
    client_trace_context = spans[1].get_span_context()
    assert client_trace_context is not None
    assert client_trace_context.trace_id == server_trace_context.trace_id
    assert server_parent_context.span_id == client_trace_context.span_id

    for span in spans:
        assert span.status.is_unset
        attrs = span.attributes
        assert attrs is not None
        assert attrs["rpc.system.name"] == "connectrpc"
        assert attrs["rpc.method"] == "connectrpc.eliza.v1.ElizaService/Say"
        assert "rpc.response.status_code" not in attrs
        assert "error.type" not in attrs
        assert attrs["server.address"] == "localhost"
        assert attrs["server.port"] == 9123

    server_attrs = spans[0].attributes
    assert server_attrs is not None
    assert server_attrs["client.address"] == "123.456.7.89"
    assert server_attrs["client.port"] == 143

from __future__ import annotations

import sys
from functools import partial
from typing import TYPE_CHECKING, ParamSpec, TypeVar, cast

from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from wrapt import register_post_import_hook, wrap_function_wrapper

if TYPE_CHECKING:
    from collections.abc import Callable, Collection
    from types import ModuleType

    from connectrpc.interceptor import Interceptor, InterceptorSync
    from opentelemetry.metrics import MeterProvider
    from opentelemetry.trace import TracerProvider

_instruments = ("connectrpc>=0.11.0",)

P = ParamSpec("P")
R = TypeVar("R")


class ConnectInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: object) -> None:
        self._meter_provider = cast(
            "MeterProvider | None", kwargs.get("meter_provider")
        )
        self._tracer_provider = cast(
            "TracerProvider | None", kwargs.get("tracer_provider")
        )

        register_post_import_hook(self._patch_client, "connectrpc.client")
        register_post_import_hook(self._patch_server, "connectrpc.server")

    def _uninstrument(self, **_kwargs: object) -> None:
        # TODO: Remove sys.modules check after
        # https://github.com/open-telemetry/opentelemetry-python-contrib/pull/4321
        if "connectrpc.client" in sys.modules:
            unwrap("connectrpc.client.ConnectClient", "__init__")
            unwrap("connectrpc.client.ConnectClientSync", "__init__")
        if "connectrpc.server" in sys.modules:
            unwrap("connectrpc.server.ConnectASGIApplication", "__init__")
            unwrap("connectrpc.server.ConnectWSGIApplication", "__init__")

    def _patch_client(self, module: ModuleType) -> None:
        wrap_function_wrapper(
            module, "ConnectClient.__init__", partial(self._init_wrapper, client=True)
        )
        wrap_function_wrapper(
            module,
            "ConnectClientSync.__init__",
            partial(self._init_wrapper, client=True),
        )

    def _patch_server(self, module: ModuleType) -> None:
        wrap_function_wrapper(
            module,
            "ConnectASGIApplication.__init__",
            partial(self._init_wrapper, client=False),
        )
        wrap_function_wrapper(
            module,
            "ConnectWSGIApplication.__init__",
            partial(self._init_wrapper, client=False),
        )

    def _init_wrapper(
        self,
        wrapped: Callable[P, R],
        _instance: object,
        args: tuple,
        kwargs: dict,
        *,
        client: bool,
    ) -> R:
        # Instrumentation doesn't eager import the library it instruments
        from connectrpc_otel import OpenTelemetryInterceptor  # noqa: PLC0415

        interceptors: list[Interceptor | InterceptorSync] | None = kwargs.get(
            "interceptors"
        )
        interceptors = [] if interceptors is None else [*interceptors]
        if any(isinstance(i, OpenTelemetryInterceptor) for i in interceptors):
            return wrapped(*args, **kwargs)

        kwargs["interceptors"] = interceptors
        # OpenTelemetry interceptor should be first so i.e. logging interceptors
        # have trace IDs.
        interceptors.insert(
            0,
            OpenTelemetryInterceptor(
                tracer_provider=self._tracer_provider,
                meter_provider=self._meter_provider,
                client=client,
            ),
        )

        return wrapped(*args, **kwargs)

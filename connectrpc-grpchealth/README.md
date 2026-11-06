# connectrpc-grpchealth

The [gRPC health checking service](https://grpc.io/docs/guides/health-checking/), for
load balancers, orchestrators such as Kubernetes, and tools such as
[`grpc-health-probe`](https://github.com/grpc-ecosystem/grpc-health-probe) that check
whether a server is ready to accept requests.

The services are standard Connect services that you use as normal.

## Example

```python
from typing import cast

from connectrpc_grpchealth import (
    HealthASGIApplication,
    HealthService,
    ServingStatus,
    StaticChecker,
)
from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.types import ASGIApp

from .gen.connectrpc.eliza.v1.eliza_connect import ElizaServiceASGIApplication
from .service import ElizaService

checker = StaticChecker("connectrpc.eliza.v1.ElizaService")
eliza_app = ElizaServiceASGIApplication(ElizaService())
health_app = HealthASGIApplication(HealthService(checker))

app = Starlette(
    routes=[
        Mount(eliza_app.path, cast(ASGIApp, eliza_app)),
        Mount(health_app.path, cast(ASGIApp, health_app)),
    ]
)

# Later, for example when a dependency becomes unavailable:
checker.set_status("connectrpc.eliza.v1.ElizaService", ServingStatus.NOT_SERVING)
```

To compute statuses some other way, pass the service any implementation of `Checker`.

# connectrpc-grpcreflect

gRPC reflection services to support tools such as [`buf curl`](https://buf.build/docs/curl/)
that query servers without knowing their schema.

The services are standard Connect services that you use as normal.

## Example

```python
from typing import cast

from connectrpc_grpcreflect import (
    ServerReflectionAlphaASGIApplication,
    ServerReflectionAlphaService,
    ServerReflectionASGIApplication,
    ServerReflectionService,
)
from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.types import ASGIApp

from .gen.connectrpc.eliza.v1 import eliza_pb
from .gen.connectrpc.eliza.v1.eliza_connect import ElizaServiceASGIApplication
from .service import ElizaService

eliza_app = ElizaServiceASGIApplication(ElizaService())
reflection_app = ServerReflectionASGIApplication(
    ServerReflectionService(eliza_pb.desc())
)
reflection_alpha_app = ServerReflectionAlphaASGIApplication(
    ServerReflectionAlphaService(eliza_pb.desc())
)

app = Starlette(
    routes=[
        Mount(eliza_app.path, cast(ASGIApp, eliza_app)),
        Mount(reflection_app.path, cast(ASGIApp, reflection_app)),
        Mount(reflection_alpha_app.path, cast(ASGIApp, reflection_alpha_app)),
    ]
)
```

Mount both `grpc.reflection.v1` and `grpc.reflection.v1alpha`. Both are
listed by `list_services`, and a client that calls v1alpha gets a 404 if it
isn't mounted.

Reflection uses the gRPC protocol which requires a server that supports HTTP/2
trailers such as [pyvoy](https://pyvoy.dev).

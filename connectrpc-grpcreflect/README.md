# connectrpc-grpcreflect

gRPC reflection services to support tools such as [`buf curl`](https://buf.build/docs/curl/)
that query servers without knowing their schema.

The services are standard Connect services that you use as normal.

## Example

```python
from typing import cast

from connectrpc_grpcreflect import reflection_apps
from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.types import ASGIApp

from .gen.connectrpc.eliza.v1 import eliza_pb
from .gen.connectrpc.eliza.v1.eliza_connect import ElizaServiceASGIApplication
from .service import ElizaService

eliza_app = ElizaServiceASGIApplication(ElizaService())

app = Starlette(
    routes=[
        Mount(eliza_app.path, cast(ASGIApp, eliza_app)),
        *(
            Mount(reflection_app.path, cast(ASGIApp, reflection_app))
            for reflection_app in reflection_apps(eliza_pb.desc())
        ),
    ]
)
```

`reflection_apps` (ASGI) and `reflection_apps_sync` (WSGI) return
applications for both `grpc.reflection.v1` and `grpc.reflection.v1alpha`. It
is recommended to mount both: even if only one is mounted, both are returned
in `ListServices`, and a client that calls the unmounted service gets a 404.

Reflection uses the gRPC protocol which requires a server that supports HTTP/2
trailers such as [pyvoy](https://pyvoy.dev).

# connectrpc-fastapi

Connect integration with [FastAPI](https://fastapi.tiangolo.com/). Service instances
come from FastAPI's dependency injection, and the RPCs appear in FastAPI's OpenAPI
schema and docs page.

## Example

```python
from typing import Annotated

from connectrpc_fastapi import add_connect_schemas, connect_router
from fastapi import Depends, FastAPI

from .database import Database, get_database
from .gen.connectrpc.eliza.v1.eliza_connect import ElizaService
from .service import DemoElizaService


async def get_eliza(
    database: Annotated[Database, Depends(get_database)],
) -> DemoElizaService:
    return DemoElizaService(database)


app = FastAPI()
app.include_router(connect_router(ElizaService, get_eliza))
add_connect_schemas(app)
```

`connect_router` returns an `APIRouter` with a POST route for each RPC, plus a GET route
for RPCs marked `NO_SIDE_EFFECTS`. For each request, FastAPI resolves `get_eliza` and
Connect serves the RPC with the instance it returns. Include the router with a prefix,
tags, or dependencies as usual; `dependency_overrides` works too. The Connect, gRPC, and
gRPC-Web protocols all work.

`add_connect_schemas` adds the request and response message schemas to the OpenAPI
document. Without it, the RPCs are still listed, but their messages show as objects
without fields.

Only async services are supported. `connect_router` raises `TypeError` for a sync
service class, but it cannot check the instances your dependency returns.

## Dependencies

The service dependency is an ordinary FastAPI dependency. It can depend on others, use
`yield`, and read the request. The RPC runs while the response is sent: after
dependencies are resolved, and before `yield` dependencies clean up.

- Dependencies with `scope="function"` clean up before the response is sent, so they
  cannot provide the service.
- Headers and cookies set on FastAPI's `Response` parameter are sent with the RPC's
  response. A status code set there is ignored.
- `HTTPException`, `ConnectError`, and validation errors raised while resolving
  dependencies produce FastAPI's usual responses, not Connect errors.

## Errors and transactions

A `ConnectError` raised by the service is a normal response, so a `yield` dependency
does not see it. It sees an exception only when the RPC fails unexpectedly. A dependency
that commits a database transaction after `yield` therefore commits even when the RPC
returned a Connect error. If that must roll back, commit inside the RPC or decide in an
interceptor.

## Interceptors and other options

`application` controls how the Connect application is created for a service instance.
Use it to pass interceptors or other options of the generated application class:

```python
import functools

from .gen.connectrpc.eliza.v1.eliza_connect import ElizaServiceASGIApplication

app.include_router(
    connect_router(
        ElizaService,
        get_eliza,
        application=functools.partial(ElizaServiceASGIApplication, interceptors=[...]),
    )
)
```

The application is created for every request, so everything passed to `partial` is
shared by all requests. Create stateful interceptors, such as `OpenTelemetryInterceptor`,
once.

Services generated with `protobuf=google` must pass their generated application class as
`application`.

## OpenAPI

Each RPC is documented with:

- its request and response messages, as JSON and binary Protobuf
- query parameters for the GET route, if any
- the Connect error response, or for streaming RPCs, a note that errors arrive at the
  end of the stream
- its comments from the Protobuf source, and whether it is deprecated

`add_connect_schemas` keys message schemas by Protobuf type name and uses source
comments as descriptions. Call it after anything else that replaces `app.openapi`.
Routers can be included before or after.

Routes are tagged with the service's type name unless you pass `tags`. Tags passed to
`include_router` are added, not substituted.

## Middleware

Most middleware works, including `GZipMiddleware`, `CORSMiddleware`,
`TrustedHostMiddleware`, and `SessionMiddleware`.

- Starlette's `BaseHTTPMiddleware` drops trailers, so gRPC does not work through it.
- For browser clients, `CORSMiddleware` needs `allow_headers` to include
  `connect-protocol-version`, `connect-timeout-ms`, `x-grpc-web`, and `x-user-agent`,
  and `expose_headers` to include `grpc-status` and `grpc-message`.

## Limitations

- A dependency that reads the request body with `request.stream()` consumes the body
  the RPC needs, and the RPC fails. `request.body()` and `request.json()` keep it.

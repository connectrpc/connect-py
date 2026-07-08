# connectrpc-grpcreflect

gRPC reflection services to support tools such as [grpcurl](https://github.com/fullstorydev/grpcurl)
that query servers without knowing their schema.

The services are standard Connect services that you use as normal.

## Example

```python

from connectrpc_otel import OpenTelemetryInterceptor

from eliza_connect import ElizaServiceWSGIApplication, ElizaServiceClientSync

from ._service import MyElizaService

app = ElizaServiceWSGIApplication(MyElizaService(), interceptors=[OpenTelemetryInterceptor()])

def make_request():
    client = ElizaServiceClientSync("http://localhost:8080", interceptors=[OpenTelemetryInterceptor(client=True)])
    resp = client.Say(SayRequest(sentence="Hello!"))
    print(resp)
```

<div align="center">

![The Connect logo](https://raw.githubusercontent.com/connectrpc/connectrpc.com/12f7ad8e95c5f784700bc280708b27cd148d0cf1/public/img/logo.svg)

# Connect for Python

[![PyPI version](https://img.shields.io/pypi/v/connect-py?style=flat-square)](https://pypi.org/project/connect-py)
[![License](https://img.shields.io/pypi/l/connect-py?style=flat-square)](https://github.com/connectrpc/connect-py/blob/main/LICENSE)
[![Slack](https://img.shields.io/badge/slack-buf-%23e01e5a?style=flat-square)](https://buf.build/links/slack)

Connect is the **easiest way to build modern APIs**.

[Docs](https://connectrpc.com/docs/python/getting-started/) •
[Example](https://github.com/connectrpc/connect-py/tree/main/example) •
[New to Connect?](https://connectrpc.com/)

</div>

## Why Connect?

Building an API usually means writing the same types at least twice: once on the server, and once in every client. Connect makes this simple - define your API schema using Protobuf, and Connect generates type-safe server stubs and idiomatic client libraries in **every major language**, including for your frontend. All that's left to write is your business logic, in plain Python.

Connect already works with your current tech stack:

- Connect is built on top of HTTP and speaks both Protobuf and JSON, so `curl` works out of the box.
- It interoperates seamlessly with gRPC and gRPC-Web, and supports streaming as a first-class feature.
- It runs on WSGI/ASGI. Build Connect APIs *alongside* your current API framework - all in the same app.

## Features

- **Servers:** WSGI and ASGI-ready, use with any [compatible server](https://connectrpc.com/docs/python/deployment/)
- **Clients:** Lightweight sync and async clients, backed by `pyqwest`
- **Protocols:** Supports Connect, gRPC, and gRPC-Web (HTTP/1.1 and HTTP/2)
- **Type safety:** Fully type-annotated generated code
- **Streaming:** Full support for server, client, and bidirectional streaming
- **Compression:** Built-in support for gzip, brotli, and zstd
- **Middleware:** Server- and client-side interceptors for telemetry, logging, etc.
- **Compliant:** Verified using the official [Connect conformance](https://github.com/connectrpc/conformance) test suite

## Getting started

Install the runtime library:

```bash
uv add connectrpc
```

For codegen, install [`buf`](https://github.com/bufbuild/buf) and create `buf.gen.yaml`:

```yaml
version: v2
plugins:
  - remote: buf.build/bufbuild/py
    out: gen
  - remote: buf.build/connectrpc/py
    out: gen
```

<details>

<summary><b>Local plugin setup</b></summary>

The example above uses a Buf-hosted plugin server. To generate code entirely locally, install the relevant plugins:

```bash
uv add --dev protoc-gen-py protoc-gen-connectrpc
```

Now edit your `buf.gen.yaml`:

```yaml
version: v2
plugins:
  - local: .venv/bin/protoc-gen-py
    out: gen
  - local: .venv/bin/protoc-gen-connectrpc
    out: gen
```

</details>

<details>

<summary><b>Compatibility with <code>google-protobuf</code></b></summary>

Connect defaults to targeting [protobuf-py](https://protobufpy.com) as the Protocol Buffers implementation, but it also supports Google's Protocol Buffers for Python. Pass `protobuf=google` to the codegen plugin to use it.

```yaml
version: v2
plugins:
  - remote: buf.build/protocolbuffers/python
    out: .
  - remote: buf.build/protocolbuffers/pyi
    out: .
  - remote: buf.build/connectrpc/py
    out: .
    opt: protobuf=google
```

If configuring a client for JSON codec, make sure to pass `connectrpc.compat.google_protobuf_json_codec` instead of `connectrpc.codec.proto_json_codec`.

</details>

## Usage

A basic Connect server is easy to set up. Just import the stubs, subclass the generated service, and serve:

```python
from connectrpc.request import RequestContext
from your_service_pb import HelloRequest, HelloResponse
from your_service_connect import HelloService, HelloServiceASGIApplication

class MyHelloService(HelloService):
    async def say_hello(self, request: HelloRequest, ctx: RequestContext) -> HelloResponse:
        return HelloResponse(message=f"Hello, {request.name}!")

# Create ASGI app
app = HelloServiceASGIApplication(MyHelloService())

# Run with any ASGI server, e.g. uvicorn:
# uvicorn server:app --port 8080
```

Client libraries are automatically generated for you. Here's what the async client looks like:

```python
from your_service_pb import HelloRequest, HelloResponse
from your_service_connect import HelloServiceClient

async def main():
    # Create async client
    async with HelloServiceClient("https://api.example.com") as client:
        response = await client.say_hello(HelloRequest(name="World"))
        print(response.message)  # "Hello, World!"
```

And the sync client:

```python
from your_service_pb import HelloRequest
from your_service_connect import HelloServiceClientSync

def main():
    # Create sync client
    with HelloServiceClientSync("https://api.example.com") as client:
        response = client.say_hello(HelloRequest(name="World"))
        print(response.message)  # "Hello, World!"

if __name__ == "__main__":
    main()
```

Check out [the docs](https://connectrpc.com/docs/python/getting-started/) for more detailed usage.

- [**Streaming:**](https://connectrpc.com/docs/python/streaming/) Connect supports server-side, client-side, and bidirectional streaming.
- [**Interceptors:**](https://connectrpc.com/docs/python/interceptors/) Set up middleware for logging, observability, and metrics.
- [**Other languages:**](https://connectrpc.com/docs/introduction/) Generate clients for use in other languages.

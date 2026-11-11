from __future__ import annotations

import threading
from wsgiref.simple_server import WSGIServer, make_server

import pytest
from example.eliza_service_sync import app as wsgi_app
from example.gen.connectrpc.eliza.v1.eliza_connect import (
    ElizaServiceClient,
    ElizaServiceClientSync,
)
from example.gen.connectrpc.eliza.v1.eliza_pb import SayRequest


@pytest.fixture(scope="module")
def sync_server():
    with make_server("", 0, wsgi_app) as httpd:
        thread = threading.Thread(target=httpd.serve_forever)
        thread.daemon = True
        thread.start()
        try:
            yield httpd
        finally:
            httpd.shutdown()


def test_sync(sync_server: WSGIServer) -> None:
    with ElizaServiceClientSync(
        f"http://localhost:{sync_server.server_port}"
    ) as client:
        response = client.say(SayRequest(sentence="Hello"))
        assert len(response.sentence) > 0


@pytest.mark.anyio
async def test_async_client_basic(sync_server: WSGIServer) -> None:
    async with ElizaServiceClient(
        f"http://localhost:{sync_server.server_port}"
    ) as client:
        response = await client.say(SayRequest(sentence="Hello"))
        assert len(response.sentence) > 0

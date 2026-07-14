from __future__ import annotations

import pytest
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc.compat import google_protobuf_binary_codec, google_protobuf_json_codec

from .google_compat.connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClient,
    HaberdasherClientSync,
    HaberdasherSync,
    HaberdasherWSGIApplication,
)
from .google_compat.connectrpc.example.haberdasher_pb2 import Hat, Size


@pytest.mark.parametrize("proto_json", [False, True])
def test_roundtrip_sync(proto_json: bool) -> None:
    class RoundtripHaberdasherSync(HaberdasherSync):
        def make_hat(self, request, ctx):
            return Hat(size=request.inches, color="green")

    app = HaberdasherWSGIApplication(RoundtripHaberdasherSync())
    with HaberdasherClientSync(
        "http://localhost",
        http_client=SyncClient(WSGITransport(app=app)),
        codec=google_protobuf_json_codec()
        if proto_json
        else google_protobuf_binary_codec(),
    ) as client:
        response = client.make_hat(request=Size(inches=10))
    assert response.size == 10
    assert response.color == "green"


@pytest.mark.parametrize("proto_json", [False, True])
@pytest.mark.asyncio
async def test_roundtrip_async(proto_json: bool) -> None:
    class DetailsHaberdasher(Haberdasher):
        async def make_hat(self, request, ctx):
            return Hat(size=request.inches, color="green")

    app = HaberdasherASGIApplication(DetailsHaberdasher())
    transport = ASGITransport(app)
    async with HaberdasherClient(
        "http://localhost",
        http_client=Client(transport),
        codec=google_protobuf_json_codec()
        if proto_json
        else google_protobuf_binary_codec(),
    ) as client:
        response = await client.make_hat(request=Size(inches=10))
    assert response.size == 10
    assert response.color == "green"

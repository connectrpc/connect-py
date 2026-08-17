from __future__ import annotations

import pytest
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc.compat import google_protobuf_binary_codec, google_protobuf_json_codec
from connectrpc.errors import ConnectError

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
        def make_hat(self, request, _ctx):
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
        async def make_hat(self, request, _ctx):
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


# A request and a response containing a field the receiver's schema doesn't have,
# as would happen when the peer is built against a newer version of the schema.
_UNKNOWN_FIELD_REQUEST = b'{"inches": 10, "notAField": true}'
_UNKNOWN_FIELD_RESPONSE = b'{"size": 10, "color": "green", "notAField": true}'


@pytest.mark.parametrize("ignore_unknown_fields", [True, False])
def test_roundtrip_sync_unknown_request_field(ignore_unknown_fields: bool) -> None:
    requests: list[Size] = []

    class RoundtripHaberdasherSync(HaberdasherSync):
        def make_hat(self, request, _ctx):
            requests.append(request)
            return Hat(size=request.inches, color="green")

    app = HaberdasherWSGIApplication(
        RoundtripHaberdasherSync(),
        codecs=[
            google_protobuf_binary_codec(),
            google_protobuf_json_codec(ignore_unknown_fields=ignore_unknown_fields),
        ],
    )
    response = SyncClient(transport=WSGITransport(app=app)).post(
        "http://localhost/connectrpc.example.Haberdasher/MakeHat",
        content=_UNKNOWN_FIELD_REQUEST,
        headers={"content-type": "application/json"},
    )
    if ignore_unknown_fields:
        assert response.status == 200
        assert response.json() == {"size": 10, "color": "green"}
        assert requests == [Size(inches=10)]
    else:
        # The WSGI and ASGI servers report a decode failure with different
        # codes, so only assert that the request was rejected.
        assert response.status >= 400
        error = response.json()
        assert isinstance(error, dict)
        assert "notAField" in error["message"]
        assert requests == []


@pytest.mark.parametrize("ignore_unknown_fields", [True, False])
@pytest.mark.asyncio
async def test_roundtrip_async_unknown_request_field(
    ignore_unknown_fields: bool,
) -> None:
    requests: list[Size] = []

    class RoundtripHaberdasher(Haberdasher):
        async def make_hat(self, request, _ctx):
            requests.append(request)
            return Hat(size=request.inches, color="green")

    app = HaberdasherASGIApplication(
        RoundtripHaberdasher(),
        codecs=[
            google_protobuf_binary_codec(),
            google_protobuf_json_codec(ignore_unknown_fields=ignore_unknown_fields),
        ],
    )
    response = await Client(ASGITransport(app)).post(
        "http://localhost/connectrpc.example.Haberdasher/MakeHat",
        content=_UNKNOWN_FIELD_REQUEST,
        headers={"content-type": "application/json"},
    )
    if ignore_unknown_fields:
        assert response.status == 200
        assert response.json() == {"size": 10, "color": "green"}
        assert requests == [Size(inches=10)]
    else:
        assert response.status >= 400
        error = response.json()
        assert isinstance(error, dict)
        assert "notAField" in error["message"]
        assert requests == []


@pytest.mark.parametrize("ignore_unknown_fields", [True, False])
def test_roundtrip_sync_unknown_response_field(ignore_unknown_fields: bool) -> None:
    def app(_environ, start_response):
        start_response(
            "200 OK",
            [
                ("content-type", "application/json"),
                ("content-length", str(len(_UNKNOWN_FIELD_RESPONSE))),
            ],
        )
        return [_UNKNOWN_FIELD_RESPONSE]

    with HaberdasherClientSync(
        "http://localhost",
        http_client=SyncClient(WSGITransport(app=app)),
        codec=google_protobuf_json_codec(ignore_unknown_fields=ignore_unknown_fields),
    ) as client:
        if ignore_unknown_fields:
            assert client.make_hat(request=Size(inches=10)) == Hat(
                size=10, color="green"
            )
        else:
            with pytest.raises(ConnectError, match="notAField"):
                client.make_hat(request=Size(inches=10))


@pytest.mark.parametrize("ignore_unknown_fields", [True, False])
@pytest.mark.asyncio
async def test_roundtrip_async_unknown_response_field(
    ignore_unknown_fields: bool,
) -> None:
    async def app(_scope, _receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(_UNKNOWN_FIELD_RESPONSE)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": _UNKNOWN_FIELD_RESPONSE})

    async with HaberdasherClient(
        "http://localhost",
        http_client=Client(ASGITransport(app)),
        codec=google_protobuf_json_codec(ignore_unknown_fields=ignore_unknown_fields),
    ) as client:
        if ignore_unknown_fields:
            assert await client.make_hat(request=Size(inches=10)) == Hat(
                size=10, color="green"
            )
        else:
            with pytest.raises(ConnectError, match="notAField"):
                await client.make_hat(request=Size(inches=10))

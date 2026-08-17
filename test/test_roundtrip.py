from __future__ import annotations

import asyncio
import struct
from typing import TYPE_CHECKING

import pytest
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc.code import Code
from connectrpc.codec import proto_binary_codec, proto_json_codec
from connectrpc.errors import ConnectError

from ._util import resolve_compression
from .connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClient,
    HaberdasherClientSync,
    HaberdasherSync,
    HaberdasherWSGIApplication,
)
from .connectrpc.example.haberdasher_pb import Hat, Size

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from asgiref.typing import HTTPDisconnectEvent, HTTPRequestEvent, HTTPScope


@pytest.mark.parametrize("proto_json", [False, True])
@pytest.mark.parametrize("compression_name", ["gzip", "br", "zstd", "identity"])
def test_roundtrip_sync(proto_json: bool, compression_name: str) -> None:
    class RoundtripHaberdasherSync(HaberdasherSync):
        def make_hat(self, request, _ctx):
            return Hat(size=request.inches, color="green")

    compression = resolve_compression(compression_name)
    app = HaberdasherWSGIApplication(
        RoundtripHaberdasherSync(), compressions=[compression]
    )
    with HaberdasherClientSync(
        "http://localhost",
        http_client=SyncClient(WSGITransport(app=app)),
        codec=proto_json_codec() if proto_json else None,
        send_compression=compression,
        accept_compression=[compression],
    ) as client:
        response = client.make_hat(request=Size(inches=10))
    assert response.size == 10
    assert response.color == "green"


@pytest.mark.parametrize("proto_json", [False, True])
@pytest.mark.parametrize("compression_name", ["gzip", "br", "zstd", "identity"])
@pytest.mark.asyncio
async def test_roundtrip_async(proto_json: bool, compression_name: str) -> None:
    class DetailsHaberdasher(Haberdasher):
        async def make_hat(self, request, _ctx):
            return Hat(size=request.inches, color="green")

    compression = resolve_compression(compression_name)
    app = HaberdasherASGIApplication(DetailsHaberdasher(), compressions=[compression])
    transport = ASGITransport(app)
    async with HaberdasherClient(
        "http://localhost",
        http_client=Client(transport),
        codec=proto_json_codec() if proto_json else None,
        send_compression=compression,
        accept_compression=[compression],
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
            proto_binary_codec(),
            proto_json_codec(ignore_unknown_fields=ignore_unknown_fields),
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
            proto_binary_codec(),
            proto_json_codec(ignore_unknown_fields=ignore_unknown_fields),
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
        codec=proto_json_codec(ignore_unknown_fields=ignore_unknown_fields),
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
        codec=proto_json_codec(ignore_unknown_fields=ignore_unknown_fields),
    ) as client:
        if ignore_unknown_fields:
            assert await client.make_hat(request=Size(inches=10)) == Hat(
                size=10, color="green"
            )
        else:
            with pytest.raises(ConnectError, match="notAField"):
                await client.make_hat(request=Size(inches=10))


def test_roundtrip_sync_connect_get_empty_request() -> None:
    class RoundtripHaberdasherSync(HaberdasherSync):
        def make_hat(self, request, _ctx):
            return Hat(size=request.inches, color="green")

    compression = resolve_compression("identity")
    app = HaberdasherWSGIApplication(
        RoundtripHaberdasherSync(), compressions=[compression]
    )
    with HaberdasherClientSync(
        "http://localhost",
        http_client=SyncClient(WSGITransport(app=app)),
        send_compression=compression,
        accept_compression=[compression],
    ) as client:
        response = client.make_hat(request=Size(), use_get=True)
    assert response.size == 0
    assert response.color == "green"


@pytest.mark.asyncio
async def test_roundtrip_async_connect_get_empty_request() -> None:
    class RoundtripHaberdasher(Haberdasher):
        async def make_hat(self, request, _ctx):
            return Hat(size=request.inches, color="green")

    compression = resolve_compression("identity")
    app = HaberdasherASGIApplication(RoundtripHaberdasher(), compressions=[compression])
    transport = ASGITransport(app)
    async with HaberdasherClient(
        "http://localhost",
        http_client=Client(transport=transport),
        send_compression=compression,
        accept_compression=[compression],
    ) as client:
        response = await client.make_hat(request=Size(), use_get=True)
    assert response.size == 0
    assert response.color == "green"


@pytest.mark.parametrize("proto_json", [False, True])
@pytest.mark.parametrize("compression_name", ["gzip", "br", "zstd", "identity"])
@pytest.mark.asyncio
async def test_roundtrip_response_stream_async(
    proto_json: bool, compression_name: str
) -> None:
    class StreamingHaberdasher(Haberdasher):
        async def make_similar_hats(self, request, _ctx):
            yield Hat(size=request.inches, color="green")
            yield Hat(size=request.inches, color="red")
            yield Hat(size=request.inches, color="blue")
            raise ConnectError(Code.RESOURCE_EXHAUSTED, "No more hats available")

    compression = resolve_compression(compression_name)
    app = HaberdasherASGIApplication(StreamingHaberdasher(), compressions=[compression])
    transport = ASGITransport(app)

    hats: list[Hat] = []
    async with HaberdasherClient(
        "http://localhost",
        http_client=Client(transport=transport),
        codec=proto_json_codec() if proto_json else None,
        send_compression=compression,
        accept_compression=[compression],
    ) as client:
        with pytest.raises(ConnectError) as exc_info:
            async for h in client.make_similar_hats(request=Size(inches=10)):
                hats.append(h)
    assert hats[0].size == 10
    assert hats[0].color == "green"
    assert hats[1].size == 10
    assert hats[1].color == "red"
    assert hats[2].size == 10
    assert hats[2].color == "blue"

    assert exc_info.value.code == Code.RESOURCE_EXHAUSTED
    assert exc_info.value.message == "No more hats available"


@pytest.mark.parametrize("client_bad", [False, True])
@pytest.mark.parametrize("compression_name", ["gzip", "br", "zstd", "identity"])
def test_message_limit_sync(client_bad: bool, compression_name: str) -> None:
    requests: list[Size] = []
    responses: list[Hat] = []

    good_size = Size(description="good")
    bad_size = Size(description="X" * 1000)
    good_hat = Hat(color="good")
    bad_hat = Hat(color="X" * 1000)

    class LargeHaberdasher(HaberdasherSync):
        def make_hat(self, request, _ctx):
            requests.append(request)
            return good_hat if client_bad else bad_hat

        def make_various_hats(self, request: Iterator[Size], _ctx) -> Iterator[Hat]:
            for size in request:
                requests.append(size)
            yield Hat(color="good")
            yield good_hat if client_bad else bad_hat

    compression = resolve_compression(compression_name)
    app = HaberdasherWSGIApplication(
        LargeHaberdasher(), read_max_bytes=100, compressions=[compression]
    )
    transport = WSGITransport(app)
    with HaberdasherClientSync(
        "http://localhost",
        http_client=SyncClient(transport),
        send_compression=compression,
        accept_compression=[compression],
        read_max_bytes=100,
    ) as client:
        with pytest.raises(ConnectError) as exc_info:
            client.make_hat(request=bad_size if client_bad else good_size)
        assert exc_info.value.code == Code.RESOURCE_EXHAUSTED
        assert exc_info.value.message == "message is larger than configured max 100"
        if client_bad:
            assert len(requests) == 0
        else:
            assert len(requests) == 1
        assert len(responses) == 0

        requests = []
        responses = []

        with pytest.raises(ConnectError) as exc_info:

            def request_stream():
                yield good_size
                yield bad_size if client_bad else good_size

            for h in client.make_various_hats(request=request_stream()):
                responses.append(h)
        assert exc_info.value.code == Code.RESOURCE_EXHAUSTED
        assert exc_info.value.message == "message is larger than configured max 100"
        if client_bad:
            assert len(requests) == 1
            assert len(responses) == 0
        else:
            assert len(requests) == 2
            assert len(responses) == 1


@pytest.mark.parametrize("client_bad", [False, True])
@pytest.mark.parametrize("compression_name", ["gzip", "br", "zstd", "identity"])
@pytest.mark.asyncio
async def test_message_limit_async(client_bad: bool, compression_name: str) -> None:
    requests: list[Size] = []
    responses: list[Hat] = []

    good_size = Size(description="good")
    bad_size = Size(description="X" * 1000)
    good_hat = Hat(color="good")
    bad_hat = Hat(color="X" * 1000)

    class LargeHaberdasher(Haberdasher):
        async def make_hat(self, request, _ctx):
            requests.append(request)
            return good_hat if client_bad else bad_hat

        async def make_various_hats(
            self, request: AsyncIterator[Size], _ctx
        ) -> AsyncIterator[Hat]:
            async for size in request:
                requests.append(size)
            yield Hat(color="good")
            yield good_hat if client_bad else bad_hat

    compression = resolve_compression(compression_name)
    app = HaberdasherASGIApplication(
        LargeHaberdasher(), read_max_bytes=100, compressions=[compression]
    )
    transport = ASGITransport(app)
    async with HaberdasherClient(
        "http://localhost",
        http_client=Client(transport=transport),
        send_compression=compression,
        accept_compression=[compression],
        read_max_bytes=100,
    ) as client:
        with pytest.raises(ConnectError) as exc_info:
            await client.make_hat(request=bad_size if client_bad else good_size)
        assert exc_info.value.code == Code.RESOURCE_EXHAUSTED
        assert exc_info.value.message == "message is larger than configured max 100"
        if client_bad:
            assert len(requests) == 0
        else:
            assert len(requests) == 1
        assert len(responses) == 0

        requests = []
        responses = []

        with pytest.raises(ConnectError) as exc_info:

            async def request_stream():
                yield good_size
                yield bad_size if client_bad else good_size

            async for h in client.make_various_hats(request=request_stream()):
                responses.append(h)
        assert exc_info.value.code == Code.RESOURCE_EXHAUSTED
        assert exc_info.value.message == "message is larger than configured max 100"
        if client_bad:
            assert len(requests) == 1
            assert len(responses) == 0
        else:
            assert len(requests) == 2
            assert len(responses) == 1


@pytest.mark.asyncio
async def test_server_stream_client_disconnect() -> None:
    """Server streaming generator should be closed when the client disconnects.

    Regression test for https://github.com/connectrpc/connect-py/issues/174.
    """
    generator_closed = asyncio.Event()

    class InfiniteHaberdasher(Haberdasher):
        async def make_similar_hats(self, request, _ctx):
            try:
                while True:
                    yield Hat(size=request.inches, color="green")
                    await asyncio.sleep(0)  # yield control to event loop
            finally:
                generator_closed.set()

    app = HaberdasherASGIApplication(InfiniteHaberdasher())

    # Encode a Connect protocol (application/connect+proto) request for Size(inches=10).
    request_bytes = Size(inches=10).to_binary()
    request_body = struct.pack(">BI", 0, len(request_bytes)) + request_bytes

    # We invoke the ASGI app directly rather than using a real client with a
    # short timeout because a real client could trigger the disconnect before the
    # request body has been fully read, which would be a different code path.
    disconnect_trigger = asyncio.Event()
    response_count = 0
    call_count = 0

    async def receive() -> HTTPRequestEvent | HTTPDisconnectEvent:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {"type": "http.request", "body": request_body, "more_body": False}
        # Block until the test is ready to simulate a disconnect.
        await disconnect_trigger.wait()
        return {"type": "http.disconnect"}

    async def send(message):
        nonlocal response_count
        if message.get("type") == "http.response.body" and message.get(
            "more_body", False
        ):
            response_count += 1
            if response_count >= 3:
                disconnect_trigger.set()

    scope: HTTPScope = {
        "type": "http",
        "asgi": {"spec_version": "2.0", "version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/connectrpc.example.Haberdasher/MakeSimilarHats",
        "raw_path": b"/connectrpc.example.Haberdasher/MakeSimilarHats",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"content-type", b"application/connect+proto")],
        "client": None,
        "server": None,
        "extensions": None,
    }

    # Without the fix the app hangs forever (generator never stopped), causing a
    # TimeoutError here.  With the fix it terminates promptly after the disconnect.
    await asyncio.wait_for(app(scope, receive, send), timeout=5.0)

    assert generator_closed.is_set(), (
        "generator should be closed after client disconnect"
    )

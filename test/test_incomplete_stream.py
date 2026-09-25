from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc.code import Code
from connectrpc.errors import ConnectError

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


def _envelope(payload: bytes, flags: int = 0) -> bytes:
    return bytes([flags]) + len(payload).to_bytes(4, "big") + payload


_SIZE = _envelope(Size(inches=10).to_binary())

_truncated_requests = [
    pytest.param(_SIZE + b"\x00\x00\x00", id="partial prefix"),
    pytest.param(_SIZE + _envelope(b"\x08\x0a")[:-1], id="partial message"),
]


def _end_message_error(content: bytes) -> dict:
    # The handler fails before any response message, so the body is only the end
    # message.
    return json.loads(content[5:])["error"]


@pytest.mark.parametrize("body", _truncated_requests)
def test_sync_server_truncated_request(body: bytes) -> None:
    class FlexibleHaberdasherSync(HaberdasherSync):
        def make_flexible_hat(self, request: Iterator[Size], _ctx) -> Hat:
            for _ in request:
                pass
            return Hat()

    transport = WSGITransport(HaberdasherWSGIApplication(FlexibleHaberdasherSync()))
    res = SyncClient(transport).post(
        "http://localhost/connectrpc.example.Haberdasher/MakeFlexibleHat",
        content=body,
        headers={"content-type": "application/connect+proto"},
    )

    assert res.status == 200
    error = _end_message_error(res.content)
    assert error["code"] == "invalid_argument"
    assert error["message"].startswith("protocol error: ")


@pytest.mark.asyncio
@pytest.mark.parametrize("body", _truncated_requests)
async def test_async_server_truncated_request(body: bytes) -> None:
    class FlexibleHaberdasher(Haberdasher):
        async def make_flexible_hat(self, request: AsyncIterator[Size], _ctx) -> Hat:
            async for _ in request:
                pass
            return Hat()

    transport = ASGITransport(HaberdasherASGIApplication(FlexibleHaberdasher()))
    res = await Client(transport).post(
        "http://localhost/connectrpc.example.Haberdasher/MakeFlexibleHat",
        content=body,
        headers={"content-type": "application/connect+proto"},
    )

    assert res.status == 200
    error = _end_message_error(res.content)
    assert error["code"] == "invalid_argument"
    assert error["message"].startswith("protocol error: ")
    assert transport.app_exception is None


_HAT = _envelope(Hat(size=10).to_binary())
_END = _envelope(b"{}", flags=0b10)

_bad_responses = [
    pytest.param(_HAT + b"\x00\x00", Code.INVALID_ARGUMENT, id="partial prefix"),
    pytest.param(
        _HAT + _envelope(b"\x08\x0a")[:-1], Code.INVALID_ARGUMENT, id="partial message"
    ),
    pytest.param(_HAT + _END + _HAT, Code.INTERNAL, id="data after end message"),
]

_RESPONSE_HEADERS = [(b"content-type", b"application/connect+proto")]


@pytest.mark.parametrize(("body", "code"), _bad_responses)
def test_sync_client_bad_stream_response(body: bytes, code: Code) -> None:
    def app(environ, start_response):
        environ["wsgi.input"].read()
        start_response(
            "200 OK", [(k.decode(), v.decode()) for k, v in _RESPONSE_HEADERS]
        )
        return [body]

    http_client = SyncClient(WSGITransport(app))
    with HaberdasherClientSync("http://localhost", http_client=http_client) as client:
        hats = client.make_similar_hats(Size(inches=10))
        assert next(hats) == Hat(size=10)
        with pytest.raises(ConnectError) as excinfo:
            next(hats)

    assert excinfo.value.code == code


@pytest.mark.asyncio
@pytest.mark.parametrize(("body", "code"), _bad_responses)
async def test_async_client_bad_stream_response(body: bytes, code: Code) -> None:
    async def app(_scope, receive, send) -> None:
        while (await receive()).get("more_body"):
            pass
        await send(
            {"type": "http.response.start", "status": 200, "headers": _RESPONSE_HEADERS}
        )
        await send({"type": "http.response.body", "body": body})

    http_client = Client(ASGITransport(app))
    async with HaberdasherClient("http://localhost", http_client=http_client) as client:
        hats = client.make_similar_hats(Size(inches=10))
        assert await anext(hats) == Hat(size=10)
        with pytest.raises(ConnectError) as excinfo:
            await anext(hats)

    assert excinfo.value.code == code

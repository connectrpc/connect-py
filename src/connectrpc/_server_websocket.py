from __future__ import annotations

import struct
from http import HTTPStatus
from typing import TYPE_CHECKING, cast

from ._codec import CODEC_NAME_JSON
from ._protocol_connect import (
    CONNECT_STREAMING_CONTENT_TYPE_PREFIX,
    CONNECT_STREAMING_HEADER_ACCEPT_COMPRESSION,
    CONNECT_STREAMING_HEADER_COMPRESSION,
    CONNECT_UNARY_CONTENT_TYPE_PREFIX,
    codec_name_from_content_type,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from asgiref.typing import (
        ASGIReceiveCallable,
        ASGIReceiveEvent,
        ASGISendCallable,
        ASGISendEvent,
        HTTPScope,
        WebSocketScope,
    )

# Prototype WebSocket transport for the Connect protocol.
#
# The wire protocol is provisional:
#
# - The client opens a WebSocket and sends a text message containing an
#   HTTP/1.1-encoded request head (request line and headers, terminated by
#   an empty line). Headers that a browser cannot set on the upgrade request
#   (content-type, connect-timeout-ms, ...) go here.
# - Request messages follow, one per WebSocket message. With the JSON codec
#   (content-type application/json), each message is a text frame containing
#   the JSON encoding of the message. With other codecs, each message is a
#   binary frame containing the raw encoded payload. There is no
#   envelope/length prefix in either case.
# - The client half-closes the request stream with an empty text message.
# - The server sends a text message containing an HTTP/1.1-encoded response
#   head, then response messages framed the same way as request messages.
# - The server signals the end of response messages with an empty text
#   message followed by a text message containing the Connect end-stream
#   JSON (metadata / error), and then closes the connection. The empty text
#   marker disambiguates the end-stream JSON from data messages when the
#   JSON codec is in use.
#
# Message compression is not part of the protocol; permessage-deflate at the
# WebSocket layer covers it. Compression negotiation headers are stripped
# from the request head so the rest of the control flow negotiates identity.
#
# Rather than a parallel implementation, the connection is adapted to look
# like a Connect streaming HTTP request to the existing control flow:
# received messages are wrapped in an envelope prefix and fed through the
# normal EnvelopeReader machinery, and envelopes produced by the
# EnvelopeWriter are unwrapped back into individual WebSocket messages.

_END_STREAM_FLAG = 0b10


async def upgrade_websocket(
    scope: WebSocketScope, receive: ASGIReceiveCallable, send: ASGISendCallable
) -> tuple[HTTPScope, ASGIReceiveCallable, ASGISendCallable] | None:
    """Accept a WebSocket connection and adapt it to an HTTP request.

    Reads the request head from the first text message and returns an HTTP
    scope along with receive/send callables that translate between WebSocket
    messages and enveloped HTTP body chunks. Returns None if the handshake
    fails, in which case the connection has been closed.
    """
    message = await receive()
    if message["type"] != "websocket.connect":
        return None
    await send({"type": "websocket.accept", "subprotocol": None, "headers": []})

    message = await receive()
    head = message.get("text") if message["type"] == "websocket.receive" else None
    if not head:
        await _close(send, reason="expected request head")
        return None
    try:
        http_method, path, head_headers = _parse_request_head(cast("str", head))
    except ValueError as e:
        await _close(send, reason=str(e))
        return None

    # Browsers cannot set headers on the upgrade request, so overlay the
    # headers from the request head on the upgrade request's headers.
    headers = {
        name.decode().lower(): value.decode()
        for name, value in scope.get("headers", ())
    }
    headers.update(head_headers)
    # The protocol does not compress individual messages; strip negotiation
    # headers so the control flow negotiates identity and envelope payloads
    # stay raw on the wire.
    for name in (
        CONNECT_STREAMING_HEADER_COMPRESSION,
        CONNECT_STREAMING_HEADER_ACCEPT_COMPRESSION,
        "content-encoding",
        "accept-encoding",
    ):
        headers.pop(name, None)

    text_frames = (
        codec_name_from_content_type(headers.get("content-type", ""), stream=False)
        == CODEC_NAME_JSON
    )

    http_scope = dict(scope)
    http_scope["type"] = "http"
    http_scope["method"] = http_method
    http_scope["path"] = path
    http_scope["scheme"] = "https" if scope.get("scheme") == "wss" else "http"
    http_scope["headers"] = [
        (name.encode(), value.encode()) for name, value in headers.items()
    ]

    return (
        cast("HTTPScope", http_scope),
        _wrap_receive(receive),
        _WebSocketSendAdapter(send, text_frames=text_frames),
    )


def _parse_request_head(head: str) -> tuple[str, str, list[tuple[str, str]]]:
    lines = head.split("\r\n")
    request_line = lines[0].split(" ")
    if len(request_line) != 3:  # method, path, version
        msg = "malformed request line"
        raise ValueError(msg)
    http_method, path, _http_version = request_line
    headers: list[tuple[str, str]] = []
    for line in lines[1:]:
        if not line:
            break
        name, sep, value = line.partition(":")
        if not sep:
            msg = "malformed header line"
            raise ValueError(msg)
        headers.append((name.strip().lower(), value.strip()))
    return http_method, path, headers


def _wrap_receive(receive: ASGIReceiveCallable) -> ASGIReceiveCallable:
    """Translate WebSocket messages into enveloped HTTP body chunks."""

    async def wrapped() -> ASGIReceiveEvent:
        while True:
            message = await receive()
            match message["type"]:
                case "websocket.receive":
                    text = message.get("text")
                    if text is not None:
                        if not text:
                            # Empty text message is the half-close marker.
                            return {
                                "type": "http.request",
                                "body": b"",
                                "more_body": False,
                            }
                        payload = text.encode()
                    else:
                        payload = message.get("bytes") or b""
                    body = struct.pack(">BI", 0, len(payload)) + payload
                    return {"type": "http.request", "body": body, "more_body": True}
                case "websocket.disconnect":
                    return {"type": "http.disconnect"}
                case _:
                    continue

    return wrapped


class _WebSocketSendAdapter:
    """Translate HTTP response events into WebSocket messages.

    The response head is sent as an HTTP/1.1-encoded text message. Body
    chunks are parsed as envelopes and each envelope payload is sent as its
    own WebSocket message, with the end-stream envelope preceded by an empty
    text marker. A non-200 response (an error before the stream started)
    instead sends its raw body as a single text message.
    """

    def __init__(self, send: ASGISendCallable, *, text_frames: bool) -> None:
        self._send = send
        self._text_frames = text_frames
        self._buffer = bytearray()
        self._raw_body = False

    async def __call__(self, message: ASGISendEvent) -> None:
        match message["type"]:
            case "http.response.start":
                self._raw_body = message["status"] != HTTPStatus.OK
                head = _encode_response_head(
                    message["status"], message.get("headers", ())
                )
                await _send_message(self._send, text=head)
            case "http.response.body":
                self._buffer.extend(message.get("body", b""))
                if not self._raw_body:
                    await self._send_envelopes()
                if not message.get("more_body", False):
                    if self._raw_body and self._buffer:
                        await _send_message(self._send, text=self._buffer.decode())
                    await _close(self._send)

    async def _send_envelopes(self) -> None:
        while len(self._buffer) >= 5:  # envelope prefix length
            length = int.from_bytes(self._buffer[1:5], "big")
            if len(self._buffer) < 5 + length:
                return
            flags = self._buffer[0]
            payload = bytes(self._buffer[5 : 5 + length])
            del self._buffer[: 5 + length]
            if flags & _END_STREAM_FLAG:
                # End-of-data marker, then the end-stream JSON.
                await _send_message(self._send, text="")
                await _send_message(self._send, text=payload.decode())
            elif self._text_frames:
                await _send_message(self._send, text=payload.decode())
            else:
                await _send_message(self._send, data=payload)


def _encode_response_head(status: int, headers: Iterable[tuple[bytes, bytes]]) -> str:
    try:
        phrase = HTTPStatus(status).phrase
    except ValueError:
        phrase = ""
    lines = [f"HTTP/1.1 {status} {phrase}".rstrip()]
    for name_bytes, value_bytes in headers:
        name = name_bytes.decode()
        value = value_bytes.decode()
        if name == "content-type" and value.startswith(
            CONNECT_STREAMING_CONTENT_TYPE_PREFIX
        ):
            # The WebSocket protocol mirrors the unary content types.
            value = (
                CONNECT_UNARY_CONTENT_TYPE_PREFIX
                + value[len(CONNECT_STREAMING_CONTENT_TYPE_PREFIX) :]
            )
        lines.append(f"{name}: {value}")
    return "\r\n".join(lines) + "\r\n\r\n"


async def _send_message(
    send: ASGISendCallable, *, text: str | None = None, data: bytes | None = None
) -> None:
    await send({"type": "websocket.send", "bytes": data, "text": text})


async def _close(send: ASGISendCallable, *, reason: str | None = None) -> None:
    await send({"type": "websocket.close", "code": 1000, "reason": reason})

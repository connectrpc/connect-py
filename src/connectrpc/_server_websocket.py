"""Server support for the Connect-over-WebSocket protocol."""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass, field
from http import HTTPStatus
from re import compile as re_compile
from typing import TYPE_CHECKING, cast
from urllib.parse import parse_qs

from ._codec import CODEC_NAME_JSON
from ._loop import current_loop
from ._protocol import ConnectWireError
from .code import Code
from .errors import ConnectError

if TYPE_CHECKING:
    from collections.abc import Collection, Container, Iterable

    from asgiref.typing import (
        ASGIReceiveCallable,
        ASGIReceiveEvent,
        ASGISendCallable,
        ASGISendEvent,
        HTTPScope,
        WebSocketScope,
    )

    from ._headers import Headers

_MARKER_BODY = "B"
_MARKER_METADATA = "M"
_MARKER_SERVER_END = "S"
_MARKER_CLIENT_END = "C"

_CLOSE_NORMAL = 1000
_CLOSE_PROTOCOL_ERROR = 1002
_CLOSE_MESSAGE_TOO_BIG = 1009

_ENVELOPE_PREFIX_LENGTH = 5
_END_STREAM_FLAG = 0b10

_SUBPROTOCOL_CODECS = {
    "connectrpc.1+proto": "proto",
    "connectrpc.1+json": CODEC_NAME_JSON,
}

# A client must not send these keys in its Leading-Metadata message. They
# are the Fetch standard's forbidden request headers, the default
# infrastructure deny list, and the names this protocol controls.
_RESERVED_METADATA_KEYS = frozenset(
    {
        "accept-charset",
        "accept-encoding",
        "access-control-request-headers",
        "access-control-request-method",
        "connection",
        "content-length",
        "cookie",
        "cookie2",
        "date",
        "dnt",
        "expect",
        "host",
        "keep-alive",
        "origin",
        "referer",
        "set-cookie",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "via",
        "x-http-method",
        "x-http-method-override",
        "x-method-override",
        "forwarded",
        "x-real-ip",
        "content-type",
        "content-encoding",
    }
)
_RESERVED_METADATA_PREFIXES = ("connect-", "proxy-", "sec-", "x-forwarded-")

# The handshake's own headers never become effective headers.
_STRIPPED_REQUEST_HEADERS = ("connection", "upgrade")
_STRIPPED_REQUEST_HEADER_PREFIXES = ("sec-websocket-", "connect-")

_FIELD_NAME_RE = re_compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
# A field value holds visible ASCII characters, spaces, and tabs.
_INVALID_FIELD_VALUE_RE = re_compile(r"[^\t\x20-\x7e]")


@dataclass
class _StreamState:
    """State the receive and send adapters share for one connection."""

    close_code: int = _CLOSE_NORMAL
    # Set once the connection has ended. The client can no longer receive
    # anything, so the send adapter drops the rest of the response.
    disconnected: bool = False
    # Set once the S message has been sent. Client messages that were
    # already in flight are discarded rather than protocol errors.
    server_done: bool = False
    # The send adapter puts this error in the S message in place of the
    # end-stream payload. The receive side sets it for a protocol error
    # that it can only report as a synthetic disconnect.
    error_override: ConnectError | None = None
    # The S message carries these headers, taken from an error response's
    # trailer-prefixed headers, as its metadata.
    error_trailers: dict[str, list[str]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class WebSocketStream:
    """An accepted Connect-over-WebSocket stream, adapted to an HTTP request.

    receive and send translate between marked WebSocket messages and
    enveloped HTTP body chunks.
    """

    scope: HTTPScope
    receive: ASGIReceiveCallable
    send: ASGISendCallable
    _state: _StreamState

    def set_protocol_error(self) -> None:
        """Close the connection as for a protocol error once S is sent."""
        self._state.close_code = _CLOSE_PROTOCOL_ERROR


def remove_reserved_response_metadata(metadata: Headers) -> ConnectError | None:
    """Remove the keys a server must not send in its M or S message."""
    reserved = [key for key in metadata if key.startswith("connect-")]
    if not reserved:
        return None
    for key in reserved:
        del metadata[key]
    return ConnectError(
        Code.INTERNAL, f"response metadata key '{reserved[0]}' is reserved"
    )


async def upgrade_websocket(
    scope: WebSocketScope,
    receive: ASGIReceiveCallable,
    send: ASGISendCallable,
    *,
    endpoints: Container[str],
    codecs: Iterable[str],
    read_max_bytes: int | None,
    allowed_origins: Collection[str] = (),
) -> WebSocketStream | None:
    """Establish a Connect-over-WebSocket stream and adapt it to HTTP.

    Negotiates the subprotocol, validates the handshake, and consumes the
    client's Leading-Metadata message. Returns None when the RPC must not be
    dispatched, in which case the connection has been rejected or closed.
    """
    message = await receive()
    if message["type"] != "websocket.connect":
        return None

    subprotocol = next(
        (
            offered
            for offered in scope.get("subprotocols", ())
            if _SUBPROTOCOL_CODECS.get(offered) in codecs
        ),
        None,
    )
    if subprotocol is None:
        # The client is not speaking Connect-over-WebSocket, or offered no
        # token whose codec this server supports.
        await _reject_handshake(scope, send, HTTPStatus.BAD_REQUEST)
        return None

    if scope.get("http_version", "1.1") != "1.1":
        await _reject_handshake(scope, send, HTTPStatus.BAD_REQUEST)
        return None

    headers: dict[str, list[str]] = {}
    for name_bytes, value_bytes in scope.get("headers", ()):
        headers.setdefault(name_bytes.decode().lower(), []).append(value_bytes.decode())

    origin = headers.get("origin", (None,))[0]
    if (
        origin is not None
        and not _origin_allowed(origin, allowed_origins)
        and not _same_origin(origin, headers.get("host", (None,))[0])
    ):
        # A cross-origin handshake must be rejected before the upgrade so no
        # page from another origin can read the error.
        await _reject_handshake(scope, send, HTTPStatus.FORBIDDEN)
        return None

    await send({"type": "websocket.accept", "subprotocol": subprotocol, "headers": []})
    # The deadline bounds the whole RPC from here, including the wait for
    # the client's Leading-Metadata message.
    loop = current_loop()
    accepted_at = loop.time()

    codec_name = _SUBPROTOCOL_CODECS[subprotocol]

    path = scope["path"]
    if path not in endpoints:
        if scope["root_path"] and path.removeprefix(scope["root_path"]) in endpoints:
            path = path.removeprefix(scope["root_path"])
        else:
            await _fail_rpc(
                send,
                ConnectError(
                    Code.UNIMPLEMENTED, f"no procedure at path {scope['path']}"
                ),
                _CLOSE_NORMAL,
            )
            return None

    query_params = parse_qs(
        scope.get("query_string", b"").decode(), keep_blank_values=True
    )
    timeouts = query_params.get("connect-timeout-ms", ())
    timeout = timeouts[0] if timeouts else None
    if timeout is not None and (
        len(timeouts) > 1
        # Check isascii explicitly since int() can accept non-ASCII digits.
        or not (timeout.isascii() and timeout.isdigit())
        or len(timeout) > 10
    ):
        await _fail_rpc(
            send,
            ConnectError(
                Code.INVALID_ARGUMENT, f"invalid connect-timeout-ms value '{timeout}'"
            ),
            _CLOSE_NORMAL,
        )
        return None
    if timeout is not None and int(timeout) == 0:
        await _fail_rpc(
            send,
            ConnectError(Code.DEADLINE_EXCEEDED, "the deadline has already passed"),
            _CLOSE_NORMAL,
        )
        return None

    deadline = None if timeout is None else accepted_at + int(timeout) / 1000

    # The first message in each direction is the Leading-Metadata message.
    try:
        async with loop.timeout_at(deadline):
            message = await receive()
    except TimeoutError:
        await _fail_rpc(
            send,
            ConnectError(Code.DEADLINE_EXCEEDED, "Request timed out"),
            _CLOSE_NORMAL,
        )
        return None
    if message["type"] != "websocket.receive":
        return None
    metadata_text = message.get("text")
    if metadata_text is None:
        await _fail_rpc(
            send,
            ConnectError(
                Code.INTERNAL,
                "the Leading-Metadata message must be sent in a text frame",
            ),
            _CLOSE_PROTOCOL_ERROR,
        )
        return None
    if not metadata_text.startswith(_MARKER_METADATA):
        await _fail_rpc(
            send,
            ConnectError(
                Code.INTERNAL, "the stream must open with a Leading-Metadata message"
            ),
            _CLOSE_PROTOCOL_ERROR,
        )
        return None
    metadata_payload = metadata_text[1:]
    if read_max_bytes is not None and len(metadata_payload.encode()) > read_max_bytes:
        await _fail_rpc(
            send,
            ConnectError(
                Code.RESOURCE_EXHAUSTED,
                f"message exceeds the {read_max_bytes} byte limit",
            ),
            _CLOSE_MESSAGE_TOO_BIG,
        )
        return None
    try:
        metadata = _parse_leading_metadata(metadata_payload, headers)
    except (TypeError, ValueError) as e:
        await _fail_rpc(
            send, ConnectError(Code.INTERNAL, str(e)), _CLOSE_PROTOCOL_ERROR
        )
        return None

    for name in _STRIPPED_REQUEST_HEADERS:
        headers.pop(name, None)
    for name in [
        name for name in headers if name.startswith(_STRIPPED_REQUEST_HEADER_PREFIXES)
    ]:
        del headers[name]
    headers.update(metadata)
    headers["content-type"] = [f"application/{codec_name}"]
    if deadline is not None:
        # The shared control flow reads the deadline from this header, as
        # the time left of a deadline that started at the handshake. The
        # header is removed from the effective headers after the request
        # context is created.
        remaining_ms = max(0, int((deadline - loop.time()) * 1000))
        headers["connect-timeout-ms"] = [str(remaining_ms)]

    http_scope = dict(scope)
    http_scope["type"] = "http"
    http_scope["method"] = "POST"
    http_scope["path"] = path
    http_scope["scheme"] = "https" if scope.get("scheme") == "wss" else "http"
    http_scope["headers"] = [
        (name.encode(), value.encode())
        for name, values in headers.items()
        for value in values
    ]

    state = _StreamState()
    binary_bodies = codec_name != CODEC_NAME_JSON
    return WebSocketStream(
        cast("HTTPScope", http_scope),
        _wrap_receive(
            receive,
            state,
            codec_name=codec_name,
            binary_bodies=binary_bodies,
            read_max_bytes=read_max_bytes,
        ),
        _WebSocketSendAdapter(send, state, binary_bodies=binary_bodies),
        state,
    )


def _origin_allowed(origin: str, patterns: Collection[str]) -> bool:
    """Match the Origin's host and port against lowercase allowed patterns.

    Like the same-origin check, the comparison ignores the scheme and does
    not normalize default ports.
    """
    if "*" in patterns:
        # Covers even an opaque "null" origin, which never parses as a host.
        return True
    _, sep, authority = origin.partition("://")
    if not sep:
        return False
    authority = authority.lower()
    return any(
        authority == pattern
        or (pattern.startswith("*") and authority.endswith(pattern[1:]))
        for pattern in patterns
    )


def _same_origin(origin: str, host: str | None) -> bool:
    """Compare the Origin's host and port with the Host header."""
    _, sep, origin_host = origin.partition("://")
    if not sep or host is None:
        # An opaque origin ("null") or a malformed value never matches.
        return False
    return origin_host.lower() == host.lower()


async def reject_websocket(
    scope: WebSocketScope, receive: ASGIReceiveCallable, send: ASGISendCallable
) -> None:
    """Reject the handshake of an endpoint that does not serve WebSockets."""
    message = await receive()
    if message["type"] != "websocket.connect":
        return
    await _reject_handshake(scope, send, HTTPStatus.BAD_REQUEST)


async def _reject_handshake(
    scope: WebSocketScope, send: ASGISendCallable, status: HTTPStatus
) -> None:
    """Reject the handshake with an HTTP status, before completing the upgrade."""
    if "websocket.http.response" in (scope.get("extensions") or {}):
        await send(
            cast(
                "ASGISendEvent",
                {
                    "type": "websocket.http.response.start",
                    "status": status.value,
                    "headers": [],
                },
            )
        )
        await send(
            cast("ASGISendEvent", {"type": "websocket.http.response.body", "body": b""})
        )
    else:
        # Without the extension, a close before accept makes the server
        # reject the handshake, though with its own status code.
        await send(
            {"type": "websocket.close", "code": _CLOSE_PROTOCOL_ERROR, "reason": None}
        )


async def _fail_rpc(
    send: ASGISendCallable, error: ConnectError, close_code: int
) -> None:
    """Report an RPC failure in-band: M, then S with the error, then close."""
    await _send_text(send, _MARKER_METADATA + "[]")
    end_stream = {"error": ConnectWireError.from_exception(error).to_dict()}
    await _send_text(send, _MARKER_SERVER_END + json.dumps(end_stream))
    await send({"type": "websocket.close", "code": close_code, "reason": None})


def _parse_leading_metadata(
    payload: str, handshake_headers: Container[str]
) -> dict[str, list[str]]:
    """Validate a Leading-Metadata payload, returning its headers."""
    parsed = json.loads(payload)
    if not isinstance(parsed, list):
        msg = "Leading-Metadata payload is not a JSON array"
        raise TypeError(msg)
    metadata: dict[str, list[str]] = {}
    for entry in parsed:
        if (
            not isinstance(entry, list)
            or len(entry) < 2
            or not all(isinstance(element, str) for element in entry)
        ):
            msg = (
                "each Leading-Metadata entry is an array of a key string "
                "and at least one value string"
            )
            raise ValueError(msg)
        key, *values = entry
        if not _FIELD_NAME_RE.match(key):
            msg = f"metadata key '{key}' is not a valid HTTP field name"
            raise ValueError(msg)
        if key != key.lower():
            msg = f"metadata key '{key}' is not lower-case"
            raise ValueError(msg)
        if key in metadata:
            msg = f"metadata key '{key}' appears in more than one entry"
            raise ValueError(msg)
        if key in _RESERVED_METADATA_KEYS or key.startswith(
            _RESERVED_METADATA_PREFIXES
        ):
            msg = f"metadata key '{key}' is reserved"
            raise ValueError(msg)
        if key in handshake_headers:
            msg = f"metadata key '{key}' matches a header of the handshake request"
            raise ValueError(msg)
        if not key.endswith("-bin"):
            for value in values:
                if _INVALID_FIELD_VALUE_RE.search(value):
                    msg = f"metadata key '{key}' has a value that is not a valid HTTP field value"
                    raise ValueError(msg)
        metadata[key] = [value.strip(" \t") for value in values]
    return metadata


def _wrap_receive(
    receive: ASGIReceiveCallable,
    state: _StreamState,
    *,
    codec_name: str,
    binary_bodies: bool,
    read_max_bytes: int | None,
) -> ASGIReceiveCallable:
    """Translate marked WebSocket messages into enveloped HTTP body chunks."""
    client_done = False

    async def wrapped() -> ASGIReceiveEvent:
        nonlocal client_done
        while True:
            message = await receive()
            match message["type"]:
                case "websocket.disconnect":
                    state.disconnected = True
                    return {"type": "http.disconnect"}
                case "websocket.receive":
                    pass
                case _:
                    continue

            text = message.get("text")
            if text is not None:
                is_text = True
                raw = text.encode()
            else:
                is_text = False
                raw = bytes(message.get("bytes") or b"")

            if client_done and state.server_done:
                continue

            # The size limit stops the message from being read at all, so it
            # is checked before the stream state.
            oversized = read_max_bytes is not None and len(raw) - 1 > read_max_bytes
            if client_done:
                # After C the client may send nothing else. This can only be
                # read by the disconnect watch, which swallows exceptions, so
                # it surfaces as a synthetic disconnect whose end-stream
                # payload the send adapter replaces.
                if oversized:
                    state.close_code = _CLOSE_MESSAGE_TOO_BIG
                    state.error_override = ConnectError(
                        Code.RESOURCE_EXHAUSTED,
                        f"message exceeds the {read_max_bytes} byte limit",
                    )
                else:
                    state.close_code = _CLOSE_PROTOCOL_ERROR
                    state.error_override = ConnectError(
                        Code.INTERNAL,
                        "received a message after the end of the client stream",
                    )
                return {"type": "http.disconnect"}
            if oversized:
                raise _stream_error(
                    state,
                    _CLOSE_MESSAGE_TOO_BIG,
                    Code.RESOURCE_EXHAUSTED,
                    f"message exceeds the {read_max_bytes} byte limit",
                )

            if not raw:
                raise _stream_error(
                    state,
                    _CLOSE_PROTOCOL_ERROR,
                    Code.INTERNAL,
                    "received an empty message with no marker",
                )
            marker_char = chr(raw[0])
            payload = raw[1:]

            if marker_char in (_MARKER_BODY, _MARKER_METADATA, _MARKER_CLIENT_END):
                expect_binary = marker_char == _MARKER_BODY and binary_bodies
                if is_text == expect_binary:
                    raise _stream_error(
                        state,
                        _CLOSE_PROTOCOL_ERROR,
                        Code.INTERNAL,
                        f"a {marker_char} message under the {codec_name} codec must be sent in a {'binary' if expect_binary else 'text'} frame",
                    )

            match marker_char:
                case "B":
                    return {
                        "type": "http.request",
                        "body": _envelope(payload),
                        "more_body": True,
                    }
                case "C":
                    if payload:
                        raise _stream_error(
                            state,
                            _CLOSE_PROTOCOL_ERROR,
                            Code.INTERNAL,
                            "a Client-End-Stream message has no payload",
                        )
                    client_done = True
                    return {"type": "http.request", "body": b"", "more_body": False}
                case "M":
                    raise _stream_error(
                        state,
                        _CLOSE_PROTOCOL_ERROR,
                        Code.INTERNAL,
                        "received a second Leading-Metadata message",
                    )
                case "S":
                    raise _stream_error(
                        state,
                        _CLOSE_PROTOCOL_ERROR,
                        Code.INTERNAL,
                        "received a Server-End-Stream message from the client",
                    )
                case _:
                    raise _stream_error(
                        state,
                        _CLOSE_PROTOCOL_ERROR,
                        Code.INTERNAL,
                        f"unknown message marker 0x{raw[0]:02x}",
                    )

    return wrapped


def _stream_error(
    state: _StreamState, close_code: int, code: Code, message: str
) -> ConnectError:
    state.close_code = close_code
    return ConnectError(code, message)


def _envelope(payload: bytes) -> bytes:
    return struct.pack(">BI", 0, len(payload)) + payload


class _WebSocketSendAdapter:
    """Translate HTTP response events into marked WebSocket messages.

    http.response.start becomes the server's Leading-Metadata message. Body
    chunks are parsed as envelopes: each data envelope is sent as a B
    message and the end-stream envelope as the S message, followed by the
    close frame. A non-200 response (an error before the stream started)
    instead wraps its wire-error body in the S message.
    """

    def __init__(
        self, send: ASGISendCallable, state: _StreamState, *, binary_bodies: bool
    ) -> None:
        self._send = send
        self._state = state
        self._binary_bodies = binary_bodies
        self._buffer = bytearray()
        self._error_status: int | None = None

    async def __call__(self, message: ASGISendEvent) -> None:
        if self._state.disconnected:
            # The client can no longer receive a response, so the server
            # must not attempt to send one.
            return
        match message["type"]:
            case "http.response.start":
                if message["status"] != HTTPStatus.OK:
                    self._error_status = message["status"]
                metadata = self._split_headers(message.get("headers", ()))
                await _send_text(self._send, _MARKER_METADATA + json.dumps(metadata))
            case "http.response.body":
                self._buffer.extend(message.get("body", b""))
                if self._error_status is None:
                    await self._send_messages()
                if not message.get("more_body", False):
                    if self._error_status is not None:
                        await self._send_error_end()
                    await self._send(
                        {
                            "type": "websocket.close",
                            "code": self._state.close_code,
                            "reason": None,
                        }
                    )

    def _split_headers(self, headers: Iterable[tuple[bytes, bytes]]) -> list[list[str]]:
        """Split response headers into M entries, stashing error trailers.

        The content type is carried by the subprotocol and compression by the
        WebSocket layer, so the headers the server sets for them do not reach
        the wire.
        """
        fields: dict[str, list[str]] = {}
        for name_bytes, value_bytes in headers:
            name = name_bytes.decode().lower()
            value = value_bytes.decode()
            if name == "content-type" or name.startswith("connect-"):
                continue
            if self._error_status is not None and name.startswith("trailer-"):
                self._state.error_trailers.setdefault(
                    name.removeprefix("trailer-"), []
                ).append(value)
                continue
            fields.setdefault(name, []).append(value)
        return [[name, *values] for name, values in fields.items()]

    async def _send_messages(self) -> None:
        while len(self._buffer) >= _ENVELOPE_PREFIX_LENGTH:
            length = int.from_bytes(self._buffer[1:_ENVELOPE_PREFIX_LENGTH], "big")
            if len(self._buffer) < _ENVELOPE_PREFIX_LENGTH + length:
                return
            flags = self._buffer[0]
            payload = bytes(
                self._buffer[_ENVELOPE_PREFIX_LENGTH : _ENVELOPE_PREFIX_LENGTH + length]
            )
            del self._buffer[: _ENVELOPE_PREFIX_LENGTH + length]
            if flags & _END_STREAM_FLAG:
                if (error := self._state.error_override) is not None:
                    payload = json.dumps(
                        {"error": ConnectWireError.from_exception(error).to_dict()}
                    ).encode()
                elif "error" not in json.loads(payload or b"{}"):
                    self._state.close_code = _CLOSE_NORMAL
                await _send_text(self._send, _MARKER_SERVER_END + payload.decode())
                self._state.server_done = True
            elif self._binary_bodies:
                await self._send(
                    {
                        "type": "websocket.send",
                        "bytes": _MARKER_BODY.encode() + payload,
                        "text": None,
                    }
                )
            else:
                await _send_text(self._send, _MARKER_BODY + payload.decode())

    async def _send_error_end(self) -> None:
        """Wrap a non-200 response's wire-error body in the S message."""
        end_stream: dict[str, object] = {}
        if self._buffer:
            end_stream["error"] = json.loads(bytes(self._buffer))
        else:
            end_stream["error"] = {
                "code": "unknown",
                "message": f"HTTP {self._error_status}",
            }
        if self._state.error_trailers:
            end_stream["metadata"] = self._state.error_trailers
        await _send_text(self._send, _MARKER_SERVER_END + json.dumps(end_stream))
        self._state.server_done = True


async def _send_text(send: ASGISendCallable, text: str) -> None:
    await send({"type": "websocket.send", "bytes": None, "text": text})

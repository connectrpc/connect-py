"""Server support for the Connect-over-WebSocket protocol."""

from __future__ import annotations

import json
import struct
from base64 import b64decode
from dataclasses import dataclass, field
from http import HTTPStatus
from re import compile as re_compile
from typing import TYPE_CHECKING, cast
from urllib.parse import parse_qs

from ._codec import CODEC_NAME_JSON
from ._protocol import ConnectWireError
from ._protocol_connect import (
    CONNECT_STREAMING_HEADER_ACCEPT_COMPRESSION,
    CONNECT_STREAMING_HEADER_COMPRESSION,
)
from .code import Code
from .errors import ConnectError

if TYPE_CHECKING:
    from collections.abc import Container, Iterable

    from asgiref.typing import (
        ASGIReceiveCallable,
        ASGIReceiveEvent,
        ASGISendCallable,
        ASGISendEvent,
        HTTPScope,
        WebSocketScope,
    )

_MARKER_BODY = "B"
_MARKER_METADATA = "M"
_MARKER_SERVER_END = "S"
_MARKER_CLIENT_END = "C"

_CLOSE_NORMAL = 1000
_CLOSE_PROTOCOL_ERROR = 1002
_CLOSE_UNSUPPORTED_DATA = 1003
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
        "connect-protocol-version",
        "connect-timeout-ms",
        "content-type",
        "content-encoding",
        "connect-content-encoding",
        "connect-accept-encoding",
    }
)
_RESERVED_METADATA_PREFIXES = ("proxy-", "sec-", "x-forwarded-")

# These names never become effective headers. The deadline arrives as a
# query parameter, and the protocol has no message-level compression.
_STRIPPED_REQUEST_HEADERS = (
    "connect-protocol-version",
    "connect-timeout-ms",
    CONNECT_STREAMING_HEADER_COMPRESSION,
    CONNECT_STREAMING_HEADER_ACCEPT_COMPRESSION,
)

_FIELD_NAME_RE = re_compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")


@dataclass
class _StreamState:
    """State the receive and send adapters share for one connection."""

    close_code: int = _CLOSE_NORMAL
    # The send adapter puts this error in the S message in place of the
    # end-stream payload. The receive side sets it for a protocol error
    # that it can only report as a synthetic disconnect.
    error_override: ConnectError | None = None
    # The S message carries these headers, taken from an error response's
    # trailer-prefixed headers, as its metadata.
    error_trailers: dict[str, list[str]] = field(default_factory=dict)


async def upgrade_websocket(
    scope: WebSocketScope,
    receive: ASGIReceiveCallable,
    send: ASGISendCallable,
    *,
    endpoints: Container[str],
    codecs: Iterable[str],
    read_max_bytes: int | None,
) -> tuple[HTTPScope, ASGIReceiveCallable, ASGISendCallable] | None:
    """Establish a Connect-over-WebSocket stream and adapt it to HTTP.

    Negotiates the subprotocol, validates the handshake, and consumes the
    client's Leading-Metadata message, then returns an HTTP scope along with
    receive/send callables that translate between marked WebSocket messages
    and enveloped HTTP body chunks. Returns None when the RPC must not be
    dispatched, in which case the connection has been rejected or closed.
    """
    message = await receive()
    if message["type"] != "websocket.connect":
        return None

    subprotocol = next(
        (
            offered
            for offered in scope.get("subprotocols", ())
            if offered in _SUBPROTOCOL_CODECS
        ),
        None,
    )
    if subprotocol is None:
        # The client is not speaking Connect-over-WebSocket.
        await _reject_handshake(scope, send, HTTPStatus.BAD_REQUEST)
        return None

    headers: dict[str, list[str]] = {}
    for name_bytes, value_bytes in scope.get("headers", ()):
        headers.setdefault(name_bytes.decode().lower(), []).append(value_bytes.decode())

    origin = headers.get("origin", (None,))[0]
    if origin is not None and not _same_origin(origin, headers.get("host", (None,))[0]):
        # A cross-origin handshake must be rejected before the upgrade so no
        # page from another origin can read the error.
        await _reject_handshake(scope, send, HTTPStatus.FORBIDDEN)
        return None

    path = scope["path"]
    if path not in endpoints:
        if scope["root_path"] and path.removeprefix(scope["root_path"]) in endpoints:
            path = path.removeprefix(scope["root_path"])
        else:
            await _reject_handshake(scope, send, HTTPStatus.NOT_FOUND)
            return None

    await send({"type": "websocket.accept", "subprotocol": subprotocol, "headers": []})

    codec_name = _SUBPROTOCOL_CODECS[subprotocol]
    if codec_name not in codecs:
        # The token is recognized but the codec is not served, so the error
        # is in-band where a browser client can read it.
        await _fail_rpc(
            send,
            ConnectError(
                Code.UNIMPLEMENTED,
                f"the '{codec_name}' codec is not supported; supported codecs are {', '.join(codecs)}",
            ),
            _CLOSE_NORMAL,
        )
        return None

    query_params = parse_qs(
        scope.get("query_string", b"").decode(), keep_blank_values=True
    )
    timeout = query_params.get("connect-timeout-ms", (None,))[0]
    # The specification allows at most ten digits.
    if timeout is not None and (not timeout.isdigit() or len(timeout) > 10):
        await _fail_rpc(
            send,
            ConnectError(
                Code.INVALID_ARGUMENT, f"invalid connect-timeout-ms value '{timeout}'"
            ),
            _CLOSE_NORMAL,
        )
        return None

    # The first message in each direction is the Leading-Metadata message.
    message = await receive()
    if message["type"] != "websocket.receive":
        return None
    metadata_text = message.get("text")
    if metadata_text is None or not metadata_text.startswith(_MARKER_METADATA):
        await _fail_rpc(
            send,
            ConnectError(
                Code.INVALID_ARGUMENT,
                "the stream must open with a Leading-Metadata message",
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
        metadata = _parse_leading_metadata(metadata_payload)
    except (TypeError, ValueError) as e:
        await _fail_rpc(
            send, ConnectError(Code.INVALID_ARGUMENT, str(e)), _CLOSE_PROTOCOL_ERROR
        )
        return None

    for name in _STRIPPED_REQUEST_HEADERS:
        headers.pop(name, None)
    headers.update(metadata)
    headers["content-type"] = [f"application/{codec_name}"]
    if timeout is not None:
        # The shared control flow reads the deadline from this header. The
        # header is removed from the effective headers after the request
        # context is created.
        headers["connect-timeout-ms"] = [timeout]

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
    return (
        cast("HTTPScope", http_scope),
        _wrap_receive(
            receive,
            state,
            codec_name=codec_name,
            binary_bodies=binary_bodies,
            read_max_bytes=read_max_bytes,
        ),
        _WebSocketSendAdapter(send, state, binary_bodies=binary_bodies),
    )


def _same_origin(origin: str, host: str | None) -> bool:
    """Compare the Origin's host and port with the Host header."""
    _, sep, origin_host = origin.partition("://")
    if not sep or host is None:
        # An opaque origin ("null") or a malformed value never matches.
        return False
    return origin_host.lower() == host.lower()


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
    await _send_text(send, _MARKER_METADATA + "{}")
    end_stream = {"error": ConnectWireError.from_exception(error).to_dict()}
    await _send_text(send, _MARKER_SERVER_END + json.dumps(end_stream))
    await send({"type": "websocket.close", "code": close_code, "reason": None})


def _parse_leading_metadata(payload: str) -> dict[str, list[str]]:
    """Validate and fold a Leading-Metadata payload into effective headers.

    Raises ValueError naming the violated rule.
    """
    parsed = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    if not isinstance(parsed, dict):
        msg = "Leading-Metadata payload is not a JSON object"
        raise TypeError(msg)
    metadata: dict[str, list[str]] = {}
    for key, values in parsed.items():
        if not _FIELD_NAME_RE.match(key):
            msg = f"metadata key '{key}' is not a valid HTTP field name"
            raise ValueError(msg)
        folded = key.lower()
        if folded in metadata:
            msg = f"metadata key '{key}' appears more than once"
            raise ValueError(msg)
        if folded in _RESERVED_METADATA_KEYS or folded.startswith(
            _RESERVED_METADATA_PREFIXES
        ):
            msg = f"metadata key '{key}' is reserved"
            raise ValueError(msg)
        if not isinstance(values, list) or not all(
            isinstance(value, str) for value in values
        ):
            msg = f"metadata key '{key}' does not map to an array of strings"
            raise ValueError(msg)
        for value in values:
            if folded.endswith("-bin"):
                try:
                    b64decode(value + "=" * (-len(value) % 4), validate=True)
                except ValueError as e:
                    msg = f"metadata key '{key}' has a value that is not valid base64"
                    raise ValueError(msg) from e
            elif "\r" in value or "\n" in value or "\x00" in value:
                msg = f"metadata key '{key}' has a value that is not a valid HTTP field value"
                raise ValueError(msg)
        metadata[folded] = values
    return metadata


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            msg = f"metadata key '{key}' appears more than once"
            raise ValueError(msg)
        result[key] = value
    return result


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

            if client_done:
                # After C the client may send nothing else. This can only be
                # read by the disconnect watch, which swallows exceptions, so
                # it surfaces as a synthetic disconnect whose end-stream
                # payload the send adapter replaces.
                state.close_code = _CLOSE_PROTOCOL_ERROR
                state.error_override = ConnectError(
                    Code.INVALID_ARGUMENT,
                    "received a message after the end of the client stream",
                )
                return {"type": "http.disconnect"}

            if not raw:
                raise _stream_error(
                    state,
                    _CLOSE_PROTOCOL_ERROR,
                    Code.INVALID_ARGUMENT,
                    "received an empty message with no marker",
                )
            marker = raw[0]
            if marker >= 0x80:  # the marker high bit is reserved
                raise _stream_error(
                    state,
                    _CLOSE_PROTOCOL_ERROR,
                    Code.INVALID_ARGUMENT,
                    "message marker has the reserved high bit set",
                )
            payload = raw[1:]
            if read_max_bytes is not None and len(payload) > read_max_bytes:
                raise _stream_error(
                    state,
                    _CLOSE_MESSAGE_TOO_BIG,
                    Code.RESOURCE_EXHAUSTED,
                    f"message exceeds the {read_max_bytes} byte limit",
                )

            match chr(marker):
                case "B" | "C" as marker_char:
                    if (
                        payload or marker_char == _MARKER_BODY
                    ) and is_text == binary_bodies:
                        raise _stream_error(
                            state,
                            _CLOSE_UNSUPPORTED_DATA,
                            Code.INVALID_ARGUMENT,
                            f"message body was sent in a {'text' if is_text else 'binary'} frame but the negotiated codec is {codec_name}",
                        )
                    if marker_char == _MARKER_CLIENT_END:
                        client_done = True
                        return {
                            "type": "http.request",
                            "body": _envelope(payload) if payload else b"",
                            "more_body": False,
                        }
                    if not payload and is_text:
                        raise _stream_error(
                            state,
                            _CLOSE_PROTOCOL_ERROR,
                            Code.INVALID_ARGUMENT,
                            "an empty JSON message is '{}', never a bare B marker",
                        )
                    return {
                        "type": "http.request",
                        "body": _envelope(payload),
                        "more_body": True,
                    }
                case "M":
                    raise _stream_error(
                        state,
                        _CLOSE_PROTOCOL_ERROR,
                        Code.INVALID_ARGUMENT,
                        "received a second Leading-Metadata message",
                    )
                case "S":
                    raise _stream_error(
                        state,
                        _CLOSE_PROTOCOL_ERROR,
                        Code.INVALID_ARGUMENT,
                        "received a Server-End-Stream message from the client",
                    )
                case _:
                    raise _stream_error(
                        state,
                        _CLOSE_PROTOCOL_ERROR,
                        Code.INVALID_ARGUMENT,
                        f"unknown message marker 0x{marker:02x}",
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

    def _split_headers(
        self, headers: Iterable[tuple[bytes, bytes]]
    ) -> dict[str, list[str]]:
        """Split response headers into M metadata, stashing error trailers.

        The content type and compression are carried by the subprotocol and
        the WebSocket layer, so their headers never reach the wire.
        """
        metadata: dict[str, list[str]] = {}
        for name_bytes, value_bytes in headers:
            name = name_bytes.decode().lower()
            value = value_bytes.decode()
            if name in ("content-type", CONNECT_STREAMING_HEADER_COMPRESSION):
                continue
            if name.startswith("trailer-"):
                self._state.error_trailers.setdefault(
                    name.removeprefix("trailer-"), []
                ).append(value)
                continue
            metadata.setdefault(name, []).append(value)
        return metadata

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
                await _send_text(self._send, _MARKER_SERVER_END + payload.decode())
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


async def _send_text(send: ASGISendCallable, text: str) -> None:
    await send({"type": "websocket.send", "bytes": None, "text": text})

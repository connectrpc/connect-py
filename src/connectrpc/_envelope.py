from __future__ import annotations

import struct
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from ._compression import Compression, IdentityCompression
from ._shared import message_too_large_error
from .code import Code
from .errors import ConnectError

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyqwest import Response, SyncResponse
    from typing_extensions import Self

    from ._codec import Codec
    from ._protocol import ConnectWireError
    from .request import Headers

_RES = TypeVar("_RES")
_T = TypeVar("_T")


class EnvelopeReader(Generic[_RES]):
    _next_message_length: int | None

    def __init__(
        self,
        message_class: type[_RES],
        codec: Codec,
        compression: Compression,
        read_max_bytes: int | None,
    ) -> None:
        self._buffer = bytearray()
        self._message_class = message_class
        self._codec = codec
        self._compression = compression
        self._read_max_bytes = read_max_bytes

        self._next_message_length = None
        self._ended = False

    def feed(self, data: bytes | memoryview | bytearray) -> Iterator[_RES]:
        self._buffer.extend(data)
        return self._read_messages()

    def _read_messages(self) -> Iterator[_RES]:
        while self._buffer and not self._ended:
            if self._next_message_length is not None:
                if len(self._buffer) < self._next_message_length + 5:
                    return

                prefix_byte = self._buffer[0]
                compressed = prefix_byte & 0b01 != 0

                message_data = self._buffer[5 : 5 + self._next_message_length]
                del self._buffer[: 5 + self._next_message_length]
                self._next_message_length = None
                if compressed:
                    if isinstance(self._compression, IdentityCompression):
                        raise ConnectError(
                            Code.INTERNAL,
                            "protocol error: sent compressed message without compression support",
                        )

                    message_data = self._compression.decompress(
                        message_data, self._read_max_bytes
                    )

                if self.handle_end_message(prefix_byte, message_data):
                    self._ended = True
                    return

                res = self._codec.decode(message_data, self._message_class)
                yield res

            if len(self._buffer) < 5:
                return

            self._next_message_length = int.from_bytes(self._buffer[1:5], "big")
            if (
                self._read_max_bytes is not None
                and self._next_message_length > self._read_max_bytes
            ):
                raise message_too_large_error(self._read_max_bytes)

    @contextmanager
    def reading(
        self, response: Response | SyncResponse | None = None
    ) -> Iterator[Self]:
        """Validate the end of the stream when the block exits without an exception.

        Raises if the body ended partway through a message or continued after
        the end message, then calls [handle_response_complete][] with the
        response, if any.
        """
        yield self
        self._check_ended()
        if response is not None:
            self.handle_response_complete(response)

    def _check_ended(self) -> None:
        if self._ended:
            if self._buffer:
                raise ConnectError(
                    Code.INTERNAL,
                    f"corrupt response: {len(self._buffer)} extra bytes after end of stream",
                )
            return
        if self._next_message_length is not None:
            raise ConnectError(
                Code.INVALID_ARGUMENT,
                f"protocol error: promised {self._next_message_length} bytes in enveloped message, got {len(self._buffer) - 5} bytes",
            )
        if self._buffer:
            raise ConnectError(
                Code.INVALID_ARGUMENT,
                "protocol error: incomplete envelope: unexpected EOF",
            )

    def handle_end_message(
        self, _prefix_byte: int, _message_data: bytes | bytearray, /
    ) -> bool:
        """Handle the end message for client protocols that have one.

        Connect and gRPC-Web are such protocols. Returns True if the end
        message was handled, False otherwise.
        """
        return False

    def handle_response_complete(
        self, response: Response | SyncResponse, /, error: ConnectError | None = None
    ) -> None:
        """Handle any client finalization needed when the response is complete.

        This is typically used to process trailers for gRPC.
        """


class EnvelopeWriter(ABC, Generic[_T]):
    def __init__(self, codec: Codec[_T, Any], compression: Compression | None) -> None:
        self._codec = codec
        self._compression = compression
        self._prefix = (
            0 if not compression or isinstance(compression, IdentityCompression) else 1
        )

    def write(self, message: _T) -> bytes:
        data = self._codec.encode(message)
        if self._compression:
            data = self._compression.compress(data)
        # This copies data into the final envelope, but it is still better than issuing
        # I/O multiple times for small prefix / length elements.
        return struct.pack(">BI", self._prefix, len(data)) + data

    @abstractmethod
    def end(
        self, user_trailers: Headers, error: ConnectWireError | None
    ) -> bytes | Headers: ...

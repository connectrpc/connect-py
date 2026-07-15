from __future__ import annotations

import struct
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from ._compression import Compression, IdentityCompression
from .code import Code
from .errors import ConnectError

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyqwest import Response, SyncResponse

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

    def feed(self, data: bytes | memoryview | bytearray) -> Iterator[_RES]:
        self._buffer.extend(data)
        return self._read_messages()

    def _read_messages(self) -> Iterator[_RES]:
        while self._buffer:
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
                    message_data = self._compression.decompress(message_data)

                if (
                    self._read_max_bytes is not None
                    and len(message_data) > self._read_max_bytes
                ):
                    raise ConnectError(
                        Code.RESOURCE_EXHAUSTED,
                        f"message is larger than configured max {self._read_max_bytes}",
                    )

                if self.handle_end_message(prefix_byte, message_data):
                    return

                res = self._codec.decode(message_data, self._message_class)
                yield res

            if len(self._buffer) < 5:
                return

            self._next_message_length = int.from_bytes(self._buffer[1:5], "big")

    def handle_end_message(
        self, prefix_byte: int, message_data: bytes | bytearray
    ) -> bool:
        """For client protocols with an end message like Connect and gRPC-Web, handle the end message.
        Returns True if the end message was handled, False otherwise.
        """
        return False

    def handle_response_complete(
        self, response: Response | SyncResponse, e: ConnectError | None = None
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

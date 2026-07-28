"""The Connect RPC error types."""

from __future__ import annotations

__all__ = ["ConnectError", "ErrorDetail"]


from typing import TYPE_CHECKING, TypeVar, overload

from protobuf import Message, Registry
from protobuf.wkt import Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from protobuf import DescMessage

    from .code import Code

T = TypeVar("T", bound=Message)


class ErrorDetail:
    """A self-describing Protobuf message attached to a [ConnectError][].

    Error details are sent over the network to clients, which can then work with
    strongly-typed data rather than trying to parse a complex error message. For
    example, you might use details to send a localized error message or retry
    parameters to a client.
    """

    def __init__(self, message: Message) -> None:
        """Create a new error detail from a Protobuf message.

        If the message is an Any, it is used directly. Otherwise, the message
        is packed into an Any, with it also stored to be recoverable from [value()][].
        """
        if isinstance(message, Any):
            self._message = None
            self._any = message
            return
        self._message = message
        self._any = Any.pack(message)

    @property
    def type_name(self) -> str:
        """The fully-qualified name of the details Protobuf message (for example, acme.foo.v1.FooDetail)."""
        return self._any.type_url.removeprefix("type.googleapis.com/")

    @property
    def message_bytes(self) -> bytes:
        """The Protobuf message serialized as bytes."""
        return self._any.value

    @overload
    def value(self) -> Message | None: ...

    @overload
    def value(self, desc: DescMessage, /) -> Message | None: ...

    @overload
    def value(self, typ: type[T], /) -> T | None: ...

    @overload
    def value(self, registry: Registry, /) -> Message | None: ...

    def value(
        self, desc_or_registry: Registry | DescMessage | type[Message] | None = None
    ) -> Message | None:
        """Get the details message as a Protobuf message, or None if it cannot be deserialized."""
        if self._message:
            return self._message
        if not desc_or_registry:
            return None
        if isinstance(desc_or_registry, Registry):
            desc = desc_or_registry.message(self.type_name)
            if not desc:
                return None
        else:
            desc = desc_or_registry
        return self._any.unpack(desc)


class ConnectError(Exception):
    """An exception in a Connect RPC.

    If a server raises a ConnectError, the same exception content will be
    raised on the client as well. Errors surfacing on the client side such as
    timeouts will also be raised as a ConnectError with an appropriate
    [Code][].
    """

    def __init__(
        self, code: Code, message: str, details: Iterable[Message | ErrorDetail] = ()
    ) -> None:
        """Create a new Connect error.

        Args:
            code: The error code.
            message: The error message.
            details: Additional details about the error.

        """
        super().__init__(message)
        self._code = code
        self._message = message

        self._details = (
            [m if isinstance(m, ErrorDetail) else ErrorDetail(m) for m in details]
            if details
            else ()
        )

    @property
    def code(self) -> Code:
        """The Connect error code."""
        return self._code

    @property
    def message(self) -> str:
        """The Connect error message."""
        return self._message

    @property
    def details(self) -> Sequence[ErrorDetail]:
        """Additional details about the error, if any."""
        return self._details

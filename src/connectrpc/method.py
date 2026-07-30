"""The Connect method information."""

from __future__ import annotations

__all__ = ["IdempotencyLevel", "MethodInfo"]


import enum
from dataclasses import dataclass
from typing import Generic, TypeVar

REQ = TypeVar("REQ")
RES = TypeVar("RES")


class IdempotencyLevel(enum.Enum):
    """The level of idempotency of an RPC method.

    This value can affect RPC behaviors, such as determining whether it is safe to
    retry a request, or what kinds of request modalities are allowed for a given
    procedure.
    """

    UNKNOWN = enum.auto()
    """The default idempotency level.

    A method with this idempotency level may not be idempotent. This is appropriate for
    any kind of method.
    """

    NO_SIDE_EFFECTS = enum.auto()
    """The idempotency level that specifies that a given call has no side-effects.

    This is equivalent to [RFC 9110 § 9.2.1] "safe" methods in terms of semantics.
    This procedure should not mutate any state. This idempotency level is appropriate
    for queries, or anything that would be suitable for an HTTP GET request. In addition,
    due to the lack of side-effects, such a procedure would be suitable to retry and
    expect that the results will not be altered by preceding attempts.

    [RFC 9110 § 9.2.1]: https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.1
    """

    IDEMPOTENT = enum.auto()
    """The idempotency level that specifies that a given call is "idempotent",
    such that multiple instances of the same request to this procedure would have
    the same side-effects as a single request.

    This is equivalent to [RFC 9110 § 9.2.2] "idempotent" methods.
    This level is a subset of the previous level. This idempotency level is
    appropriate for any procedure that is safe to retry multiple times
    and be guaranteed that the response and side-effects will not be altered
    as a result of multiple attempts, for example, entity deletion requests.

    [RFC 9110 § 9.2.2]: https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.2
    """


@dataclass(kw_only=True, frozen=True, slots=True)
class MethodInfo(Generic[REQ, RES]):
    """Information about a RPC method within a service."""

    name: str
    """The name of the method within the service."""

    service_name: str
    """The fully qualified service name containing the method."""

    input: type[REQ]
    """The input message type of the method."""

    output: type[RES]
    """The output message type of the method."""

    idempotency_level: IdempotencyLevel
    """The [IdempotencyLevel][] of the method."""

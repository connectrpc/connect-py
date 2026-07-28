"""The Connect RPC error codes."""

from __future__ import annotations

__all__ = ["Code"]


from enum import Enum


class Code(Enum):
    """Enumeration of Connect error codes."""

    CANCELED = "canceled"
    """RPC canceled, usually by the caller."""

    UNKNOWN = "unknown"
    """Catch-all for errors of unclear origin and errors without a more appropriate code."""

    INVALID_ARGUMENT = "invalid_argument"
    """Request is invalid, regardless of system state."""

    DEADLINE_EXCEEDED = "deadline_exceeded"
    """Deadline expired before RPC could complete or before the client received the response."""

    NOT_FOUND = "not_found"
    """User requested a resource (for example, a file or directory) that can't be found."""

    ALREADY_EXISTS = "already_exists"
    """Caller attempted to create a resource that already exists."""

    PERMISSION_DENIED = "permission_denied"
    """Caller isn't authorized to perform the operation."""

    RESOURCE_EXHAUSTED = "resource_exhausted"
    """Operation can't be completed because some resource is exhausted. Use unavailable if the server
    is temporarily overloaded and the caller should retry later."""

    FAILED_PRECONDITION = "failed_precondition"
    """Operation can't be completed because the system isn't in the required state."""

    ABORTED = "aborted"
    """The operation was aborted, often because of concurrency issues like a database transaction abort."""

    OUT_OF_RANGE = "out_of_range"
    """The operation was attempted past the valid range."""

    UNIMPLEMENTED = "unimplemented"
    """The operation isn't implemented, supported, or enabled."""

    INTERNAL = "internal"
    """An invariant expected by the underlying system has been broken. Reserved for serious errors."""

    UNAVAILABLE = "unavailable"
    """The service is currently unavailable, usually transiently. Clients should back off and retry
    idempotent operations."""

    DATA_LOSS = "data_loss"
    """Unrecoverable data loss or corruption."""

    UNAUTHENTICATED = "unauthenticated"
    """Caller doesn't have valid authentication credentials for the operation."""

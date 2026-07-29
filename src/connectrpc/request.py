"""The HTTP request context for a Connect invocation."""

from __future__ import annotations

__all__ = ["Headers", "RequestContext"]


import time
from typing import TYPE_CHECKING, Generic, TypeVar

from ._headers import Headers

if TYPE_CHECKING:
    from .method import MethodInfo

REQ = TypeVar("REQ")
RES = TypeVar("RES")


class RequestContext(Generic[REQ, RES]):
    """Additional context for an RPC request message."""

    _method: MethodInfo[REQ, RES]
    _http_method: str
    _request_headers: Headers
    _response_headers: Headers | None
    _response_trailers: Headers | None

    def __init__(
        self,
        *,
        method: MethodInfo[REQ, RES],
        http_method: str,
        request_headers: Headers,
        timeout_ms: int | None = None,
        server_address: str | None = None,
        client_address: str | None = None,
    ) -> None:
        """Initialize a Context object."""
        self._method = method
        self._http_method = http_method
        self._request_headers = request_headers
        self._response_headers = None
        self._response_trailers = None
        self._server_address = server_address
        self._client_address = client_address

        if timeout_ms is None:
            self._end_time = None
        else:
            self._end_time = time.monotonic() + (timeout_ms / 1000.0)

    @property
    def method(self) -> MethodInfo[REQ, RES]:
        """Returns information about the RPC method being invoked."""
        return self._method

    @property
    def http_method(self) -> str:
        """Returns the HTTP method for this request.

        This is nearly always POST, but side-effect-free unary RPCs could be made
        via GET.
        """
        return self._http_method

    @property
    def request_headers(self) -> Headers:
        """Returns the request headers associated with the context."""
        return self._request_headers

    @property
    def response_headers(self) -> Headers:
        """Returns the response headers that will be sent before the response."""
        if self._response_headers is None:
            self._response_headers = Headers()
        return self._response_headers

    @property
    def response_trailers(self) -> Headers:
        """Returns the response trailers that will be sent after the response."""
        if self._response_trailers is None:
            self._response_trailers = Headers()
        return self._response_trailers

    @property
    def timeout_ms(self) -> float | None:
        """Returns the remaining time until the timeout in milliseconds, or None if no timeout is set."""
        if self._end_time is None:
            return None
        return (self._end_time - time.monotonic()) * 1000.0

    @property
    def server_address(self) -> str | None:
        """Returns the server address for this request, if available, as a "address:port" string.

        - On the client, this is components from the URL configured when constructing the client.
        - On the server, this is determined from the Host header and scheme of the request.
        """
        return self._server_address

    @property
    def client_address(self) -> str | None:
        """Returns the client address for this request, if available, as a "address:port" string.

        - On the client, this is never populated.
        - On the server, this is the value provided by the server implementation, generally
          the IP address and ephemeral port of the client.
        """
        return self._client_address

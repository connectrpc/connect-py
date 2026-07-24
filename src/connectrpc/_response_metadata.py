from __future__ import annotations

import contextlib
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING

from .request import Headers

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from types import TracebackType

    from pyqwest import Headers as HTTPHeaders


_current_response = ContextVar["ResponseMetadata"]("connectrpc_current_response")


def handle_response_headers(status: int, headers: HTTPHeaders) -> None:
    response = _current_response.get(None)
    if not response:
        return

    response._http_status = status  # noqa: SLF001
    response_headers: Headers = Headers()
    response_trailers: Headers = Headers()
    for key, value in headers.items():
        if key.startswith("trailer-"):
            normalized_key = key[len("trailer-") :]
            obj = response_trailers
        else:
            normalized_key = key
            obj = response_headers
        obj.add(normalized_key, value)
    if response_headers:
        response._headers = response_headers  # noqa: SLF001
    if response_trailers:
        response._trailers = response_trailers  # noqa: SLF001


def handle_response_trailers(
    trailers: HTTPHeaders | Mapping[str, Sequence[str]],
) -> None:
    response = _current_response.get(None)
    if not response:
        return
    response_trailers = response.trailers
    for key, value in trailers.items():
        if isinstance(value, str):
            response_trailers.add(key, value)
        else:
            for v in value:
                response_trailers.add(key, v)
    if response_trailers:
        response._trailers = response_trailers  # noqa: SLF001


class ResponseMetadata:
    """
    Response metadata separate from the message payload.

    Commonly, RPC client invocations only need the message payload and do not need to
    directly read other data such as headers or trailers. In cases where they are needed,
    initialize this class in a context manager to access the response HTTP status,
    headers and trailers for the invocation made within the context.

    Example:
        ```python
        with ResponseMetadata() as resp_data:
            resp = client.MakeHat(Size(inches=10))
            do_something_with_response_payload(resp)
            check_response_headers(resp_data.headers())
            check_response_trailers(resp_data.trailers())
        ```
    """

    _http_status: int | None = None
    _headers: Headers | None = None
    _trailers: Headers | None = None
    _token: Token[ResponseMetadata] | None = None

    def __enter__(self) -> ResponseMetadata:
        self._token = _current_response.set(self)
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        if self._token:
            # Normal usage with context manager will always work but it is
            # theoretically possible for user to move to another thread
            # and this fails, it is fine to ignore it.
            with contextlib.suppress(Exception):
                _current_response.reset(self._token)
        self._token = None

    @property
    def http_status(self) -> int:
        """Returns the HTTP status code of the response."""
        if self._http_status is None:
            return 0
        return self._http_status

    @property
    def headers(self) -> Headers:
        """Returns the response headers."""
        if self._headers is None:
            return Headers()
        return self._headers

    @property
    def trailers(self) -> Headers:
        """Returns the response trailers."""
        if self._trailers is None:
            return Headers()
        return self._trailers

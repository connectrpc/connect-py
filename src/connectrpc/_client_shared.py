from __future__ import annotations

import base64
from typing import TYPE_CHECKING, TypeVar

from pyqwest import Headers as HTTPHeaders, StreamError, StreamErrorCode

from ._protocol_connect import CONNECT_PROTOCOL_VERSION
from .code import Code
from .errors import ConnectError

if TYPE_CHECKING:
    from ._codec import Codec
    from .request import RequestContext


REQ = TypeVar("REQ")
RES = TypeVar("RES")


def prepare_get_params(
    codec: Codec, request_data: bytes, headers: HTTPHeaders
) -> dict[str, str]:
    # Follow order in spec: https://connectrpc.com/docs/protocol/#unary-get-request
    params: dict[str, str] = {"connect": f"v{CONNECT_PROTOCOL_VERSION}", "base64": "1"}
    if "content-encoding" in headers:
        params["compression"] = headers.pop("content-encoding")
    params["encoding"] = codec.name()
    params["message"] = base64.urlsafe_b64encode(request_data).decode("ascii")
    return params


# https://github.com/connectrpc/connect-go/blob/59cc6973156cd9164d6bea493b1d106ed894f2df/error.go#L393
def maybe_map_stream_reset(
    e: Exception, ctx: RequestContext[REQ, RES]
) -> ConnectError | None:
    if not isinstance(e, StreamError):
        return None

    msg = str(e)
    match e.code:
        case (
            StreamErrorCode.NO_ERROR
            | StreamErrorCode.PROTOCOL_ERROR
            | StreamErrorCode.INTERNAL_ERROR
            | StreamErrorCode.FLOW_CONTROL_ERROR
            | StreamErrorCode.SETTINGS_TIMEOUT
            | StreamErrorCode.FRAME_SIZE_ERROR
            | StreamErrorCode.COMPRESSION_ERROR
            | StreamErrorCode.CONNECT_ERROR
        ):
            return ConnectError(Code.INTERNAL, msg)
        case StreamErrorCode.REFUSED_STREAM:
            return ConnectError(Code.UNAVAILABLE, msg)
        case StreamErrorCode.CANCEL:
            # Some servers use CANCEL when deadline expires. We can't differentiate
            # that from normal cancel without checking our own deadline.
            if (t := ctx.timeout_ms) is not None and t <= 0:
                return ConnectError(Code.DEADLINE_EXCEEDED, msg)
            return ConnectError(Code.CANCELED, msg)
        case StreamErrorCode.ENHANCE_YOUR_CALM:
            return ConnectError(Code.RESOURCE_EXHAUSTED, f"Bandwidth exhausted: {msg}")
        case StreamErrorCode.INADEQUATE_SECURITY:
            return ConnectError(
                Code.PERMISSION_DENIED, f"Transport protocol insecure: {msg}"
            )

    return None

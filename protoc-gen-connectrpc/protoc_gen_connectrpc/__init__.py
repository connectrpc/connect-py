from __future__ import annotations

import importlib.metadata
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from protobuf._sanitization import escape_identifier
from protobuf.plugin import File, Ident, Module, Schema, get_comments, run
from protobuf.wkt import MethodOptions

if TYPE_CHECKING:
    from protobuf import DescFile, DescMessage, DescMethod, DescService

_COLLECTIONS_ABC = Module("collections.abc")
_ASYNC_GENERATOR = _COLLECTIONS_ABC.ident("AsyncGenerator", type_only=True)
_ASYNC_ITERATOR = _COLLECTIONS_ABC.ident("AsyncIterator", type_only=True)
_ITERABLE = _COLLECTIONS_ABC.ident("Iterable", type_only=True)
_ITERATOR = _COLLECTIONS_ABC.ident("Iterator", type_only=True)
_MAPPING = _COLLECTIONS_ABC.ident("Mapping", type_only=True)
_TYPING = Module("typing")
_PROTOCOL = _TYPING.ident("Protocol")

_PROTOBUF = Module("protobuf")
_DESC_SERVICE = _PROTOBUF.ident("DescService")

_PYQWEST = Module("pyqwest")
_PYQWEST_CLIENT = _PYQWEST.ident("Client")
_PYQWEST_SYNC_CLIENT = _PYQWEST.ident("SyncClient")

_CONNECTRPC_CLIENT = Module("connectrpc.client")
_CONNECT_CLIENT = _CONNECTRPC_CLIENT.ident("ConnectClient")
_CONNECT_CLIENT_SYNC = _CONNECTRPC_CLIENT.ident("ConnectClientSync")
_CONNECTRPC_CODE = Module("connectrpc.code")
_CODE = _CONNECTRPC_CODE.ident("Code")
_CONNECTRPC_CODEC = Module("connectrpc.codec")
_CODEC = _CONNECTRPC_CODEC.ident("Codec", type_only=True)
_CONNECTRPC_COMPAT = Module("connectrpc.compat")
_GOOGLE_PROTOBUF_BINARY_CODEC = _CONNECTRPC_COMPAT.ident("google_protobuf_binary_codec")
_GOOGLE_PROTOBUF_CODECS = _CONNECTRPC_COMPAT.ident("google_protobuf_codecs")
_CONNECTRPC_COMPRESSION = Module("connectrpc.compression")
_COMPRESSION = _CONNECTRPC_COMPRESSION.ident("Compression", type_only=True)
_COMPRESSION_GZIP = Module("connectrpc.compression.gzip")
_GZIP_COMPRESSION = _COMPRESSION_GZIP.ident("GzipCompression")
_CONNECTRPC_ERRORS = Module("connectrpc.errors")
_CONNECT_ERROR = _CONNECTRPC_ERRORS.ident("ConnectError")
_CONNECTRPC_INTERCEPTOR = Module("connectrpc.interceptor")
_INTERCEPTOR = _CONNECTRPC_INTERCEPTOR.ident("Interceptor", type_only=True)
_INTERCEPTOR_SYNC = _CONNECTRPC_INTERCEPTOR.ident("InterceptorSync", type_only=True)
_CONNECTRPC_METHOD = Module("connectrpc.method")
_METHOD_INFO = _CONNECTRPC_METHOD.ident("MethodInfo")
_IDEMPOTENCY_LEVEL = _CONNECTRPC_METHOD.ident("IdempotencyLevel")
_CONNECTRPC_REQUEST = Module("connectrpc.request")
_HEADERS = _CONNECTRPC_REQUEST.ident("Headers", type_only=True)
_REQUEST_CONTEXT = _CONNECTRPC_REQUEST.ident("RequestContext", type_only=True)
_CONNECTRPC_PROTOCOL = Module("connectrpc.protocol")
_PROTOCOL_TYPE = _CONNECTRPC_PROTOCOL.ident("ProtocolType")
_CONNECTRPC_SERVER = Module("connectrpc.server")
_ENDPOINT = _CONNECTRPC_SERVER.ident("Endpoint")
_ENDPOINT_SYNC = _CONNECTRPC_SERVER.ident("EndpointSync")
_CONNECT_ASGI_APPLICATION = _CONNECTRPC_SERVER.ident("ConnectASGIApplication")
_CONNECT_WSGI_APPLICATION = _CONNECTRPC_SERVER.ident("ConnectWSGIApplication")


class _ProtobufOption(str, Enum):
    """Whether to generate code for google.protobuf or protobuf-py."""

    GOOGLE = "google"
    """Generates code for google.protobuf."""

    PY = "py"
    """Generates code for protobuf-py."""


class _IOOption(str, Enum):
    """Whether to generate synchronous or asynchronous code."""

    SYNC = "sync"
    """Generates only synchronous code."""

    ASYNC = "async"
    """Generates only asynchronous code."""


@dataclass
class Options:
    protobuf: _ProtobufOption = _ProtobufOption.PY
    """The protobuf implementation to generate code for."""

    io: _IOOption | None = field(default=None)
    """The I/O mode for generated code."""


def _generate(schema: Schema[Options]) -> None:
    for file in schema.files_to_generate:
        if not file.services:
            continue
        _generate_file(schema.generate_file(file, "_connect.py"), file, schema.options)


def _generate_file(f: File, desc: DescFile, options: Options) -> None:
    f.preamble(desc)
    if options.protobuf == _ProtobufOption.GOOGLE:
        f.print()
        f.print("_DEFAULT_CODECS = ", _GOOGLE_PROTOBUF_CODECS, "()")
        f.print("_PROTO_BINARY_CODEC = ", _GOOGLE_PROTOBUF_BINARY_CODEC, "()")
        f.print("_GZIP_COMPRESSION = ", _GZIP_COMPRESSION, "()")
        f.print()
    for service in desc.services:
        if not options.io or options.io == _IOOption.ASYNC:
            _generate_async_stubs(f, service, options)
        if not options.io or options.io == _IOOption.SYNC:
            _generate_sync_stubs(f, service, options)


def _generate_async_stubs(f: File, service: DescService, options: Options) -> None:
    service_name = escape_identifier(service.name)
    default_server_codecs = (
        "_DEFAULT_CODECS" if options.protobuf == _ProtobufOption.GOOGLE else "None"
    )
    with f.scope("class ", service_name, "(", _PROTOCOL, "):"):
        _print_docstring(f, service)
        for method in service.methods:
            def_prefix, request_type, response_type = _async_signature(method, options)
            with f.scope(
                def_prefix,
                "def ",
                _method_local_name(method),
                "(self, request: ",
                *request_type,
                ", ctx: ",
                _REQUEST_CONTEXT,
                "[",
                _message_ident(method, method.input, options),
                ", ",
                _message_ident(method, method.output, options),
                "]) -> ",
                *response_type,
                ":",
            ):
                _print_docstring(f, method)
                f.print(
                    "raise ",
                    _CONNECT_ERROR,
                    "(",
                    _CODE,
                    ".UNIMPLEMENTED, 'Not implemented')",
                )
                f.print()
        _generate_desc_method(f, service, options)

    f.print()
    with f.scope(
        "class ",
        escape_identifier(service.name),
        "ASGIApplication(",
        _CONNECT_ASGI_APPLICATION,
        "[",
        service_name,
        "]):",
    ):
        with f.scope("def __init__("):
            f.print("self,")
            f.print(
                "service: ",
                service_name,
                " | ",
                _ASYNC_GENERATOR,
                "[",
                service_name,
                "],",
            )
            f.print("*,")
            f.print("interceptors: ", _ITERABLE, "[", _INTERCEPTOR, "] = (),")
            f.print("read_max_bytes: int | None = None,")
            f.print("compressions: ", _ITERABLE, "[", _COMPRESSION, "] | None = None,")
            f.print(
                "codecs: ",
                _ITERABLE,
                "[",
                _CODEC,
                "] | None = ",
                default_server_codecs,
                ",",
            )
        with f.scope(") -> None:"):
            with f.scope("super().__init__("):
                f.print("service=service,")
                with f.scope("endpoints=lambda svc: {"):
                    for method in service.methods:
                        with f.scope(
                            f'"{_method_url(method)}": ',
                            _ENDPOINT,
                            ".",
                            _endpoint_type(method),
                            "(",
                        ):
                            with f.scope("method=", _METHOD_INFO, "("):
                                f.print('name="', method.name, '",')
                                f.print('service_name="', method.parent.type_name, '",')
                                f.print(
                                    "input=",
                                    _message_ident(method, method.input, options),
                                    ",",
                                )
                                f.print(
                                    "output=",
                                    _message_ident(method, method.output, options),
                                    ",",
                                )
                                f.print(
                                    "idempotency_level=",
                                    _IDEMPOTENCY_LEVEL,
                                    ".",
                                    _idempotency_level(method),
                                    ",",
                                )
                            f.print("),")
                            f.print("function=svc.", _method_local_name(method), ",")
                        f.print("),")
                f.print("},")
                f.print("interceptors=interceptors,")
                f.print("read_max_bytes=read_max_bytes,")
                f.print("compressions=compressions,")
                f.print("codecs=codecs,")
            f.print(")")
        f.print()
        f.print("@property")
        with f.scope("def path(self) -> str:"):
            with f.doc(
                "Returns the URL path to mount the application to when serving multiple applications."
            ):
                pass
            f.print(f'return "/{service.type_name}"')
    f.print()
    f.print()
    with f.scope("class ", service_name, "Client(", _CONNECT_CLIENT, "):"):
        _print_docstring(f, service)

        if options.protobuf == _ProtobufOption.GOOGLE:
            _print_google_compat_client_init(f, _INTERCEPTOR, _PYQWEST_CLIENT)

        for method in service.methods:
            def_prefix, request_type, response_type = _async_signature(method, options)

            with f.scope(def_prefix, "def ", _method_local_name(method), "("):
                f.print("self,")
                f.print("request: ", *request_type, ",")
                f.print("*,")
                f.print(
                    "headers: ",
                    _HEADERS,
                    " | ",
                    _MAPPING,
                    "[str, str]",
                    " | None = None, ",
                )
                f.print("timeout_ms: int | None = None,")
                if _supports_get(method):
                    f.print("use_get: bool = False,")
            with f.scope(") -> ", *response_type, ":"):
                _print_docstring(f, method)
                await_return = (
                    "await "
                    if method.method_kind in ("unary", "client_streaming")
                    else ""
                )
                with f.scope(
                    "return ",
                    await_return,
                    "self.",
                    _client_execute_method(method),
                    "(",
                ):
                    f.print("request=request,")
                    with f.scope("method=", _METHOD_INFO, "("):
                        f.print('name="', method.name, '",')
                        f.print('service_name="', method.parent.type_name, '",')
                        f.print(
                            "input=", _message_ident(method, method.input, options), ","
                        )
                        f.print(
                            "output=",
                            _message_ident(method, method.output, options),
                            ",",
                        )
                        f.print(
                            "idempotency_level=",
                            _IDEMPOTENCY_LEVEL,
                            ".",
                            _idempotency_level(method),
                            ",",
                        )
                    f.print("),")
                    f.print("headers=headers,")
                    f.print("timeout_ms=timeout_ms,")
                    if _supports_get(method):
                        f.print("use_get=use_get,")
                f.print(")")
            f.print()


def _generate_sync_stubs(f: File, service: DescService, options: Options) -> None:
    service_name = f"{escape_identifier(service.name)}"
    default_codecs = (
        "_DEFAULT_CODECS" if options.protobuf == _ProtobufOption.GOOGLE else "None"
    )
    with f.scope("class ", service_name, "Sync(", _PROTOCOL, "):"):
        _print_docstring(f, service)
        for method in service.methods:
            request_type, response_type = _sync_signature(method, options)
            with f.scope(
                "def ",
                _method_local_name(method),
                "(self, request: ",
                *request_type,
                ", ctx: ",
                _REQUEST_CONTEXT,
                "[",
                _message_ident(method, method.input, options),
                ", ",
                _message_ident(method, method.output, options),
                "]) -> ",
                *response_type,
                ":",
            ):
                _print_docstring(f, method)
                f.print(
                    "raise ",
                    _CONNECT_ERROR,
                    "(",
                    _CODE,
                    ".UNIMPLEMENTED, 'Not implemented')",
                )
                f.print()
        _generate_desc_method(f, service, options)

    f.print()
    with f.scope(
        "class ",
        escape_identifier(service.name),
        "WSGIApplication(",
        _CONNECT_WSGI_APPLICATION,
        "):",
    ):
        with f.scope("def __init__("):
            f.print("self,")
            f.print("service: ", service_name, "Sync,")
            f.print("interceptors: ", _ITERABLE, "[", _INTERCEPTOR_SYNC, "] = (),")
            f.print("read_max_bytes: int | None = None,")
            f.print("compressions: ", _ITERABLE, "[", _COMPRESSION, "] | None = None,")
            f.print(
                "codecs: ", _ITERABLE, "[", _CODEC, "] | None = ", default_codecs, ","
            )
        with f.scope(") -> None:"):
            with f.scope("super().__init__("):
                with f.scope("endpoints={"):
                    for method in service.methods:
                        with f.scope(
                            f'"{_method_url(method)}": ',
                            _ENDPOINT_SYNC,
                            ".",
                            _endpoint_type(method),
                            "(",
                        ):
                            with f.scope("method=", _METHOD_INFO, "("):
                                f.print('name="', method.name, '",')
                                f.print('service_name="', method.parent.type_name, '",')
                                f.print(
                                    "input=",
                                    _message_ident(method, method.input, options),
                                    ",",
                                )
                                f.print(
                                    "output=",
                                    _message_ident(method, method.output, options),
                                    ",",
                                )
                                f.print(
                                    "idempotency_level=",
                                    _IDEMPOTENCY_LEVEL,
                                    ".",
                                    _idempotency_level(method),
                                    ",",
                                )
                            f.print("),")
                            f.print(
                                "function=service.", _method_local_name(method), ","
                            )
                        f.print("),")
                f.print("},")
                f.print("interceptors=interceptors,")
                f.print("read_max_bytes=read_max_bytes,")
                f.print("compressions=compressions,")
                f.print("codecs=codecs,")
            f.print(")")
        f.print()
        f.print("@property")
        with f.scope("def path(self) -> str:"):
            with f.doc(
                "Returns the URL path to mount the application to when serving multiple applications."
            ):
                pass
            f.print(f'return "/{service.type_name}"')
    f.print()
    f.print()
    with f.scope("class ", service_name, "ClientSync(", _CONNECT_CLIENT_SYNC, "):"):
        _print_docstring(f, service)

        if options.protobuf == _ProtobufOption.GOOGLE:
            _print_google_compat_client_init(f, _INTERCEPTOR_SYNC, _PYQWEST_SYNC_CLIENT)

        for method in service.methods:
            request_type, response_type = _sync_signature(method, options)

            with f.scope("def ", _method_local_name(method), "("):
                f.print("self,")
                f.print("request: ", *request_type, ",")
                f.print("*,")
                f.print(
                    "headers: ",
                    _HEADERS,
                    " | ",
                    _MAPPING,
                    "[str, str]",
                    " | None = None, ",
                )
                f.print("timeout_ms: int | None = None,")
                if _supports_get(method):
                    f.print("use_get: bool = False,")
            with f.scope(") -> ", *response_type, ":"):
                _print_docstring(f, method)
                with f.scope("return ", "self.", _client_execute_method(method), "("):
                    f.print("request=request,")
                    with f.scope("method=", _METHOD_INFO, "("):
                        f.print('name="', method.name, '",')
                        f.print('service_name="', method.parent.type_name, '",')
                        f.print(
                            "input=", _message_ident(method, method.input, options), ","
                        )
                        f.print(
                            "output=",
                            _message_ident(method, method.output, options),
                            ",",
                        )
                        f.print(
                            "idempotency_level=",
                            _IDEMPOTENCY_LEVEL,
                            ".",
                            _idempotency_level(method),
                            ",",
                        )
                    f.print("),")
                    f.print("headers=headers,")
                    f.print("timeout_ms=timeout_ms,")
                    if _supports_get(method):
                        f.print("use_get=use_get,")
                f.print(")")


def _print_google_compat_client_init(
    f: File, interceptor_type: Ident, http_client_type: Ident
) -> None:

    with f.scope("def __init__("):
        f.print("self,")
        f.print("address: str,")
        f.print("*,")
        f.print("codec: ", _CODEC, " | None = _PROTO_BINARY_CODEC,")
        f.print("protocol: ", _PROTOCOL_TYPE, " = ", _PROTOCOL_TYPE, ".CONNECT,")
        f.print(
            "accept_compression: ", _ITERABLE, "[", _COMPRESSION, "] | None = None,"
        )
        f.print("send_compression: ", _COMPRESSION, " | None = _GZIP_COMPRESSION,")
        f.print("timeout_ms: int | None = None,")
        f.print("read_max_bytes: int | None = None,")
        f.print("interceptors: ", _ITERABLE, "[", interceptor_type, "] = (),")
        f.print("http_client: ", http_client_type, " | None = None,")
    with f.scope(") -> None:"):
        with f.scope("super().__init__("):
            f.print("address=address,")
            f.print("codec=codec,")
            f.print("protocol=protocol,")
            f.print("accept_compression=accept_compression,")
            f.print("send_compression=send_compression,")
            f.print("timeout_ms=timeout_ms,")
            f.print("read_max_bytes=read_max_bytes,")
            f.print("interceptors=interceptors,")
            f.print("http_client=http_client,")
        f.print(")")


def _async_signature(
    method: DescMethod, options: Options
) -> tuple[str, tuple[object, ...], tuple[object, ...]]:
    def_prefix = "async " if method.method_kind in ("unary", "client_streaming") else ""
    base_request_type = _message_ident(method, method.input, options)
    request_type = (
        (base_request_type,)
        if method.method_kind in ("unary", "server_streaming")
        else (_ASYNC_ITERATOR, "[", base_request_type, "]")
    )
    base_response_type = _message_ident(method, method.output, options)
    response_type = (
        (base_response_type,)
        if method.method_kind in ("unary", "client_streaming")
        else (_ASYNC_ITERATOR, "[", base_response_type, "]")
    )
    return (def_prefix, request_type, response_type)


def _message_ident(method: DescMethod, message: DescMessage, options: Options) -> Ident:
    if options.protobuf == _ProtobufOption.PY:
        return Ident.for_desc(message)
    parts = [message.name]
    parent = message.parent
    while parent:
        parts.append(parent.name)
        parent = parent.parent
    name = ".".join(reversed(parts))
    if message.file != method.parent.file:
        mod = Module(_module_name(message.file.name))
    else:
        mod = Module(f".{_module_name(message.file.name)}")
    return mod.ident(name)


def _print_docstring(f: File, desc: DescService | DescMethod) -> None:
    comments = get_comments(desc)
    text = ""
    if comments.leading:
        text += comments.leading.removesuffix("\n")
    if comments.trailing:
        if len(text) > 0:
            text += "\n\n"
        text += comments.trailing.removesuffix("\n")
    if len(text) > 0:
        text += "\n"

    if not text:
        return

    with f.doc():
        for line in text.splitlines():
            # Comments in protobuf often start with a space, this
            # leads to weird indentation in the generated docstring, so we remove it.
            f.print(line.removeprefix(" "))


def _generate_desc_method(f: File, service: DescService, options: Options) -> None:
    if options.protobuf == _ProtobufOption.PY:
        f.print("@classmethod")
        with f.scope("def desc(cls) -> ", _DESC_SERVICE, ":"):
            with f.doc("Returns the descriptor for this service."):
                pass
            f.print(
                "return next(s for s in ",
                service.file,
                ".desc().services if s.type_name == '",
                service.type_name,
                "')",
            )


# https://github.com/grpc/grpc/blob/0dd1b2cad21d89984f9a1b3c6249d649381eeb65/src/compiler/python_generator_helpers.h#L67
def _module_name(filename: str) -> str:
    filename = filename.removesuffix(".proto")
    filename = filename.replace("-", "_")
    filename = filename.replace("/", ".")
    return f"{filename}_pb2"


def _sync_signature(
    method: DescMethod, options: Options
) -> tuple[tuple[object, ...], tuple[object, ...]]:
    base_request_type = _message_ident(method, method.input, options)
    request_type = (
        (base_request_type,)
        if method.method_kind in ("unary", "server_streaming")
        else (_ITERATOR, "[", base_request_type, "]")
    )
    base_response_type = _message_ident(method, method.output, options)
    response_type = (
        (base_response_type,)
        if method.method_kind in ("unary", "client_streaming")
        else (_ITERATOR, "[", base_response_type, "]")
    )
    return (request_type, response_type)


def _endpoint_type(method: DescMethod) -> str:
    match method.method_kind:
        case "unary":
            return "unary"
        case "client_streaming":
            return "client_stream"
        case "server_streaming":
            return "server_stream"
        case "bidi_streaming":
            return "bidi_stream"


def _client_execute_method(method: DescMethod) -> str:
    match method.method_kind:
        case "unary":
            return "execute_unary"
        case "client_streaming":
            return "execute_client_stream"
        case "server_streaming":
            return "execute_server_stream"
        case "bidi_streaming":
            return "execute_bidi_stream"


def _method_local_name(method: DescMethod) -> str:
    return escape_identifier(_pascal_to_snake_case(method.name))


def _method_url(desc: DescMethod) -> str:
    return f"/{desc.parent.type_name}/{desc.name}"


def _idempotency_level(desc: DescMethod) -> str:
    match desc.idempotency:
        case MethodOptions.IdempotencyLevel.IDEMPOTENCY_UNKNOWN:
            return "UNKNOWN"
        case MethodOptions.IdempotencyLevel.NO_SIDE_EFFECTS:
            return "NO_SIDE_EFFECTS"
        case MethodOptions.IdempotencyLevel.IDEMPOTENT:
            return "IDEMPOTENT"
    msg = f"Unknown idempotency level: {desc.idempotency}"
    raise ValueError(msg)


def _supports_get(desc: DescMethod) -> bool:
    return (
        desc.method_kind == "unary"
        and desc.idempotency == MethodOptions.IdempotencyLevel.NO_SIDE_EFFECTS
    )


_RE_UPPER_TO_LOWER = re.compile("([^_])([A-Z][a-z]+)")
_RE_LOWER_TO_UPPER = re.compile("([a-z])([A-Z])")


def _pascal_to_snake_case(text: str) -> str:
    """Convert a PascalCase enum name to snake_case."""
    s1 = _RE_UPPER_TO_LOWER.sub(r"\1_\2", text)
    return _RE_LOWER_TO_UPPER.sub(r"\1_\2", s1).lower()


def main() -> None:
    run(
        "protoc-gen-connectrpc-py",
        importlib.metadata.version("protoc-gen-connectrpc"),
        Options,
        _generate,
    )

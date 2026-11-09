from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from connectrpc._codegen import method_local_name

from .connectrpc.example.haberdasher_connect import Haberdasher

if TYPE_CHECKING:
    from protobuf import DescMethod


def _method(name: str) -> DescMethod:
    return cast("DescMethod", SimpleNamespace(name=name))


@pytest.mark.parametrize(
    ("proto_name", "expected"),
    [
        ("MakeHat", "make_hat"),
        ("ListParts", "list_parts"),
        ("HTTPGet", "http_get"),
        ("GetHTTPResponse", "get_http_response"),
        ("V1Lookup", "v1_lookup"),
        ("Snake_Case", "snake_case"),
        ("lower", "lower"),
        # Python keywords are escaped with a trailing underscore.
        ("Import", "import_"),
        ("Class", "class_"),
        # Names reserved by generated service and client classes are escaped too.
        ("Desc", "desc_"),
        ("ExecuteUnary", "execute_unary_"),
        ("ExecuteBidiStream", "execute_bidi_stream_"),
    ],
)
def test_method_local_name(proto_name: str, expected: str) -> None:
    assert method_local_name(_method(proto_name)) == expected


def test_method_local_name_matches_generated_code() -> None:
    for method in Haberdasher.desc().methods:
        name = method_local_name(method)
        assert callable(getattr(Haberdasher, name)), name

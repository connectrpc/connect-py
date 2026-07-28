from __future__ import annotations

from typing import NoReturn

import pytest
from protobuf import Oneof
from protobuf.wkt import Any as AnyPb, Duration, Struct, Value
from pyqwest import Client, SyncClient
from pyqwest.testing import ASGITransport, WSGITransport

from connectrpc._protocol import ConnectWireError
from connectrpc.code import Code
from connectrpc.errors import ConnectError, ErrorDetail

from .connectrpc.example.haberdasher_connect import (
    Haberdasher,
    HaberdasherASGIApplication,
    HaberdasherClient,
    HaberdasherClientSync,
    HaberdasherSync,
    HaberdasherWSGIApplication,
)
from .connectrpc.example.haberdasher_pb import Size


def test_details_sync() -> None:
    class DetailsHaberdasherSync(HaberdasherSync):
        def make_hat(self, _request, _ctx) -> NoReturn:
            raise ConnectError(
                Code.RESOURCE_EXHAUSTED,
                "Resource exhausted",
                details=[
                    Struct(
                        fields={"animal": Value(kind=Oneof("string_value", "bear"))}
                    ),
                    AnyPb.pack(
                        Struct(
                            fields={"color": Value(kind=Oneof("string_value", "red"))}
                        )
                    ),
                ],
            )

    app = HaberdasherWSGIApplication(DetailsHaberdasherSync())
    transport = WSGITransport(app)
    with (
        HaberdasherClientSync(
            "http://localhost", http_client=SyncClient(transport)
        ) as client,
        pytest.raises(ConnectError) as exc_info,
    ):
        client.make_hat(request=Size(inches=10))
    assert not transport.app_exception
    assert exc_info.value.code == Code.RESOURCE_EXHAUSTED
    assert exc_info.value.message == "Resource exhausted"
    assert len(exc_info.value.details) == 2
    s0 = exc_info.value.details[0].value(Struct)
    assert s0 is not None
    assert s0.fields["animal"].kind == Oneof("string_value", "bear")
    s1 = exc_info.value.details[1].value(Struct)
    assert s1 is not None
    assert s1.fields["color"].kind == Oneof("string_value", "red")


@pytest.mark.asyncio
async def test_details_async() -> None:
    class DetailsHaberdasher(Haberdasher):
        async def make_hat(self, _request, _ctx) -> NoReturn:
            raise ConnectError(
                Code.RESOURCE_EXHAUSTED,
                "Resource exhausted",
                details=[
                    Struct(
                        fields={"animal": Value(kind=Oneof("string_value", "bear"))}
                    ),
                    AnyPb.pack(
                        Struct(
                            fields={"color": Value(kind=Oneof("string_value", "red"))}
                        )
                    ),
                ],
            )

    app = HaberdasherASGIApplication(DetailsHaberdasher())
    transport = ASGITransport(app)
    async with HaberdasherClient(
        "http://localhost", http_client=Client(transport=transport)
    ) as client:
        with pytest.raises(ConnectError) as exc_info:
            await client.make_hat(request=Size(inches=10))
    assert exc_info.value.code == Code.RESOURCE_EXHAUSTED
    assert exc_info.value.message == "Resource exhausted"
    assert len(exc_info.value.details) == 2
    s0 = exc_info.value.details[0].value(Struct)
    assert s0 is not None
    assert s0.fields["animal"].kind == Oneof("string_value", "bear")
    s1 = exc_info.value.details[1].value(Struct)
    assert s1 is not None
    assert s1.fields["color"].kind == Oneof("string_value", "red")


def test_error_detail_debug_field() -> None:
    """Debug field is populated when proto descriptors are available."""
    wire_error = ConnectWireError.from_exception(
        ConnectError(
            Code.RESOURCE_EXHAUSTED,
            "Resource exhausted",
            details=[
                Struct(fields={"animal": Value(kind=Oneof("string_value", "bear"))})
            ],
        )
    )
    data = wire_error.to_dict()
    assert len(data["details"]) == 1
    detail = data["details"][0]
    assert "debug" in detail
    # Struct uses proto-JSON well-known type mapping: becomes a plain JSON object
    assert detail["debug"] == {"animal": "bear"}


def test_error_detail_debug_field_well_known_type() -> None:
    """Debug field uses proto-JSON well-known type representation (e.g. Duration as string)."""
    wire_error = ConnectWireError.from_exception(
        ConnectError(
            Code.RESOURCE_EXHAUSTED, "Resource exhausted", details=[Duration(seconds=1)]
        )
    )
    data = wire_error.to_dict()
    assert len(data["details"]) == 1
    detail = data["details"][0]
    assert "debug" in detail
    # Duration uses proto-JSON well-known type mapping: serializes as "1s"
    assert detail["debug"] == "1s"


def test_error_detail_debug_field_absent_for_unknown_type() -> None:
    """Debug field is omitted when no descriptor is available for the type."""
    unknown_detail = ErrorDetail(
        AnyPb(
            type_url="type.googleapis.com/completely.Unknown.Message", value=b"\x08\x01"
        )
    )
    wire_error = ConnectWireError(
        code=Code.INTERNAL, message="test", details=[unknown_detail]
    )
    data = wire_error.to_dict()
    assert len(data["details"]) == 1
    detail = data["details"][0]
    assert "debug" not in detail

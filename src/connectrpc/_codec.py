from __future__ import annotations

from typing import Protocol, TypeVar

from protobuf import Message, Registry
from protobuf.wkt import (
    api_pb,
    cpp_features_pb,
    descriptor_pb,
    duration_pb,
    empty_pb,
    field_mask_pb,
    go_features_pb,
    java_features_pb,
    source_context_pb,
    struct_pb,
    timestamp_pb,
    type_pb,
    wrappers_pb,
)

CODEC_NAME_PROTO = "proto"
CODEC_NAME_JSON = "json"


DEFAULT_REGISTRY = Registry(
    api_pb.desc(),
    cpp_features_pb.desc(),
    descriptor_pb.desc(),
    duration_pb.desc(),
    empty_pb.desc(),
    field_mask_pb.desc(),
    go_features_pb.desc(),
    java_features_pb.desc(),
    source_context_pb.desc(),
    struct_pb.desc(),
    timestamp_pb.desc(),
    type_pb.desc(),
    wrappers_pb.desc(),
)


T_contra = TypeVar("T_contra", contravariant=True)
U = TypeVar("U")
V = TypeVar("V", bound=Message)


class Codec(Protocol[T_contra, U]):
    def name(self) -> str:
        """Return the name of the codec.

        This corresponds to the content-type used in requests.
        """
        ...

    def encode(self, message: T_contra) -> bytes:
        """Marshals the given message."""
        ...

    def decode(self, data: bytes | bytearray, message_class: type[U]) -> U:
        """Unmarshals the given message."""
        ...


class ProtoBinaryCodec(Codec[Message, V]):
    """Codec for the Protocol Buffers binary format."""

    def name(self) -> str:
        return "proto"

    def encode(self, message: Message) -> bytes:
        return message.to_binary()

    def decode(self, data: bytes | bytearray, message_class: type[V]) -> V:
        return message_class.from_binary(data)  # ty: ignore[invalid-argument-type] # TODO: Fix type in protobuf-py


class ProtoJSONCodec(Codec[Message, V]):
    """Codec for the Protocol Buffers JSON format."""

    def __init__(
        self,
        name: str = "json",
        *,
        registry: Registry | None = None,
        ignore_unknown_fields: bool = True,
    ) -> None:
        self._name = name
        self._registry = registry or DEFAULT_REGISTRY
        self._ignore_unknown_fields = ignore_unknown_fields

    def name(self) -> str:
        return self._name

    def encode(self, message: Message) -> bytes:
        return message.to_json(registry=self._registry).encode()

    def decode(self, data: bytes | bytearray, message_class: type[V]) -> V:
        return message_class.from_json(
            data,
            registry=self._registry,
            ignore_unknown_fields=self._ignore_unknown_fields,
        )


_proto_binary_codec = ProtoBinaryCodec()
_proto_json_codec = ProtoJSONCodec()
_default_codecs: list[Codec] = [_proto_binary_codec, _proto_json_codec]


def get_default_codecs() -> list[Codec]:
    return _default_codecs


def proto_binary_codec() -> Codec:
    """Return the Protocol Buffers binary codec."""
    return _proto_binary_codec


def proto_json_codec(
    registry: Registry | None = None, *, ignore_unknown_fields: bool = True
) -> Codec:
    """Return the Protocol Buffers JSON codec.

    Args:
        registry: An optional protobuf Registry to use for marshaling Any and extensions in messages.
                  If not provided, a default registry containing WKTs will be used.
        ignore_unknown_fields: Whether to ignore unknown fields when decoding JSON messages. If not
                  provided, defaults to True.

    """
    if registry or not ignore_unknown_fields:
        return ProtoJSONCodec(
            name=CODEC_NAME_JSON,
            registry=registry,
            ignore_unknown_fields=ignore_unknown_fields,
        )
    return _proto_json_codec

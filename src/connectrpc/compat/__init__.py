"""Support for the [google.protobuf][] Protobuf implementation with Connect."""

from __future__ import annotations

__all__ = [
    "google_protobuf_binary_codec",
    "google_protobuf_codecs",
    "google_protobuf_json_codec",
]

from ._codec import (
    google_protobuf_binary_codec,
    google_protobuf_codecs,
    google_protobuf_json_codec,
)

from __future__ import annotations

from typing import TYPE_CHECKING

from protobuf.wkt import FileDescriptorProto, FileDescriptorSet

if TYPE_CHECKING:
    from google.protobuf.descriptor import FileDescriptor
    from protobuf import DescFile


def google_file_desc(file: FileDescriptor) -> DescFile:
    """Return the protobuf-py descriptor for a google.protobuf file descriptor.

    The descriptor is for introspection only. Its message descriptors are not
    associated with the google.protobuf message classes, so their type property
    creates protobuf-py message classes rather than returning the google.protobuf
    ones.
    """
    files: list[FileDescriptorProto] = []
    seen: set[str] = set()

    def add(file: FileDescriptor) -> None:
        if file.name in seen:
            return
        seen.add(file.name)
        # Dependencies must be added to the registry before their dependents.
        for dependency in file.dependencies:
            add(dependency)
        files.append(FileDescriptorProto.from_binary(file.serialized_pb))

    add(file)
    desc = FileDescriptorSet(file=files).to_registry().file(file.name)
    if desc is None:
        msg = f"registry missing file {file.name}"
        raise RuntimeError(msg)
    return desc

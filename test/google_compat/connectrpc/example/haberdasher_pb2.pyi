from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Hat(_message.Message):
    __slots__ = ("size", "color", "name")
    class Part(_message.Message):
        __slots__ = ("id",)
        ID_FIELD_NUMBER: _ClassVar[int]
        id: str
        def __init__(self, id: _Optional[str] = ...) -> None: ...
    SIZE_FIELD_NUMBER: _ClassVar[int]
    COLOR_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    size: int
    color: str
    name: str
    def __init__(self, size: _Optional[int] = ..., color: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class Size(_message.Message):
    __slots__ = ("inches", "description")
    INCHES_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    inches: int
    description: str
    def __init__(self, inches: _Optional[int] = ..., description: _Optional[str] = ...) -> None: ...

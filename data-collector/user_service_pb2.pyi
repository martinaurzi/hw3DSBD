from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class UserCheckRequest(_message.Message):
    __slots__ = ("email",)
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    email: str
    def __init__(self, email: _Optional[str] = ...) -> None: ...

class UserCheckResponse(_message.Message):
    __slots__ = ("exists", "message")
    EXISTS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    exists: bool
    message: str
    def __init__(self, exists: bool = ..., message: _Optional[str] = ...) -> None: ...

class DeleteUserInterestsResponse(_message.Message):
    __slots__ = ("deleted", "message")
    DELETED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    message: str
    def __init__(self, deleted: bool = ..., message: _Optional[str] = ...) -> None: ...

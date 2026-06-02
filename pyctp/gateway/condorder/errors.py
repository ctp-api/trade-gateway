from __future__ import annotations

from enum import Enum


class CondOrderErrorCode(str, Enum):
    DUPLICATE = "duplicate"
    NOT_FOUND = "not_found"
    NOT_ACTIVE = "not_active"
    INVALID = "invalid"
    TRIGGER_FAILED = "trigger_failed"


class CondOrderError(Exception):
    def __init__(self, code: CondOrderErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message

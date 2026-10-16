"""
anedya_sparks._spark_response
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Response data model returned from Sparks Lambda handlers.
"""

from __future__ import annotations

from enum import IntEnum
from typing import Any

import cbor2


class StatusCode(IntEnum):
    """
    Standard status codes for Sparks responses, matching gRPC status codes.
    """

    OK = 0
    CANCELLED = 1
    UNKNOWN = 2
    INVALID_ARGUMENT = 3
    DEADLINE_EXCEEDED = 4
    NOT_FOUND = 5
    ALREADY_EXISTS = 6
    PERMISSION_DENIED = 7
    RESOURCE_EXHAUSTED = 8
    FAILED_PRECONDITION = 9
    ABORTED = 10
    OUT_OF_RANGE = 11
    UNIMPLEMENTED = 12
    INTERNAL = 13
    UNAVAILABLE = 14
    DATA_LOSS = 15
    UNAUTHENTICATED = 16


class SparkResponse:
    """
    Structured response object returned by Sparks Lambda handlers.

    Attributes:
        status_code: StatusCode enum (or integer) indicating task execution outcome.
        error_msg:   Error message string if an error occurred.
        payload:     Raw response bytes.
    """

    def __init__(
        self,
        status_code: StatusCode = StatusCode.OK,
        error_msg: str = "",
        payload: bytes = b"",
        **kwargs: Any,
    ) -> None:

        self.status_code = status_code
        self.error_msg = kwargs.get("errorMsg", error_msg)

        if not isinstance(payload, (bytes, bytearray)):
            raise TypeError(f"payload must be bytes, got {type(payload).__name__}")
        self.payload: bytes = bytes(payload)

    @property
    def StatusCode(self) -> StatusCode:
        """Alias for status_code."""
        return self.status_code

    @property
    def errorMsg(self) -> str:
        """Alias for error_msg."""
        return self.error_msg

    def to_dict(self) -> dict[str, Any]:
        """Convert response to dictionary."""
        return {
            "status_code": int(self.status_code),
            "error_msg": self.error_msg,
            "payload": self.payload,
        }

    def to_cbor(self) -> bytes:
        """Encode response as CBOR bytes."""
        return cbor2.dumps(self.to_dict())

    def __repr__(self) -> str:
        code_name = self.status_code.name if isinstance(self.status_code, StatusCode) else str(self.status_code)
        return (
            f"SparkResponse(status_code={code_name}, "
            f"error_msg={self.error_msg!r}, payload={len(self.payload)} bytes)"
        )

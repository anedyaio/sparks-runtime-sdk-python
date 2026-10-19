"""
anedya_sparks._spark_invocation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Invocation data model passed to Sparks Lambda handlers.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import cbor2

logger = logging.getLogger("anedya.sparks.invocation")


def _to_bytes(val: Any) -> bytes:
    """Safely convert a value (bytes, str, int, etc.) to bytes."""
    if isinstance(val, (bytes, bytearray)):
        return bytes(val)
    if isinstance(val, str):
        return val.encode("utf-8")
    if val is None:
        return b""
    return str(val).encode("utf-8")


@dataclass
class SparkInvocation:
    """
    Structured invocation object passed to Sparks Lambda handlers.

    Attributes:
        function_id: Identifier of the function being invoked (bytes).
        task_id:     Unique identifier of the task invocation (bytes).
        trigger_id:  Identifier of the trigger initiating the function (bytes).
        event_type:  Type/name of the incoming event (string).
        timestamp:   Epoch timestamp when the event was generated (int).
        payload:     Raw invocation payload bytes (CBOR-encoded).
    """

    function_id: bytes = b""
    task_id: bytes = b""
    trigger_id: bytes = b""
    event_type: str = ""
    timestamp: int = 0
    payload: bytes = b""

    def cbor_payload(self) -> Any:
        """Helper to decode payload from CBOR if needed."""
        if self.payload:
            try:
                return cbor2.loads(self.payload)
            except Exception as exc:
                logger.warning("Failed to decode payload with CBOR: %s", exc)
                return self.payload
        return None

    @classmethod
    def from_proto(cls, response: Any) -> SparkInvocation:
        """Construct a SparkInvocation instance directly from a NextInvocationResponse protobuf message."""
        return cls(
            function_id=_to_bytes(getattr(response, "function_id", b"")),
            task_id=_to_bytes(getattr(response, "task_id", b"")),
            trigger_id=_to_bytes(getattr(response, "trigger_id", b"")),
            event_type=str(getattr(response, "event_type", "")),
            timestamp=int(getattr(response, "timestamp", 0)),
            payload=_to_bytes(getattr(response, "payload", b"")),
        )

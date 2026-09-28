"""
anedya_sparks._event
~~~~~~~~~~~~~~~~~~~~

Event data model passed to Sparks Lambda handlers.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union

try:
    import cbor2
except ImportError:
    try:
        from anedya_sparks import _cbor as cbor2
    except ImportError:
        try:
            from . import _cbor as cbor2
        except ImportError:
            try:
                import _cbor as cbor2
            except ImportError:
                cbor2 = None

logger = logging.getLogger("anedya.sparks.event")


@dataclass
class Event:
    """
    Structured event object passed to Sparks Lambda handlers.

    Attributes:
        request_id:   Unique identifier for the invocation / request (alias: requestId).
        event_id:     Unique identifier for the event (alias: eventId).
        trigger_type: Type of trigger initiating the function (alias: triggerType).
        timestamp:    Epoch timestamp when the event was generated (or timestamp string).
        context:      Contextual metadata about the execution environment.
        payload:      Invocation payload, decoded from CBOR bytes (or raw bytes/str/dict).
        trigger_data: Map containing trigger-specific metadata, decoded from CBOR bytes (alias: triggerData / triggerdata).
        raw_payload:  Raw unparsed payload bytes as received from runtime.
    """

    request_id: str = ""
    event_id: str = ""
    trigger_type: str = ""
    timestamp: Union[int, float, str] = 0
    context: Dict[str, Any] = field(default_factory=dict)
    payload: Union[bytes, str, Dict[str, Any], Any] = b""
    trigger_data: Dict[str, Any] = field(default_factory=dict)
    raw_payload: bytes = b""

    # ── Property aliases for camelCase compatibility ──────────────────────────

    @property
    def requestId(self) -> str:
        """Alias for request_id."""
        return self.request_id

    @property
    def eventId(self) -> str:
        """Alias for event_id."""
        return self.event_id

    @property
    def triggerType(self) -> str:
        """Alias for trigger_type."""
        return self.trigger_type

    @property
    def triggerData(self) -> Dict[str, Any]:
        """Alias for trigger_data."""
        return self.trigger_data

    @property
    def triggerdata(self) -> Dict[str, Any]:
        """Alias for trigger_data."""
        return self.trigger_data

    # ── Helper methods ─────────────────────────────────────────────────────────

    def cbor_payload(self) -> Any:
        """
        Parse and return the payload as decoded CBOR.

        If payload is already decoded (dict/list/primitive), returns it as-is.
        If raw_payload or payload is bytes/bytearray, decodes with cbor2.loads().
        """
        if isinstance(self.payload, (dict, list, int, float, bool)) and not isinstance(self.payload, (bytes, bytearray)):
            return self.payload
        raw = self.raw_payload if self.raw_payload else self.payload
        if cbor2 is not None and isinstance(raw, (bytes, bytearray)) and raw:
            try:
                return cbor2.loads(raw)
            except Exception:
                pass
        return self.payload

    def json_payload(self) -> Any:
        """
        Parse and return the payload as JSON.

        If payload is already a dict/list/primitive, returns it as-is.
        If payload is bytes or str, parses it with json.loads().
        """
        if isinstance(self.payload, (dict, list, int, float, bool)) and not isinstance(self.payload, (bytes, bytearray)):
            return self.payload
        if isinstance(self.payload, (bytes, bytearray)):
            return json.loads(self.payload.decode("utf-8"))
        if isinstance(self.payload, str):
            return json.loads(self.payload)
        return json.loads(str(self.payload))

    def text_payload(self, encoding: str = "utf-8", errors: str = "replace") -> str:
        """
        Return the payload decoded as a text string.
        """
        raw = self.raw_payload if self.raw_payload else self.payload
        if isinstance(raw, (bytes, bytearray)):
            return raw.decode(encoding, errors=errors)
        return str(self.payload)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Event object to a dictionary representation."""
        return {
            "requestId": self.request_id,
            "eventId": self.event_id,
            "triggerType": self.trigger_type,
            "timestamp": self.timestamp,
            "context": self.context,
            "payload": self.payload,
            "triggerData": self.trigger_data,
        }

    # ── Factory methods ────────────────────────────────────────────────────────

    @classmethod
    def from_proto(cls, response: Any) -> Event:
        """
        Construct an Event instance directly from a NextInvocationResponse protobuf message.
        Decodes CBOR-encoded payload and trigger_data.
        """
        task_id = getattr(response, "taskId", "")
        request_id = getattr(response, "request_id", "") or task_id
        event_id = getattr(response, "event_id", "")
        trigger_type = getattr(response, "trigger_type", "")
        raw_timestamp = getattr(response, "timestamp", "")

        # Parse timestamp
        timestamp: Union[int, float, str] = 0
        if isinstance(raw_timestamp, (int, float)):
            timestamp = raw_timestamp
        elif isinstance(raw_timestamp, str) and raw_timestamp.strip():
            try:
                timestamp = int(raw_timestamp) if raw_timestamp.isdigit() else float(raw_timestamp)
            except ValueError:
                timestamp = raw_timestamp

        # Parse context (stored as bytes in protobuf)
        raw_context = getattr(response, "context", b"")
        context: Dict[str, Any] = {}
        if isinstance(raw_context, dict):
            context = raw_context
        elif isinstance(raw_context, (bytes, bytearray)) and raw_context:
            try:
                parsed_ctx = cbor2.loads(raw_context)
                context = parsed_ctx if isinstance(parsed_ctx, dict) else {"data": parsed_ctx}
            except Exception:
                try:
                    parsed_ctx = json.loads(raw_context.decode("utf-8"))
                    context = parsed_ctx if isinstance(parsed_ctx, dict) else {"data": parsed_ctx}
                except (ValueError, UnicodeDecodeError):
                    context = {"data": raw_context}
        elif isinstance(raw_context, str) and raw_context.strip():
            try:
                parsed_ctx = json.loads(raw_context)
                context = parsed_ctx if isinstance(parsed_ctx, dict) else {"data": parsed_ctx}
            except (ValueError, UnicodeDecodeError):
                context = {"data": raw_context}

        # Parse payload (stored as bytes in protobuf, encoded in CBOR)
        raw_payload = getattr(response, "payload", b"")
        payload: Any = raw_payload
        if isinstance(raw_payload, (bytes, bytearray)) and raw_payload:
            try:
                payload = cbor2.loads(raw_payload)
            except Exception:
                try:
                    payload = json.loads(raw_payload.decode("utf-8"))
                except Exception:
                    payload = raw_payload

        # Parse trigger_data (stored as bytes in protobuf, encoded in CBOR)
        raw_trigger_data = getattr(response, "trigger_data", b"")
        trigger_data: Dict[str, Any] = {}
        if isinstance(raw_trigger_data, dict):
            trigger_data = raw_trigger_data
        elif isinstance(raw_trigger_data, (bytes, bytearray)) and raw_trigger_data:
            try:
                parsed_td = cbor2.loads(raw_trigger_data)
                trigger_data = parsed_td if isinstance(parsed_td, dict) else {"data": parsed_td}
            except Exception:
                try:
                    parsed_td = json.loads(raw_trigger_data.decode("utf-8"))
                    trigger_data = parsed_td if isinstance(parsed_td, dict) else {"data": parsed_td}
                except (ValueError, UnicodeDecodeError):
                    trigger_data = {"raw": raw_trigger_data}
        elif isinstance(raw_trigger_data, str) and raw_trigger_data.strip():
            try:
                parsed_td = json.loads(raw_trigger_data)
                trigger_data = parsed_td if isinstance(parsed_td, dict) else {"data": parsed_td}
            except (ValueError, UnicodeDecodeError):
                trigger_data = {"data": raw_trigger_data}

        return cls(
            request_id=str(request_id),
            event_id=str(event_id),
            trigger_type=str(trigger_type),
            timestamp=timestamp,
            context=context,
            payload=payload,
            trigger_data=trigger_data,
            raw_payload=raw_payload if isinstance(raw_payload, (bytes, bytearray)) else b"",
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any], task_id: str = "") -> Event:
        """
        Construct an Event instance from a dictionary.
        Supports both snake_case and camelCase keys.
        """
        request_id = (
            data.get("requestId")
            or data.get("request_id")
            or task_id
            or ""
        )
        event_id = (
            data.get("eventId")
            or data.get("event_id")
            or data.get("id")
            or ""
        )
        trigger_type = (
            data.get("triggerType")
            or data.get("trigger_type")
            or ""
        )
        timestamp = data.get("timestamp") or 0
        context = data.get("context")
        if context is None:
            context = {}
        elif not isinstance(context, dict):
            context = {"data": context}

        raw_payload = data.get("payload", b"")
        payload = raw_payload
        if isinstance(raw_payload, (bytes, bytearray)) and raw_payload:
            try:
                payload = cbor2.loads(raw_payload)
            except Exception:
                pass

        raw_td = (
            data.get("triggerData")
            or data.get("triggerdata")
            or data.get("trigger_data")
            or {}
        )
        if isinstance(raw_td, (bytes, bytearray)) and raw_td:
            try:
                parsed_td = cbor2.loads(raw_td)
                trigger_data = parsed_td if isinstance(parsed_td, dict) else {"data": parsed_td}
            except Exception:
                trigger_data = {"raw": raw_td}
        elif isinstance(raw_td, dict):
            trigger_data = raw_td
        else:
            trigger_data = {"data": raw_td}

        return cls(
            request_id=str(request_id),
            event_id=str(event_id),
            trigger_type=str(trigger_type),
            timestamp=timestamp,
            context=context,
            payload=payload,
            trigger_data=trigger_data,
            raw_payload=raw_payload if isinstance(raw_payload, (bytes, bytearray)) else b"",
        )

    @classmethod
    def from_invocation(
        cls,
        invocation_or_task_id: Any,
        raw_payload: Optional[bytes] = None,
    ) -> Event:
        """
        Construct an Event from an incoming runtime invocation.
        Accepts either a NextInvocationResponse protobuf object or (task_id, raw_payload) pair.
        """
        # If passed a protobuf object directly
        if hasattr(invocation_or_task_id, "taskId") and (
            raw_payload is None
            or hasattr(invocation_or_task_id, "request_id")
            or hasattr(invocation_or_task_id, "trigger_type")
        ):
            return cls.from_proto(invocation_or_task_id)

        task_id = str(invocation_or_task_id)
        if not raw_payload:
            return cls(request_id=task_id, payload=b"", raw_payload=b"")

        # 1. Try CBOR decode
        try:
            parsed = cbor2.loads(raw_payload)
            if isinstance(parsed, dict):
                has_envelope_keys = any(
                    k in parsed
                    for k in (
                        "requestId",
                        "request_id",
                        "eventId",
                        "event_id",
                        "triggerType",
                        "trigger_type",
                        "triggerData",
                        "triggerdata",
                        "trigger_data",
                    )
                )
                if has_envelope_keys:
                    return cls.from_dict(parsed, task_id=task_id)
            return cls(
                request_id=task_id,
                payload=parsed,
                raw_payload=raw_payload,
            )
        except Exception:
            pass

        # 2. Try JSON decode fallback
        try:
            parsed = json.loads(raw_payload.decode("utf-8"))
            if isinstance(parsed, dict):
                has_envelope_keys = any(
                    k in parsed
                    for k in (
                        "requestId",
                        "request_id",
                        "eventId",
                        "event_id",
                        "triggerType",
                        "trigger_type",
                        "triggerData",
                        "triggerdata",
                        "trigger_data",
                    )
                )
                if has_envelope_keys:
                    return cls.from_dict(parsed, task_id=task_id)
                return cls(
                    request_id=task_id,
                    payload=parsed,
                    raw_payload=raw_payload,
                )
        except (ValueError, UnicodeDecodeError):
            pass

        # 3. Fallback to raw bytes payload
        return cls(
            request_id=task_id,
            payload=raw_payload,
            raw_payload=raw_payload,
        )

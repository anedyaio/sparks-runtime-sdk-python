"""
anedya_sparks
~~~~~~~~~~~~~

Python SDK for the Anedya Sparks Lambda service.

Quickstart::

    import anedya_sparks as sparks

    @sparks.handler
    def my_handler(event: sparks.Event) -> bytes:
        # event contains requestId, eventId, triggerType, timestamp, context, payload, triggerData
        # Return bytes (or str / None) as the task result.
        return b"Hello from Sparks!"

    sparks.run()

Configuration (via environment variables):
    ANEDYA_SPARK_AGENT_ENDPOINT — host and port of the runtime agent (default: localhost:8081)
    ANEDYA_SPARK_USE_TLS        — "true" to enable TLS        (default: false)
    ANEDYA_SPARK_INSTANCE_ID    — instance id (required)
    ANEDYA_SPARK_AUTH_TOKEN     — auth token  (required)
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any, Callable, Optional, Union

from anedya_sparks._client import SparksClient
from anedya_sparks._event import Event
from anedya_sparks._loop import run_loop
from anedya_sparks._exceptions import (
    SparksError,
    SparksConnectionError,
    SparksHandlerError,
    SparksConfigError,
)

__all__ = [
    "Event",
    "handler",
    "run",
    "SparksError",
    "SparksConnectionError",
    "SparksHandlerError",
    "SparksConfigError",
]

__version__ = "0.1.0"

logger = logging.getLogger("anedya.sparks")

# ── Internal state ─────────────────────────────────────────────────────────────

_registered_handler: Optional[Callable[[Event], Any]] = None


# ── Public API ─────────────────────────────────────────────────────────────────


def handler(fn: Callable[[Event], Any]) -> Callable[[Event], Any]:
    """
    Decorator to register a function as the Sparks Lambda handler.

    The function receives a single argument: an ``Event`` instance containing
    invocation metadata (requestId, eventId, triggerType, timestamp, context,
    payload, triggerData). It should return ``bytes``, ``str``, or ``None``.

    Only **one** handler can be registered per process. Decorating a second
    function raises ``RuntimeError``.

    Example::

        @sparks.handler
        def my_handler(event: sparks.Event) -> bytes:
            print(f"Request ID: {event.request_id}")
            print(f"Trigger Type: {event.trigger_type}")
            payload_data = event.json_payload()
            result = {"echo": payload_data}
            return json.dumps(result).encode()
    """
    global _registered_handler  # noqa: PLW0603

    if _registered_handler is not None:
        raise RuntimeError(
            "A Sparks handler has already been registered. "
            "Only one handler is allowed per process."
        )

    _registered_handler = fn
    logger.debug("Handler registered: %s", fn.__qualname__)
    return fn


def _validate_env(endpoint: Optional[str] = None, host: Optional[str] = None) -> None:
    """Validate that all required environment variables / configs are present."""
    has_endpoint = bool(
        endpoint
        or host
        or (len(sys.argv) > 1 and sys.argv[1].strip())
        or os.environ.get("ANEDYA_SPARK_AGENT_ENDPOINT", "").strip()
    )
    has_instance_id = bool(os.environ.get("ANEDYA_SPARK_INSTANCE_ID", "").strip())
    has_auth_token = bool(
        (len(sys.argv) > 2 and sys.argv[2].strip())
        or os.environ.get("ANEDYA_SPARK_AUTH_TOKEN", "").strip()
    )

    missing = []
    if not has_endpoint:
        missing.append("ANEDYA_SPARK_AGENT_ENDPOINT")
    if not has_instance_id:
        missing.append("ANEDYA_SPARK_INSTANCE_ID")
    if not has_auth_token:
        missing.append("ANEDYA_SPARK_AUTH_TOKEN")

    if missing:
        raise SparksConfigError(
            f"Missing required configuration/environment variable(s): {', '.join(missing)}"
        )


def run(
    endpoint: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    use_tls: Optional[bool] = None,
) -> None:
    """
    Start the Sparks runtime loop.

    This function **blocks indefinitely**. It should be the last call in your
    script. The process exits when:
    - A SIGTERM is received (exit 0, after finishing the current invocation).
    - A fatal gRPC error occurs (exit 1).

    Args:
        endpoint: Override ``ANEDYA_SPARK_AGENT_ENDPOINT`` env var (host:port).
        host:     Override host.
        port:     Override port.
        use_tls:  Override ``ANEDYA_SPARK_USE_TLS`` env var.
    """
    # 1. Validate required environment variables first
    _validate_env(endpoint=endpoint, host=host)

    # 2. Ensure handler is registered
    if _registered_handler is None:
        raise RuntimeError(
            "No handler registered. Use the @sparks.handler decorator before calling sparks.run()."
        )

    # 3. Initialize and connect client
    client = SparksClient(endpoint=endpoint, host=host, port=port, use_tls=use_tls)
    client.connect()

    try:
        run_loop(handler=_registered_handler, client=client)
    finally:
        client.close()

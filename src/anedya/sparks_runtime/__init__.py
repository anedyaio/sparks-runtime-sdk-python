"""
anedya_sparks
~~~~~~~~~~~~~

Python SDK for the Anedya Sparks Lambda service.

Quickstart::

    import anedya.sparks_runtime as sparks

    @sparks.handler
    def my_handler(invocation: sparks.SparkInvocation) -> sparks.SparkResponse:
        return sparks.SparkResponse(
            status_code=sparks.StatusCode.OK,
            payload=b"Hello from Sparks!",
        )

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
from collections.abc import Callable

from ._client import SparksClient
from ._exceptions import (
    SparksConfigError,
    SparksConnectionError,
    SparksError,
    SparksHandlerError,
)
from ._loop import run_loop
from ._spark_invocation import SparkInvocation
from ._spark_response import SparkResponse, StatusCode

__all__ = [
    "SparkInvocation",
    "SparkResponse",
    "SparksConfigError",
    "SparksConnectionError",
    "SparksError",
    "SparksHandlerError",
    "StatusCode",
    "handler",
    "run",
]

__version__ = "0.1.0"

logger = logging.getLogger("anedya.sparks")
logging.getLogger("anedya").addHandler(logging.NullHandler())
logging.getLogger("anedya").propagate = False

# ── Internal state ─────────────────────────────────────────────────────────────

_registered_handler: Callable[[SparkInvocation], SparkResponse] | None = None


# ── Public API ─────────────────────────────────────────────────────────────────


def handler(
    fn: Callable[[SparkInvocation], SparkResponse],
) -> Callable[[SparkInvocation], SparkResponse]:
    """
    Decorator to register a function as the Sparks Lambda handler.

    The function receives a single argument: a ``SparkInvocation`` instance containing
    invocation metadata (function_id, task_id, trigger_id, event_type, timestamp,
    payload). It must return a ``SparkResponse`` instance.

    Only **one** handler can be registered per process. Decorating a second
    function raises ``RuntimeError``.

    Example::

        @sparks.handler
        def my_handler(invocation: sparks.SparkInvocation) -> sparks.SparkResponse:
            print(f"Task ID: {invocation.task_id}")
            payload_data = invocation.cbor_payload()
            return sparks.SparkResponse(
                status_code=sparks.StatusCode.OK,
                payload=b"OK",
            )
    """
    global _registered_handler

    if _registered_handler is not None:
        raise RuntimeError(
            "A Sparks handler has already been registered. "
            "Only one handler is allowed per process."
        )

    _registered_handler = fn
    logger.debug("Handler registered: %s", fn.__qualname__)
    return fn


def _validate_env(endpoint: str | None = None, host: str | None = None) -> None:
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


def _setup_logging() -> None:
    """
    Ensure internal SDK loggers do not leak to stdout/stderr or Docker FIFO pipe.
    Only the user's registered handler output (print/logging) is emitted to the FIFO pipe.
    """
    sparks_logger = logging.getLogger("anedya")
    sparks_logger.handlers.clear()
    sparks_logger.addHandler(logging.NullHandler())

    # Only enable internal SDK console logging if explicitly requested via environment variable
    if os.environ.get("ANEDYA_ENABLE_INTERNAL_LOGS", "").lower() in ("true", "1", "yes"):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        sparks_logger.addHandler(handler)
        sparks_logger.setLevel(logging.INFO)
        sparks_logger.propagate = False
    else:
        # Prevent propagation to root logger which could print to stdout/stderr
        sparks_logger.propagate = False


def run(
    endpoint: str | None = None,
    host: str | None = None,
    port: int | None = None,
    use_tls: bool | None = None,
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
    # 0. Configure default console logging
    _setup_logging()

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

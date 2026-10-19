"""
anedya_sparks._loop
~~~~~~~~~~~~~~~~~~~

Core poll-execute-respond loop.

Lifecycle:
  1. Connect to the gRPC runtime.
  2. Call NextInvocation (blocking) — wait for a task.
  3. Call the registered user handler with the raw payload bytes.
  4. If the handler returns successfully → call send_response().
  5. Repeat from step 2.
  6. On SIGTERM → finish the current invocation, then exit(0) cleanly.
  7. On gRPC error → log CRITICAL and exit(1) (no reconnect).
"""

import logging
import signal
import sys
import traceback
from collections.abc import Callable

from ._client import SparksClient
from ._exceptions import SparksConnectionError
from ._spark_invocation import SparkInvocation
from ._spark_response import SparkResponse, StatusCode

logger = logging.getLogger("anedya.sparks.loop")


class _ShutdownFlag:
    """Simple flag set by a signal handler to request a graceful shutdown."""

    def __init__(self) -> None:
        self.requested = False

    def request(self, signum, frame) -> None:
        logger.info("SIGTERM received — will shut down after the current invocation.")
        self.requested = True


def run_loop(
    handler: Callable[[SparkInvocation], SparkResponse],
    client: SparksClient,
) -> None:
    """
    Start the main poll → execute → respond loop.

    Args:
        handler: The user-registered function. Receives a SparkInvocation instance,
                 and must return a SparkResponse instance.
        client:  A connected SparksClient instance.

    This function blocks indefinitely. It exits via sys.exit().
    """
    shutdown = _ShutdownFlag()
    signal.signal(signal.SIGTERM, shutdown.request)

    logger.info("Sparks runtime loop started. Waiting for invocations...")

    while True:
        # ── Step 1: Poll for the next task ────────────────────────────────────
        try:
            invocation = client.next_invocation()
        except SparksConnectionError as exc:
            exc_str = str(exc)
            if "CANCELLED" in exc_str or "context cancelled" in exc_str or "instance terminated" in exc_str:
                logger.info("Instance terminated by runtime (instance stopped). Exiting cleanly.")
                sys.exit(0)
            logger.critical("Fatal gRPC error during NextInvocation: %s", exc)
            sys.exit(1)

        task_id_raw = getattr(invocation, "task_id", None)
        task_id = (
            task_id_raw.decode("utf-8", errors="replace")
            if isinstance(task_id_raw, bytes)
            else str(task_id_raw)
        )
        payload = invocation.payload  # bytes

        # Check for poll timeout / keepalive (empty response from Go runtime)
        if not task_id_raw:
            logger.debug("NextInvocation poll timeout (no task received), continuing poll loop...")
            if shutdown.requested:
                logger.info("Graceful shutdown complete.")
                sys.exit(0)
            continue

        logger.info("Invoking handler for task_id=%s (%d bytes)", task_id, len(payload))

        # ── Step 2: Execute the user handler ──────────────────────────────────
        event = SparkInvocation.from_proto(invocation)
        try:
            result = handler(event)
            if not isinstance(result, SparkResponse):
                raise TypeError(
                    f"Handler must return a SparkResponse instance, got {type(result).__name__}"
                )
        except Exception:  # noqa: BLE001
            # Handler raised — capture full traceback for the error report
            exc_type, exc_value, exc_tb = sys.exc_info()
            tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
            error_type = exc_type.__name__ if exc_type else "UnknownError"
            error_msg = str(exc_value)

            logger.error(
                "Handler raised an exception for task_id=%s:\n%s", task_id, tb_str
            )
            # Emit handler error directly to stderr so it is captured on the Docker FIFO pipe
            sys.stderr.write(f"Handler exception:\n{tb_str}\n")
            sys.stderr.flush()

            error_response = SparkResponse(
                status_code=StatusCode.INTERNAL,
                error_msg=f"{error_type}: {error_msg}",
                payload=b"",
            )

            try:
                client.send_response(
                    task_id=task_id_raw,
                    payload=error_response.to_cbor(),
                )
                logger.info("Sent error response for task_id=%s", task_id)
            except SparksConnectionError as exc:
                logger.critical(
                    "Fatal gRPC error while sending error response for task_id=%s: %s",
                    task_id,
                    exc,
                )
                sys.exit(1)
        else:
            # ── Step 3a: Send successful response ─────────────────────────────
            try:
                client.send_response(
                    task_id=task_id_raw,
                    payload=result.to_cbor(),
                )
            except SparksConnectionError as exc:
                logger.critical(
                    "Fatal gRPC error while sending response for task_id=%s: %s",
                    task_id,
                    exc,
                )
                sys.exit(1)

            logger.info("Successfully responded to task_id=%s", task_id)

        # ── Step 4: Check for graceful shutdown ────────────────────────────────
        if shutdown.requested:
            logger.info("Graceful shutdown complete.")
            sys.exit(0)

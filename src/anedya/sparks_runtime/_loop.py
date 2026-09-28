"""
anedya_sparks._loop
~~~~~~~~~~~~~~~~~~~

Core poll-execute-respond loop.

Lifecycle:
  1. Connect to the gRPC runtime.
  2. Call NextInvocation (blocking) — wait for a task.
  3. Call the registered user handler with the raw payload bytes.
  4a. If the handler returns successfully → call send_response().
  4b. If the handler raises an exception → log it and call send_error()
      (TODO: send_error is not yet wired to an RPC — logs only for now).
  5. Repeat from step 2.
  6. On SIGTERM → finish the current invocation, then exit(0) cleanly.
  7. On gRPC error → log CRITICAL and exit(1) (no reconnect).
"""

import logging
import signal
import sys
import traceback
from typing import Any, Callable

from ._client import SparksClient
from ._event import Event
from ._exceptions import SparksConnectionError

logger = logging.getLogger("anedya.sparks.loop")


class _ShutdownFlag:
    """Simple flag set by a signal handler to request a graceful shutdown."""

    def __init__(self) -> None:
        self.requested = False

    def request(self, signum, frame) -> None:  # noqa: ANN001
        logger.info("SIGTERM received — will shut down after the current invocation.")
        self.requested = True


def run_loop(
    handler: Callable[[Event], Any],
    client: SparksClient,
) -> None:
    """
    Start the main poll → execute → respond loop.

    Args:
        handler: The user-registered function. Receives an Event instance,
                 must return bytes, str, or None (or something coercible to bytes).
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

        task_id = invocation.taskId
        payload = invocation.payload  # bytes

        # Check for poll timeout / keepalive (empty response from Go runtime)
        if not task_id:
            logger.debug("NextInvocation poll timeout (no task received), continuing poll loop...")
            if shutdown.requested:
                logger.info("Graceful shutdown complete.")
                sys.exit(0)
            continue

        logger.info("Invoking handler for taskId=%s (%d bytes)", task_id, len(payload))

        # ── Step 2: Execute the user handler ──────────────────────────────────
        event = Event.from_invocation(invocation)
        try:
            result = handler(event)
        except Exception:  # noqa: BLE001
            # Handler raised — capture full traceback for the error report
            exc_type, exc_value, exc_tb = sys.exc_info()
            tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
            error_type = exc_type.__name__ if exc_type else "UnknownError"
            error_msg = str(exc_value)

            logger.error(
                "Handler raised an exception for taskId=%s:\n%s", task_id, tb_str
            )

            # ── Step 3b: Report error to runtime ──────────────────────────────
            # TODO: Replace this log-only stub with client.send_error() once the
            #       TaskError RPC is defined in the proto and implemented on the
            #       server side.
            #
            # try:
            #     client.send_error(task_id, error_type, error_msg, tb_str)
            # except SparksConnectionError as conn_exc:
            #     logger.critical(
            #         "Fatal gRPC error while sending error report: %s", conn_exc
            #     )
            #     sys.exit(1)
            import json
            error_payload = json.dumps({
                "status": "error",
                "error_type": error_type,
                "error_message": error_msg,
                "traceback": tb_str,
            }).encode("utf-8")

            try:
                client.send_response(
                    task_id=task_id,
                    payload=error_payload,
                    request_id=event.request_id,
                    event_id=event.event_id,
                )
                logger.info("Sent error response for taskId=%s", task_id)
            except SparksConnectionError as exc:
                logger.critical(
                    "Fatal gRPC error while sending error response for taskId=%s: %s",
                    task_id,
                    exc,
                )
                sys.exit(1)
        else:
            # ── Step 3a: Send successful response ─────────────────────────────
            # Coerce return value to bytes if the user returned a str
            if isinstance(result, str):
                result = result.encode()
            elif result is None:
                result = b""
            elif not isinstance(result, (bytes, bytearray)):
                result = str(result).encode()

            try:
                client.send_response(
                    task_id=task_id,
                    payload=bytes(result),
                    request_id=event.request_id,
                    event_id=event.event_id,
                )
            except SparksConnectionError as exc:
                logger.critical(
                    "Fatal gRPC error while sending response for taskId=%s: %s",
                    task_id,
                    exc,
                )
                sys.exit(1)

            logger.info("Successfully responded to taskId=%s", task_id)

        # ── Step 4: Check for graceful shutdown ────────────────────────────────
        if shutdown.requested:
            logger.info("Graceful shutdown complete.")
            sys.exit(0)

"""
anedya_sparks._client
~~~~~~~~~~~~~~~~~~~~~

Thin gRPC client wrapper around the ContainerAPI service.

Responsibilities:
  - Open / close the gRPC channel.
  - Expose three high-level methods that map 1-to-1 to the proto RPCs.
  - Keep all gRPC internals away from the rest of the SDK.

Configuration (via environment variables or constructor arguments):
  SPARKS_RUNTIME_HOST  — host of the Sparks runtime agent  (default: localhost)
  SPARKS_RUNTIME_PORT  — gRPC port of the runtime agent    (default: 50051)
  SPARKS_USE_TLS       — set to "true" to enable TLS       (default: false)
  SPARKS_INSTANCE_ID   — instance id assigned to this container (required)
  SPARKS_AUTH_TOKEN    — auth token for this container          (required)
"""

import logging
import os
import sys
from typing import Optional

import grpc

from anedya_sparks.proto import sparks_pb2, sparks_pb2_grpc
from anedya_sparks._exceptions import SparksConfigError, SparksConnectionError

logger = logging.getLogger("anedya.sparks.client")

_DEFAULT_HOST = "localhost"
_DEFAULT_PORT = 50051


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SparksConfigError(
            f"Required environment variable '{name}' is not set or is empty."
        )
    return value


class SparksClient:
    """
    gRPC client for the ContainerAPI service.

    Usage::

        client = SparksClient()
        client.connect()

        response = client.next_invocation()
        # ... process response ...
        client.send_response(response.taskId, b"result payload")

        client.close()
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        use_tls: Optional[bool] = None,
        instance_id: Optional[str] = None,
        auth_token: Optional[bytes] = None,
    ) -> None:
        # Previous configuration resolution:
        # self._host = host or os.environ.get("SPARKS_RUNTIME_HOST", _DEFAULT_HOST)
        # self._port = port or int(os.environ.get("SPARKS_RUNTIME_PORT", _DEFAULT_PORT))
        # if use_tls is None:
        #     use_tls = os.environ.get("SPARKS_USE_TLS", "false").lower() == "true"
        # self._use_tls = use_tls
        # self._instance_id = instance_id or _require_env("SPARKS_INSTANCE_ID")
        # raw_token = auth_token or os.environ.get("SPARKS_AUTH_TOKEN", "")
        # if not raw_token:
        #     raise SparksConfigError(
        #         "Required environment variable 'SPARKS_AUTH_TOKEN' is not set or is empty."
        #     )
        # self._auth_token: bytes = (
        #     raw_token.encode() if isinstance(raw_token, str) else raw_token
        # )

        # New configuration supporting sys.argv and ANEDYA_SPARK_* envs:
        # 1. Resolve host and port
        cli_endpoint = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1].strip() else None
        env_endpoint = os.environ.get("ANEDYA_SPARK_AGENT_ENDPOINT", "").strip() or os.environ.get("SPARKS_RUNTIME_HOST", "").strip()
        resolved_host = host or cli_endpoint or env_endpoint or _DEFAULT_HOST

        if ":" in resolved_host and port is None:
            parts = resolved_host.split(":", 1)
            self._host = parts[0]
            try:
                self._port = int(parts[1])
            except ValueError:
                self._port = 8081
        else:
            self._host = resolved_host
            self._port = port or int(os.environ.get("SPARKS_RUNTIME_PORT", "8081" if (cli_endpoint or "ANEDYA_SPARK_AGENT_ENDPOINT" in os.environ) else _DEFAULT_PORT))

        if use_tls is None:
            use_tls = os.environ.get("SPARKS_USE_TLS", "false").lower() == "true"
        self._use_tls = use_tls

        # 2. Resolve instance_id
        self._instance_id = (
            instance_id
            or os.environ.get("ANEDYA_SPARK_INSTANCE_ID", "").strip()
            or os.environ.get("SPARKS_INSTANCE_ID", "").strip()
        )
        if not self._instance_id:
            raise SparksConfigError(
                "Required instance ID was not provided via 'ANEDYA_SPARK_INSTANCE_ID' or 'SPARKS_INSTANCE_ID'."
            )

        # 3. Resolve auth_token
        cli_token = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2].strip() else None
        env_token = os.environ.get("ANEDYA_SPARK_AUTH_TOKEN", "").strip() or os.environ.get("SPARKS_AUTH_TOKEN", "").strip()
        raw_token = auth_token or cli_token or env_token
        if not raw_token:
            raise SparksConfigError(
                "Required authentication token was not provided via arguments, 'ANEDYA_SPARK_AUTH_TOKEN', or 'SPARKS_AUTH_TOKEN'."
            )

        # Accept both str (for env var) and bytes
        self._auth_token: bytes = (
            raw_token.encode() if isinstance(raw_token, str) else raw_token
        )

        self._channel: Optional[grpc.Channel] = None
        self._stub: Optional[sparks_pb2_grpc.ContainerAPIStub] = None

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """Open the gRPC channel and create the stub."""
        target = f"{self._host}:{self._port}"
        logger.info("Connecting to Sparks runtime at %s (tls=%s)", target, self._use_tls)

        if self._use_tls:
            credentials = grpc.ssl_channel_credentials()
            self._channel = grpc.secure_channel(target, credentials)
        else:
            self._channel = grpc.insecure_channel(target)

        self._stub = sparks_pb2_grpc.ContainerAPIStub(self._channel)
        logger.debug("gRPC channel created successfully.")

    def close(self) -> None:
        """Close the gRPC channel cleanly."""
        if self._channel is not None:
            self._channel.close()
            self._channel = None
            self._stub = None
            logger.info("gRPC channel closed.")

    def _ensure_connected(self) -> sparks_pb2_grpc.ContainerAPIStub:
        if self._stub is None:
            raise SparksConnectionError(
                "SparksClient is not connected. Call connect() before making RPC calls."
            )
        return self._stub

    # ── RPC wrappers ───────────────────────────────────────────────────────────

    def next_invocation(self) -> sparks_pb2.NextInvocationResponse:
        """
        Block until the runtime delivers the next task.

        Returns:
            NextInvocationResponse with .taskId (str) and .payload (bytes).

        Raises:
            SparksConnectionError: if the RPC call fails at the transport level.
        """
        stub = self._ensure_connected()
        request = sparks_pb2.NextInvocationRequest(
            instanceId=self._instance_id,
            authToken=self._auth_token,
        )
        logger.debug("Waiting for next invocation (instanceId=%s)...", self._instance_id)
        try:
            response: sparks_pb2.NextInvocationResponse = stub.NextInvocation(request)
            logger.debug("Received task: taskId=%s", response.taskId)
            return response
        except grpc.RpcError as exc:
            raise SparksConnectionError(
                f"NextInvocation RPC failed: [{exc.code()}] {exc.details()}"
            ) from exc

    def send_response(
        self,
        task_id: str,
        payload: bytes,
        request_id: str = "",
        event_id: str = "",
    ) -> None:
        """
        Send the handler's return value back to the runtime.

        Args:
            task_id:    The taskId received from next_invocation().
            payload:    Raw bytes to return as the task result.
            request_id: Optional request ID from the invocation event.
            event_id:   Optional event ID from the invocation event.

        Raises:
            SparksConnectionError: if the RPC call fails at the transport level.
        """
        stub = self._ensure_connected()
        request = sparks_pb2.TaskResponseRequest(
            taskId=task_id,
            request_id=request_id,
            event_id=event_id,
            InstanceId=self._instance_id,
            authToken=self._auth_token,
            payload=payload,
        )
        logger.debug("Sending response for taskId=%s (%d bytes)", task_id, len(payload))
        try:
            stub.TaskResponse(request)
            logger.debug("Response sent successfully for taskId=%s", task_id)
        except grpc.RpcError as exc:
            raise SparksConnectionError(
                f"TaskResponse RPC failed: [{exc.code()}] {exc.details()}"
            ) from exc

    # TODO: Implement send_error() once the TaskError RPC is defined in the proto.
    #
    # def send_error(
    #     self,
    #     task_id: str,
    #     error_type: str,
    #     error_message: str,
    #     stacktrace: str,
    # ) -> None:
    #     """
    #     Report a handler exception back to the runtime via the error endpoint.
    #     """
    #     stub = self._ensure_connected()
    #     request = sparks_pb2.TaskErrorRequest(
    #         taskId=task_id,
    #         InstanceId=self._instance_id,
    #         authToken=self._auth_token,
    #         errorType=error_type,
    #         errorMessage=error_message,
    #         stacktrace=stacktrace,
    #     )
    #     try:
    #         stub.TaskError(request)
    #     except grpc.RpcError as exc:
    #         raise SparksConnectionError(
    #             f"TaskError RPC failed: [{exc.code()}] {exc.details()}"
    #         ) from exc

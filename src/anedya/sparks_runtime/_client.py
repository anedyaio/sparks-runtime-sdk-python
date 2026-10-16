"""
anedya_sparks._client
~~~~~~~~~~~~~~~~~~~~~

Thin gRPC client wrapper around the ContainerAPIService service.

Responsibilities:
  - Open and close the gRPC channel.
  - Expose next_invocation() and send_response() mapping to proto RPCs.
  - Keep all gRPC internals abstracted away from the rest of the SDK.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional, Union

import grpc

from .proto import sparks_pb2, sparks_pb2_grpc
from ._exceptions import SparksConfigError, SparksConnectionError

logger = logging.getLogger("anedya.sparks.client")

_DEFAULT_HOST = "localhost"
_DEFAULT_PORT = 8081


def _parse_endpoint(endpoint: str) -> tuple[str, int]:
    """Parse an endpoint string into (host, port)."""
    endpoint = endpoint.strip()
    if not endpoint:
        return _DEFAULT_HOST, _DEFAULT_PORT

    if ":" in endpoint:
        host, port_str = endpoint.rsplit(":", 1)
        try:
            return host, int(port_str)
        except ValueError:
            raise SparksConfigError(f"Invalid port '{port_str}' in endpoint: '{endpoint}'")

    return endpoint, _DEFAULT_PORT


class SparksClient:
    """
    gRPC client for the ContainerAPIService service.

    Usage::

        client = SparksClient()
        client.connect()

        response = client.next_invocation()
        # ... process response ...
        client.send_response(response.task_id, b"result payload")

        client.close()
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        use_tls: Optional[bool] = None,
        instance_id: Optional[Union[bytes, str]] = None,
        auth_token: Optional[Union[bytes, str]] = None,
    ) -> None:
        # 1. Resolve host and port
        cli_endpoint = sys.argv[1].strip() if len(sys.argv) > 1 and sys.argv[1].strip() else None
        env_endpoint = os.environ.get("ANEDYA_SPARK_AGENT_ENDPOINT", "").strip()
        raw_endpoint = endpoint or (host if host and ":" in host else None) or cli_endpoint or env_endpoint

        if not raw_endpoint and not host:
            raise SparksConfigError("Required agent endpoint was not provided.")

        if raw_endpoint:
            parsed_host, parsed_port = _parse_endpoint(raw_endpoint)
        else:
            parsed_host, parsed_port = _DEFAULT_HOST, _DEFAULT_PORT

        self._host = (host.split(":", 1)[0] if host else "") or parsed_host or _DEFAULT_HOST
        self._port = port if port is not None else parsed_port

        # 2. Resolve TLS configuration
        if use_tls is None:
            tls_env = os.environ.get("ANEDYA_SPARK_USE_TLS", "false")
            use_tls = tls_env.lower() in ("true", "1", "yes")
        self._use_tls = use_tls

        # 3. Resolve instance_id
        resolved_instance_id = instance_id or os.environ.get("ANEDYA_SPARK_INSTANCE_ID", "").strip()
        if not resolved_instance_id:
            raise SparksConfigError(
                "Required instance ID was not provided via 'ANEDYA_SPARK_INSTANCE_ID'."
            )
        self._instance_id: bytes = (
            resolved_instance_id.encode("utf-8")
            if isinstance(resolved_instance_id, str)
            else resolved_instance_id
        )

        # 4. Resolve auth_token
        cli_token = sys.argv[2].strip() if len(sys.argv) > 2 and sys.argv[2].strip() else None
        env_token = os.environ.get("ANEDYA_SPARK_AUTH_TOKEN", "").strip()
        raw_token = auth_token or cli_token or env_token
        if not raw_token:
            raise SparksConfigError(
                "Required authentication token was not provided via arguments or 'ANEDYA_SPARK_AUTH_TOKEN'."
            )
        self._auth_token: bytes = (
            raw_token.encode("utf-8") if isinstance(raw_token, str) else raw_token
        )

        self._channel: Optional[grpc.Channel] = None
        self._stub: Optional[sparks_pb2_grpc.ContainerAPIServiceStub] = None

    @property
    def host(self) -> str:
        """The runtime agent host."""
        return self._host

    @property
    def port(self) -> int:
        """The runtime agent port."""
        return self._port

    @property
    def endpoint(self) -> str:
        """The runtime agent endpoint (host:port)."""
        return f"{self._host}:{self._port}"

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

        self._stub = sparks_pb2_grpc.ContainerAPIServiceStub(self._channel)
        logger.info("gRPC channel created successfully at %s.", target)

    def close(self) -> None:
        """Close the gRPC channel cleanly."""
        if self._channel is not None:
            self._channel.close()
            self._channel = None
            self._stub = None
            logger.info("gRPC channel closed.")

    def _ensure_connected(self) -> sparks_pb2_grpc.ContainerAPIServiceStub:
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
            NextInvocationResponse with .task_id (bytes) and .payload (bytes).

        Raises:
            SparksConnectionError: if the RPC call fails at the transport level.
        """
        stub = self._ensure_connected()
        request = sparks_pb2.NextInvocationRequest(
            instance_id=self._instance_id,
            auth_token=self._auth_token,
        )
        logger.debug("Waiting for next invocation (instance_id=%s)...", self._instance_id)
        try:
            response: sparks_pb2.NextInvocationResponse = stub.NextInvocation(request)
            logger.debug("Received task: task_id=%s", response.task_id)
            return response
        except grpc.RpcError as exc:
            raise SparksConnectionError(
                f"NextInvocation RPC failed: [{exc.code()}] {exc.details()}"
            ) from exc

    def send_response(self, task_id: Union[str, bytes], payload: bytes) -> None:
        """
        Send the handler's return value back to the runtime.

        Args:
            task_id: The task_id received from next_invocation() (bytes or str).
            payload: Raw bytes to return as the task result.

        Raises:
            SparksConnectionError: if the RPC call fails at the transport level.
        """
        stub = self._ensure_connected()
        task_id_bytes = task_id.encode("utf-8") if isinstance(task_id, str) else task_id
        request = sparks_pb2.TaskResponseRequest(
            task_id=task_id_bytes,
            instance_id=self._instance_id,
            auth_token=self._auth_token,
            payload=payload,
        )
        logger.debug("Sending response for task_id=%s (%d bytes)", task_id, len(payload))
        try:
            stub.TaskResponse(request)
            logger.debug("Response sent successfully for task_id=%s", task_id)
        except grpc.RpcError as exc:
            raise SparksConnectionError(
                f"TaskResponse RPC failed: [{exc.code()}] {exc.details()}"
            ) from exc

# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Transport ABC for client-server communication.

Defines the bytes-level interface that all transports implement:
- ClientTransport: send_request(payload) → response (used by MaruShmClient)
- ServerTransport: run_loop(handler) (used by MetaServer, RM)

Serialization is handled by the layer above (RpcClient, MaruShmClient),
not by the transport itself.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable


class ClientTransport(ABC):
    """Bytes-level client transport: send request, receive response.

    Implementations:
    - TcpClientTransport: persistent TCP connection (current)
    - CxlRpcClientTransport: CXL shared memory slot
    """

    @abstractmethod
    def send_request(self, payload: bytes, timeout_ms: int = 2000) -> bytes:
        """Send request payload and return response payload.

        Args:
            payload: Raw request bytes (header + body).
            timeout_ms: Timeout in milliseconds.

        Returns:
            Raw response bytes.

        Raises:
            TimeoutError: If no response within timeout.
            ConnectionError: If transport connection fails.
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """Release transport resources."""
        ...


class ServerTransport(ABC):
    """Bytes-level server transport: receive requests, send responses.

    Implementations:
    - CxlRpcServerTransport: CXL shared memory slot polling
    """

    @abstractmethod
    def run_loop(
        self,
        handler: Callable[[int, int, bytes], bytes],
        poll_interval_us: int = 0,
    ) -> None:
        """Blocking server loop.

        Args:
            handler: Callable(channel_id, seq, request_bytes) → response_bytes.
            poll_interval_us: Microseconds between empty poll cycles.
                0 = busy-poll (lowest latency).
        """
        ...

    @abstractmethod
    def stop(self) -> None:
        """Signal the server loop to stop."""
        ...

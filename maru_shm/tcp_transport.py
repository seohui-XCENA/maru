# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""TCP client transport for the Maru Resource Manager.

Implements ClientTransport over a persistent TCP connection with
automatic reconnection on failure. The RM binary IPC protocol
(12B MsgHeader + payload) is handled at this level.
"""

import logging
import socket
import threading

from maru_common.transport import ClientTransport

from .ipc import HEADER_SIZE, MsgHeader, MsgType
from .uds_helpers import read_full, write_full

logger = logging.getLogger(__name__)


class TcpClientTransport(ClientTransport):
    """Persistent TCP connection to the Resource Manager.

    Thread-safe: the entire send+recv cycle is serialized by an internal lock.
    On connection error, closes and retries once with a fresh connection.
    """

    def __init__(self, address: str):
        """
        Args:
            address: 'host:port' string (e.g. '127.0.0.1:9850').
        """
        self._address = address
        self._sock: socket.socket | None = None
        self._conn_lock = threading.Lock()

    @staticmethod
    def _parse_address(address: str) -> tuple[str, int]:
        """Parse 'host:port' string."""
        host, _, port_str = address.rpartition(":")
        if not host:
            host = "127.0.0.1"
        return host, int(port_str)

    def is_running(self) -> bool:
        """Check if the resource manager is reachable."""
        host, port = self._parse_address(self._address)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, port))
            sock.close()
            return True
        except OSError:
            sock.close()
            return False

    def _ensure_conn(self) -> socket.socket:
        """Return the persistent connection, creating it if needed.

        Must be called with _conn_lock held.
        """
        if self._sock is not None:
            return self._sock

        host, port = self._parse_address(self._address)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(5.0)  # 5s connect timeout
            sock.connect((host, port))
            sock.settimeout(
                10.0
            )  # 10s RPC timeout (prevents infinite block on server hang)
        except OSError as e:
            sock.close()
            raise ConnectionError(
                f"Resource manager is not running "
                f"(address: {self._address}).\n"
                f"Start it first: maru-resource-manager "
                f"--host {host} --port {port}"
            ) from e

        # Warn when connecting to a remote host over plaintext TCP
        if host not in ("127.0.0.1", "localhost", "::1"):
            logger.warning(
                "Connecting to remote host %s over PLAINTEXT TCP. "
                "Auth tokens will be transmitted without encryption. "
                "Use an encrypted tunnel for production deployments.",
                host,
            )

        # Disable Nagle for low-latency RPC
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock = sock
        return self._sock

    def _close_conn(self) -> None:
        """Close the persistent connection."""
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def send_request(self, payload: bytes, timeout_ms: int = 2000) -> bytes:
        """Send RM binary IPC request and return raw response (header + payload).

        The payload must already include the MsgHeader. The response is
        returned as raw bytes: MsgHeader(12B) + response_payload.

        Retries once on connection failure (idempotency is guaranteed by
        request_id in the payload for alloc/free operations).
        """
        for attempt in range(2):
            with self._conn_lock:
                sock = self._ensure_conn()
                try:
                    write_full(sock, payload)

                    # Receive response header
                    resp_hdr_data = read_full(sock, HEADER_SIZE)
                    resp_hdr = MsgHeader.unpack(resp_hdr_data)
                    if not resp_hdr.validate():
                        raise ConnectionError(
                            f"Invalid response header: magic=0x{resp_hdr.magic:08X}"
                        )

                    resp_payload = b""
                    if resp_hdr.payload_len > 0:
                        resp_payload = read_full(sock, resp_hdr.payload_len)

                    return resp_hdr_data + resp_payload

                except (ConnectionError, OSError):
                    self._close_conn()
                    if attempt == 1:
                        raise

        raise ConnectionError("send_request failed after 2 attempts")

    def close(self) -> None:
        """Release transport resources."""
        with self._conn_lock:
            self._close_conn()

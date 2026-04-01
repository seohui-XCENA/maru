# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruShmClient — shared memory client for the Maru Resource Manager.

Communicates with the resource manager via a pluggable ClientTransport.
Default transport is TcpClientTransport (persistent TCP connection).
"""

import logging
import mmap as mmap_module
import os
import threading

from maru_common.transport import ClientTransport

from .constants import ANY_POOL_ID, DEFAULT_ADDRESS
from .ipc import (
    HEADER_SIZE,
    AllocReq,
    AllocResp,
    ErrorResp,
    FreeReq,
    FreeResp,
    GetAccessReq,
    GetAccessResp,
    MsgHeader,
    MsgType,
    StatsReq,
    StatsResp,
)
from .tcp_transport import TcpClientTransport
from .types import MaruHandle, MaruPoolInfo

logger = logging.getLogger(__name__)


def _make_client_id() -> str:
    """Build a client_id string: 'hostname:pid'."""
    import platform

    return f"{platform.node()}:{os.getpid()}"


# Module-level request ID counter shared across all MaruShmClient instances.
# Prevents idempotency cache collisions when multiple instances in the same
# process have the same client_id (hostname:pid).
_request_id_counter = 0
_request_id_lock = threading.Lock()


def _next_request_id() -> int:
    """Generate a monotonically increasing request ID (process-global)."""
    global _request_id_counter
    with _request_id_lock:
        _request_id_counter += 1
        return _request_id_counter


class MaruShmClient:
    """Client for the Maru Resource Manager.

    Uses a ClientTransport for communication. If no transport is provided,
    creates a TcpClientTransport from the given address.

    Device paths received from alloc/get_access are cached by region_id.
    mmap() opens the device path directly to create Python mmap objects.
    """

    def __init__(
        self,
        address: str | None = None,
        transport: ClientTransport | None = None,
    ):
        if transport is not None:
            self._transport = transport
        else:
            self._transport = TcpClientTransport(address or DEFAULT_ADDRESS)
        self._path_cache: dict[int, str] = {}  # region_id -> device path
        self._mmap_cache: dict[int, mmap_module.mmap] = {}  # region_id -> mmap
        self._lock = threading.Lock()
        self._client_id = _make_client_id()

    def is_running(self) -> bool:
        """Check if the resource manager is reachable."""
        if isinstance(self._transport, TcpClientTransport):
            return self._transport.is_running()
        return True

    def _rpc(self, msg_type: MsgType, payload: bytes) -> tuple[MsgHeader, bytes]:
        """Execute a single RPC via the transport.

        Builds a MsgHeader, sends header+payload through the transport,
        and parses the response header + payload from the raw bytes.

        Returns:
            (response_header, response_payload)
        """
        hdr = MsgHeader(msg_type=msg_type, payload_len=len(payload))
        raw_request = hdr.pack() + payload

        raw_response = self._transport.send_request(raw_request)

        resp_hdr = MsgHeader.unpack(raw_response[:HEADER_SIZE])
        resp_payload = raw_response[HEADER_SIZE:]
        return resp_hdr, resp_payload

    def _check_error(self, hdr: MsgHeader, payload: bytes, context: str) -> None:
        """Raise RuntimeError if response is an ERROR_RESP."""
        if hdr.msg_type == MsgType.ERROR_RESP:
            err = ErrorResp.unpack(payload)
            raise RuntimeError(f"{context} ({err.status}): {err.message}")

    # =========================================================================
    # Public API
    # =========================================================================

    def stats(self) -> list[MaruPoolInfo]:
        """Query pool statistics from the resource manager."""
        hdr, payload = self._rpc(MsgType.STATS_REQ, StatsReq().pack())
        self._check_error(hdr, payload, "Stats failed")
        resp = StatsResp.unpack(payload)
        return resp.pools or []

    def alloc(self, size: int, pool_id: int = ANY_POOL_ID) -> MaruHandle:
        """Allocate shared memory from the resource manager.

        Args:
            size: Requested allocation size in bytes.
            pool_id: Specific pool ID, or ANY_POOL_ID for any.

        Returns:
            Handle for the allocation.

        Raises:
            RuntimeError: On allocation failure.
        """
        req = AllocReq(
            size=size,
            pool_id=pool_id,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        hdr, payload = self._rpc(MsgType.ALLOC_REQ, req.pack())
        self._check_error(hdr, payload, "Alloc failed")

        resp = AllocResp.unpack(payload)
        if resp.status != 0:
            raise RuntimeError(f"Alloc failed with status {resp.status}")

        handle = resp.handle
        if resp.device_path:
            with self._lock:
                self._path_cache[handle.region_id] = resp.device_path

        logger.debug(
            "alloc(size=%d, pool_id=%d) -> region_id=%d path=%s",
            size,
            pool_id,
            handle.region_id,
            resp.device_path,
        )
        return handle

    def free(self, handle: MaruHandle) -> None:
        """Free a previously allocated handle.

        Args:
            handle: Handle from a previous alloc() call.
        """
        req = FreeReq(
            handle=handle,
            client_id=self._client_id,
            request_id=_next_request_id(),
        )
        hdr, payload = self._rpc(MsgType.FREE_REQ, req.pack())
        self._check_error(hdr, payload, "Free failed")

        resp = FreeResp.unpack(payload)
        if resp.status != 0:
            raise RuntimeError(f"Free failed with status {resp.status}")

        with self._lock:
            self._close_region_locked(handle.region_id)
        logger.debug("free(region_id=%d)", handle.region_id)

    def _request_access(self, handle: MaruHandle) -> GetAccessResp:
        """Request access info from the resource manager via GET_ACCESS_REQ."""
        req = GetAccessReq(handle=handle, client_id=self._client_id)
        hdr, payload = self._rpc(MsgType.GET_ACCESS_REQ, req.pack())
        self._check_error(hdr, payload, "GetAccess failed")

        resp = GetAccessResp.unpack(payload)
        if resp.status != 0:
            raise RuntimeError(f"GetAccess failed with status {resp.status}")
        return resp

    def mmap(self, handle: MaruHandle, prot: int) -> mmap_module.mmap:
        """Memory-map a handle into the calling process.

        Opens the device path directly and creates an mmap.

        Args:
            handle: Handle from alloc() or lookup.
            prot: Protection flags (PROT_READ | PROT_WRITE).

        Returns:
            Python mmap object with buffer protocol support.
        """
        # Fast path: check cache
        with self._lock:
            if handle.region_id in self._mmap_cache:
                return self._mmap_cache[handle.region_id]
            path = self._path_cache.get(handle.region_id)

        # Slow path: network RPC outside lock
        if path is None:
            access_resp = self._request_access(handle)
            path = access_resp.device_path

        # Create mmap and update cache
        with self._lock:
            # Double-check: another thread may have created it
            if handle.region_id in self._mmap_cache:
                return self._mmap_cache[handle.region_id]

            self._path_cache[handle.region_id] = path

            access = mmap_module.ACCESS_READ
            if prot & 0x2:  # PROT_WRITE
                access = mmap_module.ACCESS_WRITE

            fd = os.open(path, os.O_RDWR)
            try:
                mm = mmap_module.mmap(
                    fd,
                    handle.length,
                    access=access,
                    offset=handle.offset,
                )
            finally:
                os.close(fd)

            self._mmap_cache[handle.region_id] = mm

        logger.debug(
            "mmap(region_id=%d, length=%d, offset=%d, path=%s)",
            handle.region_id,
            handle.length,
            handle.offset,
            path,
        )
        return mm

    def munmap(self, handle: MaruHandle) -> None:
        """Unmap a previously mapped handle."""
        with self._lock:
            mm = self._mmap_cache.pop(handle.region_id, None)
        if mm is not None:
            mm.close()
        logger.debug("munmap(region_id=%d)", handle.region_id)

    def _close_region_locked(self, region_id: int) -> None:
        """Close mmap and path cache for a region (must hold self._lock)."""
        mm = self._mmap_cache.pop(region_id, None)
        if mm is not None:
            mm.close()
        self._path_cache.pop(region_id, None)

    def close(self) -> None:
        """Close transport and all cached mmaps."""
        self._transport.close()
        with self._lock:
            num_mmaps = len(self._mmap_cache)
            for region_id in list(self._mmap_cache.keys()):
                self._close_region_locked(region_id)
            self._mmap_cache.clear()
            self._path_cache.clear()
        logger.debug("close(): released %d mmaps", num_mmaps)

    def __del__(self) -> None:
        self.close()

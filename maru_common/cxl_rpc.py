# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""CXL-RPC transport layer: shared memory slots for inter-process RPC.

Replaces ZMQ TCP (Handler↔MetaServer) and raw TCP (MetaServer↔RM)
with CXL shared memory based producer-consumer communication.

Architecture:
    - Each client gets a dedicated Channel (no cross-client contention)
    - Writer uses ntstore (cache-bypass write)
    - Reader uses clflush + load (cache invalidate before read)
    - Channel = Header(64B) + RequestCtl(64B) + RequestPayload(4KB)
                             + ResponseCtl(64B) + ResponsePayload(4KB)

Usage:
    # Server side
    server = CxlRpcServerTransport(mm, region_offset, max_channels=32)
    server.run_loop(handler_fn)

    # Client side
    client = CxlRpcClientTransport(mm, channel_offset)
    response = client.send_request(request_bytes)
"""

import ctypes
import logging
import mmap
import struct
import time
from collections.abc import Callable

from .cxl_primitives import clflush, ntstore
from .transport import ClientTransport, ServerTransport

logger = logging.getLogger(__name__)

# =============================================================================
# Constants — must match maru_resource_manager/include/cxl_rpc_layout.h
#
# The C++ header (cxl_rpc_layout.h) is the canonical source of truth.
# These values must be kept in sync. Both sides have static/compile-time
# assertions to catch mismatches.
# =============================================================================

CACHELINE_SIZE = 64

# Slot status values
IDLE = 0
REQ_READY = 1
RESP_READY = 2

# Payload capacity per slot (request or response).
# 4KB covers most single + batch operations.
# Total message = protocol header (16B) + msgpack payload.
PAYLOAD_MAX = 4096

# Channel layout offsets
HEADER_OFFSET = 0  # [0, 64)
REQUEST_CTL_OFFSET = CACHELINE_SIZE  # [64, 128)
REQUEST_PAYLOAD_OFFSET = 2 * CACHELINE_SIZE  # [128, 4224)
RESPONSE_CTL_OFFSET = 2 * CACHELINE_SIZE + PAYLOAD_MAX  # [4224, 4288)
RESPONSE_PAYLOAD_OFFSET = 3 * CACHELINE_SIZE + PAYLOAD_MAX  # [4288, 8384)

# Total channel size
CHANNEL_SIZE = 3 * CACHELINE_SIZE + 2 * PAYLOAD_MAX  # 8384 bytes

# Consistency checks (mirrors C++ static_assert in cxl_rpc_layout.h)
assert REQUEST_CTL_OFFSET == CACHELINE_SIZE  # == sizeof(ChannelHeader)
assert REQUEST_PAYLOAD_OFFSET == REQUEST_CTL_OFFSET + CACHELINE_SIZE  # + sizeof(ControlSlot)
assert RESPONSE_CTL_OFFSET == REQUEST_PAYLOAD_OFFSET + PAYLOAD_MAX
assert RESPONSE_PAYLOAD_OFFSET == RESPONSE_CTL_OFFSET + CACHELINE_SIZE
assert CHANNEL_SIZE == RESPONSE_PAYLOAD_OFFSET + PAYLOAD_MAX == 8384

# Control slot layout: status(4) + seq_num(4) + payload_len(4)
CTL_FORMAT = "<III"  # little-endian: status, seq_num, payload_len
CTL_SIZE = struct.calcsize(CTL_FORMAT)  # 12 bytes

# Header layout: last_heartbeat(8) + pid(4) + node_id(4) + client_id(48)
HEADER_FORMAT = "<QII48s"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 64 bytes


# =============================================================================
# Low-level slot accessors
# =============================================================================


def _mmap_ptr(mm: mmap.mmap) -> int:
    """Get the raw pointer address of an mmap region."""
    return ctypes.addressof(ctypes.c_char.from_buffer(mm))


class _ControlSlot:
    """Read/write access to a control slot (status + seq_num + payload_len)."""

    def __init__(self, mm: mmap.mmap, base_offset: int):
        self._mm = mm
        self._base = base_offset
        self._ptr = _mmap_ptr(mm) + base_offset

    def read(self) -> tuple[int, int, int]:
        """clflush + load → (status, seq_num, payload_len)."""
        clflush(self._ptr, CACHELINE_SIZE)
        self._mm.seek(self._base)
        return struct.unpack(CTL_FORMAT, self._mm.read(CTL_SIZE))

    def read_status(self) -> int:
        """clflush + load status only."""
        clflush(self._ptr, 4)
        self._mm.seek(self._base)
        return struct.unpack("<I", self._mm.read(4))[0]

    def write(self, status: int, seq_num: int, payload_len: int) -> None:
        """ntstore control fields."""
        data = struct.pack(CTL_FORMAT, status, seq_num, payload_len)
        ntstore(self._ptr, data)

    def write_status(self, status: int) -> None:
        """ntstore status field only."""
        ntstore(self._ptr, struct.pack("<I", status))


class _PayloadSlot:
    """Read/write access to a payload area."""

    def __init__(self, mm: mmap.mmap, base_offset: int):
        self._mm = mm
        self._base = base_offset
        self._ptr = _mmap_ptr(mm) + base_offset

    def read(self, length: int) -> bytes:
        """clflush + load payload bytes."""
        clflush(self._ptr, length)
        self._mm.seek(self._base)
        return self._mm.read(length)

    def write(self, data: bytes) -> None:
        """ntstore payload bytes."""
        if len(data) > PAYLOAD_MAX:
            raise ValueError(
                f"Payload too large: {len(data)} > {PAYLOAD_MAX}. "
                "Consider splitting into smaller requests."
            )
        ntstore(self._ptr, data)


class ChannelHeader:
    """Read/write access to channel header (heartbeat, pid, node_id, client_id)."""

    def __init__(self, mm: mmap.mmap, base_offset: int):
        self._mm = mm
        self._base = base_offset
        self._ptr = _mmap_ptr(mm) + base_offset

    def update_heartbeat(self, pid: int = 0, node_id: int = 0) -> None:
        """ntstore current monotonic timestamp as heartbeat."""
        now = time.monotonic_ns()
        data = struct.pack("<QII", now, pid, node_id)
        ntstore(self._ptr, data)

    def read_heartbeat(self) -> int:
        """clflush + load heartbeat timestamp (nanoseconds)."""
        clflush(self._ptr, 16)
        self._mm.seek(self._base)
        return struct.unpack("<Q", self._mm.read(8))[0]

    def write_client_id(self, client_id: str) -> None:
        """Write full header with client_id."""
        now = time.monotonic_ns()
        pid = 0
        node_id = 0
        encoded = client_id.encode("utf-8")[:48].ljust(48, b"\x00")
        data = struct.pack(HEADER_FORMAT, now, pid, node_id, encoded)
        ntstore(self._ptr, data)

    def read(self) -> tuple[int, int, int, str]:
        """clflush + load → (last_heartbeat, pid, node_id, client_id)."""
        clflush(self._ptr, HEADER_SIZE)
        self._mm.seek(self._base)
        raw = self._mm.read(HEADER_SIZE)
        hb, pid, nid, cid_bytes = struct.unpack(HEADER_FORMAT, raw)
        cid = cid_bytes.rstrip(b"\x00").decode("utf-8", errors="replace")
        return hb, pid, nid, cid


# =============================================================================
# Channel: groups header + request + response slots
# =============================================================================


class Channel:
    """A single client-server communication channel."""

    def __init__(self, mm: mmap.mmap, channel_offset: int):
        self.offset = channel_offset
        self.header = ChannelHeader(mm, channel_offset + HEADER_OFFSET)
        self.req_ctl = _ControlSlot(mm, channel_offset + REQUEST_CTL_OFFSET)
        self.req_payload = _PayloadSlot(mm, channel_offset + REQUEST_PAYLOAD_OFFSET)
        self.resp_ctl = _ControlSlot(mm, channel_offset + RESPONSE_CTL_OFFSET)
        self.resp_payload = _PayloadSlot(mm, channel_offset + RESPONSE_PAYLOAD_OFFSET)


# =============================================================================
# Client Transport
# =============================================================================


class CxlRpcClientTransport(ClientTransport):
    """CXL-RPC client transport. Replaces ZMQ REQ socket.

    Each client owns a dedicated Channel. send_request() is blocking:
    writes request via ntstore, then spin-polls for response.
    """

    def __init__(self, mm: mmap.mmap, channel_offset: int):
        """
        Args:
            mm: mmap of the CXL DAX device (or shared memory for testing).
            channel_offset: Byte offset of this client's Channel within mm.
        """
        self._mm = mm
        self._channel = Channel(mm, channel_offset)
        self._seq = 0

    def send_request(
        self, payload: bytes, timeout_ms: int = 2000
    ) -> bytes:
        """Send request and wait for response (blocking).

        Args:
            payload: Raw request bytes (protocol header + msgpack).
            timeout_ms: Timeout in milliseconds.

        Returns:
            Raw response bytes.

        Raises:
            TimeoutError: If response not received within timeout.
        """
        self._seq += 1
        seq = self._seq
        ch = self._channel

        # Update heartbeat with every RPC
        ch.header.update_heartbeat()

        # Write request payload, then control (ordering matters)
        ch.req_payload.write(payload)
        ch.req_ctl.write(REQ_READY, seq, len(payload))

        # Spin-poll for response
        deadline_ns = time.monotonic_ns() + timeout_ms * 1_000_000
        while time.monotonic_ns() < deadline_ns:
            status = ch.resp_ctl.read_status()
            if status == RESP_READY:
                _, resp_seq, resp_len = ch.resp_ctl.read()
                if resp_seq == seq:
                    result = ch.resp_payload.read(resp_len)
                    # Clear response status
                    ch.resp_ctl.write_status(IDLE)
                    return result
                # Stale response from previous request, keep polling

        raise TimeoutError(
            f"CXL-RPC timeout after {timeout_ms}ms (seq={seq})"
        )

    def close(self) -> None:
        """Release resources."""
        self._mm = None
        self._channel = None


# =============================================================================
# Server Transport
# =============================================================================


class CxlRpcServerTransport(ServerTransport):
    """CXL-RPC server transport. Replaces ZMQ REP socket.

    Polls all active channels in round-robin, dispatches requests
    to a handler function, and writes responses via ntstore.
    """

    def __init__(
        self,
        mm: mmap.mmap,
        region_offset: int,
        max_channels: int,
    ):
        """
        Args:
            mm: mmap of the CXL DAX device.
            region_offset: Byte offset of the RPC slot region within mm.
            max_channels: Maximum number of client channels.
        """
        self._mm = mm
        self._region_offset = region_offset
        self._max_channels = max_channels
        self._channels: list[Channel] = []
        self._active: list[bool] = []
        self._running = False

        # Pre-create channel objects for all slots
        for i in range(max_channels):
            ch_offset = region_offset + i * CHANNEL_SIZE
            self._channels.append(Channel(mm, ch_offset))
            self._active.append(False)

    def activate_channel(self, channel_id: int) -> None:
        """Mark a channel as active (server will poll it)."""
        if 0 <= channel_id < self._max_channels:
            self._active[channel_id] = True
            logger.debug("Channel %d activated", channel_id)

    def deactivate_channel(self, channel_id: int) -> None:
        """Mark a channel as inactive (server stops polling it)."""
        if 0 <= channel_id < self._max_channels:
            self._active[channel_id] = False
            logger.debug("Channel %d deactivated", channel_id)

    @property
    def active_channel_count(self) -> int:
        return sum(self._active)

    def get_channel(self, channel_id: int) -> Channel:
        return self._channels[channel_id]

    def poll_once(self) -> tuple[int, int, bytes] | None:
        """Scan active channels once. Returns (channel_id, seq, payload) or None."""
        for i in range(self._max_channels):
            if not self._active[i]:
                continue
            ch = self._channels[i]
            status = ch.req_ctl.read_status()
            if status == REQ_READY:
                _, seq, payload_len = ch.req_ctl.read()
                payload = ch.req_payload.read(payload_len)
                return (i, seq, payload)
        return None

    def send_response(
        self, channel_id: int, seq: int, payload: bytes
    ) -> None:
        """Write response to a channel."""
        ch = self._channels[channel_id]
        # Clear request status BEFORE marking response ready.
        # This prevents a race where the client sees RESP_READY, sends the
        # next request (setting REQ_READY), and then the server overwrites
        # it with IDLE.
        ch.req_ctl.write_status(IDLE)
        # Write payload first, then control (ordering matters)
        ch.resp_payload.write(payload)
        ch.resp_ctl.write(RESP_READY, seq, len(payload))

    def run_loop(
        self,
        handler: Callable[[int, int, bytes], bytes],
        poll_interval_us: int = 0,
    ) -> None:
        """Blocking server loop. Polls channels and dispatches to handler.

        Args:
            handler: Callable(channel_id, seq, request_bytes) → response_bytes.
            poll_interval_us: Microseconds to sleep between empty poll cycles.
                              0 = busy-poll (lowest latency, highest CPU).
        """
        self._running = True
        logger.info(
            "CXL-RPC server started (max_channels=%d)", self._max_channels
        )

        while self._running:
            result = self.poll_once()
            if result is not None:
                channel_id, seq, request = result
                try:
                    response = handler(channel_id, seq, request)
                    self.send_response(channel_id, seq, response)
                except Exception:
                    logger.exception(
                        "Handler error on channel %d seq %d",
                        channel_id,
                        seq,
                    )
            elif poll_interval_us > 0:
                time.sleep(poll_interval_us / 1_000_000)

    def stop(self) -> None:
        """Signal the server loop to stop."""
        self._running = False
        logger.info("CXL-RPC server stopping")


# =============================================================================
# Channel Registration — dynamic channel allocation within a slot pool
# =============================================================================


class ChannelAllocator:
    """Manages channel allocation within a slot pool region.

    Channel 0 is reserved for registration requests. Channels 1..max-1
    are assigned to clients on HANDSHAKE.

    Used by both MetaServer (allocating channels for Handlers) and
    RM (allocating channels for MetaServers).
    """

    def __init__(self, max_channels: int):
        self._max_channels = max_channels
        # Channel 0 = registration, 1..max-1 = client channels
        self._allocated: list[str | None] = [None] * max_channels
        self._allocated[0] = "__registration__"

    def allocate(self, client_id: str) -> int | None:
        """Allocate a channel for a client. Returns channel_id or None if full."""
        for i in range(1, self._max_channels):
            if self._allocated[i] is None:
                self._allocated[i] = client_id
                logger.debug(
                    "Channel %d allocated for '%s'", i, client_id
                )
                return i
        return None

    def release(self, channel_id: int) -> None:
        """Release a channel."""
        if 1 <= channel_id < self._max_channels:
            client = self._allocated[channel_id]
            self._allocated[channel_id] = None
            logger.debug(
                "Channel %d released (was '%s')", channel_id, client
            )

    def get_client(self, channel_id: int) -> str | None:
        """Get the client_id for a channel."""
        if 0 <= channel_id < self._max_channels:
            return self._allocated[channel_id]
        return None

    @property
    def active_count(self) -> int:
        """Number of allocated client channels (excluding registration)."""
        return sum(1 for c in self._allocated[1:] if c is not None)


class RmRpcRegion:
    """Fixed RPC region at the start of CXL memory, managed by RM.

    Layout:
        [0, reg_size)        Registration channels (1 per node, for MetaServer→RM registration)
        [reg_size, end)      RM channels (allocated to MetaServers for ongoing RPC)

    The RM creates this region on startup. MetaServers connect to a
    registration channel to request an RM channel.
    """

    def __init__(
        self,
        mm: mmap.mmap,
        max_registration_channels: int = 4,
        max_rm_channels: int = 64,
    ):
        self._mm = mm
        self._max_reg = max_registration_channels
        self._max_rm = max_rm_channels

        # Registration region: channels for initial REGISTER requests
        self._reg_offset = 0
        self._reg_size = max_registration_channels * CHANNEL_SIZE

        # RM channel region: channels for ongoing MetaServer ↔ RM RPC
        self._rm_offset = self._reg_size
        self._rm_size = max_rm_channels * CHANNEL_SIZE

        self._allocator = ChannelAllocator(max_rm_channels)

        # Server transport for RM channels (activated on allocation)
        self._rm_transport = CxlRpcServerTransport(
            mm, self._rm_offset, max_rm_channels
        )

        # Server transport for registration channels (always active)
        self._reg_transport = CxlRpcServerTransport(
            mm, self._reg_offset, max_registration_channels
        )
        for i in range(max_registration_channels):
            self._reg_transport.activate_channel(i)

    @property
    def total_size(self) -> int:
        """Total bytes consumed by the RPC region."""
        return self._reg_size + self._rm_size

    @property
    def rm_transport(self) -> CxlRpcServerTransport:
        """Server transport for RM channels (MetaServer ↔ RM RPC)."""
        return self._rm_transport

    @property
    def reg_transport(self) -> CxlRpcServerTransport:
        """Server transport for registration channels."""
        return self._reg_transport

    def allocate_rm_channel(self, client_id: str) -> int | None:
        """Allocate an RM channel for a MetaServer. Returns channel_id or None."""
        ch_id = self._allocator.allocate(client_id)
        if ch_id is not None:
            self._rm_transport.activate_channel(ch_id)
        return ch_id

    def release_rm_channel(self, channel_id: int) -> None:
        """Release an RM channel."""
        self._rm_transport.deactivate_channel(channel_id)
        self._allocator.release(channel_id)

    def rm_channel_offset(self, channel_id: int) -> int:
        """Byte offset of an RM channel within the mmap."""
        return self._rm_offset + channel_id * CHANNEL_SIZE

    def reg_channel_offset(self, channel_id: int) -> int:
        """Byte offset of a registration channel within the mmap."""
        return self._reg_offset + channel_id * CHANNEL_SIZE


class SlotPool:
    """A pool of RPC channels allocated from CXL memory for Handler ↔ MetaServer.

    Created by MetaServer after receiving a slot pool allocation from RM.
    Channel 0 is reserved for Handler registration (HANDSHAKE).
    """

    def __init__(
        self,
        mm: mmap.mmap,
        pool_offset: int,
        max_channels: int = 32,
    ):
        self._mm = mm
        self._pool_offset = pool_offset
        self._max_channels = max_channels
        self._allocator = ChannelAllocator(max_channels)

        # Server transport for this slot pool
        self._transport = CxlRpcServerTransport(
            mm, pool_offset, max_channels
        )
        # Channel 0 = registration, always active
        self._transport.activate_channel(0)

    @property
    def transport(self) -> CxlRpcServerTransport:
        return self._transport

    @property
    def pool_offset(self) -> int:
        return self._pool_offset

    def allocate_channel(self, client_id: str) -> int | None:
        """Allocate a channel for a Handler. Returns channel_id or None."""
        ch_id = self._allocator.allocate(client_id)
        if ch_id is not None:
            self._transport.activate_channel(ch_id)
        return ch_id

    def release_channel(self, channel_id: int) -> None:
        """Release a Handler channel."""
        self._transport.deactivate_channel(channel_id)
        self._allocator.release(channel_id)

    def channel_offset(self, channel_id: int) -> int:
        """Absolute byte offset of a channel within the mmap."""
        return self._pool_offset + channel_id * CHANNEL_SIZE

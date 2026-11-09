# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageService local protocol: messages between a worker and its node.

Workers talk to the StorageService of their node over an AF_UNIX stream
socket (default ``/run/maru/node.sock``). Messages use the same 16-byte header
and MessagePack payload as MaruServer, split by the header's length field.
ZeroMQ is not used here because it cannot pass file descriptors: the reply to
ATTACH carries the memfd of every DRAM pool as SCM_RIGHTS on its first
sendmsg, and no other message carries descriptors. The server reads the
peer's pid with SO_PEERCRED; when the worker shares its pid namespace the pid
must match the one in ATTACH. The reply tells the worker the pid and start
time the node sees, which the worker then gives to MaruServer.

A response's code is its request's code with the 0x80 bit set. NOTIFY is
sent by the StorageService with sequence 0 and has no response.

Sockets are blocking and have no timeout. Any exception from send_frame or
recv_frame leaves the stream out of step, so the connection must be closed.
Both ends may write from more than one thread (a response and a NOTIFY), so
each connection serializes its send_frame calls with its own lock.

See the multi-storage wire spec, chapters 2 and 4.
"""

import array
import os
import socket
import struct
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import IntEnum, unique

from .protocol import HEADER_SIZE, PROTOCOL_MAGIC, PROTOCOL_VERSION, MessageHeader
from .storage_types import (
    DrainKind,
    FetchPlan,
    ItemOutcome,
    Key,
    KeyLocation,
    Location,
    NotifyKind,
    OpRef,
    PoolDescriptor,
    PoolKind,
    Status,
    WaitLevel,
)

DEFAULT_NODE_SOCKET = "/run/maru/node.sock"
# A message larger than this is a protocol error, not something to buffer.
MAX_NODE_PAYLOAD = 64 * 1024 * 1024
# The most descriptors one message may carry (one per DRAM pool).
MAX_NODE_FDS = 16
NOTIFY_SEQUENCE = 0


@unique
class NodeMessageType(IntEnum):
    """Message codes of the StorageService local endpoint."""

    ATTACH = 0x01
    DETACH = 0x02
    CREDIT = 0x10
    CREDIT_RETURN = 0x11
    WRITE_DONE = 0x12
    ABORT = 0x13
    FETCH = 0x18
    FETCH_CANCEL = 0x19
    LOCAL_PIN = 0x20
    LOCAL_UNPIN = 0x21
    DRAIN_ACK = 0x28
    UNMAP_ACK = 0x29
    NOTIFY = 0x30


# =============================================================================
# Records
# =============================================================================


@dataclass
class GroupLayout:
    """One KV cache group of the attaching worker."""

    object_bytes: int
    layout: str


@dataclass
class Budgets:
    """Node budgets in bytes, as resolved from the node config (spec 9)."""

    credit_max_bytes: int
    credit_total_max_bytes: int
    credit_refill_low: int
    credit_refill_high: int
    commit_pending_max_bytes: int
    transfer_reserve_bytes: dict[str, int] = field(default_factory=dict)
    fetch_request_max_bytes: int = 0
    fetch_max_inflight_bytes: int = 0
    fetch_max_batch_bytes: int = 0


@dataclass
class TransferDefaults:
    """Transfer-mode defaults the node derives from its tiers. None keeps the
    connector's own default."""

    async_load: bool | None = None
    async_store: bool | None = None


@dataclass
class WriteDoneItem:
    key: Key
    credit: Location
    logical_bytes: int


@dataclass
class FetchItem:
    key: Key
    plan: FetchPlan


# =============================================================================
# Messages
# =============================================================================


@dataclass
class AttachRequest:
    """ATTACH (0x01) - A worker joins the node and receives its pools.

    ``pid`` and ``pid_start`` are the worker's own view of itself; they must
    match SO_PEERCRED when the worker shares the node's pid namespace.
    """

    engine_id: str
    dp_index: int
    tp_rank: int
    pid: int
    pid_start: int
    incarnation: str
    namespace: str
    groups: list[GroupLayout]
    protocol: str
    capabilities: list[str] = field(default_factory=list)


@dataclass
class AttachResponse:
    status: Status
    error: str | None = None
    node_id: str = ""
    node_epoch: str = ""
    server: str = ""
    page_size: int = 0
    pools: list[PoolDescriptor] = field(default_factory=list)
    budgets: Budgets | None = None
    transfer_defaults: TransferDefaults = field(default_factory=TransferDefaults)
    local_pin: bool = False
    # The worker as the node sees it (its pid namespace); the worker sends
    # these in WORKER_OPEN
    pid: int = 0
    pid_start: int = 0


@dataclass
class DetachRequest:
    """DETACH (0x02) - Sent after every credit is back and copies are done."""


@dataclass
class DetachResponse:
    status: Status
    error: str | None = None


@dataclass
class CreditRequest:
    """CREDIT (0x10)."""

    op: OpRef
    worker_session: str
    want_bytes: int
    kind: PoolKind | None = None


@dataclass
class CreditResponse:
    status: Status
    error: str | None = None
    credits: list[Location] = field(default_factory=list)


@dataclass
class CreditReturnRequest:
    """CREDIT_RETURN (0x11) - Unused credit, also the answer to a return
    request."""

    credits: list[Location]


@dataclass
class CreditReturnResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class WriteDoneRequest:
    """WRITE_DONE (0x12) - Sent after the GPU copy (and flush) completed."""

    op: OpRef
    items: list[WriteDoneItem]
    wait: WaitLevel = WaitLevel.PUBLISHED


@dataclass
class WriteDoneResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class AbortRequest:
    """ABORT (0x13) - Sent after submitted GPU work on these pages drained."""

    op: OpRef
    credits: list[Location]


@dataclass
class AbortResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class FetchRequest:
    """FETCH (0x18)."""

    op: OpRef
    worker_session: str
    items: list[FetchItem]
    max_bytes: int
    deadline_ms: int


@dataclass
class FetchResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class FetchCancelRequest:
    """FETCH_CANCEL (0x19) - ``target`` is the op of the FETCH to cancel."""

    target: OpRef


@dataclass
class FetchCancelResponse:
    status: Status
    error: str | None = None


@dataclass
class LocalPinRequest:
    """LOCAL_PIN (0x20) - Off in v1 (answered with DISABLED); fields in S9."""

    items: list[KeyLocation] = field(default_factory=list)


@dataclass
class LocalPinResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class LocalUnpinRequest:
    """LOCAL_UNPIN (0x21) - Off in v1 (answered with DISABLED); fields in S9."""

    pin_ids: list[str] = field(default_factory=list)


@dataclass
class LocalUnpinResponse:
    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class DrainAckRequest:
    """DRAIN_ACK (0x28) - Every access of the ended epoch has finished."""

    kind: DrainKind
    epoch: str


@dataclass
class DrainAckResponse:
    status: Status
    error: str | None = None


@dataclass
class UnmapAckRequest:
    """UNMAP_ACK (0x29) - The pool's mapping and CUDA registration are gone."""

    pool_id: str
    pool_generation: int


@dataclass
class UnmapAckResponse:
    status: Status
    error: str | None = None


@dataclass
class Notify:
    """NOTIFY (0x30) - StorageService to worker, no response.

    Fields by kind: drain (drain_kind, epoch), credit_return (bytes),
    pool_add (pool), pool_remove (pool_id, pool_generation),
    epoch (node_epoch, server_epoch).
    """

    kind: NotifyKind
    drain_kind: DrainKind | None = None
    epoch: str | None = None
    bytes: int | None = None
    pool: PoolDescriptor | None = None
    pool_id: str | None = None
    pool_generation: int | None = None
    node_epoch: str | None = None
    server_epoch: str | None = None


@dataclass
class NotifyAck:
    """Placeholder response type; NOTIFY is never answered."""

    status: Status = Status.OK


NODE_MESSAGE_CLASSES = {
    NodeMessageType.ATTACH: (AttachRequest, AttachResponse),
    NodeMessageType.DETACH: (DetachRequest, DetachResponse),
    NodeMessageType.CREDIT: (CreditRequest, CreditResponse),
    NodeMessageType.CREDIT_RETURN: (CreditReturnRequest, CreditReturnResponse),
    NodeMessageType.WRITE_DONE: (WriteDoneRequest, WriteDoneResponse),
    NodeMessageType.ABORT: (AbortRequest, AbortResponse),
    NodeMessageType.FETCH: (FetchRequest, FetchResponse),
    NodeMessageType.FETCH_CANCEL: (FetchCancelRequest, FetchCancelResponse),
    NodeMessageType.LOCAL_PIN: (LocalPinRequest, LocalPinResponse),
    NodeMessageType.LOCAL_UNPIN: (LocalUnpinRequest, LocalUnpinResponse),
    NodeMessageType.DRAIN_ACK: (DrainAckRequest, DrainAckResponse),
    NodeMessageType.UNMAP_ACK: (UnmapAckRequest, UnmapAckResponse),
    NodeMessageType.NOTIFY: (Notify, NotifyAck),
}


# =============================================================================
# Framing over AF_UNIX stream sockets
# =============================================================================


class FrameError(ConnectionError):
    """The stream broke or carried something that is not a message. The
    connection is out of step and must be closed."""


def _check_frame(frame: bytes) -> None:
    if len(frame) < HEADER_SIZE:
        raise ValueError(f"frame shorter than a header: {len(frame)}")
    header = MessageHeader.unpack(frame)
    if header.payload_length != len(frame) - HEADER_SIZE:
        raise ValueError(
            f"header says {header.payload_length} payload bytes, frame has "
            f"{len(frame) - HEADER_SIZE}"
        )
    if header.payload_length > MAX_NODE_PAYLOAD:
        raise ValueError(f"message too large: {header.payload_length} bytes")


def send_frame(sock: socket.socket, frame: bytes, fds: Sequence[int] = ()) -> None:
    """Send one encoded message; ``fds`` ride on its first byte.

    The caller holds the connection's send lock.
    """
    _check_frame(frame)
    if len(fds) > MAX_NODE_FDS:
        raise ValueError(f"too many descriptors: {len(fds)} > {MAX_NODE_FDS}")
    view = memoryview(frame)
    if fds:
        sent = socket.send_fds(sock, [view], list(fds))
        if sent <= 0:
            raise FrameError("connection closed while sending descriptors")
        view = view[sent:]
    if view:
        sock.sendall(view)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    chunks = []
    while n:
        chunk = sock.recv(n)
        if not chunk:
            raise FrameError("connection closed in the middle of a message")
        chunks.append(chunk)
        n -= len(chunk)
    return b"".join(chunks)


def recv_frame(sock: socket.socket, max_fds: int = 0) -> tuple[bytes, list[int]] | None:
    """Receive one message and the descriptors sent with it.

    ``max_fds`` is how many descriptors this message may bring: 0 for every
    message except the reply to ATTACH. Descriptors beyond it are closed and
    the message is refused, so a peer cannot pile descriptors on the
    receiver. Returns None when the peer closed the connection between
    messages. The caller owns the returned descriptors (close-on-exec).
    """
    if not 0 <= max_fds <= MAX_NODE_FDS:
        raise ValueError(f"max_fds must be within 0..{MAX_NODE_FDS}, got {max_fds}")
    data, fds, flags = _recv_with_fds(sock, HEADER_SIZE)
    if not data:
        for fd in fds:
            _close_quietly(fd)
        return None
    try:
        if flags & socket.MSG_CTRUNC:
            raise FrameError("descriptors were truncated")
        if len(fds) > max_fds:
            raise FrameError(f"{len(fds)} descriptors where at most {max_fds} may come")
        if len(data) < HEADER_SIZE:
            data += _recv_exact(sock, HEADER_SIZE - len(data))
        header = MessageHeader.unpack(data)
        if header.magic != PROTOCOL_MAGIC:
            raise FrameError(f"not a maru message (magic 0x{header.magic:04X})")
        if header.version != PROTOCOL_VERSION:
            raise FrameError(
                f"protocol version {header.version}, this side speaks {PROTOCOL_VERSION}"
            )
        if header.payload_length > MAX_NODE_PAYLOAD:
            raise FrameError(f"message too large: {header.payload_length} bytes")
        payload = _recv_exact(sock, header.payload_length)
    except BaseException:
        for fd in fds:
            _close_quietly(fd)
        raise
    return data + payload, fds


def _recv_with_fds(sock: socket.socket, n: int) -> tuple[bytes, list[int], int]:
    """recvmsg with descriptors marked close-on-exec as they arrive.

    socket.recv_fds() accepts a flags argument but does not pass it on, so
    MSG_CMSG_CLOEXEC would be lost there. Room is always made for
    MAX_NODE_FDS so that unexpected descriptors arrive and can be closed.
    """
    fds = array.array("i")
    data, ancdata, flags, _ = sock.recvmsg(
        n, socket.CMSG_SPACE(MAX_NODE_FDS * fds.itemsize), socket.MSG_CMSG_CLOEXEC
    )
    for level, kind, payload in ancdata:
        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
            fds.frombytes(payload[: len(payload) - (len(payload) % fds.itemsize)])
    return data, list(fds), flags


def _close_quietly(fd: int) -> None:
    try:
        os.close(fd)
    except OSError:
        pass


# struct ucred: pid_t pid; uid_t uid; gid_t gid
_UCRED = struct.Struct("iII")


def peer_credentials(sock: socket.socket) -> tuple[int, int, int]:
    """(pid, uid, gid) of the process on the other end of a unix socket, in
    this process's pid namespace."""
    raw = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, _UCRED.size)
    return _UCRED.unpack(raw)


def process_start_time(pid: int) -> int:
    """Start time of ``pid`` in clock ticks since boot (/proc/<pid>/stat field
    22), the ``pid_start`` that tells a reused pid apart."""
    with open(f"/proc/{pid}/stat", "rb") as f:
        stat = f.read()
    # The command name is in parentheses and may hold spaces; fields after it
    # start at field 3.
    rest = stat[stat.rindex(b")") + 2 :].split()
    return int(rest[19])


def same_pid_namespace(pid: int) -> bool:
    """Whether ``pid`` lives in this process's pid namespace."""
    return os.stat(f"/proc/{pid}/ns/pid").st_ino == os.stat("/proc/self/ns/pid").st_ino

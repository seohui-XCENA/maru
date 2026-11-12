# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""RemoteServer protocol, between a GPU node's remote tier and a memory node.

One TCP connection per GPU node run, opened by the key proof and HELLO; the
same header and MessagePack payload as MaruServer (``Serializer(
REMOTE_MESSAGE_CLASSES)``), responses matched by the header sequence. Only
the GPU node posts RDMA; the memory node reserves, commits and frees its own
pages.

Requests in LEDGER_CODES change pages: they carry an OpRef whose origin is
``peer_origin`` of the HELLO, and the memory node keeps their results in an
OpLedger until acked, across connections, for its run. Release evidence is
only a transfer that ended DONE, or one never posted; anything else leaves
the page quarantined until the memory node's NIC registration ends.
"""

from dataclasses import dataclass, field
from enum import IntEnum, StrEnum, unique

from .storage_types import (
    ItemOutcome,
    Key,
    Location,
    MissReason,
    OpRef,
    PoolKind,
    Status,
)

REMOTE_WIRE_VERSION = 1


def peer_origin(node_id: str, node_epoch: str) -> str:
    """The OpLedger origin, and credit holder, of a GPU node run."""
    return f"peer:{node_id}@{node_epoch}"


@unique
class RemoteMessageType(IntEnum):
    """Message codes of the RemoteServer endpoint."""

    HELLO = 0x01
    PING = 0x02
    GOODBYE = 0x03
    RESERVE = 0x10
    WRITE_DONE = 0x11
    CANCEL = 0x12
    DROP = 0x13
    STAGE = 0x18
    UNSTAGE = 0x19


@unique
class TransferEnd(StrEnum):
    """How an RDMA transfer on a page ended, as its poster saw it."""

    DONE = "done"  # the poll saw DONE: every byte landed
    NOT_POSTED = "not_posted"  # never posted
    UNKNOWN = "unknown"  # ERR, timeout or lost: the page may still change


@unique
class HelloRefusal(StrEnum):
    """Why a HELLO was refused (BUSY: not_ready, server_epoch; else REJECTED)."""

    VERSION = "version"
    SERVER_EPOCH = "server_epoch"
    PAGE_SIZE = "page_size"
    RETIRED = "retired"  # a later run of the node said HELLO, or it said GOODBYE
    NOT_READY = "not_ready"


@unique
class RoomLimit(StrEnum):
    """Why a memory node gives a peer no room."""

    QUARANTINE = "quarantine"  # the peer's or the node's quarantine cap


# =============================================================================
# Records
# =============================================================================


@dataclass
class RemotePool:
    """A memory node pool its NIC registered for this run."""

    pool_id: str
    kind: PoolKind
    pool_generation: int
    capacity: int
    base: int  # registered address of the pool's byte 0
    metadata: bytes  # NIXL partial metadata of the registration


@dataclass
class Written:
    """A credited page the peer posted a WRITE to; ``end`` is DONE or
    UNKNOWN (one never posted goes back by CANCEL)."""

    key: Key
    location: Location
    logical_bytes: int
    end: TransferEnd


# =============================================================================
# Messages
# =============================================================================


@dataclass
class HelloRequest:
    """HELLO (0x01) - The first request of a connection, after the key proof.

    ``capabilities``: those MaruServer accepted for the sender's run.
    ``conn_gen`` counts the run's connections to this memory node from 1.
    ``ack``: as OpRef.ack, for the run's ops so far.
    """

    version: int
    node_id: str
    node_epoch: str
    host_boot_id: str
    pid: int
    server_epoch: str
    agent_name: str
    agent_metadata: bytes
    page_size: int
    conn_gen: int
    capabilities: list[str] = field(default_factory=list)
    ack: int = 0


@dataclass
class HelloResponse:
    """The memory node's run, its registered pools and the peer's room
    (bytes it would credit now)."""

    status: Status
    error: str | None = None
    refusal: HelloRefusal | None = None
    node_id: str = ""
    node_epoch: str = ""
    server_epoch: str = ""
    capabilities: list[str] = field(default_factory=list)
    agent_name: str = ""
    pools: list[RemotePool] = field(default_factory=list)
    room: int = 0
    limit: RoomLimit | None = None


@dataclass
class PingRequest:
    """PING (0x02) - Liveness."""


@dataclass
class PingResponse:
    status: Status
    error: str | None = None
    room: int = 0
    limit: RoomLimit | None = None


@dataclass
class GoodbyeRequest:
    """GOODBYE (0x03) - The sender's run ends. Frees nothing: its credits and
    staging pages still open are quarantined, and its node_epoch is retired."""


@dataclass
class GoodbyeResponse:
    status: Status
    error: str | None = None


@dataclass
class ReserveRequest:
    """RESERVE (0x10) - Credit ``pages`` pages to the sender's run."""

    op: OpRef
    pages: int


@dataclass
class ReserveResponse:
    """``credits`` may be fewer than asked; ``room`` is what is left."""

    status: Status
    error: str | None = None
    credits: list[Location] = field(default_factory=list)
    room: int = 0
    limit: RoomLimit | None = None


@dataclass
class RemoteWriteDoneRequest:
    """WRITE_DONE (0x11) - DONE items are committed with no finalizer,
    UNKNOWN ones quarantined."""

    op: OpRef
    items: list[Written]


@dataclass
class RemoteWriteDoneResponse:
    """Items: ADDED, DUPLICATE (location: the committed one) or REJECTED."""

    status: Status
    error: str | None = None
    items: list[ItemOutcome] = field(default_factory=list)


@dataclass
class CancelRequest:
    """CANCEL (0x12) - Credits never posted to go back: those left of the
    RESERVE ``reserve_seq`` (one not received yet then gives none), or
    ``locations``."""

    op: OpRef
    reserve_seq: int | None = None
    locations: list[Location] = field(default_factory=list)


@dataclass
class CancelResponse:
    status: Status
    error: str | None = None
    freed: int = 0


@dataclass
class DropRequest:
    """DROP (0x13) - Retire ``key`` at exactly ``location``, if READY."""

    op: OpRef
    key: Key
    location: Location


@dataclass
class DropResponse:
    """``outcome``: as an item of RETIRE."""

    status: Status
    error: str | None = None
    outcome: ItemOutcome | None = None


@dataclass
class StageRequest:
    """STAGE (0x18) - Load the SSD page ``source`` (exact, with its
    generation) into a staging page for the sender to READ; stage_v1 only.

    ``stage_id`` is unique in the sender's run. ``deadline_ms`` counts from
    the first receipt and is not part of the op, so a resend may carry less.
    """

    op: OpRef
    stage_id: int
    key: Key
    source: Location
    deadline_ms: int


@dataclass
class StageResponse:
    """``staging`` to READ, or a miss: ``reason`` (busy, timeout, absent,
    io_error), none when an UNSTAGE of the stage_id came first. ``limit``
    says the peer has no room (``room`` 0) until the memory run ends."""

    status: Status
    error: str | None = None
    staging: Location | None = None
    reason: MissReason | None = None
    room: int = 0
    limit: RoomLimit | None = None


@dataclass
class UnstageRequest:
    """UNSTAGE (0x19) - The READ of a staging page ended so; DONE and
    NOT_POSTED free it, UNKNOWN quarantines it."""

    op: OpRef
    stage_id: int
    end: TransferEnd


@dataclass
class UnstageResponse:
    status: Status
    error: str | None = None


REMOTE_MESSAGE_CLASSES = {
    RemoteMessageType.HELLO: (HelloRequest, HelloResponse),
    RemoteMessageType.PING: (PingRequest, PingResponse),
    RemoteMessageType.GOODBYE: (GoodbyeRequest, GoodbyeResponse),
    RemoteMessageType.RESERVE: (ReserveRequest, ReserveResponse),
    RemoteMessageType.WRITE_DONE: (RemoteWriteDoneRequest, RemoteWriteDoneResponse),
    RemoteMessageType.CANCEL: (CancelRequest, CancelResponse),
    RemoteMessageType.DROP: (DropRequest, DropResponse),
    RemoteMessageType.STAGE: (StageRequest, StageResponse),
    RemoteMessageType.UNSTAGE: (UnstageRequest, UnstageResponse),
}

# Requests recorded in the memory node's OpLedger
LEDGER_CODES = frozenset(
    {
        RemoteMessageType.RESERVE,
        RemoteMessageType.WRITE_DONE,
        RemoteMessageType.CANCEL,
        RemoteMessageType.DROP,
        RemoteMessageType.STAGE,
        RemoteMessageType.UNSTAGE,
    }
)

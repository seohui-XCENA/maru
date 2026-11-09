# SPDX-License-Identifier: Apache-2.0
"""Descriptors for the storage directory.

The first half serves the session backends (cpu, mixed, remote_cxl) and stays
until they are removed. The second half holds the identifiers and records of
the multi-storage structure: a central MaruServer, one StorageService on every
node, and workers that read and write the node pools directly. Their meaning
and rules are in the multi-storage wire spec (chapters 1, 2 and 5).
"""

from dataclasses import dataclass
from enum import StrEnum, unique

from maru_shm.types import MaruHandle

STORAGE_CAPABILITY = "cpu_storage_v1"
MIXED_STORAGE_CAPABILITY = "mixed_storage_v1"
REMOTE_CXL_STORAGE_CAPABILITY = "remote_cxl_storage_v1"
MAX_STORAGE_BATCH = 1024


@dataclass(frozen=True)
class StorageLocation:
    """Owner-local allocation identity, never a process virtual address."""

    pool_id: str
    allocation_id: str
    generation: int
    offset: int
    length: int
    medium: str = "cpu"


# Compatibility for the first CPU-only protocol and callers.
CpuLocation = StorageLocation


class StorageError(RuntimeError):
    """A metadata operation was explicitly rejected by the server."""


class StorageUnavailableError(ConnectionError):
    """Outcome unknown: allocation ownership must be retained until resolved."""


# =============================================================================
# Multi-storage: identifiers, records and result codes
# =============================================================================

MS_PROTOCOL = "ms1"
# Items in one multi-storage request (spec 9, MAX_BATCH)
MAX_BATCH = MAX_STORAGE_BATCH
# A generation is epoch_seq << GENERATION_EPOCH_SHIFT | counter, so every
# generation a new StorageService gives is above those of its earlier runs.
# It travels as an unsigned 64-bit integer, which bounds epoch_seq.
GENERATION_EPOCH_SHIFT = 40
GENERATION_COUNTER_MAX = (1 << GENERATION_EPOCH_SHIFT) - 1
EPOCH_SEQ_MAX = (1 << (64 - GENERATION_EPOCH_SHIFT)) - 1


def make_generation(epoch_seq: int, counter: int) -> int:
    """The generation of the ``counter``-th page given in run ``epoch_seq``."""
    if (
        not 0 <= epoch_seq <= EPOCH_SEQ_MAX
        or not 0 <= counter <= GENERATION_COUNTER_MAX
    ):
        raise ValueError(f"generation out of range: {epoch_seq=} {counter=}")
    return (epoch_seq << GENERATION_EPOCH_SHIFT) | counter


def generation_epoch_seq(generation: int) -> int:
    """The StorageService run (epoch_seq) that gave this generation."""
    return generation >> GENERATION_EPOCH_SHIFT


@unique
class PoolKind(StrEnum):
    """Physical medium of a pool. ``cpu`` is read as the old name of dram,
    on the wire and in the node config."""

    DRAM = "dram"
    CXL = "cxl"
    SSD = "ssd"

    @classmethod
    def _missing_(cls, value):
        return cls.DRAM if value == "cpu" else None

    @classmethod
    def parse(cls, value: str) -> "PoolKind":
        return cls(value)


@unique
class Visibility(StrEnum):
    """How writes on a CXL region become visible to other hosts."""

    COHERENT = "coherent"
    FLUSH = "flush"


@unique
class Status(StrEnum):
    """Outcome of a whole request."""

    OK = "OK"
    EPOCH = "EPOCH"
    BUSY = "BUSY"
    REJECTED = "REJECTED"
    DISABLED = "DISABLED"


@unique
class ItemResult(StrEnum):
    """Outcome of one item of a request."""

    OK = "OK"
    HIT = "HIT"
    FETCH = "FETCH"
    MISS = "MISS"
    RECOVERING = "RECOVERING"
    ADDED = "ADDED"
    DUPLICATE = "DUPLICATE"
    RETIRED = "RETIRED"
    KEPT = "KEPT"
    STALE = "STALE"
    REJECTED = "REJECTED"


@unique
class KeptCause(StrEnum):
    """Why a retire or a move left the location in place."""

    PINNED = "PINNED"
    MOVED = "MOVED"
    INVALID = "INVALID"


@unique
class MissReason(StrEnum):
    ABSENT = "absent"
    OFFLINE = "offline"


@unique
class SessionRole(StrEnum):
    WORKER = "worker"
    PROBE = "probe"


@unique
class CloseMode(StrEnum):
    RESTART = "restart"
    LEAVE = "leave"


@unique
class ExitCause(StrEnum):
    EXIT = "exit"
    LOST = "lost"


@unique
class CommitPin(StrEnum):
    NONE = "none"
    NODE = "node"


@unique
class WaitLevel(StrEnum):
    PUBLISHED = "published"
    LOWER = "lower"


@unique
class DrainKind(StrEnum):
    SERVER = "server"
    NODE = "node"


@unique
class NotifyKind(StrEnum):
    DRAIN = "drain"
    CREDIT_RETURN = "credit_return"
    POOL_ADD = "pool_add"
    POOL_REMOVE = "pool_remove"
    EPOCH = "epoch"


@dataclass(frozen=True)
class Key:
    """A KV object: the engine's cache namespace and the key inside it."""

    namespace: str
    key: str


@dataclass(frozen=True)
class Location:
    """Where one copy of a KV lies. The session backends use StorageLocation,
    which has the same fields with ``medium`` instead of ``kind``."""

    pool_id: str
    allocation_id: str
    generation: int
    offset: int
    length: int
    kind: PoolKind


@dataclass(frozen=True)
class KeyLocation:
    key: Key
    location: Location


@dataclass(frozen=True)
class OpRef:
    """Identifies a state-changing request for resends (spec 1, op).

    ``digest`` is op_digest() of the request without its ``op`` field, and
    ``ack`` is the highest seq whose result the sender already holds, along
    with every seq below it.
    """

    origin: str
    seq: int
    digest: bytes
    ack: int


@dataclass(frozen=True)
class PoolInfo:
    pool_id: str
    kind: PoolKind
    node_id: str
    pool_generation: int
    capacity: int
    page_size: int
    rm_domain: str | None = None
    region_id: int | None = None
    device_id: str | None = None
    path: str | None = None
    # CXL only; fields that do not apply to a pool are None
    visibility: Visibility | None = None


@dataclass(frozen=True)
class DaxMapping:
    """A CXL region a worker maps: its RM domain and the RM handle."""

    rm_domain: str
    handle: MaruHandle


@dataclass(frozen=True)
class PoolDescriptor:
    """What a worker needs to map a pool. DRAM gives the index of the memfd
    sent with the message, CXL gives the region; SSD has no descriptor."""

    pool: PoolInfo
    memfd: int | None = None
    dax: DaxMapping | None = None


@dataclass(frozen=True)
class WorkerRef:
    engine_id: str
    dp_index: int
    tp_rank: int
    pid: int
    pid_start: int
    incarnation: str
    worker_session: str | None = None


@dataclass(frozen=True)
class FetchPlan:
    source: Location
    source_node: str
    bytes: int


@dataclass(frozen=True)
class ItemOutcome:
    """The result of one item. Only the fields its result uses are set."""

    result: ItemResult
    location: Location | None = None
    pin_id: str | None = None
    plan: FetchPlan | None = None
    reason: MissReason | None = None
    cause: KeptCause | None = None


@dataclass(frozen=True)
class RmDomain:
    rm_domain: str
    device_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class PoolUsage:
    """Bytes of one pool in each page state (spec 6.2); they add up to
    ``capacity``."""

    pool_id: str
    capacity: int
    free: int
    credited: int
    transfer: int
    pending: int
    ready: int
    retiring: int
    quarantined: int


@dataclass(frozen=True)
class AccessInfo:
    """Reads MaruServer saw on the reference path, for the node's LRU."""

    pool_id: str
    allocation_id: str
    generation: int
    hits: int
    last_ts: float


@dataclass(frozen=True)
class BlockRequest:
    """A removal MaruServer starts; the node blocks new local pins first."""

    block_id: str
    items: tuple[KeyLocation, ...]
    reason: str

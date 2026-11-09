# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageManager: op ledger, WRITE_DONE -> COMMIT, removal, worker exit and
restart drain."""

import collections
import dataclasses
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Protocol, TypeVar

from maru_common.node_protocol import (
    AbortRequest,
    AbortResponse,
    CreditRequest,
    CreditResponse,
    CreditReturnRequest,
    CreditReturnResponse,
    DetachResponse,
    DrainAckRequest,
    DrainAckResponse,
    Notify,
    UnmapAckRequest,
    UnmapAckResponse,
    WriteDoneRequest,
    WriteDoneResponse,
)
from maru_common.protocol import CommitItem, NodeHeartbeatResponse
from maru_common.serializer import DecodedRequest, op_digest
from maru_common.storage_types import (
    MAX_BATCH,
    AccessInfo,
    BlockRequest,
    CommitPin,
    DrainKind,
    ExitCause,
    ItemOutcome,
    ItemResult,
    KeptCause,
    KeyLocation,
    NotifyKind,
    OpRef,
    PoolKind,
    Status,
    WorkerRef,
)

from .local_pin import LocalPins
from .peer_watcher import PeerWatcher
from .placement import PlacementPolicy
from .pool_manager import (
    RECOVERY_TAG,
    InvalidTransitionError,
    PageRef,
    PageState,
    PoolManager,
    holder_tag,
)
from .server_client import EpochChanged, ReplyError, ServerClient, ServerStopped

logger = logging.getLogger(__name__)

R = TypeVar("R")

# Unacknowledged ops kept per origin
OP_WINDOW = 65536
DEFAULT_GPU_DRAIN_GRACE_S = 5.0
DEFAULT_EVICT_INTERVAL_S = 0.05
# Requests of one connection waiting for the same op (resends)
WAITERS_PER_CONNECTION = 8
# Wait for more WRITE_DONE items to join a COMMIT batch
COMMIT_LINGER_S = 0.002
EXIT_RETRY_S = 0.5
EXIT_RETRY_MAX_S = 30.0


# ============================================================================
# Op ledger (receiver side of ops)
# ============================================================================


class Verdict(Enum):
    NEW = auto()
    REPLAY = auto()
    STALE = auto()
    CONFLICT = auto()
    BUSY = auto()


class LedgerEntry:
    """The result of one op, or the requests waiting for it."""

    def __init__(self, seq: int, digest: bytes):
        self.seq = seq
        self.digest = digest
        self.done = threading.Event()
        self.response: Any = None
        # Waiters per connection; dropped when it closes
        self.waiters: dict[object, list[Callable]] = {}


@dataclass
class _OriginLog:
    ack: int = 0
    entries: dict[int, LedgerEntry] = field(default_factory=dict)
    dropped: bool = False


class OpLedger:
    """Receiver side of ops: results per origin until acked."""

    def __init__(self, window: int = OP_WINDOW):
        self.window = window
        self._lock = threading.Lock()
        self._origins: dict[str, _OriginLog] = {}

    def begin(
        self, origin: str, op: OpRef, digest: bytes
    ) -> tuple[Verdict, LedgerEntry | None]:
        with self._lock:
            log = self._origins.setdefault(origin, _OriginLog())
            if op.ack > log.ack:
                log.ack = op.ack
                for seq in [s for s in log.entries if s <= log.ack]:
                    if log.entries[seq].done.is_set():
                        del log.entries[seq]
            if op.seq <= log.ack:
                return Verdict.STALE, None
            entry = log.entries.get(op.seq)
            if entry is not None:
                if entry.digest != digest:
                    return Verdict.CONFLICT, entry
                return Verdict.REPLAY, entry
            if len(log.entries) >= self.window:
                return Verdict.BUSY, None
            entry = LedgerEntry(op.seq, digest)
            log.entries[op.seq] = entry
            return Verdict.NEW, entry

    def finish(self, origin: str, entry: LedgerEntry, response) -> list[Callable]:
        """Record the result; returns the waiters to answer."""
        with self._lock:
            entry.response = response
            entry.done.set()
            waiters = [w for ws in entry.waiters.values() for w in ws]
            entry.waiters = {}
            log = self._origins.get(origin)
            if log is not None and log.dropped:
                self._drop_if_idle(origin, log)
            return waiters

    def attach(
        self, entry: LedgerEntry, reply: Callable, key: object
    ) -> tuple[bool, Callable | None]:
        """Answer ``reply`` with the result when the entry finishes. Returns
        whether it waits (False: already finished) and, past
        WAITERS_PER_CONNECTION waiters of ``key`` (a connection), the oldest
        one, displaced."""
        with self._lock:
            if entry.done.is_set():
                return False, None
            waiters = entry.waiters.setdefault(key, [])
            waiters.append(reply)
            if len(waiters) > WAITERS_PER_CONNECTION:
                return True, waiters.pop(0)
            return True, None

    def forget_waiter(self, origin: str, key: object) -> None:
        """A connection closed: its waiters will never be answered."""
        with self._lock:
            log = self._origins.get(origin)
            if log is not None:
                for entry in log.entries.values():
                    entry.waiters.pop(key, None)

    def waiters(self, origin: str) -> int:
        with self._lock:
            log = self._origins.get(origin)
            if log is None:
                return 0
            return sum(
                len(ws) for e in log.entries.values() for ws in e.waiters.values()
            )

    def drop(self, origin: str) -> None:
        """The origin ended; drop its log once no op of it runs."""
        with self._lock:
            log = self._origins.get(origin)
            if log is not None:
                log.dropped = True
                self._drop_if_idle(origin, log)

    def size(self, origin: str) -> int:
        with self._lock:
            log = self._origins.get(origin)
            return len(log.entries) if log else 0

    def running(self, origin: str) -> bool:
        """An op of ``origin`` has not finished."""
        with self._lock:
            log = self._origins.get(origin)
            return log is not None and not all(
                e.done.is_set() for e in log.entries.values()
            )

    def origins(self) -> int:
        with self._lock:
            return len(self._origins)

    def _drop_if_idle(self, origin: str, log: _OriginLog) -> None:
        if all(e.done.is_set() for e in log.entries.values()):
            del self._origins[origin]


# ============================================================================
# Workers
# ============================================================================


@dataclass
class AttachIdentity:
    """A checked worker; pid and pid_start in this node's pid namespace."""

    incarnation: str
    engine_id: str
    dp_index: int
    tp_rank: int
    pid: int
    pid_start: int
    namespace: str


class WorkerConn(Protocol):
    """What the manager uses of a LocalServer connection."""

    def close(self) -> None: ...

    def post_notify(self, notify: Notify) -> None: ...


@dataclass(eq=False)
class WorkerRecord:
    ident: AttachIdentity
    conn: WorkerConn | None = None
    worker_session: str | None = None
    detached: bool = False
    exited: bool = False

    @property
    def incarnation(self) -> str:
        return self.ident.incarnation

    def ref(self) -> WorkerRef:
        i = self.ident
        return WorkerRef(
            engine_id=i.engine_id,
            dp_index=i.dp_index,
            tp_rank=i.tp_rank,
            pid=i.pid,
            pid_start=i.pid_start,
            incarnation=i.incarnation,
            worker_session=self.worker_session,
        )


class OldState(Enum):
    WAITING = auto()  # neither attached again nor seen ending
    DRAINING = auto()  # attached again; NOTIFY drain sent, DRAIN_ACK awaited
    CONFIRMED = auto()


@dataclass
class OldWorker:
    ref: WorkerRef
    state: OldState = OldState.WAITING
    how: str = ""


@dataclass(eq=False)
class _WriteDone:
    origin: str
    entry: LedgerEntry
    outcomes: list[ItemOutcome | None]
    remaining: int


@dataclass(eq=False)
class _Ticket:
    ref: PageRef
    item: CommitItem
    write_done: _WriteDone
    index: int


def _worker_key(incarnation: str) -> str:
    return f"worker:{incarnation}"


def _old_key(incarnation: str) -> str:
    return f"old:{incarnation}"


# ============================================================================
# StorageManager
# ============================================================================


class StorageManager:
    def __init__(
        self,
        *,
        node_epoch: str,
        pool_manager: PoolManager,
        placement: PlacementPolicy,
        local_pins: LocalPins,
        server: ServerClient,
        gpu_drain_grace_s: float = DEFAULT_GPU_DRAIN_GRACE_S,
        evict_interval_s: float = DEFAULT_EVICT_INTERVAL_S,
        op_window: int = OP_WINDOW,
    ):
        self.node_epoch = node_epoch
        self.previous_node_epoch: str | None = None
        self.pools = pool_manager
        self.placement = placement
        self.local_pins = local_pins
        self.server = server
        self.gpu_drain_grace_s = gpu_drain_grace_s
        self.evict_interval_s = evict_interval_s
        self.ledger = OpLedger(op_window)
        self.peer_watcher = PeerWatcher(self._on_process_exit)

        self._lock = threading.RLock()
        self._workers: dict[str, WorkerRecord] = {}
        self._old: dict[str, OldWorker] = {}
        self._adopted: list[str] = []
        self._adopted_released = False

        self._cv = threading.Condition(self._lock)
        self._tickets: collections.deque[_Ticket] = collections.deque()
        self._exits: collections.deque[WorkerRef] = collections.deque()
        self._retiring: set[PageRef] = set()
        # Pages to RETIRE at a time: of block requests (_central, they stay
        # blocked) or with an unknown COMMIT result (_settle)
        self._retire_queue: dict[PageRef, float] = {}
        self._central: set[PageRef] = set()
        self._settle: set[PageRef] = set()
        self._blocks: dict[str, set[PageRef]] = {}
        self._evict_wake = threading.Event()
        self._safe: str | None = None
        self._stopping = False
        self._threads: list[threading.Thread] = []
        self._timers: set[threading.Timer] = set()
        self._unmapped: set[tuple[str, str, int]] = set()
        # RETIRE results by outcome ("RETIRED", "KEPT:PINNED", ...)
        self.retire_results: collections.Counter[str] = collections.Counter()

    # ------------------------------------------------------------- lifecycle

    def start(self) -> None:
        self.peer_watcher.start()
        for target, name in (
            (self._commit_loop, "maru-commit"),
            (self._evict_loop, "maru-evict"),
            (self._exit_loop, "maru-worker-exit"),
        ):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self._threads.append(thread)

    def stop(self) -> None:
        """Stop the threads; unresolved requests are given up."""
        with self._cv:
            self._stopping = True
            timers = list(self._timers)
            self._timers.clear()
            self._cv.notify_all()
        for timer in timers:
            timer.cancel()
        self._evict_wake.set()
        self.server.stop()
        for thread in self._threads:
            thread.join(timeout=10.0)
        self._threads.clear()
        self.peer_watcher.stop()

    @property
    def safe_state(self) -> str | None:
        """Why the node is in the safe state, or None."""
        return self._safe

    def enter_safe_state(self, reason: str) -> None:
        """No new credit, no eviction, no page reused; leaving it needs
        re-registration, which is not supported yet."""
        with self._cv:
            if self._safe is not None:
                return
            self._safe = reason
            self._cv.notify_all()
        self.pools.stop_credit(reason)
        logger.error(
            "safe state (%s): no credit, no eviction until the StorageService restarts",
            reason,
        )

    # --------------------------------------------------------------- workers

    def attach(
        self, conn: WorkerConn, ident: AttachIdentity
    ) -> tuple[WorkerRecord | None, str]:
        """Register or re-bind an attaching worker; (None, reason) to refuse."""
        with self._lock:
            rec = self._workers.get(ident.incarnation)
            if rec is not None:
                return self._rebind(rec, conn, ident)
            old = self._old.get(ident.incarnation)
            if old is not None and (old.ref.pid, old.ref.pid_start) != (
                ident.pid,
                ident.pid_start,
            ):
                return None, (
                    "MaruServer lists this incarnation for another process "
                    f"(pid {old.ref.pid})"
                )
        # pidfd_open and /proc reads happen outside the lock
        key = _worker_key(ident.incarnation)
        if not self.peer_watcher.watch(key, ident.pid, ident.pid_start):
            return None, "the process has ended"
        with self._lock:
            rec = self._workers.get(ident.incarnation)
            if rec is not None:  # attached meanwhile on another connection
                return self._rebind(rec, conn, ident)
            rec = WorkerRecord(ident=ident, conn=conn)
            self._workers[ident.incarnation] = rec
            old = self._old.get(ident.incarnation)
            unwatch_old = False
            if old is not None and old.state == OldState.WAITING:
                old.state = OldState.DRAINING
                unwatch_old = True
        if unwatch_old:
            self.peer_watcher.unwatch(_old_key(ident.incarnation))
        if not self.peer_watcher.watching(key):
            # It ended between watch() and the record: its exit found no record
            self._worker_exited(ident.incarnation)
            return None, "the process has ended"
        return rec, ""

    def _rebind(
        self, rec: WorkerRecord, conn: WorkerConn, ident: AttachIdentity
    ) -> tuple[WorkerRecord | None, str]:
        """Caller holds the lock. The same incarnation attaches again."""
        if rec.exited:
            return None, "this incarnation has ended"
        if rec.ident != ident:
            return None, (
                f"the incarnation is attached with another identity (pid {rec.ident.pid})"
            )
        old_conn, rec.conn = rec.conn, conn
        rec.detached = False
        if old_conn is not None and old_conn is not conn:
            old_conn.close()  # only shuts the socket down; no lock taken
        return rec, ""

    def after_attach_reply(self, rec: WorkerRecord) -> None:
        """Send NOTIFY drain to an old worker that attached again."""
        with self._lock:
            old = self._old.get(rec.incarnation)
            draining = old is not None and old.state == OldState.DRAINING
            conn = rec.conn
        if draining and conn is not None:
            conn.post_notify(
                Notify(
                    kind=NotifyKind.DRAIN,
                    drain_kind=DrainKind.NODE,
                    epoch=self.previous_node_epoch,
                )
            )

    def connection_lost(self, rec: WorkerRecord, conn) -> None:
        """The UDS closed; the credit stays until the process exits."""
        with self._lock:
            if rec.conn is conn:
                rec.conn = None
        self.ledger.forget_waiter(rec.incarnation, conn)

    def detach(self, rec: WorkerRecord) -> DetachResponse:
        with self._lock:
            rec.detached = True
            held = self.pools.held_bytes(rec.incarnation)
        if held:
            logger.warning(
                "worker %s detached holding %d bytes of credit; they wait for its exit",
                rec.incarnation,
                held,
            )
        return DetachResponse(status=Status.OK)

    def after_detach_reply(self, rec: WorkerRecord) -> None:
        """Forget a worker that detached holding nothing: its record, pidfd
        and op records go. One with credit, an op running or an old run to
        drain waits for its exit."""
        incarnation = rec.incarnation
        with self._lock:
            old = self._old.get(incarnation)
            if (
                not rec.detached
                or rec.exited
                or self._workers.get(incarnation) is not rec
                or (old is not None and old.state != OldState.CONFIRMED)
                or self.ledger.running(incarnation)
                or not self.pools.forget_holder(incarnation)
            ):
                return
            del self._workers[incarnation]
            # Under the lock, so a re-attach of the incarnation (which takes
            # it) never loses its new watch or op records; neither the
            # watcher nor the ledger calls back while holding its own lock
            self.peer_watcher.unwatch(_worker_key(incarnation))
            self.ledger.drop(incarnation)
        logger.info("worker %s detached and is forgotten", incarnation)

    def worker(self, incarnation: str) -> WorkerRecord | None:
        with self._lock:
            return self._workers.get(incarnation)

    def _on_process_exit(self, key: str) -> None:
        kind, _, incarnation = key.partition(":")
        if kind == "worker":
            self._worker_exited(incarnation)
        elif kind == "old":
            self._old_worker_exited(incarnation)

    def _worker_exited(self, incarnation: str) -> None:
        with self._lock:
            rec = self._workers.pop(incarnation, None)
            if rec is None or rec.exited:
                return
            rec.exited = True
            conn, rec.conn = rec.conn, None
        if conn is not None:
            conn.close()
        refs = self.pools.quarantine_holder(incarnation)
        logger.info(
            "worker %s (pid %d) ended: %d credited pages quarantined for %.1f s",
            incarnation,
            rec.ident.pid,
            len(refs),
            self.gpu_drain_grace_s,
        )
        self._queue_exit(rec.ref())
        self._after(self.gpu_drain_grace_s, self._grace_over, incarnation)
        self.ledger.drop(incarnation)
        with self._lock:
            old = self._old.get(incarnation)
        if old is not None:
            self._confirm_old(old, "exited")

    def _grace_over(self, incarnation: str) -> None:
        freed = self.pools.release_quarantine(holder_tag(incarnation))
        if freed:
            logger.info("freed %d pages of ended worker %s", freed, incarnation)

    def _after(self, delay: float, fn: Callable, *args) -> None:
        def run() -> None:
            with self._lock:
                self._timers.discard(timer)
                if self._stopping:
                    return
            fn(*args)

        timer = threading.Timer(delay, run)
        timer.daemon = True
        with self._lock:
            if self._stopping:
                return
            self._timers.add(timer)
        timer.start()

    # -------------------------------------------------------- restart drain

    def start_restart_drain(
        self,
        old_workers: list[WorkerRef],
        adopted: list[str],
        previous_node_epoch: str | None = None,
    ) -> None:
        """Start confirming the old workers; before workers attach."""
        with self._lock:
            self.previous_node_epoch = previous_node_epoch
            self._adopted = list(adopted)
            for ref in old_workers:
                self._old.setdefault(ref.incarnation, OldWorker(ref))
            pending = [o for o in self._old.values() if o.state == OldState.WAITING]
        for old in pending:
            if not self.peer_watcher.watch(
                _old_key(old.ref.incarnation), old.ref.pid, old.ref.pid_start
            ):
                self._old_worker_exited(old.ref.incarnation)
        self._maybe_release_adopted()

    def _old_worker_exited(self, incarnation: str) -> None:
        with self._lock:
            old = self._old.get(incarnation)
            if old is None or old.state != OldState.WAITING:
                return
        self._queue_exit(old.ref)
        self._confirm_old(old, "exited")

    def _confirm_old(self, old: OldWorker, how: str) -> None:
        with self._lock:
            if old.state == OldState.CONFIRMED:
                return
            old.state = OldState.CONFIRMED
            old.how = how
        logger.info("old worker %s confirmed (%s)", old.ref.incarnation, how)
        self._maybe_release_adopted()

    def old_workers(self) -> dict[str, OldWorker]:
        with self._lock:
            return {k: dataclasses.replace(v) for k, v in self._old.items()}

    def _maybe_release_adopted(self) -> None:
        with self._lock:
            if self._adopted_released:
                return
            if any(o.state != OldState.CONFIRMED for o in self._old.values()):
                return
            self._adopted_released = True
            adopted = list(self._adopted)
        for pool_id in adopted:
            freed = self.pools.release_quarantine(RECOVERY_TAG, pool_id)
            logger.info(
                "every old worker is confirmed: %d quarantined pages of adopted "
                "pool %s are free",
                freed,
                pool_id,
            )

    # -------------------------------------------------------------- requests

    def _check_op(self, rec: WorkerRecord, op: OpRef, decoded: DecodedRequest):
        """The digest of a worker op, or None and the reason it is malformed."""
        if op.origin != rec.incarnation:
            return None, "the op origin is not this worker's incarnation"
        if op.seq < 1 or op.ack < 0 or op.ack >= op.seq:
            return None, f"bad op seq {op.seq} / ack {op.ack}"
        digest = op_digest(decoded.payload)
        if digest != op.digest:
            return None, "the op digest does not match the request"
        return digest, ""

    def _begin_sync(
        self,
        rec: WorkerRecord,
        op: OpRef,
        decoded: DecodedRequest,
        response_cls: Callable[..., R],
        stale: Callable[[], R],
    ) -> LedgerEntry | R:
        """Op rules for a synchronous request: a new LedgerEntry to run it,
        or the response to answer now."""
        digest, reason = self._check_op(rec, op, decoded)
        if digest is None:
            return response_cls(status=Status.REJECTED, error=reason)
        verdict, entry = self.ledger.begin(rec.incarnation, op, digest)
        if verdict == Verdict.NEW and entry is not None:
            return entry
        if verdict == Verdict.REPLAY and entry is not None:
            entry.done.wait()
            replay: R = entry.response
            return replay
        if verdict == Verdict.STALE:
            return stale()
        if verdict == Verdict.CONFLICT:
            return response_cls(
                status=Status.REJECTED,
                error=f"op {op.seq} was sent before with another digest",
            )
        return response_cls(status=Status.BUSY, error="too many unacknowledged ops")

    def credit(self, rec: WorkerRecord, decoded: DecodedRequest) -> CreditResponse:
        req: CreditRequest = decoded.request
        if not req.worker_session:
            return CreditResponse(
                status=Status.REJECTED,
                error="credit needs the worker_session of MaruServer",
            )
        if req.kind not in (None, PoolKind.DRAM):
            return CreditResponse(
                status=Status.REJECTED, error=f"this node has no {req.kind} tier"
            )
        if req.want_bytes <= 0:
            return CreditResponse(
                status=Status.REJECTED, error="want_bytes must be positive"
            )
        entry = self._begin_sync(
            rec,
            req.op,
            decoded,
            CreditResponse,
            lambda: CreditResponse(
                status=Status.REJECTED, error="stale op: seq is at or below its ack"
            ),
        )
        if not isinstance(entry, LedgerEntry):
            return entry
        asks: list[tuple[WorkerConn, int]] = []
        try:
            with self._lock:
                # Under the lock _worker_exited sets ``exited`` before it
                # quarantines, so no credit goes to a holder that ended; a
                # detached record may be forgotten (after_detach_reply)
                if rec.exited or rec.detached:
                    response = CreditResponse(status=Status.REJECTED, error="ended")
                else:
                    rec.worker_session = req.worker_session
                    credits = self.pools.credit(
                        rec.incarnation, req.want_bytes, req.kind
                    )
                    response = CreditResponse(status=Status.OK, credits=credits)
                    asks = [
                        (other.conn, nbytes)
                        for incarnation, nbytes in self.pools.credit_asks(
                            rec.incarnation
                        )
                        if (other := self._workers.get(incarnation)) is not None
                        and other.conn is not None
                    ]
        except Exception as e:
            logger.exception("CREDIT failed")
            response = CreditResponse(status=Status.REJECTED, error=f"internal: {e}")
        self.ledger.finish(rec.incarnation, entry, response)
        for conn, nbytes in asks:  # idle credit of others, for this waiting worker
            conn.post_notify(Notify(kind=NotifyKind.CREDIT_RETURN, bytes=nbytes))
        return response

    def credit_return(
        self, rec: WorkerRecord, req: CreditReturnRequest
    ) -> CreditReturnResponse:
        if len(req.credits) > MAX_BATCH:
            return CreditReturnResponse(
                status=Status.REJECTED, error=f"more than {MAX_BATCH} items"
            )
        items = []
        for credit in req.credits:
            ok, reason = self.pools.credit_return(rec.incarnation, credit)
            if not ok:
                logger.debug("CREDIT_RETURN item refused: %s", reason)
            items.append(ItemOutcome(ItemResult.OK if ok else ItemResult.REJECTED))
        return CreditReturnResponse(status=Status.OK, items=items)

    def abort(self, rec: WorkerRecord, decoded: DecodedRequest) -> AbortResponse:
        req: AbortRequest = decoded.request
        if not req.credits or len(req.credits) > MAX_BATCH:
            return AbortResponse(
                status=Status.REJECTED, error=f"an ABORT carries 1..{MAX_BATCH} items"
            )
        entry = self._begin_sync(
            rec,
            req.op,
            decoded,
            AbortResponse,
            lambda: AbortResponse(
                status=Status.OK,
                items=[ItemOutcome(ItemResult.STALE)] * len(req.credits),
            ),
        )
        if not isinstance(entry, LedgerEntry):
            return entry
        items = []
        for credit in req.credits:
            try:
                ok, reason = self.pools.abort(rec.incarnation, credit)
            except Exception as e:
                logger.exception("ABORT item failed")
                ok, reason = False, str(e)
            if not ok:
                logger.debug("ABORT item refused: %s", reason)
            items.append(ItemOutcome(ItemResult.OK if ok else ItemResult.REJECTED))
        response = AbortResponse(status=Status.OK, items=items)
        self.ledger.finish(rec.incarnation, entry, response)
        return response

    def write_done(
        self,
        rec: WorkerRecord,
        decoded: DecodedRequest,
        reply: Callable[[WriteDoneResponse], None],
        waiter: object = None,
    ) -> None:
        """Answer via ``reply`` once every item is published."""
        req: WriteDoneRequest = decoded.request
        if not req.items or len(req.items) > MAX_BATCH:
            reply(
                WriteDoneResponse(
                    status=Status.REJECTED,
                    error=f"a WRITE_DONE carries 1..{MAX_BATCH} items",
                )
            )
            return
        digest, reason = self._check_op(rec, req.op, decoded)
        if digest is None:
            reply(WriteDoneResponse(status=Status.REJECTED, error=reason))
            return
        verdict, entry = self.ledger.begin(rec.incarnation, req.op, digest)
        if verdict == Verdict.STALE:
            reply(
                WriteDoneResponse(
                    status=Status.OK,
                    items=[ItemOutcome(ItemResult.STALE)] * len(req.items),
                )
            )
            return
        if verdict == Verdict.CONFLICT:
            reply(
                WriteDoneResponse(
                    status=Status.REJECTED,
                    error=f"op {req.op.seq} was sent before with another digest",
                )
            )
            return
        if verdict == Verdict.BUSY:
            reply(
                WriteDoneResponse(
                    status=Status.BUSY, error="too many unacknowledged ops"
                )
            )
            return
        assert entry is not None  # NEW and REPLAY carry one
        key = reply if waiter is None else waiter
        if verdict == Verdict.REPLAY:
            waiting, displaced = self.ledger.attach(entry, reply, key)
            if not waiting:
                reply(entry.response)
            elif displaced is not None:
                # Only past the bound: that request asks again later
                displaced(
                    WriteDoneResponse(
                        status=Status.BUSY, error="too many resends of this op"
                    )
                )
            return
        self.ledger.attach(entry, reply, key)
        wd = _WriteDone(
            origin=rec.incarnation,
            entry=entry,
            outcomes=[None] * len(req.items),
            remaining=len(req.items),
        )
        tickets = []
        for i, item in enumerate(req.items):
            try:
                ref, location, why = self.pools.write_done(
                    rec.incarnation, item.credit, item.key, item.logical_bytes
                )
            except Exception as e:
                logger.exception("WRITE_DONE item failed")
                ref, location, why = None, None, str(e)
            if ref is None or location is None:
                logger.debug("WRITE_DONE item %d refused: %s", i, why)
                wd.outcomes[i] = ItemOutcome(ItemResult.REJECTED)
                wd.remaining -= 1
                continue
            tickets.append(
                _Ticket(
                    ref=ref,
                    item=CommitItem(
                        key=item.key,
                        location=location,
                        logical_bytes=item.logical_bytes,
                        pin=CommitPin.NONE,
                    ),
                    write_done=wd,
                    index=i,
                )
            )
        if not tickets:
            self._finish_write_done(wd)
            return
        with self._cv:
            self._tickets.extend(tickets)
            self._cv.notify_all()

    def _finish_write_done(self, wd: _WriteDone) -> None:
        items = [o for o in wd.outcomes if o is not None]
        assert len(items) == len(wd.outcomes)  # every item has its outcome
        response = WriteDoneResponse(status=Status.OK, items=items)
        for reply in self.ledger.finish(wd.origin, wd.entry, response):
            try:
                reply(response)
            except Exception:
                logger.debug("could not answer a WRITE_DONE", exc_info=True)

    def drain_ack(self, rec: WorkerRecord, req: DrainAckRequest) -> DrainAckResponse:
        if req.kind == DrainKind.NODE:
            if req.epoch == self.node_epoch:
                return DrainAckResponse(
                    status=Status.REJECTED, error="the current node epoch has not ended"
                )
            with self._lock:
                old = self._old.get(rec.incarnation)
                if old is not None and old.state != OldState.DRAINING:
                    old = None
            if old is not None:
                self._confirm_old(old, f"drained node epoch {req.epoch}")
        return DrainAckResponse(status=Status.OK)

    def unmap_ack(self, rec: WorkerRecord, req: UnmapAckRequest) -> UnmapAckResponse:
        # Recorded only: no pool is removed yet
        with self._lock:
            self._unmapped.add((rec.incarnation, req.pool_id, req.pool_generation))
        return UnmapAckResponse(status=Status.OK)

    # ---------------------------------------------------------------- commit

    def _commit_loop(self) -> None:
        while True:
            with self._cv:
                while not self._stopping and (not self._tickets or self._safe):
                    self._cv.wait()
                if self._stopping:
                    return
                if len(self._tickets) < MAX_BATCH:
                    self._cv.wait(COMMIT_LINGER_S)
                    if self._stopping:
                        return
                batch = [
                    self._tickets.popleft()
                    for _ in range(min(MAX_BATCH, len(self._tickets)))
                ]
            if not batch:
                continue
            try:
                response = self.server.commit([t.item for t in batch])
            except EpochChanged as e:
                with self._cv:
                    self._tickets.extendleft(reversed(batch))
                self.enter_safe_state(f"MaruServer answered EPOCH to COMMIT ({e})")
                continue
            except ServerStopped:
                with self._cv:
                    self._tickets.extendleft(reversed(batch))
                return
            except ReplyError:
                # It ran, but the results are unusable: settle each page
                self._apply_commit(batch, None)
                continue
            except Exception:
                logger.exception("COMMIT failed; the pages stay PENDING")
                with self._cv:
                    self._tickets.extendleft(reversed(batch))
                time.sleep(0.1)
                continue
            self._apply_commit(batch, response)

    def _apply_commit(self, batch: list[_Ticket], response) -> None:
        outcomes: list[ItemOutcome | None]
        if response is None:
            outcomes = [None] * len(batch)
        elif response.status == Status.OK:
            outcomes = response.items
        else:
            # Not run: none of these locations is in the Catalog
            logger.error(
                "COMMIT refused as a whole (%s %s); %d pages are freed",
                response.status,
                response.error,
                len(batch),
            )
            outcomes = [ItemOutcome(ItemResult.REJECTED)] * len(batch)
        added = False
        finished = []
        for ticket, outcome in zip(batch, outcomes, strict=True):
            try:
                answer, freshly_added = self._apply_outcome(ticket, outcome)
                added = added or freshly_added
            except Exception:
                logger.exception("applying the COMMIT result of %s failed", ticket.ref)
                answer = ItemOutcome(ItemResult.REJECTED)
            wd = ticket.write_done
            with self._lock:
                wd.outcomes[ticket.index] = answer
                wd.remaining -= 1
                if wd.remaining == 0:
                    finished.append(wd)
        for wd in finished:
            self._finish_write_done(wd)
        if added:
            self._evict_wake.set()

    def _apply_outcome(self, ticket: _Ticket, outcome: ItemOutcome | None):
        """Move the page by its COMMIT result; returns (answer, added)."""
        result = outcome.result if outcome is not None else None
        if result == ItemResult.ADDED:
            self.pools.commit_added(ticket.ref)
            self.placement.on_ready(ticket.ref)
            return outcome, True
        if result in (ItemResult.DUPLICATE, ItemResult.REJECTED):
            self.pools.commit_freed(ticket.ref)
            return outcome, False
        # Unknown whether the location is in the Catalog: treat it as there
        # and settle it with a RETIRE (RETIRED frees, PINNED proves it there)
        logger.error("COMMIT item answered %s; settling page %s", result, ticket.ref)
        self.pools.commit_added(ticket.ref)
        with self._lock:
            self._settle.add(ticket.ref)
            self._retire_queue[ticket.ref] = 0.0
        self._evict_wake.set()
        return ItemOutcome(ItemResult.REJECTED), False

    # --------------------------------------------------------------- removal

    def _evict_loop(self) -> None:
        while True:
            self._evict_wake.wait(self.evict_interval_s)
            self._evict_wake.clear()
            with self._lock:
                if self._stopping:
                    return
                safe = self._safe is not None
            try:
                self._reclaim_retiring()
                if not safe:
                    self._retire_queued()
                    self._evict_once()
            except ServerStopped:
                return
            except Exception:
                logger.exception("eviction pass failed")

    def _evict_once(self) -> None:
        for pool_id in self.pools.pool_ids():
            info = self.pools.pool_info(pool_id)
            if (
                info is None
                or info.kind != PoolKind.DRAM
                or pool_id in self._adopted
                or not self.pools.creditable(pool_id)
            ):
                continue
            candidates = self.placement.candidates(
                pool_id,
                capacity=info.capacity,
                free_bytes=self.pools.bytes_in(PageState.FREE, pool_id),
                retiring_bytes=self.pools.bytes_in(PageState.RETIRING, pool_id),
                page_size=info.page_size,
                limit=MAX_BATCH,
            )
            refs, items = self._ready_items(candidates)
            if refs:
                self.retire(refs, items)

    def _ready_items(self, candidates) -> tuple[list[PageRef], list[KeyLocation]]:
        refs, items = [], []
        for ref in candidates:
            page = self.pools.page(ref)
            if (
                page is None
                or page.state != PageState.READY
                or page.key is None
                or page.location is None
            ):
                self.placement.on_gone(ref)
                self._forget_queued(ref)
                continue
            refs.append(ref)
            items.append(KeyLocation(key=page.key, location=page.location))
        return refs, items

    def _retire_queued(self) -> None:
        """RETIRE pages of block requests and pages being settled."""
        now = time.monotonic()
        with self._lock:
            due = [r for r, at in self._retire_queue.items() if at <= now]
            for ref in due:
                self._retire_queue[ref] = float("inf")  # in flight
        for i in range(0, len(due), MAX_BATCH):
            refs, items = self._ready_items(due[i : i + MAX_BATCH])
            if refs:
                self.retire(refs, items)

    def retire(self, refs: list[PageRef], items: list[KeyLocation]) -> None:
        """Block local pins, RETIRE, act on each result."""
        self.local_pins.block(refs)
        try:
            response = self.server.retire(items)
        except EpochChanged as e:
            # Unknown: the pages stay READY with their local pins blocked
            self._hold(refs)
            self.enter_safe_state(f"MaruServer answered EPOCH to RETIRE ({e})")
            return
        except ServerStopped:
            self._hold(refs)
            raise
        except ReplyError:
            self._retire_failed(refs)
            return
        except Exception:
            logger.exception("RETIRE failed")
            self._hold(refs)
            return
        if response.status != Status.OK:
            logger.error(
                "RETIRE refused as a whole: %s %s", response.status, response.error
            )
            self._retire_failed(refs)
            return
        for ref, outcome in zip(refs, response.items, strict=True):
            try:
                self._apply_retire(ref, outcome)
            except Exception:
                logger.exception("applying the RETIRE result of %s failed", ref)
                self._hold([ref])
        self._reclaim_retiring()

    def _apply_retire(self, ref: PageRef, outcome: ItemOutcome) -> None:
        with self._lock:
            central = ref in self._central
            settling = ref in self._settle
            cause = f":{outcome.cause.value}" if outcome.cause else ""
            self.retire_results[f"{outcome.result.value}{cause}"] += 1
        if outcome.result == ItemResult.RETIRED:
            self._retired(ref)
        elif outcome.result == ItemResult.KEPT and outcome.cause == KeptCause.PINNED:
            if central:
                self._retry_queued([ref])  # stays blocked
            elif settling:
                # Pinned, so it is in the Catalog: an ordinary READY page now
                self._forget_queued(ref)
                self.local_pins.unblock([ref])
                self.placement.on_ready(ref)
            else:
                self.local_pins.after_kept(ref, KeptCause.PINNED)
                self.placement.kept(ref, KeptCause.PINNED)
        elif outcome.result == ItemResult.KEPT and outcome.cause == KeptCause.MOVED:
            self._reconcile_moved(ref, outcome.key, outcome.location)
        elif outcome.result == ItemResult.KEPT and outcome.cause == KeptCause.INVALID:
            self._pool_invalid(ref)
        else:
            logger.error("RETIRE item answered %s for %s", outcome.result, ref)
            self._retire_failed([ref])

    def _retired(self, ref: PageRef) -> None:
        self.pools.retired(ref)
        self.placement.retired(ref)
        self._forget_queued(ref)
        with self._lock:
            self._retiring.add(ref)

    def _reconcile_moved(self, ref: PageRef, key, occupant) -> None:
        """KEPT(MOVED): another published location occupies the page. Never
        free it; the ledger takes the occupant as its READY entry."""
        self._forget_queued(ref)
        if key is None or occupant is None:
            logger.error("RETIRE of %s: MOVED without an occupant; page held", ref)
            self.placement.hold(ref)
            return
        try:
            new = self.pools.reconcile(ref, key, occupant)
        except InvalidTransitionError as e:
            logger.error(
                "RETIRE of %s: MOVED to %s does not fit (%s); page held",
                ref,
                occupant,
                e,
            )
            self.placement.hold(ref)
            return
        logger.warning("page %s holds %s %s per the Catalog", ref, key, occupant)
        self.placement.on_gone(ref)
        self.local_pins.forget(ref)
        self.placement.on_ready(new)

    def _pool_invalid(self, ref: PageRef) -> None:
        if self.pools.disable_pool(ref.pool_id):
            logger.error(
                "MaruServer says pool %s is no longer valid for this node; it is "
                "not used any more",
                ref.pool_id,
            )
        self._hold([ref])

    def _retire_failed(self, refs: list[PageRef]) -> None:
        """No usable result: queued pages are tried again, others reopened."""
        with self._lock:
            queued = [r for r in refs if r in self._central or r in self._settle]
        self._retry_queued(queued)
        others = [r for r in refs if r not in queued]
        self.local_pins.unblock(others)
        self.placement.release(others)

    def _hold(self, refs: list[PageRef]) -> None:
        for ref in refs:
            self._forget_queued(ref)
            self.placement.hold(ref)

    def _retry_queued(self, refs: list[PageRef]) -> None:
        at = time.monotonic() + self.placement.kept_backoff_s
        with self._lock:
            for ref in refs:
                self._retire_queue[ref] = at

    def _forget_queued(self, ref: PageRef) -> None:
        with self._lock:
            self._retire_queue.pop(ref, None)
            self._central.discard(ref)
            self._settle.discard(ref)

    def _reclaim_retiring(self) -> None:
        with self._lock:
            refs = list(self._retiring)
        for ref in refs:
            if self.local_pins.readers(ref):
                continue
            self.pools.reclaim(ref)
            self.local_pins.forget(ref)
            with self._lock:
                self._retiring.discard(ref)

    # ------------------------------------------------------------- heartbeat

    def on_heartbeat(self, response: NodeHeartbeatResponse) -> None:
        if response.access:
            self.placement.on_access(response.access, self._resolve_access)
        for block in response.blocks:
            self._block(block)
        sent = {b.block_id for b in response.blocks}
        with self._lock:
            for block_id in list(self._blocks):
                if block_id not in sent and not self._blocks[block_id] & self._central:
                    del self._blocks[block_id]

    def _resolve_access(self, info: AccessInfo) -> PageRef | None:
        ref = self.pools.find_allocation(
            info.pool_id, info.allocation_id, info.generation
        )
        if ref is None:
            return None
        page = self.pools.page(ref)
        return ref if page is not None and page.state == PageState.READY else None

    def _block(self, block: BlockRequest) -> None:
        """A central delete: block new local pins, then RETIRE the items. It is
        sent again until done, so pages already taken are skipped."""
        refs: list[PageRef] = []
        for item in block.items:
            ref = self.pools.find(item.location)
            page = self.pools.page(ref) if ref is not None else None
            if (
                ref is not None
                and page is not None
                and page.state == PageState.READY
                and page.key == item.key
            ):
                refs.append(ref)
        with self._lock:
            done = self._blocks.setdefault(block.block_id, set())
            refs = [r for r in refs if r not in done]
            done.update(refs)
            for ref in refs:
                self._central.add(ref)
                self._retire_queue[ref] = 0.0
        if not refs:
            return
        self.local_pins.block(refs)
        for ref in refs:
            self.placement.hold(ref)  # never an eviction candidate
        logger.info(
            "block %s (%s): %d pages blocked", block.block_id, block.reason, len(refs)
        )
        self._evict_wake.set()

    # ----------------------------------------------------------- worker exit

    def _queue_exit(self, ref: WorkerRef) -> None:
        with self._cv:
            self._exits.append(ref)
            self._cv.notify_all()

    def _exit_loop(self) -> None:
        pause = EXIT_RETRY_S
        while True:
            with self._cv:
                while not self._stopping and (not self._exits or self._safe):
                    self._cv.wait()
                if self._stopping:
                    return
                batch = [
                    self._exits.popleft()
                    for _ in range(min(MAX_BATCH, len(self._exits)))
                ]
            try:
                response = self.server.worker_exit(batch, ExitCause.EXIT)
                ok = response.status == Status.OK
                if not ok:
                    logger.error(
                        "WORKER_EXIT refused as a whole: %s %s; trying again",
                        response.status,
                        response.error,
                    )
            except EpochChanged as e:
                with self._cv:
                    self._exits.extendleft(reversed(batch))
                self.enter_safe_state(f"MaruServer answered EPOCH to WORKER_EXIT ({e})")
                continue
            except ServerStopped:
                return
            except Exception:
                logger.exception("WORKER_EXIT failed; trying again")
                ok = False
            if not ok:
                with self._cv:
                    self._exits.extendleft(reversed(batch))
                    self._cv.wait(pause)
                pause = min(pause * 2, EXIT_RETRY_MAX_S)
                continue
            pause = EXIT_RETRY_S
            for ref, outcome in zip(batch, response.items, strict=True):
                if outcome.result != ItemResult.OK:
                    logger.warning(
                        "WORKER_EXIT of %s answered %s", ref.incarnation, outcome.result
                    )
            logger.info("reported the exit of %s", [r.incarnation for r in batch])

    # ----------------------------------------------------------------- state

    def pending_exits(self) -> int:
        with self._lock:
            return len(self._exits)

    def pending_commits(self) -> int:
        with self._lock:
            return len(self._tickets)

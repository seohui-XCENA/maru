# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""WorkerRegistry: the workers attached to this node and the old workers of
the run before it.

A record lives from ATTACH until the process exits, or until a DETACH that
leaves nothing behind. An exit quarantines the worker's credited pages for the
GPU drain grace and is reported through ``on_exit``. Once every old worker is
confirmed (exited or drained), the quarantined pages of adopted pools are free.
After a MaruServer restart, the workers attached then drain the ended server
epoch (ServerDrain) before its pages are reused.
"""

import dataclasses
import itertools
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TypeVar

from maru_common.node_protocol import (
    DetachResponse,
    DrainAckRequest,
    DrainAckResponse,
    Notify,
)
from maru_common.storage_types import (
    DrainKind,
    NotifyKind,
    SessionRole,
    Status,
    WorkerRef,
)

from .op_ledger import OpLedger
from .pool_manager import RECOVERY_TAG, PoolManager, holder_tag
from .ports import ProcessWatcher, WorkerEndpoint

logger = logging.getLogger(__name__)

R = TypeVar("R")


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


@dataclass(eq=False)
class WorkerRecord:
    ident: AttachIdentity
    conn: WorkerEndpoint | None = None
    worker_session: str | None = None
    detached: bool = False
    exited: bool = False
    # A probe holds nothing and is forgotten with its connection
    role: SessionRole = SessionRole.WORKER

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


# How often the workers an open server drain waits for are logged
DRAIN_LOG_S = 30.0


@dataclass(eq=False)
class ServerDrain:
    """One MaruServer restart: the workers attached when it was seen end
    their accesses of the ended server epoch before its pages are reused."""

    index: int
    old_epoch: str
    waiting: set[str]
    started: float
    # quarantine tag of each holder's credit given before the restart
    credit_tags: dict[str, str] = field(default_factory=dict)

    @property
    def tag(self) -> str:
        """Quarantine tag of the pages freed while it is open."""
        return f"restart:{self.index}"


def _worker_key(incarnation: str) -> str:
    return f"worker:{incarnation}"


def _old_key(incarnation: str) -> str:
    return f"old:{incarnation}"


class WorkerRegistry:
    """Worker records and their lifecycle. Thread safe; ``on_exit`` is called
    without the lock held."""

    def __init__(
        self,
        *,
        node_epoch: str,
        pools: PoolManager,
        ledger: OpLedger,
        watcher: ProcessWatcher,
        on_exit: Callable[[WorkerRef], None],
        gpu_drain_grace_s: float,
    ):
        self.node_epoch = node_epoch
        self.previous_node_epoch: str | None = None
        self.gpu_drain_grace_s = gpu_drain_grace_s
        self._pools = pools
        self._ledger = ledger
        self._watcher = watcher
        self._on_exit = on_exit
        self._lock = threading.RLock()
        self._workers: dict[str, WorkerRecord] = {}
        self._probes: dict[str, WorkerRecord] = {}
        self._old: dict[str, OldWorker] = {}
        self._adopted: list[str] = []
        self._adopted_released = False
        self._stopping = False
        self._timers: set[threading.Timer] = set()
        self._drains: list[ServerDrain] = []  # open, oldest first
        self._drain_ids = itertools.count(1)
        self._drain_logged = 0.0
        self.server_drains_done = 0

    def stop(self) -> None:
        """Cancel the grace timers; no new one starts."""
        with self._lock:
            self._stopping = True
            timers = list(self._timers)
            self._timers.clear()
        for timer in timers:
            timer.cancel()

    # ------------------------------------------------------- attach, detach

    def attach(
        self, conn: WorkerEndpoint, ident: AttachIdentity
    ) -> tuple[WorkerRecord | None, str]:
        """Register or re-bind an attaching worker; (None, reason) to refuse."""
        with self._lock:
            if ident.incarnation in self._probes:
                return None, "the incarnation is attached as a probe"
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
        if not self._watcher.watch(key, ident.pid, ident.pid_start):
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
            self._watcher.unwatch(_old_key(ident.incarnation))
        if not self._watcher.watching(key):
            # It ended between watch() and the record: its exit found no record
            self._worker_exited(ident.incarnation)
            return None, "the process has ended"
        return rec, ""

    def attach_probe(
        self, conn: WorkerEndpoint, ident: AttachIdentity
    ) -> tuple[WorkerRecord | None, str]:
        """Record a probe (a scheduler): no pidfd, credit or drain; it is
        forgotten when its connection closes."""
        with self._lock:
            if ident.incarnation in self._workers:
                return None, "the incarnation is attached as a worker"
            rec = self._probes.get(ident.incarnation)
            if rec is not None:
                return self._rebind(rec, conn, ident)
            rec = WorkerRecord(ident=ident, conn=conn, role=SessionRole.PROBE)
            self._probes[ident.incarnation] = rec
            return rec, ""

    def probes(self) -> list[str]:
        """Incarnations of the attached probes."""
        with self._lock:
            return list(self._probes)

    def _forget_probe(self, rec: WorkerRecord, conn: WorkerEndpoint | None) -> None:
        """Caller holds the lock; ``conn`` None forgets it whatever its link."""
        if self._probes.get(rec.incarnation) is rec and (
            conn is None or rec.conn is conn
        ):
            del self._probes[rec.incarnation]
            rec.conn = None
            rec.detached = True

    def _rebind(
        self, rec: WorkerRecord, conn: WorkerEndpoint, ident: AttachIdentity
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
        """Send NOTIFY drain to an old worker that attached again, and to a
        worker an open server drain still waits for."""
        with self._lock:
            old = self._old.get(rec.incarnation)
            draining = old is not None and old.state == OldState.DRAINING
            conn = rec.conn
            server = [d for d in self._drains if rec.incarnation in d.waiting]
        if conn is None:
            return
        if draining:
            conn.post_notify(
                Notify(
                    kind=NotifyKind.DRAIN,
                    drain_kind=DrainKind.NODE,
                    epoch=self.previous_node_epoch,
                )
            )
        for drain in server:  # oldest first: each epoch it may still read
            conn.post_notify(
                Notify(
                    kind=NotifyKind.DRAIN,
                    drain_kind=DrainKind.SERVER,
                    epoch=drain.old_epoch,
                )
            )

    def connection_lost(self, rec: WorkerRecord, conn: WorkerEndpoint) -> None:
        """The UDS closed; the credit stays until the process exits. A probe
        is forgotten."""
        with self._lock:
            if rec.role == SessionRole.PROBE:
                self._forget_probe(rec, conn)
                return
            if rec.conn is conn:
                rec.conn = None
        self._ledger.forget_waiter(rec.incarnation, conn)

    def detach(self, rec: WorkerRecord) -> DetachResponse:
        with self._lock:
            if rec.role == SessionRole.PROBE:
                self._forget_probe(rec, None)
                return DetachResponse(status=Status.OK)
            rec.detached = True
            held = self._pools.held_bytes(rec.incarnation)
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
                or self._ledger.running(incarnation)
                or not self._pools.forget_holder(incarnation)
            ):
                return
            del self._workers[incarnation]
            # Under the lock, so a re-attach of the incarnation (which takes
            # it) never loses its new watch or op records; neither the
            # watcher nor the ledger calls back while holding its own lock
            self._watcher.unwatch(_worker_key(incarnation))
            self._ledger.drop(incarnation)
            # a worker detaches only once it reads and writes nothing
            tags = self._confirm_server_drain(incarnation, None)
        logger.info("worker %s detached and is forgotten", incarnation)
        self._release(tags)
        self._finish_server_drains()

    def get(self, incarnation: str) -> WorkerRecord | None:
        with self._lock:
            return self._workers.get(incarnation)

    def endpoint(self, incarnation: str) -> WorkerEndpoint | None:
        """The connection of an attached worker, if it has one."""
        with self._lock:
            rec = self._workers.get(incarnation)
            return rec.conn if rec is not None else None

    def while_attached(self, rec: WorkerRecord, fn: Callable[[], R]) -> R | None:
        """``fn()`` under the lock, or None if ``rec`` ended or detached. An
        exit sets ``exited`` under the lock before it quarantines, so credit
        given in ``fn`` never goes to a holder that ended; a detached record
        may be forgotten (after_detach_reply). ``fn`` may only touch the pool
        manager and this registry: taking the manager lock or doing I/O in it
        would invert the lock order."""
        with self._lock:
            if rec.exited or rec.detached:
                return None
            return fn()

    # ------------------------------------------------------------------ exit

    def on_process_exit(self, key: str) -> None:
        """A watched process ended."""
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
            # Under the lock a server drain starts with: its credit is never
            # tagged as a live worker's
            refs = self._pools.quarantine_holder(incarnation)
        if conn is not None:
            conn.close()
        logger.info(
            "worker %s (pid %d) ended: %d credited pages quarantined for %.1f s",
            incarnation,
            rec.ident.pid,
            len(refs),
            self.gpu_drain_grace_s,
        )
        self._on_exit(rec.ref())
        with self._lock:
            # its credit quarantined at a server restart waits the grace too
            server_tags = self._confirm_server_drain(incarnation, None)
        self._after(self.gpu_drain_grace_s, self._grace_over, incarnation, server_tags)
        self._ledger.drop(incarnation)
        with self._lock:
            old = self._old.get(incarnation)
        if old is not None:
            self._confirm_old(old, "exited")
        self._finish_server_drains()

    def _grace_over(self, incarnation: str, tags: list[str]) -> None:
        freed = self._pools.release_quarantine(holder_tag(incarnation))
        freed += self._release(tags)
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

    # --------------------------------------------------------- restart drain

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
            if not self._watcher.watch(
                _old_key(old.ref.incarnation), old.ref.pid, old.ref.pid_start
            ):
                self._old_worker_exited(old.ref.incarnation)
        self._maybe_release_adopted()

    def drain_ack(self, rec: WorkerRecord, req: DrainAckRequest) -> DrainAckResponse:
        if req.kind == DrainKind.SERVER:
            self._server_drain_ack(rec.incarnation, req.epoch)
        elif req.kind == DrainKind.NODE:
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

    # --------------------------------------------------- MaruServer restart

    def start_server_drain(self, old_epoch: str) -> ServerDrain | None:
        """MaruServer lost the session of ``old_epoch``: quarantine the credit
        given so far, hold back frees of the pages given so far, and ask every
        attached worker to drain. None if that epoch is drained already."""
        with self._lock:
            if self._stopping or any(d.old_epoch == old_epoch for d in self._drains):
                return None
            index = next(self._drain_ids)
            # attached workers, and old workers of the run before that have
            # not attached again if a pool was kept (they may read it; the
            # old run's DRAM is never reused)
            waiting = {r.incarnation for r in self._workers.values() if not r.exited}
            if self._adopted:
                waiting |= {
                    i for i, o in self._old.items() if o.state == OldState.WAITING
                }
            drain = ServerDrain(
                index=index,
                old_epoch=old_epoch,
                waiting=waiting,
                started=time.monotonic(),
            )
            self._pools.hold_frees(drain.tag)
            # Under the lock CREDIT takes too: credit given after this
            # belongs to the new server epoch. An ended worker's credit waits
            # its exit grace instead
            live = {r.incarnation for r in self._workers.values() if not r.exited}
            counts = self._pools.quarantine_credited(
                lambda holder: f"server:{index}:{holder}", live
            )
            drain.credit_tags = {h: f"server:{index}:{h}" for h in counts}
            self._drains.append(drain)
            conns = [
                r.conn
                for r in self._workers.values()
                if r.incarnation in drain.waiting and r.conn is not None
            ]
        notify = Notify(
            kind=NotifyKind.DRAIN, drain_kind=DrainKind.SERVER, epoch=old_epoch
        )
        for conn in conns:
            conn.post_notify(notify)
        logger.warning(
            "MaruServer epoch %s ended: %d credited pages quarantined; eviction "
            "and space reclaim wait for %d workers to drain it",
            old_epoch,
            sum(counts.values()),
            len(drain.waiting),
        )
        self._finish_server_drains()
        return drain

    def notify_all(self, notify: Notify) -> None:
        """Post ``notify`` to every attached worker and probe."""
        with self._lock:
            records = [*self._workers.values(), *self._probes.values()]
            conns = [r.conn for r in records if r.conn is not None]
        for conn in conns:
            conn.post_notify(notify)

    def server_drain_open(self) -> bool:
        with self._lock:
            return bool(self._drains)

    def server_drain_pending(self) -> dict[str, list[str]]:
        """Workers the open server drains wait for, with the ended epochs."""
        with self._lock:
            out: dict[str, list[str]] = {}
            for drain in self._drains:
                for incarnation in sorted(drain.waiting):
                    out.setdefault(incarnation, []).append(drain.old_epoch)
            return out

    def log_server_drain(self) -> None:
        """Name the workers an open drain waits for, every DRAIN_LOG_S."""
        now = time.monotonic()
        with self._lock:
            if not self._drains or now - self._drain_logged < DRAIN_LOG_S:
                return
            self._drain_logged = now
            oldest = self._drains[0]
        logger.warning(
            "eviction and space reclaim paused for %.0f s after MaruServer epoch "
            "%s ended: waiting for DRAIN_ACK or the exit of %s",
            now - oldest.started,
            oldest.old_epoch,
            sorted(self.server_drain_pending()),
        )

    def _server_drain_ack(self, incarnation: str, epoch: str) -> None:
        with self._lock:
            upto = next((d.index for d in self._drains if d.old_epoch == epoch), None)
            if upto is None:
                return  # unknown, or that drain is over
            tags = self._confirm_server_drain(incarnation, upto)
        logger.info("worker %s drained server epoch %s", incarnation, epoch)
        self._release(tags)
        self._finish_server_drains()

    def _confirm_server_drain(self, incarnation: str, upto: int | None) -> list[str]:
        """Caller holds the lock. ``incarnation`` ended its accesses of the
        open drains up to index ``upto`` (all if None); returns the tags of
        its quarantined credit."""
        tags = []
        for drain in self._drains:
            if upto is not None and drain.index > upto:
                continue
            drain.waiting.discard(incarnation)
            tag = drain.credit_tags.pop(incarnation, None)
            if tag is not None:
                tags.append(tag)
        return tags

    def _finish_server_drains(self) -> None:
        """End the drains no worker is left in. The provisional rule lives
        here: a worker counts only once it sent DRAIN_ACK, exited or
        detached, never by time, so one that stays silent keeps eviction and
        space reclaim paused (a time limit would go here). A later drain that
        ends ends the earlier ones: its workers include theirs, and a
        DRAIN_ACK covers earlier epochs."""
        with self._lock:
            upto = max((d.index for d in self._drains if not d.waiting), default=None)
            if upto is None:
                return
            done = [d for d in self._drains if d.index <= upto]
            self._drains = [d for d in self._drains if d.index > upto]
            if not self._drains:
                self._pools.hold_frees(None)
            self.server_drains_done += len(done)
            tags = [t for d in done for t in d.credit_tags.values()]
            tags += [d.tag for d in done]
        freed = self._release(tags)
        for drain in done:
            logger.info(
                "MaruServer epoch %s drained in %.1f s",
                drain.old_epoch,
                time.monotonic() - drain.started,
            )
        if freed:
            logger.info("%d pages held since the MaruServer restart are free", freed)

    def _release(self, tags: list[str]) -> int:
        return sum(self._pools.release_quarantine(tag) for tag in tags)

    def old_workers(self) -> dict[str, OldWorker]:
        with self._lock:
            return {k: dataclasses.replace(v) for k, v in self._old.items()}

    def is_adopted(self, pool_id: str) -> bool:
        """A pool of the run before, kept until its old workers are confirmed."""
        with self._lock:
            return pool_id in self._adopted

    def _old_worker_exited(self, incarnation: str) -> None:
        with self._lock:
            old = self._old.get(incarnation)
            if old is None or old.state != OldState.WAITING:
                return
        self._on_exit(old.ref)
        self._confirm_old(old, "exited")
        with self._lock:
            tags = self._confirm_server_drain(incarnation, None)
        self._release(tags)
        self._finish_server_drains()

    def _confirm_old(self, old: OldWorker, how: str) -> None:
        with self._lock:
            if old.state == OldState.CONFIRMED:
                return
            old.state = OldState.CONFIRMED
            old.how = how
        logger.info("old worker %s confirmed (%s)", old.ref.incarnation, how)
        self._maybe_release_adopted()

    def _maybe_release_adopted(self) -> None:
        with self._lock:
            if self._adopted_released:
                return
            if any(o.state != OldState.CONFIRMED for o in self._old.values()):
                return
            self._adopted_released = True
            adopted = list(self._adopted)
        for pool_id in adopted:
            freed = self._pools.release_quarantine(RECOVERY_TAG, pool_id)
            logger.info(
                "every old worker is confirmed: %d quarantined pages of adopted "
                "pool %s are free",
                freed,
                pool_id,
            )

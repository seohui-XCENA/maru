# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageManager: credit, WRITE_DONE -> COMMIT, removal (eviction, write-back
demotion, RETIRE and central deletes), WORKER_EXIT reports and the recovery
after a MaruServer restart, each in the order it needs.

Worker records live in a WorkerRegistry, op results in an OpLedger; MaruServer
is reached through a MetadataPort.
"""

import collections
import dataclasses
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

from maru_common.node_protocol import (
    AbortRequest,
    AbortResponse,
    CreditRequest,
    CreditResponse,
    CreditReturnRequest,
    CreditReturnResponse,
    Notify,
    UnmapAckRequest,
    UnmapAckResponse,
    WriteDoneRequest,
    WriteDoneResponse,
)
from maru_common.protocol import (
    CommitItem,
    MoveItem,
    NodeHeartbeatResponse,
    ReregisterItem,
)
from maru_common.serializer import DecodedRequest, op_digest
from maru_common.storage_types import (
    MAX_BATCH,
    AccessInfo,
    BlockRequest,
    ExitCause,
    ItemOutcome,
    ItemResult,
    KeptCause,
    Key,
    KeyLocation,
    Location,
    NotifyKind,
    OpRef,
    PoolKind,
    Status,
    WorkerRef,
)

from .config import TierTraits
from .local_pin import LocalPins
from .op_ledger import OP_WINDOW, LedgerEntry, OpLedger, Verdict
from .placement import PlacementPolicy
from .pool_manager import InvalidTransitionError, PageRef, PageState, PoolManager
from .ports import (
    EpochChanged,
    MetadataPort,
    NodeOpenError,
    ProcessWatcher,
    ReplyError,
    ServerStopped,
    StaleOp,
    WorkerEndpoint,
)
from .transfer_engine import Copy, TransferEngine
from .worker_registry import WorkerRecord, WorkerRegistry

logger = logging.getLogger(__name__)

R = TypeVar("R")

DEFAULT_GPU_DRAIN_GRACE_S = 5.0
DEFAULT_EVICT_INTERVAL_S = 0.05
# Wait for more WRITE_DONE items to join a COMMIT batch
COMMIT_LINGER_S = 0.002
EXIT_RETRY_S = 0.5
EXIT_RETRY_MAX_S = 30.0
RECOVERY_RETRY_S = 1.0


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


@dataclass(eq=False)
class _Move:
    """A READY page moving to the tier below (write-back demotion)."""

    src: PageRef
    key: Key
    source: Location
    dest: PageRef
    dest_location: Location  # what is committed: the logical bytes
    finalizer_id: str = ""  # once ADDED: FINALIZE_MOVE releases it
    # The copy is READY below already (left by a KEPT or unknown move): the
    # COMMIT of the same location only places a finalizer on it again
    existing: bool = False


class StorageManager:
    def __init__(
        self,
        *,
        node_epoch: str,
        pool_manager: PoolManager,
        placement: PlacementPolicy,
        local_pins: LocalPins,
        metadata: MetadataPort,
        watcher: ProcessWatcher,
        gpu_drain_grace_s: float = DEFAULT_GPU_DRAIN_GRACE_S,
        evict_interval_s: float = DEFAULT_EVICT_INTERVAL_S,
        op_window: int = OP_WINDOW,
        traits: dict[PoolKind, TierTraits] | None = None,
        on_worker_settled: Callable[[str], None] = lambda incarnation: None,
        transfers: TransferEngine | None = None,
    ):
        self.node_epoch = node_epoch
        self.traits = traits or {
            PoolKind.DRAM: TierTraits(PoolKind.DRAM, credit=True, evicts=True)
        }
        self.credit_kind = next(k for k, t in self.traits.items() if t.credit)
        # Credit comes from the top tier, then from the fallback tiers in order
        self.credit_kinds = [self.credit_kind] + [
            k for k, t in self.traits.items() if t.credit_fallback
        ]
        self.pools = pool_manager
        self.placement = placement
        # Copies for demotion; without it a tier that demotes drops instead
        self.transfers = transfers
        self.local_pins = local_pins
        self.metadata = metadata
        self.evict_interval_s = evict_interval_s
        self.ledger = OpLedger(op_window)
        self.watcher = watcher
        self.workers = WorkerRegistry(
            node_epoch=node_epoch,
            pools=pool_manager,
            ledger=self.ledger,
            watcher=self.watcher,
            on_exit=self._queue_exit,
            on_settled=on_worker_settled,
            gpu_drain_grace_s=gpu_drain_grace_s,
        )

        self._lock = threading.RLock()
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
        # MaruServer lost the node session: no COMMIT, RETIRE or WORKER_EXIT
        # until it is resumed and every READY page is re-registered
        self._recovering = False
        self._recovery_wake = threading.Event()
        # a COMMIT, RETIRE or WORKER_EXIT is out, or its result is applied
        self._commit_busy = False
        self._evict_busy = False
        self._exit_busy = False
        self.recoveries = 0
        self.last_reregistration: dict | None = None
        # The server epoch whose Catalog has every READY page (the first
        # open's, then each completed re-registration's), and the one whose
        # pages were sorted for re-registration (a retry re-sends them all)
        self._complete_at: str | None = None
        self._sorted_for: str | None = None
        # Pages held because their RETIRE result is unknown; how many were
        # still held after a resume that kept the Catalog
        self._retire_unknown: set[PageRef] = set()
        self.held_after_resume = 0
        self._stopping = False
        self._threads: list[threading.Thread] = []
        self._unmapped: set[tuple[str, str, int]] = set()
        # RETIRE results by outcome ("RETIRED", "KEPT:PINNED", ...)
        self.retire_results: collections.Counter[str] = collections.Counter()
        # Demotions: FINALIZE_MOVE results as above (and "UNKNOWN"), plus
        # "COMMIT:<result>", "COPY_FAILED" and "DROPPED" (no room below past
        # the high watermark)
        self.move_results: collections.Counter[str] = collections.Counter()
        # Copied demotions whose COMMIT goes out once MaruServer takes it
        # (right after a re-registration), and finalized ones whose
        # FINALIZE_MOVE waits for the next eviction pass
        self._moves: list[_Move] = []
        self._finals: list[_Move] = []
        # Source -> its READY copy below, left by a KEPT, DUPLICATE or unknown
        # result: the next demotion finalizes onto that copy instead of copying
        self._copies: dict[PageRef, PageRef] = {}
        # Pools MaruServer called INVALID: no demotion goes there
        self._invalid_pools: set[str] = set()

    # ------------------------------------------------------------- lifecycle

    def start(self) -> None:
        """After the session opened: its Catalog has every page so far."""
        self._complete_at = self.metadata.server_epoch
        self.watcher.start(self.workers.on_process_exit)
        for target, name in (
            (self._commit_loop, "maru-commit"),
            (self._evict_loop, "maru-evict"),
            (self._exit_loop, "maru-worker-exit"),
            (self._recovery_loop, "maru-recovery"),
        ):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self._threads.append(thread)

    def start_heartbeat(self) -> None:
        """Report pool usage and act on each reply; a lost session recovers."""
        self.metadata.start_heartbeat(
            self.pools.usage, self.on_heartbeat, self.server_lost
        )

    def stop(self) -> None:
        """Stop the threads; unresolved requests are given up."""
        with self._cv:
            self._stopping = True
            self._cv.notify_all()
        self.workers.stop()
        self._evict_wake.set()
        self._recovery_wake.set()
        self.metadata.stop()
        for thread in self._threads:
            thread.join(timeout=10.0)
        self._threads.clear()
        self.watcher.stop()

    @property
    def safe_state(self) -> str | None:
        """Why the node is in the safe state, or None."""
        return self._safe

    def enter_safe_state(self, reason: str) -> None:
        """No new credit, no eviction, no page reused, until the
        StorageService restarts: MaruServer will not take this run back."""
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

    @property
    def recovering(self) -> bool:
        """The node session is lost or its locations are being re-registered."""
        return self._recovering

    def server_lost(self, reason: str) -> None:
        """MaruServer no longer knows the node session (not an expiry it
        renewed): resume it and re-register."""
        with self._cv:
            if self._safe is not None or self._stopping:
                return
            if not self._recovering:
                self._recovering = True
                logger.warning("node session lost (%s): recovering", reason)
            self._cv.notify_all()
        self._recovery_wake.set()

    def local_pins_open(self) -> bool:
        """Whether a new local pin may be given (none is while local pins are
        off): never while recovering, nor once MaruServer has not confirmed
        the session for node_stale_s (half the session TTL)."""
        if self._safe is not None or self._recovering or self.metadata.epoch_lost:
            return False
        last = self.metadata.last_heartbeat_ok
        stale_s = self.metadata.session_ttl_s / 2
        return last > 0 and time.monotonic() - last < stale_s

    def stats(self) -> dict:
        with self._lock:
            pending = len(self._tickets)
            retire = dict(self.retire_results)
            demotions = dict(self.move_results)
            pending_moves = len(self._moves) + len(self._finals)
        return {
            "safe_state": self._safe,
            "recovering": self._recovering,
            "recoveries": self.recoveries,
            "last_reregistration": self.last_reregistration,
            "held_after_resume": self.held_after_resume,
            "server_drain_waiting": self.workers.server_drain_pending(),
            "pending_commits": pending,
            "retire_results": retire,
            "demotions": demotions,
            "pending_moves": pending_moves,
            "credit_short": dict(self.pools.credit_short),
            "credit_short_bytes": dict(self.pools.credit_short_bytes),
        }

    # -------------------------------------------------------------- recovery

    def _recovery_loop(self) -> None:
        while True:
            self._recovery_wake.wait()
            self._recovery_wake.clear()
            with self._cv:
                if self._stopping:
                    return
                if self._safe is not None or not self._recovering:
                    continue
            try:
                self._recover()
            except ServerStopped:
                return
            except (EpochChanged, StaleOp) as e:
                # restarted again meanwhile: start over with the new epoch
                logger.warning("MaruServer lost the session again (%s)", e)
                self._recovery_wake.set()
            except NodeOpenError as e:
                self.enter_safe_state(f"MaruServer does not resume this run: {e}")
            except Exception:
                logger.exception("recovery failed; trying again")
                with self._cv:
                    self._cv.wait(RECOVERY_RETRY_S)
                self._recovery_wake.set()

    def _recover(self) -> None:
        """Drain the ended server epoch, resume the session, re-register the
        READY pages; COMMIT, RETIRE and WORKER_EXIT go on after that, and
        eviction once the drain is over."""
        old_epoch = self.metadata.server_epoch
        new_epoch = self.metadata.new_server_epoch
        draining = False
        if old_epoch is not None and new_epoch not in (None, old_epoch):
            # Not when only the session was refused: the epoch goes on
            self.workers.start_server_drain(old_epoch)
            draining = True
        with self._cv:
            # A result applied after the snapshot below could leave a
            # location out of step with the ledger, and an op of the ended
            # epoch must be resolved before the session moves on
            self._cv.wait_for(
                lambda: (
                    self._stopping
                    or not (self._commit_busy or self._evict_busy or self._exit_busy)
                )
            )
            if self._stopping:
                raise ServerStopped("stopping")
            deleted = set(self._central)  # blocked by the ended run
        started = time.monotonic()
        opened = self.metadata.resume()
        epoch = opened.server_epoch
        if epoch == self._complete_at:
            # A MaruServer whose Catalog has every page took the session back
            self._kept_catalog(epoch)
            return
        if epoch != old_epoch and not draining and old_epoch is not None:
            # A restart seen only now (between a HANDSHAKE and its open)
            self.workers.start_server_drain(old_epoch)
        self.workers.notify_all(
            Notify(
                kind=NotifyKind.EPOCH,
                node_epoch=self.node_epoch,
                server_epoch=opened.server_epoch,
            )
        )
        # Pages of the lost Catalog era are sorted once per server epoch; a
        # retry after a failure part way sends every READY page again
        first = self._sorted_for != epoch
        sent, added = self._reregister(epoch, deleted if first else None)
        self._finalizers_lost()
        self._complete_at = epoch
        seconds = time.monotonic() - started
        self._recovered(
            opened.server_epoch,
            {
                "server_epoch": opened.server_epoch,
                "locations": sent,
                "added": added,
                "seconds": seconds,
            },
        )
        logger.warning(
            "re-registered %d of %d READY pages with MaruServer epoch %s in "
            "%.3f s; commits go on, eviction once the old epoch is drained",
            added,
            sent,
            opened.server_epoch,
            seconds,
        )

    def _kept_catalog(self, server_epoch: str) -> None:
        """Resumed where nothing was lost. Pages held for an unknown RETIRE
        stay held (never offered again): they are counted and logged."""
        with self._lock:
            unknown = list(self._retire_unknown)
        held = 0
        for ref in unknown:
            page = self.pools.page(ref)
            if page is not None and page.state == PageState.READY:
                held += self.placement.is_held(ref)
            else:
                with self._lock:
                    self._retire_unknown.discard(ref)  # left the ledger
        self.held_after_resume = held
        if held:
            logger.warning(
                "session resumed at MaruServer epoch %s with its Catalog; %d "
                "READY pages held for an unknown RETIRE stay held",
                server_epoch,
                held,
            )
        self._recovered(server_epoch, None)

    def _recovered(self, server_epoch: str, reregistration: dict | None) -> None:
        """Requests go to MaruServer again, unless the session was lost
        again meanwhile (then the recovery runs again)."""
        # Demotion copies left PENDING go out after the last batch, not
        # behind the server drain: they would hold back credit
        self._commit_queued_moves()
        with self._cv:
            self.recoveries += 1
            if reregistration is not None:
                self.last_reregistration = reregistration
            # A loss signalled before this point is seen here; one after it
            # finds _recovering False and sets it again
            again = self.metadata.epoch_lost
            self._recovering = again
            self._cv.notify_all()
        if again:
            logger.warning("the session was lost again during the recovery")
            self._recovery_wake.set()
        else:
            logger.info("node session back at MaruServer epoch %s", server_epoch)
        self._evict_wake.set()

    def _reregister(self, epoch: str, deleted: set[PageRef] | None) -> tuple[int, int]:
        """Send the READY pages again in batches, the last one marked; a page
        MaruServer does not take is dropped. With ``deleted`` (the first try
        at a new server epoch) a page the ended run was deleting and one
        whose RETIRE result is unknown are dropped first; a retry sends every
        READY page (those taken already come back ADDED). Returns (sent,
        added)."""
        if deleted is not None:
            with self._lock:
                self._blocks.clear()  # the block requests went with the server
        pages: list[tuple[PageRef, ReregisterItem]] = []
        for ref, key, location in self.pools.ready_pages():
            if deleted is not None and (ref in deleted or self.placement.is_held(ref)):
                self._drop_page(ref)
            else:
                pages.append((ref, ReregisterItem(key, location, location.length)))
        # Sorted: from the first batch on, MaruServer may list what is sent
        self._sorted_for = epoch
        added = 0
        for start in range(0, max(len(pages), 1), MAX_BATCH):
            batch = pages[start : start + MAX_BATCH]
            last = start + MAX_BATCH >= len(pages)
            try:
                response = self.metadata.reregister([i for _, i in batch], last)
            except ReplyError:
                for ref, _ in batch:  # unknown whether added: settle later
                    self._settle_page(ref)
                continue
            if response.status != Status.OK:
                logger.error(
                    "NODE_REREGISTER refused as a whole: %s %s; %d pages dropped",
                    response.status,
                    response.error,
                    len(batch),
                )
                for ref, _ in batch:
                    self._drop_page(ref)
                continue
            for (ref, _), outcome in zip(batch, response.items, strict=True):
                if outcome.result == ItemResult.ADDED:
                    added += 1
                    self._readded(ref)
                else:
                    self._drop_page(ref)
        return len(pages), added

    def _readded(self, ref: PageRef) -> None:
        """MaruServer took the page again. One that was settling is an
        ordinary READY page now; a delete that came meanwhile stays."""
        with self._lock:
            settling = ref in self._settle
            self._settle.discard(ref)
            deleting = ref in self._central
            if settling and not deleting:
                self._retire_queue.pop(ref, None)
        if settling and not deleting:
            self.local_pins.unblock([ref])
            self.placement.on_ready(ref)

    def _drop_page(self, ref: PageRef) -> None:
        """A READY page not in the Catalog leaves the ledger (it is held back
        from reuse until the server drain is over)."""
        try:
            self.pools.drop_ready(ref)
        except InvalidTransitionError as e:
            logger.error("dropping page %s: %s", ref, e)
            return
        self.placement.on_gone(ref)
        self._forget_queued(ref)
        self.local_pins.forget(ref)
        with self._lock:
            self._retire_unknown.discard(ref)

    def _settle_page(self, ref: PageRef) -> None:
        """A READY page that may or may not be in the Catalog: a RETIRE
        tells (RETIRED frees it, PINNED proves it there)."""
        with self._lock:
            self._settle.add(ref)
            self._retire_queue[ref] = 0.0

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
        if req.kind is not None and req.kind not in self.credit_kinds:
            return CreditResponse(
                status=Status.REJECTED,
                error=f"credit comes from the {self.credit_kinds} tiers, not {req.kind}",
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

        def grant() -> tuple[list[Location], list[tuple[WorkerEndpoint, int]]]:
            rec.worker_session = req.worker_session
            credits = self.pools.credit(
                rec.incarnation,
                req.want_bytes,
                self.credit_kinds if req.kind is None else req.kind,
            )
            asks = [
                (conn, nbytes)
                for incarnation, nbytes in self.pools.credit_asks(rec.incarnation)
                if (conn := self.workers.endpoint(incarnation)) is not None
            ]
            return credits, asks

        asks: list[tuple[WorkerEndpoint, int]] = []
        try:
            granted = self.workers.while_attached(rec, grant)
            if granted is None:
                response = CreditResponse(status=Status.REJECTED, error="ended")
            else:
                credits, asks = granted
                response = CreditResponse(status=Status.OK, credits=credits)
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
        waiter: WorkerEndpoint | None = None,
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

    def unmap_ack(self, rec: WorkerRecord, req: UnmapAckRequest) -> UnmapAckResponse:
        with self._cv:
            self._unmapped.add((rec.incarnation, req.pool_id, req.pool_generation))
            self._cv.notify_all()
        return UnmapAckResponse(status=Status.OK)

    def unmap_pools(self, pools: list[tuple[str, int]], timeout_s: float) -> set[str]:
        """Ask every worker of this run that may map ``pools`` (pool_id,
        generation) to unmap them; the pool_ids each one unmapped (UNMAP_ACK)
        or settled (detached, or ended with its GPU drain grace over) within
        ``timeout_s``. A worker that reconnects is asked again; one whose
        connection dropped counts only once it settles. A timeout is no
        confirmation. The caller stops new ATTACHes from mapping them first.
        Whatever this answers, the RM keeps a region out of reuse while any
        worker holds it."""
        asked: dict[str, WorkerEndpoint | None] = {}
        deadline = time.monotonic() + timeout_s
        with self._cv:
            while True:
                workers = self.workers.mapping()
                for inc in workers:
                    asked.setdefault(inc, None)
                for inc, conn in workers.items():
                    if conn is not None and asked.get(inc) is not conn:
                        asked[inc] = conn
                        for pool_id, generation in pools:
                            conn.post_notify(
                                Notify(
                                    kind=NotifyKind.POOL_REMOVE,
                                    pool_id=pool_id,
                                    pool_generation=generation,
                                )
                            )
                done = {
                    pool_id
                    for pool_id, generation in pools
                    if all(
                        (inc, pool_id, generation) in self._unmapped
                        or self.workers.settled(inc)
                        for inc in asked
                    )
                }
                left = deadline - time.monotonic()
                if len(done) == len(pools) or left <= 0:
                    return done
                # An exit or a reconnect does not notify: look again shortly
                self._cv.wait(min(left, 0.05))

    # ---------------------------------------------------------------- commit

    def _paused(self) -> bool:
        """Caller holds the lock: nothing goes to MaruServer now."""
        return self._safe is not None or self._recovering

    def _commit_loop(self) -> None:
        while True:
            with self._cv:
                while not self._stopping and (not self._tickets or self._paused()):
                    self._cv.wait()
                if self._stopping:
                    return
                if len(self._tickets) < MAX_BATCH:
                    self._cv.wait(COMMIT_LINGER_S)
                    if self._stopping:
                        return
                    if self._paused():
                        continue
                batch = [
                    self._tickets.popleft()
                    for _ in range(min(MAX_BATCH, len(self._tickets)))
                ]
                self._commit_busy = bool(batch)
            try:
                if batch:
                    self._commit_batch(batch)
            finally:
                with self._cv:
                    self._commit_busy = False
                    self._cv.notify_all()
            if self._stopping:
                return

    def _commit_batch(self, batch: list[_Ticket]) -> None:
        try:
            response = self.metadata.commit([t.item for t in batch])
        except EpochChanged as e:
            # Sent again with a new op once re-registered
            with self._cv:
                self._tickets.extendleft(reversed(batch))
            self.server_lost(f"MaruServer answered EPOCH to COMMIT ({e})")
            return
        except StaleOp:
            with self._cv:
                self._tickets.extendleft(reversed(batch))
            return
        except ServerStopped:
            with self._cv:
                self._tickets.extendleft(reversed(batch))
            return
        except ReplyError:
            # It ran, but the results are unusable: settle each page
            self._apply_commit(batch, None)
            return
        except Exception:
            logger.exception("COMMIT failed; the pages stay PENDING")
            with self._cv:
                self._tickets.extendleft(reversed(batch))
            time.sleep(0.1)
            return
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
            draining = self.workers.server_drain_open()
            with self._cv:
                if self._stopping:
                    return
                # After a MaruServer restart nothing is removed until every
                # worker drained the ended epoch (see WorkerRegistry)
                run = not (self._paused() or draining)
                self._evict_busy = run
            try:
                self._reclaim_retiring()
                if run:
                    self._retire_queued()
                    self._evict_once()
                elif draining:
                    self.workers.log_server_drain()
            except ServerStopped:
                return
            except Exception:
                logger.exception("eviction pass failed")
            finally:
                with self._cv:
                    self._evict_busy = False
                    self._cv.notify_all()

    def _evict_once(self) -> None:
        with self._lock:
            queued, self._moves = self._moves, []
            finals, self._finals = self._finals, []
        finals += self._commit_queued(queued, recovering=False)
        self._finalize_queued(finals)
        # Lower tiers first: the room they make takes the pages from above
        for kind in reversed(list(self.traits)):
            if self._halted():  # the session went during this pass
                return
            traits = self.traits[kind]
            if not traits.evicts:
                continue
            pools = self._evictable_pools(kind)
            if not pools:
                continue
            capacity, free, retiring, page_size = self._tier_usage(pools)
            candidates = self.placement.candidates(
                kind.value,
                pools,
                capacity=capacity,
                free_bytes=free,
                retiring_bytes=retiring,
                page_size=page_size,
                limit=MAX_BATCH,
            )
            refs, items = self._ready_items(candidates)
            if not refs:
                continue
            if traits.demotes_to is not None and self.transfers is not None:
                self._demote(kind, pools, traits.demotes_to, refs, items)
            else:
                self.retire(refs, items)

    def _halted(self) -> bool:
        """Nothing may go to MaruServer now."""
        with self._lock:
            paused = self._paused() or self._stopping
        return paused or self.metadata.epoch_lost

    def _evictable_pools(self, kind: PoolKind) -> list[str]:
        """The tier's pools pressure is measured over and removed from: not
        invalid, not quarantined for an earlier run and (in the credit tier)
        creditable."""
        traits = self.traits[kind]
        with self._lock:
            invalid = set(self._invalid_pools)
        pools = []
        for pool_id in self.pools.pool_ids():
            info = self.pools.pool_info(pool_id)
            if (
                info is None
                or info.kind != kind
                or pool_id in invalid
                or self.pools.pool_quarantined(pool_id)
                or (
                    (traits.credit or traits.credit_fallback)
                    and not self.pools.creditable(pool_id)
                )
            ):
                continue
            pools.append(pool_id)
        return pools

    def _tier_usage(self, pools: list[str]) -> tuple[int, int, int, int]:
        """Capacity, FREE bytes, RETIRING bytes and page size over ``pools``."""
        capacity = free = retiring = page_size = 0
        for pool_id in pools:
            info = self.pools.pool_info(pool_id)
            if info is None:
                continue
            capacity += info.capacity
            free += self.pools.bytes_in(PageState.FREE, pool_id)
            retiring += self.pools.bytes_in(PageState.RETIRING, pool_id)
            page_size = info.page_size
        return capacity, free, retiring, page_size

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

    # ------------------------------------------------------------- demotion

    def _demote(
        self,
        kind: PoolKind,
        pools: list[str],
        below: PoolKind,
        refs: list[PageRef],
        items: list[KeyLocation],
    ) -> None:
        """Write-back: copy READY pages to the tier below, COMMIT the copies
        with a finalizer, then FINALIZE_MOVE the sources. A source with a copy
        below already gets a finalizer on that copy instead. Pages that find no
        room wait for the tier below to evict (it ran first in this pass);
        while the tier (``pools``) is past its high watermark they are dropped
        instead: writes must not stall."""
        moves: list[_Move] = []
        todo = []
        for ref, item in zip(refs, items, strict=True):
            copy = self._copy_of(ref, item.key)
            if copy is None:
                todo.append((ref, item))
            else:
                moves.append(
                    _Move(ref, item.key, item.location, copy[0], copy[1], existing=True)
                )
        dests = self._reserve_below(below, len(todo))
        for (ref, item), dest in zip(todo, dests, strict=False):
            location = self.pools.transfer_location(dest)
            moves.append(
                _Move(
                    ref,
                    item.key,
                    item.location,
                    dest,
                    dataclasses.replace(location, length=item.location.length),
                )
            )
        rest = todo[len(dests) :]
        drop = []
        if rest:
            capacity, free, _, _ = self._tier_usage(pools)
            if self.placement.above_high(
                kind.value, capacity=capacity, free_bytes=free
            ):
                logger.debug("no room below %s: dropping %d pages", kind, len(rest))
                with self._lock:
                    self.move_results["DROPPED"] += len(rest)
                drop = rest
            else:
                self.placement.release([ref for ref, _ in rest], backoff=False)
        # Every reserved page and every candidate is settled, also on an error
        try:
            if moves:
                self._run_moves(moves)
        finally:
            if drop:
                self.retire([r for r, _ in drop], [i for _, i in drop])

    def _copy_of(self, ref: PageRef, key: Key) -> tuple[PageRef, Location] | None:
        """The source's READY copy below and its location, if it has one."""
        with self._lock:
            dest = self._copies.get(ref)
            invalid = dest is not None and dest.pool_id in self._invalid_pools
        if dest is None:
            return None
        page = self.pools.page(dest)
        if (
            not invalid
            and page is not None
            and page.state == PageState.READY
            and page.key == key
            and page.location is not None
        ):
            return dest, page.location
        with self._lock:
            self._copies.pop(ref, None)
        return None

    def _is_ready(self, ref: PageRef, key: Key) -> bool:
        page = self.pools.page(ref)
        return page is not None and page.state == PageState.READY and page.key == key

    def _reserve_below(self, kind: PoolKind, count: int) -> list[PageRef]:
        """Destination pages, each from the pool of ``kind`` with the most
        room: one region filled past its high watermark would drop pages
        while the others have room."""
        assert self.transfers is not None
        with self._lock:
            invalid = set(self._invalid_pools)
        room: dict[str, int] = {}
        for pool_id in self.pools.pool_ids():
            info = self.pools.pool_info(pool_id)
            if (
                info is not None
                and info.kind == kind
                and pool_id not in invalid
                and self.transfers.has_pool(pool_id)
                and not self.pools.pool_quarantined(pool_id)
            ):
                free = self.pools.bytes_in(PageState.FREE, pool_id)
                room[pool_id] = free // info.page_size
        take = dict.fromkeys(room, 0)
        for _ in range(count):
            best = max(room, key=lambda p: room[p] - take[p], default=None)
            if best is None or room[best] - take[best] <= 0:
                break
            take[best] += 1
        out: list[PageRef] = []
        for pool_id, n in take.items():
            if n:
                out += self.pools.reserve_transfer(pool_id, n)
        return out

    def _run_moves(self, moves: list[_Move]) -> None:
        assert self.transfers is not None
        fresh = [m for m in moves if not m.existing]
        copied = [m for m in moves if m.existing]
        if self._halted():  # no copy started: all go back
            for move in fresh:
                self.pools.transfer_failed(move.dest)
            self.placement.release([m.src for m in moves], backoff=False)
            return
        copies = [Copy(m.source, m.dest_location, m.source.length) for m in fresh]
        results = self.transfers.run(copies) if copies else []
        for move, ok in zip(fresh, results, strict=True):
            try:
                if ok:
                    self.pools.transfer_done(move.dest, move.key, move.source.length)
                    copied.append(move)
                    continue
                # run() answers once the copy has ended
                self.pools.transfer_failed(move.dest)
                with self._lock:
                    self.move_results["COPY_FAILED"] += 1
            except Exception:
                logger.exception("ending the copy to %s failed", move.dest)
            self.placement.release([move.src])
        if self._halted():
            # Sent once MaruServer takes the session again
            self._queue_moves(copied)
        elif copied:
            finals = self._commit_moves(copied)
            if finals:
                self._finalize_moves(finals)
        self._reclaim_retiring()

    def _commit_queued(self, moves: list[_Move], recovering: bool) -> list[_Move]:
        """COMMIT queued moves in batches until the session goes again."""
        finals = []
        for start in range(0, len(moves), MAX_BATCH):
            if self.metadata.epoch_lost if recovering else self._halted():
                self._queue_moves(moves[start:])
                break
            finals += self._commit_moves(moves[start : start + MAX_BATCH])
        return finals

    def _commit_queued_moves(self) -> None:
        """After a re-registration; FINALIZE_MOVE waits for eviction."""
        with self._lock:
            queued, self._moves = self._moves, []
        finals = self._commit_queued(queued, recovering=True)
        with self._lock:
            self._finals.extend(finals)

    def _finalizers_lost(self) -> None:
        """A new MaruServer took no finalizer over: moves waiting for their
        FINALIZE_MOVE finalize again later (both pages were re-registered)."""
        with self._lock:
            finals, self._finals = self._finals, []
        for move in finals:
            self._keep_copy(move)
        self.placement.release([m.src for m in finals], backoff=False)

    def _commit_moves(self, moves: list[_Move]) -> list[_Move]:
        """COMMIT the copies with a finalizer; returns those to FINALIZE_MOVE.
        A result lost with the session is sent again as a new op once
        MaruServer takes the session again; one left unknown otherwise is
        settled as the commit path does."""
        live = []
        for move in moves:
            if move.existing and not self._is_ready(move.dest, move.key):
                self.placement.release([move.src])  # the copy went while queued
            elif self._is_ready(move.src, move.key):
                live.append(move)
            elif not move.existing:
                self._settle_copies([move])  # the source went while queued
        if not live:
            return []
        try:
            response = self.metadata.commit(
                [
                    CommitItem(
                        key=m.key,
                        location=m.dest_location,
                        logical_bytes=m.source.length,
                        finalizer=True,
                    )
                    for m in live
                ]
            )
        except EpochChanged as e:
            self._queue_moves(live)
            self.server_lost(f"MaruServer answered EPOCH to COMMIT ({e})")
            return []
        except StaleOp:
            self._queue_moves(live)
            return []
        except ServerStopped:
            self._queue_moves(live)
            raise
        except Exception as e:
            # Sent, maybe applied: not sent again as a new op
            if isinstance(e, ReplyError):
                logger.error("COMMIT of %d demoted pages: %s", len(live), e)
            else:
                logger.exception("COMMIT of %d demoted pages failed", len(live))
            self._commit_unknown(live)
            return []
        if response.status == Status.OK:
            outcomes = response.items
        else:
            logger.error(
                "COMMIT of %d demoted pages refused as a whole: %s %s",
                len(live),
                response.status,
                response.error,
            )
            outcomes = [ItemOutcome(ItemResult.REJECTED)] * len(live)
        finals: list[_Move] = []
        unknown: list[_Move] = []
        for move, outcome in zip(live, outcomes, strict=True):
            try:
                answer = self._apply_move_commit(move, outcome)
            except Exception:
                logger.exception("applying the COMMIT result of %s failed", move.dest)
                self.placement.release([move.src])
                continue
            if answer is True:
                finals.append(move)
            elif answer is None:
                unknown.append(move)
        if unknown:
            self._commit_unknown(unknown)
        return finals

    def _apply_move_commit(self, move: _Move, outcome: ItemOutcome) -> bool | None:
        """True if the copy has its finalizer for FINALIZE_MOVE, None if the
        result is unknown."""
        result = outcome.result
        with self._lock:
            self.move_results[f"COMMIT:{result.value}"] += 1
        if result == ItemResult.ADDED:
            if not move.existing:
                self.pools.commit_added(move.dest)
            if outcome.finalizer_id:
                move.finalizer_id = outcome.finalizer_id
                return True
            self._keep_copy(move)  # no finalizer to move under
        elif result in (ItemResult.DUPLICATE, ItemResult.REJECTED):
            if move.existing:
                with self._lock:
                    self._copies.pop(move.src, None)
            else:
                self.pools.commit_freed(move.dest)
            if result == ItemResult.DUPLICATE and outcome.location is not None:
                # The key is in that pool already: the next demotion
                # finalizes onto that copy
                self._note_copy(move.src, move.key, outcome.location)
        else:
            logger.error("COMMIT of demoted page %s answered %s", move.dest, result)
            return None
        self.placement.release([move.src])
        return False

    def _commit_unknown(self, moves: list[_Move]) -> None:
        """Maybe in the Catalog with a finalizer whose id is unknown: release
        it by location, then settle a new copy with a RETIRE (RETIRED frees,
        PINNED proves it there); an existing copy stays a copy."""
        try:
            self._release_finalizers(moves)
            new = [m for m in moves if not m.existing]
            if new:
                self._settle_copies(new)
        finally:
            self.placement.release([m.src for m in moves])

    def _release_finalizers(self, moves: list[_Move]) -> None:
        """RELEASE_FINALIZER on the copies, once. Without a usable answer the
        finalizers go when the node session ends."""
        items = [KeyLocation(key=m.key, location=m.dest_location) for m in moves]
        try:
            response = self.metadata.release_finalizer(items)
        except EpochChanged as e:
            self.server_lost(f"MaruServer answered EPOCH to RELEASE_FINALIZER ({e})")
            return
        except ServerStopped:
            raise
        except Exception as e:
            logger.error("RELEASE_FINALIZER of %d copies failed: %s", len(items), e)
            return
        if response.status != Status.OK:
            logger.error(
                "RELEASE_FINALIZER refused as a whole: %s %s",
                response.status,
                response.error,
            )
            return
        with self._lock:
            for outcome in response.items:
                self.move_results[f"RELEASE_FINALIZER:{outcome.result.value}"] += 1

    def _note_copy(self, src: PageRef, key: Key, location: Location) -> None:
        ref = self.pools.find(location)
        if ref is not None and self._is_ready(ref, key):
            with self._lock:
                self._copies[src] = ref

    def _finalize_queued(self, moves: list[_Move]) -> None:
        for start in range(0, len(moves), MAX_BATCH):
            if self._halted():
                with self._lock:
                    self._finals.extend(moves[start:])
                return
            self._finalize_moves(moves[start : start + MAX_BATCH])

    def _finalize_moves(self, moves: list[_Move]) -> None:
        """Remove the sources now that the copies have their finalizers."""
        self.local_pins.block([m.src for m in moves])
        try:
            response = self.metadata.finalize_move(
                [
                    MoveItem(
                        key=m.key,
                        source=m.source,
                        dest=m.dest_location,
                        finalizer_id=m.finalizer_id,
                    )
                    for m in moves
                ]
            )
        except EpochChanged as e:
            self._move_unknown(moves)
            self.server_lost(f"MaruServer answered EPOCH to FINALIZE_MOVE ({e})")
            return
        except ServerStopped:
            self._move_unknown(moves)
            raise
        except (StaleOp, ReplyError):
            self._move_unknown(moves)
            return
        except Exception:
            logger.exception("FINALIZE_MOVE failed")
            self._move_unknown(moves)
            return
        if response.status != Status.OK:
            logger.error(
                "FINALIZE_MOVE refused as a whole: %s %s",
                response.status,
                response.error,
            )
            # Not run: the finalizers are released; the next demotion places
            # them again on the copies and finalizes
            try:
                self._release_finalizers(moves)
            finally:
                self._move_unknown(moves)
                self.local_pins.unblock([m.src for m in moves])
            return
        # Copies not kept as locations of their key: retired, freed once RETIRED
        retire: list[_Move] = []
        for move, outcome in zip(moves, response.items, strict=True):
            try:
                if self._apply_move(move, outcome):
                    retire.append(move)
            except Exception:
                logger.exception(
                    "applying the FINALIZE_MOVE result of %s failed", move.src
                )
                self._hold([move.src])
        if retire:
            for move in retire:
                self.placement.on_ready(move.dest)
            self.retire(
                [m.dest for m in retire],
                [KeyLocation(key=m.key, location=m.dest_location) for m in retire],
            )

    def _apply_move(self, move: _Move, outcome: ItemOutcome) -> bool:
        """Act on one FINALIZE_MOVE result; whether to retire the copy."""
        result, cause = outcome.result, outcome.cause
        if result == ItemResult.REJECTED:
            # The copy had no finalizer of this session: the source stays
            with self._lock:
                self.move_results["REJECTED"] += 1
            if self._is_ready(move.src, move.key):
                self._retire_failed([move.src])
            return True
        if result not in (ItemResult.RETIRED, ItemResult.KEPT):
            logger.error("FINALIZE_MOVE item answered %s for %s", result, move.src)
            self._move_unknown([move])
            return False
        if (
            result == ItemResult.KEPT
            and cause == KeptCause.INVALID
            and outcome.location is not None
            and outcome.location.pool_id == move.dest_location.pool_id
        ):
            # The pool below is the invalid one: the source stays, the copy
            # is not used
            with self._lock:
                self.move_results["KEPT:INVALID"] += 1
            self._retire_failed([move.src])
            self._pool_invalid(move.dest)
            return False
        if not self._is_ready(move.src, move.key):
            self.placement.on_ready(move.dest)  # the source went meanwhile
            return False
        # The page holds another key per the Catalog: its bytes were not
        # this key's
        foreign = (
            result == ItemResult.KEPT
            and cause == KeptCause.MOVED
            and outcome.key != move.key
        )
        if not foreign:
            self.placement.on_ready(move.dest)  # a location of the key either way
            if result == ItemResult.KEPT:
                with self._lock:
                    self._copies[move.src] = move.dest
        # The source as after a RETIRE with this result
        self._apply_retire(move.src, outcome, self.move_results)
        return foreign

    def _move_unknown(self, moves: list[_Move]) -> None:
        """No usable FINALIZE_MOVE result: both stay READY (re-registered
        after a MaruServer restart), the source's local pins stay blocked,
        and its next demotion finalizes onto the copy again."""
        for move in moves:
            self._keep_copy(move)
        with self._lock:
            self.move_results["UNKNOWN"] += len(moves)
        self.placement.release([m.src for m in moves])

    def _keep_copy(self, move: _Move) -> None:
        self.placement.on_ready(move.dest)
        with self._lock:
            self._copies[move.src] = move.dest

    def _settle_copies(self, moves: list[_Move]) -> None:
        """Copies whose COMMIT result is unknown, as the commit path settles
        them: a RETIRE tells (RETIRED frees, PINNED proves them there)."""
        for move in moves:
            self.pools.commit_added(move.dest)
            self._settle_page(move.dest)
        self._evict_wake.set()

    def _queue_moves(self, moves: list[_Move]) -> None:
        with self._lock:
            self._moves.extend(moves)

    # --------------------------------------------------------------- removal

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
        with self._lock:
            paused = self._paused()
        lost = self.metadata.epoch_lost
        if paused or lost:
            self._retire_failed(refs)  # not sent: offered again later
            if lost:
                self.server_lost("the node session is lost")
            return
        self.local_pins.block(refs)
        try:
            response = self.metadata.retire(items)
        except EpochChanged as e:
            # Unknown: the pages stay READY with their local pins blocked,
            # and are not re-registered
            self._hold_unknown(refs)
            self.server_lost(f"MaruServer answered EPOCH to RETIRE ({e})")
            return
        except StaleOp:
            # never reached the MaruServer the session is at now
            self._retire_failed(refs)
            return
        except ServerStopped:
            self._hold_unknown(refs)
            raise
        except ReplyError:
            self._retire_failed(refs)
            return
        except Exception:
            logger.exception("RETIRE failed")
            self._hold_unknown(refs)
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

    def _apply_retire(
        self,
        ref: PageRef,
        outcome: ItemOutcome,
        results: collections.Counter[str] | None = None,
    ) -> None:
        with self._lock:
            central = ref in self._central
            settling = ref in self._settle
            cause = f":{outcome.cause.value}" if outcome.cause else ""
            counts = self.retire_results if results is None else results
            counts[f"{outcome.result.value}{cause}"] += 1
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
        with self._lock:
            self._invalid_pools.add(ref.pool_id)
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

    def _hold_unknown(self, refs: list[PageRef]) -> None:
        """Held: their RETIRE result is unknown."""
        self._hold(refs)
        with self._lock:
            self._retire_unknown.update(refs)

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
            self._copies.pop(ref, None)

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
                while not self._stopping and (not self._exits or self._paused()):
                    self._cv.wait()
                if self._stopping:
                    return
                batch = [
                    self._exits.popleft()
                    for _ in range(min(MAX_BATCH, len(self._exits)))
                ]
                self._exit_busy = True
            try:
                response = self.metadata.worker_exit(batch, ExitCause.EXIT)
                ok = response.status == Status.OK
                if not ok:
                    logger.error(
                        "WORKER_EXIT refused as a whole: %s %s; trying again",
                        response.status,
                        response.error,
                    )
            except (EpochChanged, StaleOp) as e:
                # sent again with a new op once the session is back
                with self._cv:
                    self._exits.extendleft(reversed(batch))
                    self._exit_busy = False
                    self._cv.notify_all()
                if isinstance(e, EpochChanged):
                    self.server_lost(f"MaruServer answered EPOCH to WORKER_EXIT ({e})")
                continue
            except ServerStopped:
                return
            except Exception:
                logger.exception("WORKER_EXIT failed; trying again")
                ok = False
            finally:
                with self._cv:
                    self._exit_busy = False
                    self._cv.notify_all()
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

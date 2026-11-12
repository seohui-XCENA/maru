# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PeerOps: a memory node's side of its RemoteServer ops.

A GPU node run (a peer, ``origin`` = ``peer:{node_id}@{node_epoch}``) gets
credit as a worker does, RDMA-writes the pages and reports how each write
ended; the memory node commits the DONE ones (no finalizer), and the pages
are then its own to tier and retire. Release evidence for a page a transfer
may touch is only a transfer that ended DONE, or one never posted (CANCEL);
a write whose end is unknown, a credit past its TTL, and every credit of a
lost or retired peer run are quarantined (tag: the run's origin). Evidence
from the same run releases them; otherwise they stay until the memory run
ends. A peer node (all peers) past its quarantine cap gets no more room.
"""

from __future__ import annotations

import collections
import dataclasses
import logging
import threading
import time
from collections.abc import Callable

from maru_common.protocol import CommitItem
from maru_common.remote_protocol import (
    CancelRequest,
    CancelResponse,
    DropRequest,
    DropResponse,
    RemoteWriteDoneRequest,
    RemoteWriteDoneResponse,
    ReserveRequest,
    ReserveResponse,
    RoomLimit,
    StageRequest,
    StageResponse,
    TransferEnd,
    UnstageRequest,
    UnstageResponse,
)
from maru_common.storage_types import (
    MAX_BATCH,
    ItemOutcome,
    ItemResult,
    KeyLocation,
    Location,
    MissReason,
    Status,
)

from .pool_manager import PageRef, PageState
from .storage_manager import StorageManager
from .transfer_engine import Copy

logger = logging.getLogger(__name__)

# How often credits are checked against their TTL
SWEEP_S = 1.0


def peer_node(origin: str) -> str:
    """The node_id of a peer run's origin."""
    return origin.removeprefix("peer:").rpartition("@")[0]


def _page_key(location: Location) -> tuple[str, int, int]:
    return (location.pool_id, location.offset, location.generation)


class PeerOps:
    """The RemoteOps of one memory node run. Thread safe."""

    def __init__(
        self,
        manager: StorageManager,
        *,
        reserve_ttl_s: float,
        peer_quarantine_max: int,
        quarantine_max: int,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.manager = manager
        self.pools = manager.pools
        self.page_size = self.pools.page_size
        self.reserve_ttl_s = reserve_ttl_s
        self.peer_quarantine_max = peer_quarantine_max
        self.quarantine_max = quarantine_max
        self._clock = clock
        self._lock = threading.Lock()
        # Quarantined bytes by peer node_id (all its runs)
        self.quarantined: collections.Counter[str] = collections.Counter()
        # Credits given and not written yet: page -> (origin, location, TTL end)
        self._credited: dict[tuple[str, int, int], tuple[str, Location, float]] = {}
        # Each RESERVE's credits by origin and seq (CANCEL by reserve_seq)
        self._reserves: dict[str, dict[int, list[Location]]] = {}
        # RESERVE seqs a CANCEL came before: they get no credit
        self._tombstones: dict[str, set[int]] = {}
        # Peer runs that ended: no op of theirs is served again
        self._retired: set[str] = set()
        # Staging pages answered to a peer run, by (origin, stage_id): ref,
        # location, TTL end; those quarantined; UNSTAGEs before their STAGE;
        # the bytes of a run's staging pages (in its credit cap); runs whose
        # connection went since their last op
        self._staged: dict[tuple[str, int], tuple[PageRef, Location, float]] = {}
        self._lost: set[str] = set()
        self._staged_quarantine: dict[tuple[str, int], Location] = {}
        self._unstaged: set[tuple[str, int]] = set()
        self._staging_bytes: collections.Counter[str] = collections.Counter()
        self._stop = threading.Event()
        self._sweeper: threading.Thread | None = None

    # ------------------------------------------------------------ lifecycle

    def start(self) -> None:
        self._sweeper = threading.Thread(
            target=self._sweep_loop, name="maru-peer-ttl", daemon=True
        )
        self._sweeper.start()

    def stop(self) -> None:
        self._stop.set()
        if (
            self._sweeper is not None
            and self._sweeper is not threading.current_thread()
        ):
            self._sweeper.join(timeout=5.0)

    # ---------------------------------------------------------------- room

    def room(self, origin: str) -> tuple[int, RoomLimit | None]:
        """The bytes RESERVE would credit the peer now, 0 at a quarantine cap."""
        with self._lock:
            if self._capped(peer_node(origin)):
                return 0, RoomLimit.QUARANTINE
        return self.pools.credit_room(origin, self.manager.credit_tiers), None

    def _capped(self, node_id: str) -> bool:
        """Caller holds the lock."""
        return (
            self.quarantined[node_id] >= self.peer_quarantine_max
            or sum(self.quarantined.values()) >= self.quarantine_max
        )

    # ----------------------------------------------------------------- ops

    def reserve(
        self,
        origin: str,
        request: ReserveRequest,
        done: Callable[[ReserveResponse], None],
    ) -> None:
        if not 0 < request.pages <= MAX_BATCH:
            done(
                ReserveResponse(
                    status=Status.REJECTED,
                    error=f"a RESERVE asks for 1..{MAX_BATCH} pages",
                )
            )
            return
        credits = self._reserve(origin, request)
        if isinstance(credits, ReserveResponse):
            done(credits)
            return
        room, limit = self.room(origin)
        done(ReserveResponse(status=Status.OK, credits=credits, room=room, limit=limit))

    def _reserve(
        self, origin: str, request: ReserveRequest
    ) -> list[Location] | ReserveResponse:
        """The RESERVE's credits, or the answer that gives none."""
        with self._lock:
            if origin in self._retired:
                return ReserveResponse(status=Status.REJECTED, error="the run ended")
            self._acked(origin, request.op.ack)
            seq = request.op.seq
            if seq in self._tombstones.get(origin, ()):
                return []  # canceled before it came
            if self._capped(peer_node(origin)):
                return ReserveResponse(
                    status=Status.OK, room=0, limit=RoomLimit.QUARANTINE
                )
            self._lost.discard(origin)
            # Its staging pages count in its credit cap too
            cap = self.pools.budgets.credit_max_bytes - self._staging_bytes[origin]
            want = min(request.pages * self.page_size, max(0, cap))
            credits = (
                self.pools.credit(origin, want, self.manager.credit_tiers)
                if want >= self.page_size
                else []
            )
            deadline = self._clock() + self.reserve_ttl_s
            for credit in credits:
                self._credited[_page_key(credit)] = (origin, credit, deadline)
            self._reserves.setdefault(origin, {})[seq] = list(credits)
        short = want - len(credits) * self.page_size
        if short > 0 and self.manager.credit_tiers:
            self.manager.want_room(self.manager.credit_tiers[0], short)
        return credits

    def write_done(
        self,
        origin: str,
        request: RemoteWriteDoneRequest,
        done: Callable[[RemoteWriteDoneResponse], None],
    ) -> None:
        """DONE items are committed (a quarantined page of the same run too);
        UNKNOWN ones are quarantined."""
        if not 0 < len(request.items) <= MAX_BATCH:
            done(
                RemoteWriteDoneResponse(
                    status=Status.REJECTED,
                    error=f"a WRITE_DONE carries 1..{MAX_BATCH} items",
                )
            )
            return
        outcomes: list[ItemOutcome | None] = [None] * len(request.items)
        pages: dict[int, tuple[PageRef, CommitItem]] = {}
        with self._lock:
            retired = origin in self._retired
            for i, item in enumerate(request.items):
                if retired:
                    outcomes[i] = ItemOutcome(ItemResult.REJECTED)
                    continue
                if item.end == TransferEnd.DONE:
                    ref, location, why = self._written(origin, item.location, item)
                    if ref is None or location is None:
                        logger.debug("WRITE_DONE item %d refused: %s", i, why)
                        outcomes[i] = ItemOutcome(ItemResult.REJECTED)
                        continue
                    pages[i] = (
                        ref,
                        CommitItem(
                            key=item.key,
                            location=location,
                            logical_bytes=item.logical_bytes,
                        ),
                    )
                elif item.end == TransferEnd.UNKNOWN:
                    quarantined = self._quarantine_credit(origin, item.location)
                    outcomes[i] = ItemOutcome(
                        ItemResult.OK if quarantined else ItemResult.REJECTED
                    )
                else:  # one never posted goes back by CANCEL
                    outcomes[i] = ItemOutcome(ItemResult.REJECTED)

        def finish(items: list[ItemOutcome]) -> None:
            done(RemoteWriteDoneResponse(status=Status.OK, items=items))

        self.manager.commit_written(outcomes, pages, finish)

    def _written(self, origin: str, location: Location, item):
        """Caller holds the lock. The page a DONE write filled, PENDING now:
        credited to ``origin``, or quarantined by it."""
        ref, committed, why = self.pools.write_done(
            origin, location, item.key, item.logical_bytes
        )
        if ref is not None:
            self._credited.pop(_page_key(location), None)
            return ref, committed, why
        ref, committed, why2 = self.pools.resolve_quarantined(
            location, origin, item.key, item.logical_bytes
        )
        if ref is not None:
            self.quarantined[peer_node(origin)] -= self.page_size
            return ref, committed, ""
        return None, None, f"{why}; {why2}"

    def cancel(
        self,
        origin: str,
        request: CancelRequest,
        done: Callable[[CancelResponse], None],
    ) -> None:
        """Credits never posted go back: what is left of the RESERVE
        ``reserve_seq`` (a tombstone if it did not come yet), or
        ``locations``."""
        freed = 0
        with self._lock:
            retired = origin in self._retired
            if not retired:
                self._acked(origin, request.op.ack)
                locations = list(request.locations)
                seq = request.reserve_seq
                if seq is not None:
                    reserved = self._reserves.get(origin, {}).pop(seq, None)
                    if reserved is None:
                        self._tombstones.setdefault(origin, set()).add(seq)
                    else:
                        locations += reserved
                for location in locations:
                    freed += self._not_posted(origin, location)
        if retired:
            done(CancelResponse(status=Status.REJECTED, error="the run ended"))
        else:
            done(CancelResponse(status=Status.OK, freed=freed))

    def _acked(self, origin: str, ack: int) -> None:
        """Caller holds the lock. The peer holds every result up to ``ack``:
        no CANCEL names those RESERVEs any more."""
        reserves = self._reserves.get(origin, {})
        for seq in [s for s in reserves if s <= ack]:
            del reserves[seq]
        tombstones = self._tombstones.get(origin)
        if tombstones:
            tombstones.difference_update([s for s in tombstones if s <= ack])

    def _not_posted(self, origin: str, location: Location) -> int:
        """Caller holds the lock. Free a page of ``origin`` never written:
        credited, or quarantined by it. 1 if freed."""
        ok, _ = self.pools.abort(origin, location)
        if ok:
            self._credited.pop(_page_key(location), None)
            return 1
        ref, _, _ = self.pools.resolve_quarantined(location, origin)
        if ref is not None:
            self.quarantined[peer_node(origin)] -= self.page_size
            return 1
        return 0

    def drop(
        self, origin: str, request: DropRequest, done: Callable[[DropResponse], None]
    ) -> None:
        """RETIRE the key at exactly its READY location, off the caller's
        thread (MaruServer answers it)."""
        ref = self.pools.find(request.location)
        page = self.pools.page(ref) if ref is not None else None
        if (
            ref is None
            or page is None
            or page.state != PageState.READY
            or page.key != request.key
            or page.location != request.location
        ):
            done(
                DropResponse(status=Status.OK, outcome=ItemOutcome(ItemResult.REJECTED))
            )
            return

        def retire() -> None:
            try:
                items = self.manager.retire(
                    [ref], [KeyLocation(key=request.key, location=request.location)]
                )
            except Exception as e:
                logger.exception("DROP of %s failed", request.location)
                done(DropResponse(status=Status.REJECTED, error=str(e)))
                return
            if not items:
                done(DropResponse(status=Status.BUSY, error="RETIRE was not answered"))
                return
            done(DropResponse(status=Status.OK, outcome=items[0]))

        threading.Thread(target=retire, name="maru-peer-drop", daemon=True).start()

    def stage(
        self,
        origin: str,
        request: StageRequest,
        deadline: float,
        done: Callable[[StageResponse], None],
    ) -> None:
        """On a stage worker: load the exact READY SSD page into a staging
        page of the top tier for the peer to READ, by ``deadline``. A
        staging page counts in the peer's credit cap; it is never told to
        anyone, so it is FREE again unless its location was answered."""
        done(self._stage(origin, request, deadline))

    def _stage(
        self, origin: str, request: StageRequest, deadline: float
    ) -> StageResponse:
        key = (origin, request.stage_id)
        ps = self.page_size
        with self._lock:
            if origin in self._retired:
                return StageResponse(status=Status.REJECTED, error="the run ended")
            self._lost.discard(origin)
            if key in self._unstaged:  # its UNSTAGE came first
                return StageResponse(status=Status.OK)
            if key in self._staged or key in self._staged_quarantine:
                return StageResponse(status=Status.OK)  # answered before
            if self._capped(peer_node(origin)):
                return StageResponse(
                    status=Status.OK,
                    reason=MissReason.BUSY,
                    limit=RoomLimit.QUARANTINE,
                )
            held = self.pools.held_bytes(origin) + self._staging_bytes[origin]
            if held + ps > self.pools.budgets.credit_max_bytes:
                return StageResponse(status=Status.OK, reason=MissReason.BUSY)
            self._staging_bytes[origin] += ps  # this run's, while it loads
        try:
            staged = self._load(request, deadline)
        except Exception:
            logger.exception("STAGE of %s failed", request.source)
            staged = MissReason.IO_ERROR
        with self._lock:
            if isinstance(staged, MissReason):
                self._staging_bytes[origin] -= ps
                return StageResponse(status=Status.OK, reason=staged)
            ref, location = staged
            gone = origin in self._retired or origin in self._lost
            late = self._clock() >= deadline or gone
            if late or key in self._unstaged:
                # Never answered: nobody reads it
                self.pools.transfer_failed(ref)
                self._staging_bytes[origin] -= ps
                if late:
                    return StageResponse(status=Status.OK, reason=MissReason.TIMEOUT)
                return StageResponse(status=Status.OK)
            ttl = self._clock() + self.reserve_ttl_s
            # The whole page is what a quarantine and its evidence name
            self._staged[key] = (ref, self.pools.transfer_location(ref), ttl)
        return StageResponse(status=Status.OK, staging=location)

    def _load(
        self, request: StageRequest, deadline: float
    ) -> tuple[PageRef, Location] | MissReason:
        """The SSD page read into a new staging page (TRANSFER), or why not."""
        source = request.source
        transfers = self.manager.transfers
        src = self.pools.find(source)
        page = self.pools.page(src) if src is not None else None
        if (
            transfers is None
            or src is None
            or page is None
            or page.state != PageState.READY
            or page.key != request.key
            or page.location != source
        ):
            return MissReason.ABSENT
        if self._clock() >= deadline:
            return MissReason.TIMEOUT
        top = self.manager.credit_tiers[0] if self.manager.credit_tiers else None
        refs = self.manager._reserve_below(top, 1) if top is not None else []
        if not refs:
            return MissReason.BUSY
        staging = refs[0]
        if not self.pools.io_acquire(src, staging):
            self.pools.transfer_failed(staging)
            return MissReason.ABSENT
        location = dataclasses.replace(
            self.pools.transfer_location(staging), length=source.length
        )
        ended = threading.Event()
        try:
            ticket = transfers.submit_io(
                Copy(source, location, source.length),
                src,
                staging,
                callback=lambda _t: ended.set(),
                deadline=deadline,
            )
        except Exception:
            self.pools.io_release([src, staging])
            self.pools.transfer_failed(staging)
            return MissReason.BUSY
        ended.wait()  # a file read always ends
        self.pools.io_release([src, staging])
        if not ticket.ok:
            self.pools.transfer_failed(staging)
            return MissReason.TIMEOUT if ticket.late else MissReason.IO_ERROR
        return staging, location

    def unstage(
        self,
        origin: str,
        request: UnstageRequest,
        done: Callable[[UnstageResponse], None],
    ) -> None:
        """How the peer's READ of a staging page ended: DONE or never
        posted frees it, UNKNOWN quarantines it (until that run's late
        DONE). One before its STAGE leaves a tombstone."""
        key = (origin, request.stage_id)
        evidence = request.end in (TransferEnd.DONE, TransferEnd.NOT_POSTED)
        with self._lock:
            if origin in self._retired:
                done(UnstageResponse(status=Status.REJECTED, error="the run ended"))
                return
            entry = self._staged.pop(key, None)
            if entry is not None:
                ref, location, _ = entry
                self._staging_bytes[origin] -= self.page_size
                if evidence:
                    self.pools.transfer_failed(ref)
                else:
                    self.pools.quarantine_transfer(ref, origin)
                    self._staged_quarantine[key] = location
                    self.quarantined[peer_node(origin)] += self.page_size
            elif key in self._staged_quarantine:
                if evidence:
                    location = self._staged_quarantine[key]
                    freed, _, _ = self.pools.resolve_quarantined(location, origin)
                    if freed is not None:
                        del self._staged_quarantine[key]
                        self.quarantined[peer_node(origin)] -= self.page_size
            else:
                self._unstaged.add(key)
        done(UnstageResponse(status=Status.OK))

    # ------------------------------------------------------------ peer ends

    def peer_lost(self, origin: str) -> None:
        """No live connection: the run's credits and answered staging pages
        are quarantined; its own evidence still releases them."""
        with self._lock:
            self._quarantine_holder(origin)
            self._quarantine_staged(origin)
            self._lost.add(origin)

    def peer_retired(self, origin: str) -> None:
        """The run will never come back: its credits are quarantined until
        the memory run ends, and nothing of it is served again."""
        with self._lock:
            self._quarantine_holder(origin)
            self._quarantine_staged(origin)
            self._retired.add(origin)
            self._reserves.pop(origin, None)
            self._tombstones.pop(origin, None)
            self._unstaged = {k for k in self._unstaged if k[0] != origin}
        self.pools.forget_holder(origin)

    def _quarantine_staged(self, origin: str, before: float | None = None) -> int:
        """Caller holds the lock: the answered staging pages of ``origin``
        (those past ``before`` only, if given) are quarantined."""
        n = 0
        for key in [k for k in self._staged if k[0] == origin]:
            ref, location, ttl = self._staged[key]
            if before is not None and ttl > before:
                continue
            del self._staged[key]
            self.pools.quarantine_transfer(ref, origin)
            self._staged_quarantine[key] = location
            self._staging_bytes[origin] -= self.page_size
            self.quarantined[peer_node(origin)] += self.page_size
            n += 1
        return n

    def _quarantine_holder(self, origin: str) -> None:
        """Caller holds the lock."""
        refs = self.pools.quarantine_holder(origin, tag=origin)
        if refs:
            self.quarantined[peer_node(origin)] += len(refs) * self.page_size
            logger.warning(
                "peer run %s: %d credited pages quarantined", origin, len(refs)
            )
        for key in [k for k, v in self._credited.items() if v[0] == origin]:
            del self._credited[key]

    def _quarantine_credit(self, origin: str, location: Location) -> bool:
        """Caller holds the lock."""
        if self.pools.quarantine_credit(origin, location, origin) is None:
            return False
        self._credited.pop(_page_key(location), None)
        self.quarantined[peer_node(origin)] += self.page_size
        return True

    # ------------------------------------------------------------------ TTL

    def sweep(self) -> int:
        """Quarantine the credits past their TTL; returns how many."""
        now = self._clock()
        n = 0
        with self._lock:
            expired = [v for v in self._credited.values() if v[2] <= now]
            for origin, location, _ in expired:
                if self._quarantine_credit(origin, location):
                    n += 1
            for origin in {k[0] for k in self._staged}:
                n += self._quarantine_staged(origin, before=now)
        if n:
            logger.warning("%d credited pages past their TTL are quarantined", n)
        return n

    def _sweep_loop(self) -> None:
        interval = min(SWEEP_S, self.reserve_ttl_s / 4)
        while not self._stop.wait(interval):
            try:
                self.sweep()
            except Exception:
                logger.exception("credit TTL sweep failed")

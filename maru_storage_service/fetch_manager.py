# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""FetchManager: FETCH from workers' loaders, for plans whose source is this
node's SSD.

One flight per key at a time: one destination page in the top tier and one
read, shared by every FETCH of the key. A flight takes its quota and its page,
pins the source (SOURCE_PIN), reads it through the I/O executor, then
publishes the copy with FETCH_COMPLETE: the source is unpinned and the copy
pinned for the leading worker, plus a node pin (hold_copy) that keeps the copy
while the other waiters get their own pins on it (the exact copy). The node
pin goes once no waiter is left.

A FETCH is answered once every item has its outcome; at its deadline or its
cancel it is answered at once, with MISS for each item without a pin handed
over (pins already made are unpinned here). The answer hands the HIT pins to
the worker, which UNPINs them after its GPU reads. A pin that comes after the
answer is unpinned here. No metadata lock is held during an RPC or an I/O.
"""

import collections
import concurrent.futures
import logging
import threading
import time
from dataclasses import dataclass, field

from maru_common.node_protocol import (
    FetchCancelRequest,
    FetchCancelResponse,
    FetchRequest,
    FetchResponse,
)
from maru_common.protocol import FetchCompleteItem
from maru_common.serializer import DecodedRequest
from maru_common.storage_types import (
    MAX_BATCH,
    ItemOutcome,
    ItemResult,
    Key,
    KeyLocation,
    Location,
    MissReason,
    PoolKind,
    Status,
)

from .drivers.ssd import SsdPool
from .op_ledger import LedgerEntry, Verdict
from .page_table import PageRef, PageState
from .ports import EpochChanged, ServerStopped
from .transfer_engine import Copy, IOTicket
from .worker_registry import WorkerRecord

logger = logging.getLogger(__name__)

FETCH_THREADS = 8
# Reads of one FETCH queued at a time; the executor keeps the rest
FETCH_IO_DEPTH = 8
FETCH_DEADLINE_MAX_MS = 60_000
# How often a FETCH short of room looks again, while eviction makes it
ROOM_POLL_S = 0.005
# Cancels kept per worker for FETCHes not seen yet
CANCEL_MARKS = 64


def _miss(reason: MissReason | None = None) -> ItemOutcome:
    return ItemOutcome(ItemResult.MISS, reason=reason)


@dataclass(eq=False)
class _Op:
    """One FETCH of a worker."""

    origin: str
    seq: int
    entry: LedgerEntry
    session: str
    deadline: float
    outcomes: list[ItemOutcome | None]
    flights: list["_Flight | None"]
    answered: bool = False
    timer: threading.Timer | None = None


@dataclass(eq=False)
class _Waiter:
    op: _Op
    index: int


@dataclass(eq=False)
class _Flight:
    """One read of a key and the requests waiting for it."""

    key: Key
    source: Location
    source_ref: PageRef
    bounce: int
    leader: _Waiter
    waiters: list[_Waiter] = field(default_factory=list)
    # Bytes of the node quota it holds: its page, and its bounce until read
    charged: int = 0
    dest: PageRef | None = None
    source_pin: str | None = None
    ticket: IOTicket | None = None
    # The copy waiters get pins on, and the node pin holding it
    copy: Location | None = None
    node_pin: str | None = None
    closed: bool = False


class FetchManager:
    """Thread safe. Built per run with the StorageManager it serves."""

    def __init__(
        self,
        manager,
        *,
        ssd: SsdPool,
        node_id: str,
        page_size: int,
        kind: PoolKind,
        request_max_bytes: int,
        inflight_max_bytes: int,
        threads: int = FETCH_THREADS,
    ):
        """``kind``: the top tier, where copies go (workers map it)."""
        assert manager.transfers is not None
        self.manager = manager
        self.pools = manager.pools
        self.metadata = manager.metadata
        self.transfers = manager.transfers
        self.ssd = ssd
        self.node_id = node_id
        self.page_size = page_size
        self.kind = kind
        self.request_max_bytes = request_max_bytes
        self.inflight_max_bytes = inflight_max_bytes
        self._lock = threading.Lock()
        self._flights: dict[Key, _Flight] = {}
        self._ops: dict[tuple[str, int], _Op] = {}
        self._cancelled: dict[str, dict[int, None]] = {}
        self._inflight = 0
        self._stop = threading.Event()
        self._tasks = concurrent.futures.ThreadPoolExecutor(
            threads, thread_name_prefix="maru-fetch"
        )
        self._unpins: collections.deque[str] = collections.deque()
        self._unpin_cv = threading.Condition()
        self._releaser = threading.Thread(
            target=self._release_loop, name="maru-fetch-unpin", daemon=True
        )
        self._releaser.start()
        # Reads, joins, outcomes ("HIT", "MISS:busy", ...) and late pins
        self.results: collections.Counter[str] = collections.Counter()

    # ------------------------------------------------------------ lifecycle

    def stop(self) -> None:
        """Answer what is open (MISS busy) and end the threads."""
        self._stop.set()
        with self._lock:
            ops = list(self._ops.values())
        for op in ops:
            self._answer(op, MissReason.BUSY)
        self._tasks.shutdown(wait=True, cancel_futures=True)
        with self._unpin_cv:
            self._unpin_cv.notify_all()
        self._releaser.join(timeout=10.0)

    @property
    def inflight_bytes(self) -> int:
        """Quota held: pages of open flights and bounces of running reads."""
        with self._lock:
            return self._inflight

    def flights(self) -> int:
        with self._lock:
            return len(self._flights)

    # ------------------------------------------------------------- requests

    def fetch(self, rec: WorkerRecord, decoded: DecodedRequest, reply, waiter=None):
        """Answer via ``reply`` once every item has its outcome, or at the
        deadline or a cancel."""
        req: FetchRequest = decoded.request
        error = self._check(req)
        if error is not None:
            reply(FetchResponse(status=Status.REJECTED, error=error))
            return
        digest, reason = self.manager._check_op(rec, req.op, decoded)
        if digest is None:
            reply(FetchResponse(status=Status.REJECTED, error=reason))
            return
        ledger = self.manager.ledger
        verdict, entry = ledger.begin(rec.incarnation, req.op, digest)
        if verdict == Verdict.STALE:
            stale = [ItemOutcome(ItemResult.STALE)] * len(req.items)
            reply(FetchResponse(status=Status.OK, items=stale))
            return
        if verdict == Verdict.CONFLICT:
            error = f"op {req.op.seq} was sent before with another digest"
            reply(FetchResponse(status=Status.REJECTED, error=error))
            return
        if verdict == Verdict.BUSY:
            error = "too many unacknowledged ops"
            reply(FetchResponse(status=Status.BUSY, error=error))
            return
        assert entry is not None
        key = reply if waiter is None else waiter
        if verdict == Verdict.REPLAY:
            # A resend joins the same fetch; its deadline stays
            waiting, displaced = ledger.attach(entry, reply, key)
            if not waiting:
                reply(entry.response)
            elif displaced is not None:
                displaced(FetchResponse(status=Status.BUSY, error="too many resends"))
            return
        ledger.attach(entry, reply, key)
        rec.worker_session = req.worker_session
        count = len(req.items)
        op = _Op(
            origin=rec.incarnation,
            seq=req.op.seq,
            entry=entry,
            session=req.worker_session,
            deadline=time.monotonic() + req.deadline_ms / 1000,
            outcomes=[None] * count,
            flights=[None] * count,
        )
        # Checked against this node's pages before the lock
        sources = [self._source(rec, item.key, item.plan) for item in req.items]
        led: list[_Flight] = []
        with self._lock:
            marks = self._cancelled.get(op.origin)
            if marks:
                for seq in [s for s in marks if s <= req.op.ack]:
                    del marks[seq]
            if self._stop.is_set():
                op.outcomes = [_miss(MissReason.BUSY)] * count
            elif marks is not None and op.seq in marks:
                del marks[op.seq]
                op.outcomes = [_miss()] * count  # cancelled before it came
            else:
                for i, (item, source) in enumerate(
                    zip(req.items, sources, strict=True)
                ):
                    if isinstance(source, ItemOutcome):
                        op.outcomes[i] = source
                        continue
                    flight = self._flights.get(item.key)
                    if flight is not None and not flight.closed:
                        flight.waiters.append(_Waiter(op, i))
                        op.flights[i] = flight
                        self.results["joined"] += 1
                        continue
                    ref, bounce = source
                    cost = self.page_size + bounce
                    if self._inflight + cost > self.inflight_max_bytes:
                        op.outcomes[i] = _miss(MissReason.BUSY)
                        continue
                    flight = _Flight(
                        key=item.key,
                        source=item.plan.source,
                        source_ref=ref,
                        bounce=bounce,
                        leader=_Waiter(op, i),
                        charged=cost,
                    )
                    self._inflight += cost
                    self._flights[item.key] = flight
                    op.flights[i] = flight
                    led.append(flight)
            self._ops[(op.origin, op.seq)] = op
        if all(o is not None for o in op.outcomes):
            self._answer(op)
            return
        timer = threading.Timer(
            max(0.0, op.deadline - time.monotonic()),
            self._answer,
            (op, MissReason.TIMEOUT),
        )
        timer.daemon = True
        op.timer = timer
        timer.start()
        if led:
            try:
                self._tasks.submit(self._lead, op, led)
            except RuntimeError:  # stopping
                for flight in led:
                    self._fail(flight, _miss(MissReason.BUSY))

    def cancel(self, rec: WorkerRecord, req: FetchCancelRequest) -> FetchCancelResponse:
        """No longer needed: an open FETCH is answered at once; one not seen
        yet is answered MISS when it comes."""
        target = req.target
        if target.origin != rec.incarnation:
            return FetchCancelResponse(
                status=Status.REJECTED, error="the target is another worker's op"
            )
        with self._lock:
            op = self._ops.get((target.origin, target.seq))
            if op is None:
                marks = self._cancelled.setdefault(target.origin, {})
                marks[target.seq] = None
                while len(marks) > CANCEL_MARKS:
                    del marks[next(iter(marks))]
        if op is not None:
            self._answer(op, None)
        return FetchCancelResponse(status=Status.OK)

    def _check(self, req: FetchRequest) -> str | None:
        """Why the request is malformed or too large, or None."""
        if not 1 <= len(req.items) <= MAX_BATCH:
            return f"a FETCH carries 1..{MAX_BATCH} items"
        if not req.worker_session:
            return "a FETCH needs the worker_session of MaruServer"
        if not 0 < req.deadline_ms <= FETCH_DEADLINE_MAX_MS:
            return f"deadline_ms must be within 1..{FETCH_DEADLINE_MAX_MS}"
        ps = self.page_size
        if any(not 0 < item.plan.bytes <= ps for item in req.items):
            return f"an item's bytes are not within 1..{ps}"
        rounded = sum(-(-item.plan.bytes // ps) * ps for item in req.items)
        cap = min(req.max_bytes, self.request_max_bytes)
        if rounded > cap:
            return f"{rounded} bytes of pages exceed the cap of {cap}"
        return None

    def _source(self, rec: WorkerRecord, key: Key, plan) -> ItemOutcome | tuple:
        """(the source page, its bounce bytes), or the item's outcome. Only a
        READY page of this node's SSD holding exactly the plan's location."""
        source = plan.source
        if (
            key.namespace != rec.ident.namespace
            or plan.source_node != self.node_id
            or source.pool_id != self.ssd.pool_id
            or source.kind != PoolKind.SSD
        ):
            return ItemOutcome(ItemResult.REJECTED)
        ref = self.pools.find(source)
        page = self.pools.page(ref) if ref is not None else None
        if (
            page is None
            or page.state != PageState.READY
            or page.key != key
            or page.location != source
            or source.length != plan.bytes
        ):
            return _miss(MissReason.ABSENT)
        return ref, self.ssd.io_size(plan.bytes)

    # ------------------------------------------------------------- outcomes

    def _live(self, flight: _Flight) -> bool:
        """A waiter of the flight is still unanswered."""
        with self._lock:
            return not flight.leader.op.answered or any(
                not w.op.answered for w in flight.waiters
            )

    def _resolve(self, waiter: _Waiter, outcome: ItemOutcome) -> None:
        op = waiter.op
        with self._lock:
            # Answered already, or this item has its outcome
            late = op.answered or op.outcomes[waiter.index] is not None
            if not late:
                op.outcomes[waiter.index] = outcome
                complete = all(o is not None for o in op.outcomes)
        if late:
            if outcome.pin_id:
                self.results["late_pins"] += 1
                self._unpin_later([outcome.pin_id])
            return
        if complete:
            self._answer(op)

    def _answer(self, op: _Op, fill: MissReason | None = None) -> None:
        """Answer ``op`` once. Short of an outcome (a deadline, a cancel, a
        stop) every item is MISS ``fill`` and pins already made go."""
        with self._lock:
            if op.answered:
                return
            op.answered = True
            self._ops.pop((op.origin, op.seq), None)
            partial = any(o is None for o in op.outcomes)
            pins = []
            items = []
            for outcome in op.outcomes:
                if outcome is None or (partial and outcome.pin_id):
                    if outcome is not None:
                        pins.append(outcome.pin_id)
                    outcome = _miss(fill)
                items.append(outcome)
            for outcome in items:
                reason = f":{outcome.reason.value}" if outcome.reason else ""
                self.results[f"{outcome.result.value}{reason}"] += 1
        if op.timer is not None:
            op.timer.cancel()
        if pins:
            self._unpin_later(pins)
        response = FetchResponse(status=Status.OK, items=items)
        for reply in self.manager.ledger.finish(op.origin, op.entry, response):
            try:
                reply(response)
            except Exception:
                logger.debug("could not answer a FETCH", exc_info=True)

    def _fail(self, flight: _Flight, outcome: ItemOutcome) -> None:
        """End the flight: every waiter gets ``outcome``. The caller settled
        its page and source pin."""
        with self._lock:
            waiters = [flight.leader, *flight.waiters]
            flight.waiters = []
            self._close(flight)
        for waiter in waiters:
            self._resolve(waiter, outcome)

    def _close(self, flight: _Flight) -> None:
        """Caller holds the lock: no one joins it, its quota goes back."""
        flight.closed = True
        if self._flights.get(flight.key) is flight:
            del self._flights[flight.key]
        self._inflight -= flight.charged
        flight.charged = 0

    # --------------------------------------------------------------- flights

    def _lead(self, op: _Op, flights: list[_Flight]) -> None:
        """Run the flights ``op`` started, then serve their other waiters."""
        try:
            todo = self._take_pages(flights, op.deadline)
            todo = self._pin_sources(todo)
            todo = self._read(todo)
            for flight in self._publish(op, todo):
                self._fan_out(flight)
        except Exception:
            logger.exception("a fetch failed; its pages stay as they are")
            for flight in flights:
                if not flight.closed:
                    self._fail(flight, _miss(MissReason.BUSY))

    def _take_pages(self, flights: list[_Flight], deadline: float) -> list[_Flight]:
        """A destination page for each flight, from the top tier's mapped
        pools. Short of room, eviction is asked to make it (want_room) until
        the deadline."""
        out: list[_Flight] = []
        need = list(flights)
        while need:
            for flight in [f for f in need if not self._live(f)]:
                need.remove(flight)
                self._fail(flight, _miss())
            refs = self._reserve(len(need))
            for flight, ref in zip(need, refs, strict=False):
                flight.dest = ref
                out.append(flight)
            need = need[len(refs) :]
            if not need:
                break
            if time.monotonic() >= deadline or self._stop.is_set():
                for flight in need:
                    self._fail(flight, _miss(MissReason.BUSY))
                break
            self.manager.want_room(self.kind, len(need) * self.page_size)
            self._stop.wait(ROOM_POLL_S)
        return out

    def _reserve(self, count: int) -> list[PageRef]:
        """FREE -> TRANSFER pages of the top tier, roomiest pool first: the
        ones that give credit (workers map them, none quarantined)."""
        pools = []
        for pool_id in self.pools.pool_ids():
            info = self.pools.pool_info(pool_id)
            if (
                info is not None
                and info.kind == self.kind
                and self.pools.creditable(pool_id)
                and self.transfers.has_pool(pool_id)
            ):
                pools.append((self.pools.bytes_in(PageState.FREE, pool_id), pool_id))
        out: list[PageRef] = []
        for _, pool_id in sorted(pools, reverse=True):
            if len(out) >= count:
                break
            out += self.pools.reserve_transfer(pool_id, count - len(out))
        return out

    def _pin_sources(self, flights: list[_Flight]) -> list[_Flight]:
        """SOURCE_PIN the exact sources, one request."""
        live = []
        for flight in flights:
            if self._live(flight):
                live.append(flight)
            else:
                self._drop_page(flight)
                self._fail(flight, _miss())
        if not live:
            return []
        try:
            response = self.metadata.source_pin(
                [KeyLocation(key=f.key, location=f.source) for f in live]
            )
        except Exception as e:
            # No read started: the pages go back; the run ends unless stopping
            self._unknown("SOURCE_PIN", e)
            for flight in live:
                self._drop_page(flight)
                self._fail(flight, _miss(MissReason.BUSY))
            return []
        if response.status != Status.OK:
            logger.error("SOURCE_PIN refused: %s %s", response.status, response.error)
            for flight in live:
                self._drop_page(flight)
                self._fail(flight, _miss(MissReason.BUSY))
            return []
        out = []
        for flight, outcome in zip(live, response.items, strict=True):
            if outcome.result == ItemResult.HIT and outcome.pin_id:
                flight.source_pin = outcome.pin_id
                out.append(flight)
            else:
                self._drop_page(flight)
                self._fail(flight, _miss(outcome.reason or MissReason.ABSENT))
        return out

    def _read(self, flights: list[_Flight]) -> list[_Flight]:
        """Read each source into its page, FETCH_IO_DEPTH at a time; the
        flights whose read moved every byte."""
        out: list[_Flight] = []
        running: collections.deque[_Flight] = collections.deque()

        def wait_one() -> None:
            flight = running.popleft()
            ticket = flight.ticket
            assert ticket is not None
            ticket.done.wait()
            if ticket.ok:
                self.results["reads"] += 1
                out.append(flight)
                return
            self._release_source(flight)
            self._drop_page(flight)
            if ticket.cancelled:
                reason = None
            elif isinstance(ticket.error, TimeoutError):
                reason = MissReason.TIMEOUT
            else:
                reason = MissReason.IO_ERROR
                logger.warning("fetch read of %s failed: %s", flight.key, ticket.error)
            self._fail(flight, _miss(reason))

        for flight in flights:
            while len(running) >= FETCH_IO_DEPTH:
                wait_one()
            assert flight.dest is not None
            if not self._live(flight):
                self._release_source(flight)
                self._drop_page(flight)
                self._fail(flight, _miss())
                continue
            refs = [flight.source_ref, flight.dest]
            if not self.pools.io_acquire(refs):
                self._release_source(flight)
                self._drop_page(flight)
                self._fail(flight, _miss(MissReason.ABSENT))
                continue
            dest = self.pools.transfer_location(flight.dest)
            try:
                flight.ticket = self.transfers.submit_io(
                    Copy(flight.source, dest, flight.source.length),
                    flight.source_ref,
                    flight.dest,
                    callback=lambda _t, f=flight, refs=refs: self._read_ended(f, refs),
                )
            except Exception as e:  # a full queue, or the engine stopped
                logger.debug("cannot read %s: %s", flight.key, e)
                self.pools.io_release(refs)
                self._release_source(flight)
                self._drop_page(flight)
                self._fail(flight, _miss(MissReason.BUSY))
                continue
            running.append(flight)
        while running:
            wait_one()
        return out

    def _read_ended(self, flight: _Flight, refs: list[PageRef]) -> None:
        """The read's syscalls ended: its pages and bounce are free of it."""
        self.pools.io_release(refs)
        with self._lock:
            if flight.charged:
                flight.charged -= flight.bounce
                self._inflight -= flight.bounce

    def _publish(self, op: _Op, flights: list[_Flight]) -> list[_Flight]:
        """FETCH_COMPLETE the copies, one request: each is published (or the
        node's copy there already is used), pinned for ``op``'s worker and
        held by a node pin; the sources are unpinned."""
        if not flights:
            return []
        copies = []
        for flight in flights:
            assert flight.dest is not None
            self.pools.transfer_done(flight.dest, flight.key, flight.source.length)
            page = self.pools.page(flight.dest)
            assert page is not None and page.location is not None
            copies.append(page.location)
        with self._lock:
            # No pin for a worker already answered
            session = "" if op.answered else op.session
        items = [
            FetchCompleteItem(
                key=f.key,
                source_pin_id=f.source_pin or "",
                copy=copy,
                logical_bytes=copy.length,
                hold_copy=True,
            )
            for f, copy in zip(flights, copies, strict=True)
        ]
        try:
            response = self.metadata.fetch_complete(session, items)
        except Exception as e:
            # Unknown whether published: the pages stay PENDING, with the run
            self._unknown("FETCH_COMPLETE", e)
            for flight in flights:
                self._fail(flight, _miss(MissReason.BUSY))
            return []
        if response.status != Status.OK:
            logger.error(
                "FETCH_COMPLETE refused: %s %s", response.status, response.error
            )
            for flight in flights:
                self._release_source(flight)
                assert flight.dest is not None
                self.pools.commit_freed(flight.dest)
                self._fail(flight, _miss(MissReason.BUSY))
            return []
        out = []
        for flight, copy, outcome in zip(flights, copies, response.items, strict=True):
            assert flight.dest is not None
            location = outcome.location
            if (
                outcome.result
                not in (ItemResult.HIT, ItemResult.ADDED, ItemResult.DUPLICATE)
                or location is None
            ):
                self.pools.commit_freed(flight.dest)
                self._fail(flight, _miss(MissReason.ABSENT))
                continue
            if location == copy:
                self.pools.commit_added(flight.dest)
                self.manager.placement.on_ready(flight.dest)
                # Evicted later by dropping it onto the SSD page, not a write
                self.manager._note_copy(flight.dest, flight.key, flight.source)
            else:
                self.pools.commit_freed(flight.dest)  # the node had a copy
            flight.copy, flight.node_pin = location, outcome.node_pin_id
            if not self._usable(flight.key, location):
                self._unpin_later(
                    [p for p in (outcome.pin_id, outcome.node_pin_id) if p]
                )
                self._fail(flight, _miss(MissReason.ABSENT))
                continue
            if outcome.result == ItemResult.HIT and outcome.pin_id:
                hit = ItemOutcome(
                    ItemResult.HIT, location=location, pin_id=outcome.pin_id
                )
                self._resolve(flight.leader, hit)
            else:
                self._resolve(flight.leader, _miss())
            out.append(flight)
        return out

    def _usable(self, key: Key, location: Location) -> bool:
        """A READY page of the key in a pool workers map (never handed over
        otherwise)."""
        ref = self.pools.find(location)
        page = self.pools.page(ref) if ref is not None else None
        return (
            page is not None
            and page.state == PageState.READY
            and page.key == key
            and location.kind == self.kind
            and self.pools.creditable(location.pool_id)
        )

    def _fan_out(self, flight: _Flight) -> None:
        """Pin the held copy for each other waiter, also those joining now;
        then release the node pin."""
        copy = flight.copy
        assert copy is not None
        while True:
            with self._lock:
                batch = [w for w in flight.waiters if not w.op.answered]
                flight.waiters = []
                if not batch:
                    self._close(flight)
                    break
            sessions: dict[str, list[_Waiter]] = {}
            for waiter in batch:
                sessions.setdefault(waiter.op.session, []).append(waiter)
            for session, waiters in sessions.items():
                for i in range(0, len(waiters), MAX_BATCH):
                    self._pin_copy(
                        flight.key, copy, session, waiters[i : i + MAX_BATCH]
                    )
        if flight.node_pin:
            self._unpin_later([flight.node_pin])

    def _pin_copy(
        self, key: Key, copy: Location, session: str, waiters: list[_Waiter]
    ) -> None:
        """FETCH_COMPLETE of the exact copy: one pin per waiter."""
        items = [
            FetchCompleteItem(key, "", copy, copy.length) for _ in range(len(waiters))
        ]
        try:
            response = self.metadata.fetch_complete(session, items)
        except Exception as e:
            self._unknown("FETCH_COMPLETE", e)
            outcomes = [_miss(MissReason.BUSY)] * len(waiters)
        else:
            if response.status == Status.OK:
                outcomes = [
                    ItemOutcome(ItemResult.HIT, location=copy, pin_id=o.pin_id)
                    if o.result == ItemResult.HIT and o.pin_id
                    else _miss()
                    for o in response.items
                ]
            else:
                outcomes = [_miss(MissReason.BUSY)] * len(waiters)
        for waiter, outcome in zip(waiters, outcomes, strict=True):
            self._resolve(waiter, outcome)

    # --------------------------------------------------------------- cleanup

    def _drop_page(self, flight: _Flight) -> None:
        """TRANSFER -> FREE; only once no read runs on it."""
        if flight.dest is not None:
            self.pools.transfer_failed(flight.dest)
            flight.dest = None

    def _release_source(self, flight: _Flight) -> None:
        if flight.source_pin:
            self._unpin_later([flight.source_pin])
            flight.source_pin = None

    def _unknown(self, what: str, error: Exception) -> None:
        """A result stays unknown: the run ends (S5c), unless it is stopping."""
        if isinstance(error, ServerStopped) or self._stop.is_set():
            return
        if isinstance(error, EpochChanged):
            reason = f"MaruServer no longer knows the session ({error})"
        else:
            logger.error("%s of a fetch: result unknown (%s)", what, error)
            reason = f"{what} of a fetch: result unknown ({error})"
        self.manager.end_run(reason)

    def _unpin_later(self, pin_ids: list[str]) -> None:
        with self._unpin_cv:
            self._unpins.extend(pin_ids)
            self._unpin_cv.notify()

    def _release_loop(self) -> None:
        """UNPIN, a batch at a time, off the fetch path."""
        while True:
            with self._unpin_cv:
                while not self._unpins and not self._stop.is_set():
                    self._unpin_cv.wait()
                if self._stop.is_set():
                    return  # the run is over: its pins go with it
                batch = [
                    self._unpins.popleft()
                    for _ in range(min(MAX_BATCH, len(self._unpins)))
                ]
            try:
                response = self.metadata.unpin(batch)
            except (EpochChanged, ServerStopped):
                return
            except Exception:
                logger.exception("UNPIN of %d fetch pins failed", len(batch))
                continue
            refused = [
                p
                for p, o in zip(batch, response.items, strict=False)
                if o.result != ItemResult.OK
            ]
            if response.status != Status.OK or refused:
                logger.error(
                    "UNPIN of fetch pins refused: %s %s %s",
                    response.status,
                    response.error,
                    refused,
                )

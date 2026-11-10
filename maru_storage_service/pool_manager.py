# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PoolManager: the node's pools, credit budgets, generations and quarantine.

The pages of each pool are in its PageTable. A page gets a new generation each
time it leaves FREE. Worker requests that do not match the ledger are refused;
a wrong transition asked by the service is a bug (InvalidTransitionError).
"""

import collections
import logging
import math
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import replace

from maru_common.node_protocol import Budgets
from maru_common.storage_types import (
    GENERATION_COUNTER_MAX,
    Key,
    Location,
    PoolInfo,
    PoolKind,
    PoolUsage,
    generation_epoch_seq,
    make_generation,
)

from .page_table import (
    RECOVERY_TAG,
    InvalidTransitionError,
    Page,
    PageRef,
    PageState,
    PageTable,
    Transitions,
)

__all__ = [
    "RECOVERY_TAG",
    "InvalidTransitionError",
    "Page",
    "PageRef",
    "PageState",
    "PoolManager",
    "holder_tag",
]

logger = logging.getLogger(__name__)

# A holder short of credit by the node-total budget keeps its claim this long
# (it asks again meanwhile); a holder asked to return credit is asked again
# only after CREDIT_ASK_S
CREDIT_WAIT_S = 1.0
CREDIT_ASK_S = 0.5


def holder_tag(holder: str) -> str:
    """Quarantine tag of a worker's pages; never equal to RECOVERY_TAG."""
    return f"worker:{holder}"


class PoolManager:
    """The pools of this node and their PageTables. Thread safe."""

    def __init__(self, budgets: Budgets, page_size: int):
        self.budgets = budgets
        self.page_size = page_size
        self._lock = threading.RLock()
        self._tables: dict[str, PageTable] = {}
        # Pools that give credit; an adopted pool (until released) or a
        # disabled one does not
        self._creditable: set[str] = set()
        self._epoch_seq: int | None = None
        self._counter = 0
        self._credit_stopped: str | None = None
        # Credited bytes per holder (worker incarnation), and the cap of
        # each holder whose ATTACH sized it
        self._held: collections.Counter[str] = collections.Counter()
        self._credit_max: dict[str, int] = {}
        # Holders below their fair share, short by the node-total budget:
        # bytes still owed and when they last asked
        self._waiting: dict[str, tuple[int, float]] = {}
        # When each holder was last asked to return credit
        self._asked: dict[str, float] = {}
        # (tag, generation): a page given before that generation is
        # quarantined under tag instead of freed (see hold_frees)
        self._hold: tuple[str, int] | None = None
        # How many times each transition ran, for tests and metrics
        self.transitions: Transitions = collections.Counter()

    # ------------------------------------------------------------------ pools

    def add_pool(
        self,
        info: PoolInfo,
        *,
        creditable: bool = True,
        state: PageState = PageState.FREE,
    ) -> None:
        """Add a pool: FREE if made here, QUARANTINED if adopted."""
        if state not in (PageState.FREE, PageState.QUARANTINED):
            raise ValueError(f"a pool starts FREE or QUARANTINED, not {state}")
        if info.page_size != self.page_size:
            raise ValueError(
                f"pool {info.pool_id}: page_size {info.page_size} is not the "
                f"node's {self.page_size}"
            )
        with self._lock:
            if info.pool_id in self._tables:
                raise ValueError(f"pool {info.pool_id} added twice")
            self._tables[info.pool_id] = PageTable(info, state, self.transitions)
            if creditable:
                self._creditable.add(info.pool_id)

    def pool_info(self, pool_id: str) -> PoolInfo | None:
        with self._lock:
            table = self._tables.get(pool_id)
            return table.info if table else None

    def creditable(self, pool_id: str) -> bool:
        with self._lock:
            return pool_id in self._creditable

    def disable_pool(self, pool_id: str) -> bool:
        """No more credit from a pool MaruServer no longer accepts (KEPT INVALID);
        whether it was enabled."""
        with self._lock:
            if pool_id not in self._creditable:
                return False
            self._creditable.discard(pool_id)
            return True

    def enable_pool(self, pool_id: str) -> None:
        """Credit from a pool from now on (an adopted pool once released)."""
        with self._lock:
            if pool_id in self._tables:
                self._creditable.add(pool_id)

    def pool_ids(self) -> list[str]:
        with self._lock:
            return list(self._tables)

    # ------------------------------------------------------------------ epoch

    def set_epoch_seq(self, epoch_seq: int) -> None:
        """The epoch_seq MaruServer gave in NODE_OPEN; credit starts with it."""
        make_generation(epoch_seq, 0)  # range check
        with self._lock:
            if self._epoch_seq is not None and self._epoch_seq != epoch_seq:
                raise InvalidTransitionError(
                    f"epoch_seq already {self._epoch_seq}; changing it is not supported"
                )
            self._epoch_seq = epoch_seq

    @property
    def epoch_seq(self) -> int | None:
        return self._epoch_seq

    def hold_frees(self, tag: str | None) -> None:
        """From now on, a page given before this call that would become FREE
        is QUARANTINED under ``tag`` instead: who still reads it is not known.
        Releasing a quarantine is not held. None ends the hold."""
        with self._lock:
            if tag is None:
                self._hold = None
                return
            if self._epoch_seq is None:
                raise InvalidTransitionError("no hold before the epoch_seq")
            self._hold = (tag, make_generation(self._epoch_seq, self._counter))

    def stop_credit(self, reason: str) -> None:
        """No new credit from now on (safe state, exhausted counter)."""
        with self._lock:
            if self._credit_stopped is None:
                self._credit_stopped = reason
                logger.warning("New credit stopped: %s", reason)

    @property
    def credit_stopped(self) -> str | None:
        return self._credit_stopped

    def _next_generation(self) -> int:
        """Caller holds the lock and checked the counter is not exhausted."""
        if self._epoch_seq is None:
            raise InvalidTransitionError("no page leaves FREE before the epoch_seq")
        generation = make_generation(self._epoch_seq, self._counter)
        self._counter += 1
        return generation

    # ----------------------------------------------------------------- credit

    def credit(
        self, holder: str, want_bytes: int, kind: PoolKind | None = None
    ) -> list[Location]:
        """FREE pages for ``holder`` within every budget; empty when
        credit is stopped or the commit-pending bytes reached their cap."""
        if want_bytes <= 0:
            return []
        with self._lock:
            if self._credit_stopped is not None or self._epoch_seq is None:
                return []
            table = self._credit_pool(kind)
            if table is None:
                return []
            if self._bytes(PageState.PENDING) >= self.budgets.commit_pending_max_bytes:
                return []
            ps = table.page_size
            # The transfer reserve is the tier's, over all its pools
            reserve = self.budgets.transfer_reserve_bytes.get(table.info.kind.value, 0)
            tier_free = sum(
                t.bytes[PageState.FREE]
                for pool_id, t in self._tables.items()
                if t.info.kind == table.info.kind and pool_id in self._creditable
            )
            now = time.monotonic()
            self._waiting = {
                h: w for h, w in self._waiting.items() if now - w[1] < CREDIT_WAIT_S
            }
            # Room owed to other waiting holders is not given away
            owed = sum(w[0] for h, w in self._waiting.items() if h != holder)
            uncapped = min(
                math.ceil(want_bytes / ps),
                (
                    self._credit_max.get(holder, self.budgets.credit_max_bytes)
                    - self._held[holder]
                )
                // ps,
                (tier_free - reserve) // ps,
                table.free_count(),
            )
            count = min(
                uncapped,
                (
                    self.budgets.credit_total_max_bytes
                    - self._bytes(PageState.CREDITED)
                    - owed
                )
                // ps,
            )
            granted = max(count, 0)
            self._note_waiting(
                holder, (uncapped - granted) * ps, self._held[holder] + granted * ps, ps
            )
            if count <= 0:
                return []
            left = GENERATION_COUNTER_MAX - self._counter + 1
            if count > left:
                count = left
                if count <= 0:
                    self._credit_stopped = (
                        "the generation counter is exhausted; restart the "
                        "StorageService"
                    )
                    logger.error("New credit stopped: %s", self._credit_stopped)
                    return []
            out = []
            for _ in range(count):
                page = table.take(PageState.CREDITED, self._next_generation())
                page.holder = holder
                self._held[holder] += ps
                out.append(table.location(page))
            return out

    def _fair_share(self, holder: str, ps: int) -> int:
        """The node-total budget split among holders and waiting holders."""
        holders = {h for h, b in self._held.items() if b > 0}
        holders.update(self._waiting, (holder,))
        return max(ps, self.budgets.credit_total_max_bytes // len(holders) // ps * ps)

    def _note_waiting(self, holder: str, short: int, held: int, ps: int) -> None:
        """Caller holds the lock. ``holder``, holding ``held`` bytes, got
        ``short`` bytes less than it asked for (beyond its own limits)."""
        owed = min(short, self._fair_share(holder, ps) - held)
        if owed > 0:
            self._waiting[holder] = (owed, time.monotonic())
        else:
            self._waiting.pop(holder, None)

    def credit_asks(self, holder: str) -> list[tuple[str, int]]:
        """Holders above their fair share to ask for credit back while
        ``holder`` waits, largest first; each is asked once per CREDIT_ASK_S.
        Credit only comes back by their CREDIT_RETURN."""
        with self._lock:
            waiting = self._waiting.get(holder)
            if waiting is None:
                return []
            need = waiting[0]
            ps = self.page_size
            fair = self._fair_share(holder, ps)
            now = time.monotonic()
            asks = []
            for other, held in self._held.most_common():
                if need <= 0 or held <= fair:
                    break
                if other == holder:
                    continue
                ask = min(held - fair, math.ceil(need / ps) * ps)
                need -= ask
                if now - self._asked.get(other, -CREDIT_ASK_S) >= CREDIT_ASK_S:
                    self._asked[other] = now
                    asks.append((other, ask))
            return asks

    def _credit_pool(self, kind: PoolKind | None) -> PageTable | None:
        """The pool credit comes from: the creditable one of ``kind`` with
        the most FREE pages (the first of them on a tie)."""
        best = None
        for pool_id, table in self._tables.items():
            if pool_id in self._creditable and (
                kind is None or table.info.kind == kind
            ):
                if best is None or table.free_count() > best.free_count():
                    best = table
        return best

    def set_credit_max(self, holder: str, nbytes: int) -> None:
        """``holder``'s own credit cap (the default otherwise)."""
        with self._lock:
            self._credit_max[holder] = nbytes

    def held_bytes(self, holder: str) -> int:
        with self._lock:
            return self._held[holder]

    def forget_holder(self, holder: str) -> bool:
        """Drop a holder's records; False (nothing dropped) while it holds credit."""
        with self._lock:
            if self._held[holder]:
                return False
            del self._held[holder]
            self._credit_max.pop(holder, None)
            self._waiting.pop(holder, None)
            self._asked.pop(holder, None)
            return True

    # ------------------------------------------------- worker-named transitions

    def check_credit(
        self, holder: str, credit: Location, *, returning: bool = False
    ) -> tuple[Page | None, str]:
        """The page ``credit`` names if ``holder`` holds it, else the reason.
        Credit of a disabled pool can still come back (``returning``), not be
        written."""
        table = self._tables.get(credit.pool_id)
        if table is None or (credit.pool_id not in self._creditable and not returning):
            return None, f"pool {credit.pool_id!r} is not a pool of this node"
        if generation_epoch_seq(credit.generation) != self._epoch_seq:
            return None, "the credit comes from another StorageService run"
        if credit.kind != table.info.kind:
            return None, f"kind {credit.kind} is not the pool's {table.info.kind}"
        page = table.at(credit.offset)
        if page is None:
            return None, f"offset {credit.offset} is not a page of the pool"
        if credit.length != table.page_size:
            return None, f"length {credit.length} is not the page size"
        if page.generation != credit.generation:
            return None, "the page is at another generation"
        if page.allocation_id != credit.allocation_id:
            return None, "the allocation id does not match"
        if page.state != PageState.CREDITED:
            return None, f"the page is {page.state}, not credited"
        if page.holder != holder:
            return None, "the page is credited to another worker"
        return page, ""

    def write_done(
        self, holder: str, credit: Location, key: Key, logical_bytes: int
    ) -> tuple[PageRef | None, Location | None, str]:
        """CREDITED -> PENDING; the location to commit has length=logical_bytes."""
        with self._lock:
            page, reason = self.check_credit(holder, credit)
            if page is None:
                return None, None, reason
            if not 0 < logical_bytes <= credit.length:
                return (
                    None,
                    None,
                    (f"logical_bytes {logical_bytes} is not within 1..{credit.length}"),
                )
            table = self._tables[credit.pool_id]
            table.move(page, PageState.CREDITED, PageState.PENDING)
            self._held[holder] -= table.page_size
            page.holder = None
            page.key = key
            page.location = replace(credit, length=logical_bytes)
            return table.ref(page), page.location, ""

    def abort(self, holder: str, credit: Location) -> tuple[bool, str]:
        """CREDITED -> FREE: the worker drained its GPU work on the page."""
        with self._lock:
            page, reason = self.check_credit(holder, credit, returning=True)
            if page is None:
                return False, reason
            self._free_credited(page, credit.pool_id, holder)
            return True, ""

    def credit_return(self, holder: str, credit: Location) -> tuple[bool, str]:
        """CREDITED -> FREE. CREDIT_RETURN has no op, so a credit already back is
        OK again (no change)."""
        with self._lock:
            page, reason = self.check_credit(holder, credit, returning=True)
            if page is not None:
                self._free_credited(page, credit.pool_id, holder)
                return True, ""
            table = self._tables.get(credit.pool_id)
            if (
                table is not None
                and generation_epoch_seq(credit.generation) == self._epoch_seq
            ):
                current = table.at(credit.offset)
                if current is not None and (
                    (
                        current.generation == credit.generation
                        and current.allocation_id == credit.allocation_id
                        and current.state == PageState.FREE
                    )
                    or current.generation > credit.generation
                ):
                    return True, "already returned"
            return False, reason

    def _free_credited(self, page: Page, pool_id: str, holder: str) -> None:
        """Not held by hold_frees: no reader can have a credited page."""
        table = self._tables[pool_id]
        table.free(page, PageState.CREDITED)
        self._held[holder] -= table.page_size

    # ------------------------------------------------ service-side transitions

    def commit_added(self, ref: PageRef) -> Location:
        """PENDING -> READY: MaruServer added the location."""
        with self._lock:
            table, page = self._page(ref, PageState.PENDING)
            if page.location is None:
                raise InvalidTransitionError(f"PENDING page {ref} has no location")
            table.move(page, PageState.PENDING, PageState.READY)
            return page.location

    def commit_freed(self, ref: PageRef) -> None:
        """PENDING -> FREE: DUPLICATE or REJECTED."""
        with self._lock:
            table, page = self._page(ref, PageState.PENDING)
            self._to_free(table, page, PageState.PENDING)

    def retired(self, ref: PageRef) -> None:
        """READY -> RETIRING: MaruServer removed the location (RETIRED)."""
        with self._lock:
            table, page = self._page(ref, PageState.READY)
            table.move(page, PageState.READY, PageState.RETIRING)

    def reclaim(self, ref: PageRef) -> None:
        """RETIRING -> FREE: local accesses and transfers on it ended."""
        with self._lock:
            table, page = self._page(ref, PageState.RETIRING)
            self._to_free(table, page, PageState.RETIRING)

    def reconcile(self, ref: PageRef, key: Key, location: Location) -> PageRef:
        """READY page takes the Catalog's occupant (KEPT MOVED): its key,
        allocation and generation. Returns the page at its new generation."""
        with self._lock:
            table, page = self._page(ref, PageState.READY)
            if (
                location.pool_id != ref.pool_id
                or location.offset != page.index * table.page_size
                or not 0 < location.length <= table.page_size
                or location.kind != table.info.kind
            ):
                raise InvalidTransitionError(f"{location} is not page {page.index}")
            other = table.index_of(location.allocation_id)
            if other is not None and other != page.index:
                raise InvalidTransitionError(
                    f"allocation {location.allocation_id} is page {other} already"
                )
            table.bind(page, location.allocation_id, location.generation)
            page.key = key
            page.location = location
            return table.ref(page)

    def quarantine_holder(self, holder: str) -> list[PageRef]:
        """CREDITED -> QUARANTINED for an ended holder."""
        with self._lock:
            out = []
            for table in self._tables.values():
                for page in table.pages:
                    if page.state == PageState.CREDITED and page.holder == holder:
                        table.quarantine(page, PageState.CREDITED, holder_tag(holder))
                        self._held[holder] -= table.page_size
                        out.append(table.ref(page))
            if self._held[holder] == 0:
                del self._held[holder]
            self._credit_max.pop(holder, None)
            self._waiting.pop(holder, None)
            self._asked.pop(holder, None)
            return out

    def quarantine_credited(
        self, tag_of: Callable[[str], str], holders: set[str]
    ) -> dict[str, int]:
        """CREDITED -> QUARANTINED for each of ``holders``, under
        ``tag_of(holder)``; returns the page count by holder."""
        with self._lock:
            out: dict[str, int] = {}
            for table in self._tables.values():
                for page in table.pages:
                    holder = page.holder
                    if (
                        page.state != PageState.CREDITED
                        or holder is None
                        or holder not in holders
                    ):
                        continue
                    table.quarantine(page, PageState.CREDITED, tag_of(holder))
                    self._held[holder] -= table.page_size
                    out[holder] = out.get(holder, 0) + 1
            for holder in out:
                if self._held[holder] == 0:
                    del self._held[holder]
            return out

    def release_quarantine(self, tag: str, pool_id: str | None = None) -> int:
        """QUARANTINED -> FREE for pages waiting on ``tag``; returns the count."""
        with self._lock:
            freed = 0
            for table in self._tables.values():
                if pool_id is not None and table.pool_id != pool_id:
                    continue
                for page in table.pages:
                    if page.state == PageState.QUARANTINED and page.tag == tag:
                        self._to_free(table, page, PageState.QUARANTINED)
                        freed += 1
            return freed

    def recovered(
        self, pool_id: str, items: Iterable[tuple[Key, Location]]
    ) -> list[PageRef]:
        """QUARANTINED -> READY for pages NODE_RECOVER_LIST lists."""
        with self._lock:
            table = self._tables.get(pool_id)
            if table is None:
                raise InvalidTransitionError(f"no pool {pool_id}")
            out = []
            for key, location in items:
                page = table.at(location.offset)
                if location.pool_id != pool_id or page is None:
                    raise InvalidTransitionError(
                        f"{location} is not a page of {pool_id}"
                    )
                if page.state != PageState.QUARANTINED or page.tag != RECOVERY_TAG:
                    raise InvalidTransitionError(f"page {page.index} listed twice")
                table.move(page, PageState.QUARANTINED, PageState.READY)
                page.tag = None
                page.key = key
                page.location = location
                table.bind(page, location.allocation_id, location.generation)
                out.append(table.ref(page))
            return out

    def drop_ready(self, ref: PageRef) -> None:
        """READY -> FREE for a page not in the Catalog (held: QUARANTINED)."""
        with self._lock:
            table, page = self._page(ref, PageState.READY)
            self._to_free(table, page, PageState.READY)

    def ready_pages(self) -> list[tuple[PageRef, Key, Location]]:
        """Every READY page with its key and location."""
        with self._lock:
            out = []
            for table in self._tables.values():
                for page in table.pages:
                    if (
                        page.state == PageState.READY
                        and page.key is not None
                        and page.location is not None
                    ):
                        out.append((table.ref(page), page.key, page.location))
            return out

    def reserve_transfer(self, pool_id: str, count: int) -> list[PageRef]:
        """FREE -> TRANSFER for the service's own transfers; may use the reserve."""
        with self._lock:
            if self._epoch_seq is None:
                return []
            table = self._tables[pool_id]
            count = min(
                count, table.free_count(), GENERATION_COUNTER_MAX - self._counter + 1
            )
            out = []
            for _ in range(max(0, count)):
                page = table.take(PageState.TRANSFER, self._next_generation())
                out.append(table.ref(page))
            return out

    def transfer_location(self, ref: PageRef) -> Location:
        with self._lock:
            table, page = self._page(ref, PageState.TRANSFER)
            return table.location(page)

    def transfer_done(self, ref: PageRef, key: Key, logical_bytes: int) -> None:
        """TRANSFER -> PENDING."""
        with self._lock:
            table, page = self._page(ref, PageState.TRANSFER)
            if not 0 < logical_bytes <= table.page_size:
                raise InvalidTransitionError(
                    f"logical_bytes {logical_bytes} out of range"
                )
            location = replace(table.location(page), length=logical_bytes)
            table.move(page, PageState.TRANSFER, PageState.PENDING)
            page.key = key
            page.location = location

    def transfer_failed(self, ref: PageRef) -> None:
        """TRANSFER -> FREE, once the failed transfer has ended."""
        with self._lock:
            table, page = self._page(ref, PageState.TRANSFER)
            self._to_free(table, page, PageState.TRANSFER)

    # ---------------------------------------------------------------- queries

    def page(self, ref: PageRef) -> Page | None:
        """A copy of the page if it is still at ``ref``'s generation."""
        with self._lock:
            table = self._tables.get(ref.pool_id)
            if table is None or not 0 <= ref.index < len(table.pages):
                return None
            page = table.pages[ref.index]
            if page.generation != ref.generation:
                return None
            return replace(page)

    def find(self, location: Location) -> PageRef | None:
        """The non-FREE page at exactly this location."""
        with self._lock:
            table = self._tables.get(location.pool_id)
            return table.find(location) if table else None

    def find_allocation(
        self, pool_id: str, allocation_id: str, generation: int
    ) -> PageRef | None:
        """The page holding this allocation at this generation, if any."""
        with self._lock:
            table = self._tables.get(pool_id)
            return table.find_allocation(allocation_id, generation) if table else None

    def pages_in(self, pool_id: str, state: PageState) -> list[PageRef]:
        with self._lock:
            return self._tables[pool_id].pages_in(state)

    def bytes_in(self, state: PageState, pool_id: str | None = None) -> int:
        with self._lock:
            if pool_id is not None:
                return self._tables[pool_id].bytes[state]
            return self._bytes(state)

    def usage(self) -> list[PoolUsage]:
        with self._lock:
            return [
                PoolUsage(
                    pool_id=table.pool_id,
                    capacity=table.info.capacity,
                    free=table.bytes[PageState.FREE],
                    credited=table.bytes[PageState.CREDITED],
                    transfer=table.bytes[PageState.TRANSFER],
                    pending=table.bytes[PageState.PENDING],
                    ready=table.bytes[PageState.READY],
                    retiring=table.bytes[PageState.RETIRING],
                    quarantined=table.bytes[PageState.QUARANTINED],
                )
                for table in self._tables.values()
            ]

    def check(self) -> None:
        """Assert the invariants of every pool and the credit per holder."""
        with self._lock:
            held: collections.Counter[str] = collections.Counter()
            for table in self._tables.values():
                held += table.check()
            assert +held == +self._held, (held, self._held)

    # ---------------------------------------------------------------- helpers

    def _bytes(self, state: PageState) -> int:
        return sum(table.bytes[state] for table in self._tables.values())

    def _page(self, ref: PageRef, state: PageState) -> tuple[PageTable, Page]:
        table = self._tables.get(ref.pool_id)
        if table is None:
            raise InvalidTransitionError(f"no page {ref}")
        return table, table.get(ref, state)

    def _to_free(self, table: PageTable, page: Page, src: PageState) -> None:
        """``src`` -> FREE, or QUARANTINED while hold_frees holds a page this old."""
        if (
            self._hold is not None
            and src != PageState.QUARANTINED
            and page.generation < self._hold[1]
        ):
            table.quarantine(page, src, self._hold[0])
            return
        table.free(page, src)

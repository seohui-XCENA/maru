# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PoolManager: page states, holders, credit budgets and bytes per state.

A page gets a new generation each time it leaves FREE. Worker requests that do
not match the ledger are refused; a wrong transition asked by the service is a
bug (InvalidTransitionError).
"""

import collections
import logging
import math
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from enum import StrEnum, unique

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

logger = logging.getLogger(__name__)

# A holder short of credit by the node-total budget keeps its claim this long
# (it asks again meanwhile); a holder asked to return credit is asked again
# only after CREDIT_ASK_S
CREDIT_WAIT_S = 1.0
CREDIT_ASK_S = 0.5


@unique
class PageState(StrEnum):
    FREE = "FREE"
    CREDITED = "CREDITED"
    TRANSFER = "TRANSFER"
    PENDING = "PENDING"
    READY = "READY"
    RETIRING = "RETIRING"
    QUARANTINED = "QUARANTINED"


# Quarantine tags: the restart drain of adopted pools, or one ended worker
RECOVERY_TAG = "recovery"


def holder_tag(holder: str) -> str:
    """Quarantine tag of a worker's pages; never equal to RECOVERY_TAG."""
    return f"worker:{holder}"


class InvalidTransitionError(RuntimeError):
    """The service asked for a page transition the ledger does not allow."""


@dataclass(frozen=True)
class PageRef:
    """One page at one generation."""

    pool_id: str
    index: int
    generation: int


@dataclass
class Page:
    index: int
    state: PageState = PageState.FREE
    allocation_id: str = ""
    # Last given; kept while FREE
    generation: int = 0
    # CREDITED: the worker incarnation writing it
    holder: str | None = None
    # PENDING, READY, RETIRING
    key: Key | None = None
    location: Location | None = None
    # QUARANTINED: holder_tag(...) or RECOVERY_TAG
    tag: str | None = None


class _Pool:
    def __init__(self, info: PoolInfo, creditable: bool, state: PageState):
        if info.page_size <= 0 or info.capacity % info.page_size:
            raise ValueError(f"pool {info.pool_id}: capacity is not whole pages")
        self.info = info
        self.creditable = creditable
        self.page_size = info.page_size
        count = info.capacity // info.page_size
        self.pages = [Page(i, state=state) for i in range(count)]
        if state == PageState.QUARANTINED:
            for page in self.pages:
                page.tag = RECOVERY_TAG
        self.free: collections.deque[int] = collections.deque(
            range(count) if state == PageState.FREE else ()
        )
        self.bytes = dict.fromkeys(PageState, 0)
        self.bytes[state] = info.capacity
        # allocation_id -> page index, for pages out of FREE
        self.by_allocation: dict[str, int] = {}


class PoolManager:
    """The page ledger of every pool of this node. Thread safe."""

    def __init__(self, budgets: Budgets, page_size: int):
        self.budgets = budgets
        self.page_size = page_size
        self._lock = threading.RLock()
        self._pools: dict[str, _Pool] = {}
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
        self.transitions: collections.Counter[tuple[PageState, PageState]] = (
            collections.Counter()
        )

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
            if info.pool_id in self._pools:
                raise ValueError(f"pool {info.pool_id} added twice")
            self._pools[info.pool_id] = _Pool(info, creditable, state)

    def pool_info(self, pool_id: str) -> PoolInfo | None:
        with self._lock:
            pool = self._pools.get(pool_id)
            return pool.info if pool else None

    def creditable(self, pool_id: str) -> bool:
        with self._lock:
            pool = self._pools.get(pool_id)
            return pool is not None and pool.creditable

    def disable_pool(self, pool_id: str) -> bool:
        """No more credit from a pool MaruServer no longer accepts (KEPT INVALID);
        whether it was enabled."""
        with self._lock:
            pool = self._pools.get(pool_id)
            if pool is None or not pool.creditable:
                return False
            pool.creditable = False
            return True

    def pool_ids(self) -> list[str]:
        with self._lock:
            return list(self._pools)

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
            pool = self._credit_pool(kind)
            if pool is None:
                return []
            if self._bytes(PageState.PENDING) >= self.budgets.commit_pending_max_bytes:
                return []
            ps = pool.page_size
            reserve = self.budgets.transfer_reserve_bytes.get(pool.info.kind.value, 0)
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
                (pool.bytes[PageState.FREE] - reserve) // ps,
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
                page = pool.pages[pool.free.popleft()]
                self._leave_free(pool, page)
                self._move(pool, page, PageState.FREE, PageState.CREDITED)
                page.holder = holder
                self._held[holder] += ps
                out.append(self._credit_location(pool, page))
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

    def _credit_pool(self, kind: PoolKind | None) -> _Pool | None:
        """The pool credit comes from: the first creditable one of ``kind``.
        Choosing among several pools of a tier belongs here."""
        for pool in self._pools.values():
            if pool.creditable and (kind is None or pool.info.kind == kind):
                return pool
        return None

    def _credit_location(self, pool: _Pool, page: Page) -> Location:
        return Location(
            pool_id=pool.info.pool_id,
            allocation_id=page.allocation_id,
            generation=page.generation,
            offset=page.index * pool.page_size,
            length=pool.page_size,
            kind=pool.info.kind,
        )

    def _leave_free(self, pool: _Pool, page: Page) -> None:
        if self._epoch_seq is None:
            raise InvalidTransitionError("no page leaves FREE before the epoch_seq")
        page.allocation_id = uuid.uuid4().hex
        page.generation = make_generation(self._epoch_seq, self._counter)
        self._counter += 1
        pool.by_allocation[page.allocation_id] = page.index

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

    def check_credit(self, holder: str, credit: Location) -> tuple[Page | None, str]:
        """The page ``credit`` names if ``holder`` holds it, else the reason."""
        pool = self._pools.get(credit.pool_id)
        if pool is None or not pool.creditable:
            return None, f"pool {credit.pool_id!r} is not a pool of this node"
        if generation_epoch_seq(credit.generation) != self._epoch_seq:
            return None, "the credit comes from another StorageService run"
        if credit.kind != pool.info.kind:
            return None, f"kind {credit.kind} is not the pool's {pool.info.kind}"
        if (
            credit.offset < 0
            or credit.offset % pool.page_size
            or credit.offset >= pool.info.capacity
        ):
            return None, f"offset {credit.offset} is not a page of the pool"
        if credit.length != pool.page_size:
            return None, f"length {credit.length} is not the page size"
        page = pool.pages[credit.offset // pool.page_size]
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
            pool = self._pools[credit.pool_id]
            self._move(pool, page, PageState.CREDITED, PageState.PENDING)
            self._held[holder] -= pool.page_size
            page.holder = None
            page.key = key
            page.location = replace(credit, length=logical_bytes)
            return (
                PageRef(credit.pool_id, page.index, page.generation),
                page.location,
                "",
            )

    def abort(self, holder: str, credit: Location) -> tuple[bool, str]:
        """CREDITED -> FREE: the worker drained its GPU work on the page."""
        with self._lock:
            page, reason = self.check_credit(holder, credit)
            if page is None:
                return False, reason
            self._free_credited(page, credit.pool_id, holder)
            return True, ""

    def credit_return(self, holder: str, credit: Location) -> tuple[bool, str]:
        """CREDITED -> FREE. CREDIT_RETURN has no op, so a credit already back is
        OK again (no change)."""
        with self._lock:
            page, reason = self.check_credit(holder, credit)
            if page is not None:
                self._free_credited(page, credit.pool_id, holder)
                return True, ""
            pool = self._pools.get(credit.pool_id)
            if (
                pool is not None
                and pool.creditable
                and generation_epoch_seq(credit.generation) == self._epoch_seq
                and credit.offset >= 0
                and credit.offset % pool.page_size == 0
                and credit.offset < pool.info.capacity
            ):
                current = pool.pages[credit.offset // pool.page_size]
                if (
                    current.generation == credit.generation
                    and current.allocation_id == credit.allocation_id
                    and current.state == PageState.FREE
                ) or current.generation > credit.generation:
                    return True, "already returned"
            return False, reason

    def _free_credited(self, page: Page, pool_id: str, holder: str) -> None:
        pool = self._pools[pool_id]
        self._move(pool, page, PageState.CREDITED, PageState.FREE)
        self._held[holder] -= pool.page_size
        page.holder = None
        pool.by_allocation.pop(page.allocation_id, None)
        pool.free.append(page.index)

    # ------------------------------------------------ service-side transitions

    def commit_added(self, ref: PageRef) -> Location:
        """PENDING -> READY: MaruServer added the location."""
        with self._lock:
            pool, page = self._page(ref, PageState.PENDING)
            if page.location is None:
                raise InvalidTransitionError(f"PENDING page {ref} has no location")
            self._move(pool, page, PageState.PENDING, PageState.READY)
            return page.location

    def commit_freed(self, ref: PageRef) -> None:
        """PENDING -> FREE: DUPLICATE or REJECTED."""
        with self._lock:
            pool, page = self._page(ref, PageState.PENDING)
            self._to_free(pool, page, PageState.PENDING)

    def retired(self, ref: PageRef) -> None:
        """READY -> RETIRING: MaruServer removed the location (RETIRED)."""
        with self._lock:
            pool, page = self._page(ref, PageState.READY)
            self._move(pool, page, PageState.READY, PageState.RETIRING)

    def reclaim(self, ref: PageRef) -> None:
        """RETIRING -> FREE: local accesses and transfers on it ended."""
        with self._lock:
            pool, page = self._page(ref, PageState.RETIRING)
            self._to_free(pool, page, PageState.RETIRING)

    def reconcile(self, ref: PageRef, key: Key, location: Location) -> PageRef:
        """READY page takes the Catalog's occupant (KEPT MOVED): its key,
        allocation and generation. Returns the page at its new generation."""
        with self._lock:
            pool, page = self._page(ref, PageState.READY)
            if (
                location.pool_id != ref.pool_id
                or location.offset != page.index * pool.page_size
                or not 0 < location.length <= pool.page_size
                or location.kind != pool.info.kind
            ):
                raise InvalidTransitionError(f"{location} is not page {page.index}")
            other = pool.by_allocation.get(location.allocation_id)
            if other is not None and other != page.index:
                raise InvalidTransitionError(
                    f"allocation {location.allocation_id} is page {other} already"
                )
            pool.by_allocation.pop(page.allocation_id, None)
            page.allocation_id = location.allocation_id
            page.generation = location.generation
            page.key = key
            page.location = location
            pool.by_allocation[page.allocation_id] = page.index
            return PageRef(ref.pool_id, page.index, page.generation)

    def quarantine_holder(self, holder: str) -> list[PageRef]:
        """CREDITED -> QUARANTINED for an ended holder."""
        with self._lock:
            out = []
            for pool in self._pools.values():
                for page in pool.pages:
                    if page.state == PageState.CREDITED and page.holder == holder:
                        self._move(
                            pool, page, PageState.CREDITED, PageState.QUARANTINED
                        )
                        self._held[holder] -= pool.page_size
                        page.holder = None
                        page.tag = holder_tag(holder)
                        out.append(
                            PageRef(pool.info.pool_id, page.index, page.generation)
                        )
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
            for pool in self._pools.values():
                for page in pool.pages:
                    holder = page.holder
                    if (
                        page.state != PageState.CREDITED
                        or holder is None
                        or holder not in holders
                    ):
                        continue
                    self._move(pool, page, PageState.CREDITED, PageState.QUARANTINED)
                    self._held[holder] -= pool.page_size
                    page.holder = None
                    page.tag = tag_of(holder)
                    out[holder] = out.get(holder, 0) + 1
            for holder in out:
                if self._held[holder] == 0:
                    del self._held[holder]
            return out

    def release_quarantine(self, tag: str, pool_id: str | None = None) -> int:
        """QUARANTINED -> FREE for pages waiting on ``tag``; returns the count."""
        with self._lock:
            freed = 0
            for pool in self._pools.values():
                if pool_id is not None and pool.info.pool_id != pool_id:
                    continue
                for page in pool.pages:
                    if page.state == PageState.QUARANTINED and page.tag == tag:
                        self._to_free(pool, page, PageState.QUARANTINED)
                        freed += 1
            return freed

    def recovered(
        self, pool_id: str, items: Iterable[tuple[Key, Location]]
    ) -> list[PageRef]:
        """QUARANTINED -> READY for pages NODE_RECOVER_LIST lists."""
        with self._lock:
            pool = self._pools.get(pool_id)
            if pool is None:
                raise InvalidTransitionError(f"no pool {pool_id}")
            out = []
            for key, location in items:
                if (
                    location.pool_id != pool_id
                    or location.offset < 0
                    or location.offset % pool.page_size
                    or location.offset >= pool.info.capacity
                ):
                    raise InvalidTransitionError(
                        f"{location} is not a page of {pool_id}"
                    )
                page = pool.pages[location.offset // pool.page_size]
                if page.state != PageState.QUARANTINED or page.tag != RECOVERY_TAG:
                    raise InvalidTransitionError(f"page {page.index} listed twice")
                self._move(pool, page, PageState.QUARANTINED, PageState.READY)
                page.tag = None
                page.key = key
                page.location = location
                page.allocation_id = location.allocation_id
                page.generation = location.generation
                pool.by_allocation[page.allocation_id] = page.index
                out.append(PageRef(pool_id, page.index, page.generation))
            return out

    def drop_ready(self, ref: PageRef) -> None:
        """READY -> FREE for a page not in the Catalog (held: QUARANTINED)."""
        with self._lock:
            pool, page = self._page(ref, PageState.READY)
            self._to_free(pool, page, PageState.READY)

    def ready_pages(self) -> list[tuple[PageRef, Key, Location]]:
        """Every READY page with its key and location."""
        with self._lock:
            out = []
            for pool in self._pools.values():
                for page in pool.pages:
                    if (
                        page.state == PageState.READY
                        and page.key is not None
                        and page.location is not None
                    ):
                        ref = PageRef(pool.info.pool_id, page.index, page.generation)
                        out.append((ref, page.key, page.location))
            return out

    def reserve_transfer(self, pool_id: str, count: int) -> list[PageRef]:
        """FREE -> TRANSFER for the service's own transfers; may use the reserve."""
        with self._lock:
            if self._epoch_seq is None:
                return []
            pool = self._pools[pool_id]
            count = min(
                count, len(pool.free), GENERATION_COUNTER_MAX - self._counter + 1
            )
            out = []
            for _ in range(max(0, count)):
                page = pool.pages[pool.free.popleft()]
                self._leave_free(pool, page)
                self._move(pool, page, PageState.FREE, PageState.TRANSFER)
                out.append(PageRef(pool_id, page.index, page.generation))
            return out

    def transfer_location(self, ref: PageRef) -> Location:
        with self._lock:
            pool, page = self._page(ref, PageState.TRANSFER)
            return self._credit_location(pool, page)

    def transfer_done(self, ref: PageRef, key: Key, logical_bytes: int) -> None:
        """TRANSFER -> PENDING."""
        with self._lock:
            pool, page = self._page(ref, PageState.TRANSFER)
            if not 0 < logical_bytes <= pool.page_size:
                raise InvalidTransitionError(
                    f"logical_bytes {logical_bytes} out of range"
                )
            location = replace(self._credit_location(pool, page), length=logical_bytes)
            self._move(pool, page, PageState.TRANSFER, PageState.PENDING)
            page.key = key
            page.location = location

    def transfer_failed(self, ref: PageRef) -> None:
        """TRANSFER -> FREE, once the failed transfer has ended."""
        with self._lock:
            pool, page = self._page(ref, PageState.TRANSFER)
            self._to_free(pool, page, PageState.TRANSFER)

    # ---------------------------------------------------------------- queries

    def page(self, ref: PageRef) -> Page | None:
        """A copy of the page if it is still at ``ref``'s generation."""
        with self._lock:
            pool = self._pools.get(ref.pool_id)
            if pool is None or not 0 <= ref.index < len(pool.pages):
                return None
            page = pool.pages[ref.index]
            if page.generation != ref.generation:
                return None
            return replace(page)

    def find(self, location: Location) -> PageRef | None:
        """The non-FREE page at exactly this location."""
        with self._lock:
            pool = self._pools.get(location.pool_id)
            if (
                pool is None
                or location.offset < 0
                or location.offset % pool.page_size
                or location.offset >= pool.info.capacity
            ):
                return None
            page = pool.pages[location.offset // pool.page_size]
            if (
                page.state == PageState.FREE
                or page.generation != location.generation
                or page.allocation_id != location.allocation_id
            ):
                return None
            return PageRef(location.pool_id, page.index, page.generation)

    def find_allocation(
        self, pool_id: str, allocation_id: str, generation: int
    ) -> PageRef | None:
        """The page holding this allocation at this generation, if any."""
        with self._lock:
            pool = self._pools.get(pool_id)
            if pool is None:
                return None
            index = pool.by_allocation.get(allocation_id)
            if index is None or pool.pages[index].generation != generation:
                return None
            return PageRef(pool_id, index, generation)

    def pages_in(self, pool_id: str, state: PageState) -> list[PageRef]:
        with self._lock:
            pool = self._pools[pool_id]
            return [
                PageRef(pool_id, p.index, p.generation)
                for p in pool.pages
                if p.state == state
            ]

    def bytes_in(self, state: PageState, pool_id: str | None = None) -> int:
        with self._lock:
            if pool_id is not None:
                return self._pools[pool_id].bytes[state]
            return self._bytes(state)

    def usage(self) -> list[PoolUsage]:
        with self._lock:
            return [
                PoolUsage(
                    pool_id=pool.info.pool_id,
                    capacity=pool.info.capacity,
                    free=pool.bytes[PageState.FREE],
                    credited=pool.bytes[PageState.CREDITED],
                    transfer=pool.bytes[PageState.TRANSFER],
                    pending=pool.bytes[PageState.PENDING],
                    ready=pool.bytes[PageState.READY],
                    retiring=pool.bytes[PageState.RETIRING],
                    quarantined=pool.bytes[PageState.QUARANTINED],
                )
                for pool in self._pools.values()
            ]

    def check(self) -> None:
        """Assert the invariants: per pool the state bytes add up to capacity."""
        with self._lock:
            held: collections.Counter[str] = collections.Counter()
            for pool in self._pools.values():
                counts = collections.Counter(p.state for p in pool.pages)
                total = sum(pool.bytes.values())
                assert total == pool.info.capacity, (pool.info.pool_id, pool.bytes)
                for state in PageState:
                    assert pool.bytes[state] == counts[state] * pool.page_size, (
                        pool.info.pool_id,
                        state,
                    )
                assert sorted(pool.free) == sorted(
                    p.index for p in pool.pages if p.state == PageState.FREE
                ), pool.info.pool_id
                assert pool.by_allocation == {
                    p.allocation_id: p.index
                    for p in pool.pages
                    if p.state != PageState.FREE and p.allocation_id
                }, pool.info.pool_id
                for page in pool.pages:
                    if page.state == PageState.CREDITED:
                        assert page.holder is not None
                        held[page.holder] += pool.page_size
                    else:
                        assert page.holder is None, page
            assert +held == +self._held, (held, self._held)

    # ---------------------------------------------------------------- helpers

    def _bytes(self, state: PageState) -> int:
        return sum(pool.bytes[state] for pool in self._pools.values())

    def _page(self, ref: PageRef, state: PageState) -> tuple[_Pool, Page]:
        pool = self._pools.get(ref.pool_id)
        if pool is None or not 0 <= ref.index < len(pool.pages):
            raise InvalidTransitionError(f"no page {ref}")
        page = pool.pages[ref.index]
        if page.generation != ref.generation or page.state != state:
            raise InvalidTransitionError(
                f"page {ref} is {page.state} at generation {page.generation}, "
                f"expected {state}"
            )
        return pool, page

    def _move(self, pool: _Pool, page: Page, src: PageState, dst: PageState) -> None:
        if page.state != src:
            raise InvalidTransitionError(
                f"page {page.index} is {page.state}, not {src}"
            )
        page.state = dst
        pool.bytes[src] -= pool.page_size
        pool.bytes[dst] += pool.page_size
        self.transitions[(src, dst)] += 1

    def _to_free(self, pool: _Pool, page: Page, src: PageState) -> None:
        if (
            self._hold is not None
            and src != PageState.QUARANTINED
            and page.generation < self._hold[1]
        ):
            self._move(pool, page, src, PageState.QUARANTINED)
            page.key = None
            page.location = None
            page.tag = self._hold[0]
            return
        self._move(pool, page, src, PageState.FREE)
        pool.by_allocation.pop(page.allocation_id, None)
        page.key = None
        page.location = None
        page.tag = None
        pool.free.append(page.index)

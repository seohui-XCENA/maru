# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PlacementPolicy: LRU removal candidates per pool.

A candidate is not an approval: the policy sends no RPC. A tier that demotes
moves its candidates to the tier below; one that does not drops them.
"""

import collections
import threading
import time
from collections.abc import Callable, Iterable

from maru_common.storage_types import AccessInfo, KeptCause

from .pool_manager import PageRef

DEFAULT_KEPT_BACKOFF_S = 1.0


class PlacementPolicy:
    """LRU per pool and the watermark rule. Thread safe."""

    def __init__(
        self,
        high_watermark: float,
        low_watermark: float,
        *,
        kept_backoff_s: float = DEFAULT_KEPT_BACKOFF_S,
        clock: Callable[[], float] = time.monotonic,
    ):
        if not 0 < low_watermark < high_watermark <= 1:
            raise ValueError("need 0 < low_watermark < high_watermark <= 1")
        self.high = high_watermark
        self.low = low_watermark
        self.kept_backoff_s = kept_backoff_s
        self._clock = clock
        self._lock = threading.Lock()
        # Least recently used first
        self._lru: dict[str, collections.OrderedDict[PageRef, None]] = {}
        self._inflight: set[PageRef] = set()
        self._backoff: dict[PageRef, float] = {}
        # Never offered again (KEPT MOVED/INVALID, unknown outcome)
        self._held: set[PageRef] = set()
        self._evicting: dict[str, bool] = {}
        # (high, low) of pools whose tier is not the top one
        self._marks: dict[str, tuple[float, float]] = {}

    def set_watermarks(self, pool_id: str, high: float, low: float) -> None:
        """The watermarks of ``pool_id`` (its tier's) instead of the top tier's."""
        if not 0 < low < high <= 1:
            raise ValueError("need 0 < low_watermark < high_watermark <= 1")
        with self._lock:
            self._marks[pool_id] = (high, low)

    def above_high(self, pool_id: str, *, capacity: int, free_bytes: int) -> bool:
        """The pool uses more than its high watermark."""
        with self._lock:
            high, _ = self._marks.get(pool_id, (self.high, self.low))
        return capacity - free_bytes > high * capacity

    def on_ready(self, ref: PageRef) -> None:
        """A page became READY: most recently used."""
        with self._lock:
            lru = self._lru.setdefault(ref.pool_id, collections.OrderedDict())
            lru[ref] = None
            lru.move_to_end(ref)

    def on_gone(self, ref: PageRef) -> None:
        """The page left READY for good."""
        with self._lock:
            self._forget(ref)

    def on_access(
        self,
        infos: Iterable[AccessInfo],
        resolve: Callable[[AccessInfo], PageRef | None],
    ) -> int:
        """Move pages MaruServer saw read to the recent end; returns the count."""
        moved = 0
        for info in sorted(infos, key=lambda i: i.last_ts):
            ref = resolve(info)
            if ref is None:
                continue
            with self._lock:
                lru = self._lru.get(ref.pool_id)
                if lru is not None and ref in lru:
                    lru.move_to_end(ref)
                    moved += 1
        return moved

    def candidates(
        self,
        pool_id: str,
        *,
        capacity: int,
        free_bytes: int,
        retiring_bytes: int,
        page_size: int,
        limit: int,
    ) -> list[PageRef]:
        """READY pages to retire, LRU first: from above ``high`` down to ``low``.
        In-flight and RETIRING pages count as leaving."""
        with self._lock:
            high, low = self._marks.get(pool_id, (self.high, self.low))
            used = capacity - free_bytes
            evicting = self._evicting.get(pool_id, False)
            if not evicting and used > high * capacity:
                evicting = True
            if used <= low * capacity:
                evicting = False
            self._evicting[pool_id] = evicting
            if not evicting:
                return []
            inflight = sum(1 for r in self._inflight if r.pool_id == pool_id)
            need = used - retiring_bytes - inflight * page_size - low * capacity
            now = self._clock()
            out: list[PageRef] = []
            for ref in self._lru.get(pool_id, ()):
                if need <= 0 or len(out) >= limit:
                    break
                if ref in self._inflight or ref in self._held:
                    continue
                until = self._backoff.get(ref)
                if until is not None:
                    if until > now:
                        continue
                    del self._backoff[ref]
                out.append(ref)
                need -= page_size
            self._inflight.update(out)
            return out

    def retired(self, ref: PageRef) -> None:
        with self._lock:
            self._forget(ref)

    def kept(self, ref: PageRef, cause: KeptCause) -> None:
        """PINNED: retried after a backoff; MOVED, INVALID: never again."""
        with self._lock:
            self._inflight.discard(ref)
            if cause == KeptCause.PINNED:
                self._backoff[ref] = self._clock() + self.kept_backoff_s
            else:
                self._held.add(ref)

    def hold(self, ref: PageRef) -> None:
        """Never offer the page again."""
        with self._lock:
            self._inflight.discard(ref)
            self._held.add(ref)

    def release(self, refs: Iterable[PageRef], backoff: bool = True) -> None:
        """Not retired; offered again, after a backoff unless ``backoff`` is
        False."""
        with self._lock:
            until = self._clock() + self.kept_backoff_s
            for ref in refs:
                self._inflight.discard(ref)
                if backoff:
                    self._backoff[ref] = until

    def is_held(self, ref: PageRef) -> bool:
        with self._lock:
            return ref in self._held

    def _forget(self, ref: PageRef) -> None:
        lru = self._lru.get(ref.pool_id)
        if lru is not None:
            lru.pop(ref, None)
        self._inflight.discard(ref)
        self._backoff.pop(ref, None)
        self._held.discard(ref)

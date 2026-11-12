# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""TransferEngine: copies page bytes between the node's mapped pools.

Copies run on the engine's own thread, outside every metadata lock, paced to
a bandwidth budget. A batch is answered once every copy in it has ended, one
result per copy; after stop() what has not started fails. Pages of an L2
tier (SSD) move by that tier's own I/O, to and from the mapped pools.
"""

import collections
import logging
import mmap
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from maru_common.storage_types import Location

from .drivers.io_executor import DEFAULT_TRANSFER_BYTES_PER_S
from .drivers.io_executor import IO_THREADS as IO_THREADS
from .drivers.io_executor import WRITE_AFTER_READS as WRITE_AFTER_READS
from .drivers.io_executor import WRITE_MAX_WAIT_S as WRITE_MAX_WAIT_S
from .drivers.io_executor import IOQueueFullError as IOQueueFullError
from .page_table import PageRef
from .tiers import IOResult, L2Tier, Span

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Copy:
    """``nbytes`` from the start of ``src`` to the start of ``dst``."""

    src: Location
    dst: Location
    nbytes: int


@dataclass(eq=False)
class _Batch:
    copies: list[Copy]
    results: list[bool] = field(default_factory=list)
    done: threading.Event = field(default_factory=threading.Event)


class TransferEngine:
    """Thread safe. The engine owns the mappings it is given and closes them."""

    def __init__(self, bytes_per_s: float = DEFAULT_TRANSFER_BYTES_PER_S):
        if bytes_per_s <= 0:
            raise ValueError(f"bytes_per_s must be positive, got {bytes_per_s}")
        self.bytes_per_s = bytes_per_s
        # Held while a copy reads or writes a mapping
        self._lock = threading.Lock()
        self._maps: dict[str, mmap.mmap] = {}
        # L2 pools: only their tier moves their pages
        self._l2: dict[str, L2Tier] = {}
        # Mappings a tier could not detach: never closed
        self._kept: dict[str, mmap.mmap] = {}
        self._cv = threading.Condition()
        self._queue: collections.deque[_Batch] = collections.deque()
        self._stopped = threading.Event()
        self._thread: threading.Thread | None = None
        # When the budget lets the next copy start
        self._next_at = 0.0
        self.copied_bytes = 0

    def add_pool(self, pool_id: str, mapping: mmap.mmap) -> None:
        with self._lock:
            self._maps[pool_id] = mapping
            for tier in self._tiers():
                tier.attach(pool_id, mapping)

    def add_l2(self, tier: L2Tier) -> None:
        """An L2 tier's pools; stop() drains it, its owner closes it after."""
        with self._lock:
            for info in tier.pool_infos():
                self._l2[info.pool_id] = tier
            for pool_id, mapping in self._maps.items():
                tier.attach(pool_id, mapping)

    def has_pool(self, pool_id: str) -> bool:
        """Whether transfers reach the pool: mapped here, or an L2 pool."""
        with self._lock:
            return pool_id in self._maps or pool_id in self._l2

    def l2_pools(self) -> dict[str, L2Tier]:
        """The L2 tier of each L2 pool."""
        with self._lock:
            return dict(self._l2)

    def _tiers(self) -> list[L2Tier]:
        """Caller holds the lock: each L2 tier once, in the order added."""
        return list({id(t): t for t in self._l2.values()}.values())

    def submit_io(
        self,
        copy: Copy,
        src: PageRef,
        dst: PageRef,
        *,
        callback: Callable[[IOResult], None] | None = None,
        deadline: float | None = None,
    ) -> IOResult:
        """Move ``copy`` between a mapped page and an L2 pool page through
        its tier: a store when ``copy.dst`` is in the L2 pool, else a load.
        ``src`` and ``dst`` are the pages; the caller holds their I/O
        references until ``callback``. ``deadline`` (monotonic) bounds the
        I/O. ``ok`` only once all bytes moved (``late`` if past the
        deadline). Raises if not accepted (e.g. IOQueueFullError, or
        RuntimeError once drained)."""
        n = copy.nbytes
        with self._lock:
            write = copy.dst.pool_id in self._l2
            tier = self._l2.get(copy.dst.pool_id if write else copy.src.pool_id)
            mapping = self._maps.get(copy.src.pool_id if write else copy.dst.pool_id)
        offset = (copy.src if write else copy.dst).offset
        if (
            tier is None
            or mapping is None
            or not 0 < n <= min(copy.src.length, copy.dst.length)
            or offset < 0
            or offset + n > len(mapping)
        ):
            raise ValueError(f"no file I/O for {copy}")
        refs = (src, dst)
        if write:
            span = Span(copy.src.pool_id, offset, n)
            return tier.submit_store(
                span, copy.dst, refs=refs, callback=callback, deadline=deadline
            )
        span = Span(copy.dst.pool_id, offset, n)
        return tier.submit_load(
            copy.src, span, refs=refs, callback=callback, deadline=deadline
        )

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._loop, name="maru-transfer", daemon=True
        )
        self._thread.start()

    def run(self, copies: list[Copy]) -> list[bool]:
        """Copy and wait until every copy has ended; True for each that
        completed."""
        batch = _Batch(copies)
        with self._cv:
            if self._stopped.is_set() or self._thread is None:
                return [False] * len(copies)
            self._queue.append(batch)
            self._cv.notify()
        batch.done.wait()
        return batch.results

    def stop(self) -> None:
        """The copy under way ends; the rest fail. Answers every batch."""
        with self._cv:
            self._stopped.set()
            self._cv.notify_all()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join()
        with self._lock:
            tiers = self._tiers()
        for tier in tiers:
            tier.drain()

    def close(self) -> set[str]:
        """Stop, then unmap every pool the L2 tiers detached (L2 pools stay
        open). Returns the pools that stay mapped: a tier could not detach
        them, so the engine keeps them."""
        self.stop()
        with self._lock:
            maps, self._maps = self._maps, {}
            tiers, self._l2 = self._tiers(), {}
        for pool_id, mapping in maps.items():
            try:
                for tier in tiers:
                    tier.detach(pool_id)
            except Exception:
                logger.exception("cannot detach %s, so it stays mapped", pool_id)
                self._kept[pool_id] = mapping
                continue
            mapping.close()
        return set(self._kept)

    def _loop(self) -> None:
        while True:
            with self._cv:
                while not self._queue and not self._stopped.is_set():
                    self._cv.wait()
                if not self._queue:
                    return
                batch = self._queue.popleft()
            for copy in batch.copies:
                try:
                    ok = self._pace(copy.nbytes) and self._copy(copy)
                except Exception:  # every copy gets its answer
                    logger.exception("transfer failed: %s", copy)
                    ok = False
                batch.results.append(ok)
            batch.done.set()

    def _pace(self, nbytes: int) -> bool:
        """Wait until the budget allows ``nbytes``; False if stopped meanwhile."""
        now = time.monotonic()
        start = max(now, self._next_at)
        if start > now and self._stopped.wait(start - now):
            return False
        self._next_at = start + nbytes / self.bytes_per_s
        return not self._stopped.is_set()

    def _copy(self, copy: Copy) -> bool:
        n = copy.nbytes
        if not 0 < n <= min(copy.src.length, copy.dst.length):
            logger.error("transfer of %d bytes does not fit %s", n, copy)
            return False
        with self._lock:
            src = self._maps.get(copy.src.pool_id)
            dst = self._maps.get(copy.dst.pool_id)
            if src is None or dst is None:
                logger.error("transfer between unmapped pools: %s", copy)
                return False
            s, d = copy.src.offset, copy.dst.offset
            try:
                if s < 0 or d < 0 or s + n > len(src) or d + n > len(dst):
                    logger.error("transfer outside its pools: %s", copy)
                    return False
                with memoryview(src) as sv, memoryview(dst) as dv:
                    dv[d : d + n] = sv[s : s + n]
            except (OSError, ValueError, BufferError) as e:
                logger.error("transfer failed: %s (%s)", copy, e)
                return False
            self.copied_bytes += n
        return True

# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""TransferEngine: copies page bytes between the node's mapped pools.

Copies run on the engine's own thread, outside every metadata lock, paced to
a bandwidth budget. A batch is answered once every copy in it has ended, one
result per copy; after stop() what has not started fails. RDMA and SSD I/O
are not implemented yet.
"""

import collections
import logging
import mmap
import threading
import time
from dataclasses import dataclass, field

from maru_common.storage_types import Location

logger = logging.getLogger(__name__)

# Provisional: leaves most of a CXL device's bandwidth to the workers' reads
DEFAULT_TRANSFER_BYTES_PER_S = 4 << 30


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

    def has_pool(self, pool_id: str) -> bool:
        with self._lock:
            return pool_id in self._maps

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

    def close(self) -> None:
        """Stop, then unmap every pool."""
        self.stop()
        with self._lock:
            maps, self._maps = self._maps, {}
        for mapping in maps.values():
            mapping.close()

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

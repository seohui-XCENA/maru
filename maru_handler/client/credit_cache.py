# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""CreditCache: unused credit, taken without an RPC (design 5.4). Credit of an
ended StorageService run is dropped, never written."""

import collections
import threading

from maru_common.storage_types import Location


class CreditCache:
    def __init__(self):
        self._lock = threading.Lock()
        self._credits: collections.deque[Location] = collections.deque()
        self.node_epoch: str | None = None

    def reset(self, node_epoch: str) -> list[Location]:
        """Start holding credit of ``node_epoch``; returns what was dropped."""
        with self._lock:
            dropped = list(self._credits) if node_epoch != self.node_epoch else []
            if dropped:
                self._credits.clear()
            self.node_epoch = node_epoch
            return dropped

    def put(self, credits: list[Location], node_epoch: str) -> bool:
        """Add credit received from ``node_epoch``; refused for another run.
        A page already held is not added twice."""
        with self._lock:
            if node_epoch != self.node_epoch:
                return False
            held = {_page(c) for c in self._credits}
            for credit in credits:
                if _page(credit) not in held:
                    held.add(_page(credit))
                    self._credits.append(credit)
            return True

    def take(self) -> tuple[Location, str] | None:
        """One unused credit and the node epoch it belongs to."""
        with self._lock:
            if not self._credits:
                return None
            return self._credits.popleft(), self.node_epoch

    def take_bytes(self, nbytes: int) -> list[Location]:
        """Unused credit worth up to ``nbytes``, for CREDIT_RETURN."""
        with self._lock:
            out = []
            total = 0
            while self._credits and total + self._credits[0].length <= nbytes:
                credit = self._credits.popleft()
                total += credit.length
                out.append(credit)
            return out

    def take_all(self) -> list[Location]:
        with self._lock:
            out = list(self._credits)
            self._credits.clear()
            return out

    def remove_pool(self, pool_id: str) -> list[Location]:
        with self._lock:
            out = [c for c in self._credits if c.pool_id == pool_id]
            self._credits = collections.deque(
                c for c in self._credits if c.pool_id != pool_id
            )
            return out

    @property
    def unused_bytes(self) -> int:
        with self._lock:
            return sum(c.length for c in self._credits)

    def __len__(self) -> int:
        with self._lock:
            return len(self._credits)


def _page(credit: Location) -> tuple[str, int, int]:
    return (credit.pool_id, credit.offset, credit.generation)

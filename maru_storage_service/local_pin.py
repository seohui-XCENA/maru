# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Local pin block and reopen (design 5.5). Issuing pins is off in v1 (S9), but
every removal still blocks first so the order stays when it comes."""

import threading
from collections.abc import Iterable

from maru_common.storage_types import KeptCause

from .pool_manager import PageRef

# Reopen after KEPT, by cause (spec 5); UNKNOWN stays blocked
REOPEN_ON_KEPT = {
    KeptCause.PINNED: True,  # another pin or a hold; retried later
    KeptCause.MOVED: False,  # decide again from the central record
    KeptCause.INVALID: False,
}


class LocalPins:
    """Blocked pages and local readers."""

    enabled = False

    def __init__(self):
        self._lock = threading.Lock()
        self._blocked: set[PageRef] = set()

    def block(self, refs: Iterable[PageRef]) -> None:
        """No new local pin on these pages."""
        with self._lock:
            self._blocked.update(refs)

    def unblock(self, refs: Iterable[PageRef]) -> None:
        with self._lock:
            self._blocked.difference_update(refs)

    def after_kept(self, ref: PageRef, cause: KeptCause) -> None:
        """Reopen if the KEPT cause allows it."""
        if REOPEN_ON_KEPT[cause]:
            self.unblock([ref])

    def is_blocked(self, ref: PageRef) -> bool:
        with self._lock:
            return ref in self._blocked

    def forget(self, ref: PageRef) -> None:
        """The page left the blocked generation."""
        self.unblock([ref])

    def readers(self, ref: PageRef) -> int:
        """Local readers on the page (none while local pins are off)."""
        return 0

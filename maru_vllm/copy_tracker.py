# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""What a GPU copy may still use, and when it may go.

A copy's owner (pages, pins, staging, the handler that maps them) is
registered where the copy path holds it, sealed with what its caller waits
on, and then either confirmed, once that caller's own wait or query says
the copy ended, or kept until the process exits. The caller waits and
releases; a record only decides, once. No torch, no thread.
"""

from __future__ import annotations

import threading
from typing import Any

# Owners of copies whose end could not be confirmed. Never released: neither
# the worker's GC nor a later shutdown frees what they may still use.
_PROCESS_KEPT: list[CopyRecord] = []

_OPEN, _CONFIRMED, _KEPT = "open", "confirmed", "kept"


class CopyRecord:
    """One copy: its owner, what its end is waited on, and its state."""

    __slots__ = ("error", "id", "owner", "proof", "req_ids", "state")

    def __init__(self, record_id: int, owner: Any, req_ids: str | None) -> None:
        self.id = record_id
        self.owner = owner
        self.req_ids = req_ids
        self.proof: Any = None
        self.state = _OPEN
        self.error: BaseException | None = None


class CopyTracker:
    """The copies of one worker. Its lock guards state changes only."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next_id = 0
        # This worker's kept records, also in _PROCESS_KEPT
        self.kept: list[CopyRecord] = []

    def register(self, owner: Any, req_ids: str | None = None) -> CopyRecord:
        """Hold ``owner`` for a copy; ``req_ids`` name its requests in logs."""
        with self._lock:
            self._next_id += 1
            return CopyRecord(self._next_id, owner, req_ids)

    def seal(self, record: CopyRecord, proof: Any) -> None:
        """Note what the copy's end is waited on: an event, say."""
        record.proof = proof

    def confirm(self, record: CopyRecord, completed: bool) -> bool:
        """Whether the caller now releases or hands on the owner.

        True once, after the caller's own wait or query said the copy
        ended; never for a kept record.
        """
        if not completed:
            return False
        with self._lock:
            if record.state != _OPEN:
                return False
            record.state = _CONFIRMED
        # Outside the lock: an owner's finalizer may register a copy
        record.owner = record.proof = None
        return True

    def keep(self, record: CopyRecord, error: BaseException | None = None) -> bool:
        """Keep the owner until the process exits. True the first time."""
        with self._lock:
            if record.state != _OPEN:
                return False
            record.state = _KEPT
            record.error = error
            self.kept.append(record)
            _PROCESS_KEPT.append(record)
        return True

# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unholds: RM holds to drop, sent on a thread of their own and sent again
until the RM confirms each, so an RM that is briefly down never keeps a
region held for good."""

import logging
import threading
from collections.abc import Callable, Hashable

logger = logging.getLogger(__name__)

# Pause before a failed drop is sent again
RETRY_S = 1.0


class Unholds:
    def __init__(self, retry_s: float = RETRY_S) -> None:
        self.retry_s = retry_s
        self._cv = threading.Condition()
        self._queued: dict[Hashable, tuple[Callable[[], None], str]] = {}
        self._sending: Hashable | None = None
        self._thread: threading.Thread | None = None
        self._wake = threading.Event()

    def put(self, key: Hashable, send: Callable[[], None], what: str) -> None:
        """Queue ``send()`` (the UNHOLD of one hold, raising if it failed)."""
        with self._cv:
            self._queued[key] = (send, what)
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._loop, name="maru-unhold", daemon=True
                )
                self._thread.start()
            self._cv.notify_all()

    def take_back(self, key: Hashable) -> None:
        """The hold is wanted again: drop a queued drop of it, after one under
        way ends."""
        with self._cv:
            self._cv.wait_for(lambda: self._sending != key)
            self._queued.pop(key, None)

    def flush(self, timeout: float) -> bool:
        """Wait until every queued drop is confirmed; whether it was."""
        self._wake.set()
        with self._cv:
            return self._cv.wait_for(
                lambda: not self._queued and self._sending is None, timeout
            )

    def pending(self) -> int:
        with self._cv:
            return len(self._queued) + (self._sending is not None)

    def _loop(self) -> None:
        warned: set[Hashable] = set()
        while True:
            with self._cv:
                self._cv.wait_for(lambda: self._queued)
                key = next(iter(self._queued))
                send, what = self._queued.pop(key)
                self._sending = key
            try:
                send()
                failed = None
            except Exception as e:
                failed = e
            with self._cv:
                self._sending = None
                if failed is not None:
                    self._queued.setdefault(key, (send, what))
                self._cv.notify_all()
            if failed is None:
                warned.discard(key)
                continue
            if key not in warned:
                warned.add(key)
                logger.warning(
                    "could not drop %s (%s); sending it again until the RM confirms it",
                    what,
                    failed,
                )
            self._wake.wait(self.retry_s)
            self._wake.clear()

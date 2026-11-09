# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Reference pins on exact locations (spec 3.3, 6.1).

Pins never expire by time. Not thread safe: callers hold the service lock.
"""

import itertools
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum, unique

from maru_common.storage_types import Key, Location


@unique
class HolderKind(StrEnum):
    WORKER = "worker"  # LOOKUP_AND_PIN, FETCH_COMPLETE copies
    NODE = "node"  # SOURCE_PIN, COMMIT pin=node


@dataclass(frozen=True)
class Pin:
    pin_id: str
    key: Key
    location: Location
    holder: str  # session token
    holder_kind: HolderKind
    # (node_id, node_epoch) that made a worker's copy pin; it may UNPIN it
    creator: tuple[str, str] | None = None


class PinManager:
    def __init__(self) -> None:
        # unique across MaruServer runs: random prefix and a counter
        self._prefix = uuid.uuid4().hex[:12]
        self._counter = itertools.count(1)
        self._pins: dict[str, Pin] = {}
        self._by_holder: dict[str, set[str]] = {}
        self._counts: dict[tuple[Key, Location], int] = {}
        # called with (key, location) when its last pin goes
        self.on_last_unpin: Callable[[Key, Location], None] | None = None

    def __len__(self) -> int:
        return len(self._pins)

    def pin(
        self,
        key: Key,
        location: Location,
        holder: str,
        holder_kind: HolderKind,
        creator: tuple[str, str] | None = None,
    ) -> str:
        pin_id = f"{self._prefix}-{next(self._counter):x}"
        self._pins[pin_id] = Pin(pin_id, key, location, holder, holder_kind, creator)
        self._by_holder.setdefault(holder, set()).add(pin_id)
        target = (key, location)
        self._counts[target] = self._counts.get(target, 0) + 1
        return pin_id

    def get(self, pin_id: str) -> Pin | None:
        return self._pins.get(pin_id)

    def count(self, key: Key, location: Location) -> int:
        return self._counts.get((key, location), 0)

    def held_by(self, holder: str) -> list[Pin]:
        return [self._pins[p] for p in self._by_holder.get(holder, ())]

    def unpin(self, pin_id: str) -> Pin | None:
        pin = self._pins.pop(pin_id, None)
        if pin is None:
            return None
        held = self._by_holder.get(pin.holder)
        if held is not None:
            held.discard(pin_id)
            if not held:
                del self._by_holder[pin.holder]
        self._drop_count(pin)
        return pin

    def release_holder(
        self,
        holder: str,
        limit: int | None = None,
        keep: Callable[[Pin], bool] | None = None,
    ) -> int:
        """Release a session's pins (but those ``keep`` accepts), at most
        ``limit``; returns how many."""
        held = self._by_holder.get(holder)
        if not held:
            return 0
        if keep is not None:
            victims = [p for p in held if not keep(self._pins[p])][:limit]
            for pin_id in victims:
                self.unpin(pin_id)
            return len(victims)
        released = 0
        while held and (limit is None or released < limit):
            pin = self._pins.pop(held.pop())
            self._drop_count(pin)
            released += 1
        if not held:
            del self._by_holder[holder]
        return released

    def move_holder(self, old: str, new: str) -> int:
        """Give session ``old``'s pins to ``new`` (same-incarnation reopen)."""
        moved = self._by_holder.pop(old, set())
        if not moved:
            return 0
        for pin_id in moved:
            pin = self._pins[pin_id]
            self._pins[pin_id] = Pin(
                pin.pin_id, pin.key, pin.location, new, pin.holder_kind, pin.creator
            )
        self._by_holder.setdefault(new, set()).update(moved)
        return len(moved)

    def _drop_count(self, pin: Pin) -> None:
        target = (pin.key, pin.location)
        left = self._counts[target] - 1
        if left:
            self._counts[target] = left
            return
        del self._counts[target]
        if self.on_last_unpin is not None:
            self.on_last_unpin(pin.key, pin.location)

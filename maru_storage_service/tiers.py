# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tiers: the media of a node's pools, by how their bytes are reached.

An L1 tier's pools are mapped by workers, who copy themselves. Only the node
moves an L2 tier's pages, by I/O between an L1 mapping and the tier.
"""

import mmap
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from maru_common.storage_types import Location, PoolInfo

from .page_table import PageRef


class Level(StrEnum):
    L1 = "l1"  # workers map its pools and copy themselves
    L2 = "l2"  # only the node moves its pages, by I/O


@dataclass(frozen=True)
class Span:
    """``nbytes`` at ``offset`` of an attached L1 mapping."""

    pool_id: str
    offset: int
    nbytes: int


class IOResult(Protocol):
    """One L2 transfer. ``ok`` only once every byte moved (``late`` if past
    its deadline); ``done`` is set after its callback and cleanup ended."""

    @property
    def run(self) -> str: ...

    @property
    def src(self) -> PageRef: ...

    @property
    def dst(self) -> PageRef: ...

    @property
    def ok(self) -> bool: ...

    @property
    def late(self) -> bool: ...

    @property
    def cancelled(self) -> bool: ...

    @property
    def error(self) -> Exception | None: ...

    @property
    def done(self) -> threading.Event: ...


class Tier(ABC):
    """The common lifetime of a tier."""

    name: str  # the config store name
    level: Level

    def open(self) -> None:  # noqa: B027
        """Local resources, before NODE_OPEN."""

    @abstractmethod
    def pool_infos(self) -> list[PoolInfo]:
        """This node's pools of the tier."""

    def stop(self) -> None:  # noqa: B027
        """Its threads end."""

    def close(self) -> None:  # noqa: B027
        """Last: its resources go."""

    @abstractmethod
    def describe(self, adopted: set[str]) -> str:
        """The start log line."""


class L1Tier(Tier):
    level = Level.L1


class L2Tier(Tier):
    """Transfers are asynchronous. A submit raises when not accepted (e.g. a
    full queue, ValueError for a bad range, RuntimeError once drained);
    other failures come in the result. The callback runs on the tier's I/O
    thread, or on the caller's in drain or a queued cancel, never under the
    tier's own locks, and only once the medium no longer touches the memory.
    The caller holds the pages' I/O references until it runs."""

    level = Level.L2

    @abstractmethod
    def io_size(self, nbytes: int) -> int:
        """Bytes a transfer of ``nbytes`` holds in flight."""

    @abstractmethod
    def attach(self, pool_id: str, memory: mmap.mmap) -> None:
        """An L1 mapping transfers use."""

    @abstractmethod
    def detach(self, pool_id: str) -> None:
        """Before ``memory`` is unmapped; if this raises, it stays mapped."""

    @abstractmethod
    def submit_store(
        self,
        src: Span,
        dst: Location,
        *,
        refs: tuple[PageRef, PageRef],
        callback: Callable[[IOResult], None] | None = None,
        deadline: float | None = None,
    ) -> IOResult:
        """Write ``src`` to the start of the tier page ``dst``."""

    @abstractmethod
    def submit_load(
        self,
        src: Location,
        dst: Span,
        *,
        refs: tuple[PageRef, PageRef],
        callback: Callable[[IOResult], None] | None = None,
        deadline: float | None = None,
    ) -> IOResult:
        """Read the start of the tier page ``src`` into ``dst``."""

    @abstractmethod
    def drain(self) -> None:
        """No new submit; queued ones are cancelled; returns after every
        callback ran."""

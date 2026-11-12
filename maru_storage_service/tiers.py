# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tiers: the media of a node's pools, by how their bytes are reached.

An L1 tier's pools are mapped by workers, who copy themselves. Only the node
moves an L2 tier's pages, by I/O between an L1 mapping and the tier.
"""

import mmap
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from maru_common.storage_types import Location, PoolDescriptor, PoolInfo, RmDomain

from .page_table import PageRef

if TYPE_CHECKING:
    from .drivers.cxl import CxlRegionPool, RegionSource


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


@dataclass(frozen=True)
class TierEnv:
    """What a tier is made with besides its config: the run, and the
    service's pieces a tier uses."""

    node_id: str
    node_epoch: str
    page_size: int
    own_holder: str  # this run's RM holder of the node's own mappings
    transfer_bytes_per_s: float
    interrupted: threading.Event  # a start waits no longer once set
    later_run: bool  # an earlier run of this process may still map its memory
    on_lost: Callable[[str], None]  # a leased tier must let go (its thread)
    renew_s: float
    # Test injection: the RM (else a client at the tier's rm) and the
    # node's own mapping of a region
    region_source: "RegionSource | None" = None
    map_region: "Callable[[CxlRegionPool], mmap.mmap] | None" = None


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


class L1Pool(Protocol):
    """A pool workers map."""

    @property
    def pool_id(self) -> str: ...

    @property
    def capacity(self) -> int: ...

    @property
    def pool_generation(self) -> int: ...

    def info(self) -> PoolInfo: ...

    def descriptor(self, memfd: int | None = None) -> PoolDescriptor:
        """What a worker maps it by; ``memfd`` indexes the ATTACH fds."""
        ...

    def worker_fd(self) -> int | None:
        """The fd sent with ATTACH, or None."""
        ...


class L1Tier(Tier):
    """Workers map its pools."""

    level = Level.L1
    pools: Sequence[L1Pool]

    def pool_infos(self) -> list[PoolInfo]:
        return [p.info() for p in self.pools]

    @abstractmethod
    def map_for_transfers(self, pool: L1Pool) -> mmap.mmap | None:
        """The node's own mapping of ``pool`` for transfers; None (logged)
        if it takes none."""


class Leased(ABC):
    """Hooks of a tier whose pools the node leases (an RM's regions): taken
    before NODE_OPEN, renewed while the run lasts, given back on leave."""

    # Workers unmap its pools before they go back
    unmap_on_leave = True
    # The lessor gave the node to another run: never taken back
    claimed_elsewhere: bool

    @abstractmethod
    def claim(self) -> None:
        """Take the pools; returns early, with none, once the start is
        interrupted."""

    @abstractmethod
    def start_renewing(self) -> None:
        """Keep the lease while the run lasts."""

    @abstractmethod
    def taken_over(self, pool_id: str) -> bool:
        """Claimed from an earlier run, which may have written it."""

    @abstractmethod
    def rm_domains(self) -> list[RmDomain]:
        """The RM domains of its pools, for NODE_OPEN."""

    @abstractmethod
    def earlier_holds(self) -> dict[str, set[str]]:
        """Its pools an earlier run still holds, with those holders."""

    @abstractmethod
    def watch_holds(self, on_clear: Callable[[str], None]) -> None:
        """``on_clear(pool_id)`` once no earlier run holds the pool."""

    @abstractmethod
    def worker_settled(self, incarnation: str) -> None:
        """The worker's holds can go."""

    @abstractmethod
    def after_transfers_closed(self) -> None:
        """The node's own mappings are gone."""

    @abstractmethod
    def release(self) -> None:
        """Give the pools back (leave)."""

    @abstractmethod
    def time_left(self) -> float:
        """Seconds until the lessor may take the pools back."""


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

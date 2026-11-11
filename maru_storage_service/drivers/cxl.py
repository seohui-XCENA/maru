# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""CXL driver: the regions this node owns in the RM, as pools of the cxl tier.

The node, not a process, owns its regions (spec 4.4). A new run claims them
back with its node_epoch before NODE_OPEN, renews the lease while it runs and
gives them back on leave. A run that cannot renew for half the lease must let
go before the RM may give the regions to another node. A claimed region that
an earlier run still holds in the RM (a worker's or the node's own mapping)
is watched with NODE_HOLDS until those holds are gone.
"""

import logging
import mmap
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from maru_common.node_protocol import process_identity
from maru_common.storage_types import (
    DaxMapping,
    PoolDescriptor,
    PoolInfo,
    PoolKind,
    Visibility,
    run_holder,
)
from maru_shm.ipc import GetAccessResp
from maru_shm.types import (
    MaruHandle,
    NodeClaim,
    NodeLeaseExpiredError,
    NodeRegion,
    RetryableNodeError,
    StaleNodeEpochError,
)

from ..config import TierConfig
from . import PoolAccess

logger = logging.getLogger(__name__)

DEFAULT_RENEW_S = 10.0
# How often NODE_HOLDS is asked while a region waits for an earlier run
HOLDS_POLL_S = 1.0


class CxlTierError(RuntimeError):
    """The cxl tier could not be set up."""


class RegionSource(Protocol):
    """The RM's node requests (MaruShmClient)."""

    def node_claim(self, node_id: str, node_epoch: str) -> NodeClaim: ...

    def node_alloc(
        self, node_id: str, node_epoch: str, size: int, dax_path: str = ""
    ) -> NodeRegion: ...

    def node_free(self, node_id: str, node_epoch: str, handle: MaruHandle) -> None: ...

    def node_renew(self, node_id: str, node_epoch: str) -> float: ...

    def node_release(self, node_id: str, node_epoch: str) -> int: ...

    def node_hold(
        self,
        node_id: str,
        node_epoch: str,
        handle: MaruHandle,
        holder: str,
        *,
        boot_id: str,
        pid: int,
        pid_start: int,
        pid_ns: int,
    ) -> GetAccessResp: ...

    def node_holds(self, node_id: str, node_epoch: str) -> list[NodeRegion]: ...

    def unhold(self, handle: MaruHandle, holder: str, instance: str) -> None: ...


def cxl_pool_id(rm_domain: str, region_id: int) -> str:
    return f"cxl:{rm_domain}:{region_id}"


@dataclass
class CxlRegionPool:
    """One RM region as a pool. A DriverPool; workers map it by its handle."""

    node_id: str
    rm_domain: str
    region: NodeRegion
    page_size: int
    visibility: Visibility
    rm_address: str = ""
    # This run: a worker's hold names it, so the RM refuses an older run's
    node_epoch: str = ""
    # Claimed from the run before (it may hold its pages); else allocated now
    claimed: bool = False
    pool_generation: int = 1

    @property
    def access(self) -> PoolAccess:
        return PoolAccess.MAPPED

    @property
    def pool_id(self) -> str:
        return cxl_pool_id(self.rm_domain, self.region.handle.region_id)

    @property
    def capacity(self) -> int:
        return self.region.handle.length // self.page_size * self.page_size

    def info(self) -> PoolInfo:
        return PoolInfo(
            pool_id=self.pool_id,
            kind=PoolKind.CXL,
            node_id=self.node_id,
            pool_generation=self.pool_generation,
            capacity=self.capacity,
            page_size=self.page_size,
            rm_domain=self.rm_domain,
            region_id=self.region.handle.region_id,
            device_id=self.region.device_uuid or None,
            path=self.region.dax_path or None,
            visibility=self.visibility,
        )

    def descriptor(self, memfd: int | None = None) -> PoolDescriptor:
        return PoolDescriptor(
            pool=self.info(),
            dax=DaxMapping(
                self.rm_domain,
                self.region.handle,
                self.rm_address,
                node_id=self.node_id,
                node_epoch=self.node_epoch,
            ),
        )

    def worker_fd(self) -> int | None:
        return None

    def close(self) -> None:
        """Nothing to undo: the region stays the node's in the RM."""


def map_region(source: RegionSource, pool: CxlRegionPool, holder: str) -> mmap.mmap:
    """The region's bytes for the node's own copies, held in the RM by
    ``holder`` (this process in this run) with NODE_HOLD, which also checks
    the handle's token. The caller drops the hold after unmapping."""
    handle = pool.region.handle
    boot_id, pid, pid_start, pid_ns = process_identity()
    access = source.node_hold(
        pool.node_id,
        pool.node_epoch,
        handle,
        holder,
        boot_id=boot_id,
        pid=pid,
        pid_start=pid_start,
        pid_ns=pid_ns,
    )
    try:
        fd = os.open(access.dax_path, os.O_RDWR)
        try:
            return mmap.mmap(
                fd,
                handle.length,
                flags=mmap.MAP_SHARED,
                prot=mmap.PROT_READ | mmap.PROT_WRITE,
                offset=handle.offset,
            )
        finally:
            os.close(fd)
    except BaseException:
        source.unhold(handle, holder, holder)  # nothing maps it
        raise


class CxlTier:
    """The node's regions in one RM: claimed at start, renewed while the run
    lasts, given back on leave. ``on_lost`` is called once, from the renew
    thread, when the run must let go of them."""

    def __init__(
        self,
        source: RegionSource,
        tier: TierConfig,
        *,
        node_id: str,
        node_epoch: str,
        page_size: int,
        on_lost: Callable[[str], None],
        renew_s: float = DEFAULT_RENEW_S,
        clock: Callable[[], float] = time.monotonic,
    ):
        if tier.size is None or tier.region_size is None:
            raise CxlTierError("the cxl tier needs size and region_size")
        self.source = source
        self.tier = tier
        self.node_id = node_id
        self.node_epoch = node_epoch
        self.page_size = page_size
        self.renew_s = renew_s
        self._on_lost = on_lost
        self._clock = clock
        self.rm_domain: str | None = None
        self.lease_s = 0.0
        self.pools: list[CxlRegionPool] = []
        self._renewed_at = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._watcher: threading.Thread | None = None
        self.lost: str | None = None
        # Claimed pool -> names of the earlier runs' holds on it, as last
        # listed (by NODE_CLAIM, then NODE_HOLDS); a pool without any is gone
        self._lock = threading.Lock()
        self._earlier: dict[str, set[str]] = {}

    def claim(self, stopped: threading.Event, retry_s: float = 1.0) -> None:
        """Take the node's regions back, then allocate up to the tier size.
        Retries until the RM answers: no pool is announced that this run has
        not claimed. Returns early, with no pools, once ``stopped`` is set."""
        claim = None
        while claim is None and not stopped.is_set():
            sent = self._clock()  # the RM's lease restarts no earlier than this
            try:
                claim = self.source.node_claim(self.node_id, self.node_epoch)
            except (OSError, ConnectionError, TimeoutError, RetryableNodeError) as e:
                logger.warning(
                    "RM %s not answering NODE_CLAIM (%s); retrying", self.tier.rm, e
                )
                stopped.wait(retry_s)
            except RuntimeError as e:  # another host's node, an old RM
                raise CxlTierError(f"RM {self.tier.rm}: {e}") from e
        if claim is None:
            return
        self._renewed_at = sent
        self.rm_domain = claim.rm_domain
        self.lease_s = claim.lease_s
        # A region on a device the RM does not serve now has no path, and one
        # smaller than a (new) page holds none: still the node's, not used
        unusable = [
            r
            for r in claim.regions
            if not r.dax_path or r.handle.length < self.page_size
        ]
        if unusable:
            logger.warning(
                "cxl tier: %d regions are on devices the RM does not serve now or "
                "smaller than a page; kept, not used",
                len(unusable),
            )
        self.pools = [
            self._pool(r, claimed=True) for r in claim.regions if r not in unusable
        ]
        with self._lock:
            for pool in self.pools:
                held = self._earlier_holds(pool.region)
                if held:
                    self._earlier[pool.pool_id] = held
        total = sum(p.capacity for p in self.pools)
        size = self.tier.size
        assert size is not None and self.tier.region_size is not None
        while total < size and not stopped.is_set():
            sent = self._clock()
            try:
                region = self.source.node_alloc(
                    self.node_id, self.node_epoch, self.tier.region_size
                )
            except StaleNodeEpochError as e:
                raise CxlTierError(
                    f"RM {self.tier.rm}: another run claimed the node: {e}"
                ) from e
            except (OSError, RuntimeError) as e:
                logger.warning(
                    "cxl tier: the RM gave %d of %d bytes; going on with them (%s)",
                    total,
                    size,
                    e,
                )
                break
            self._renewed_at = sent
            pool = self._pool(region, claimed=False)
            self.pools.append(pool)
            total += pool.capacity
        if total > size:
            logger.info(
                "cxl tier: %d bytes claimed, more than its size %d", total, size
            )

    def _pool(self, region: NodeRegion, claimed: bool) -> CxlRegionPool:
        assert self.rm_domain is not None and self.tier.visibility is not None
        return CxlRegionPool(
            node_id=self.node_id,
            rm_domain=self.rm_domain,
            region=region,
            page_size=self.page_size,
            visibility=self.tier.visibility,
            rm_address=self.tier.rm or "",
            node_epoch=self.node_epoch,
            claimed=claimed,
        )

    # ------------------------------------------------------------ the lease

    def start_renewing(self) -> None:
        self._thread = threading.Thread(
            target=self._renew_loop, name="maru-cxl-lease", daemon=True
        )
        self._thread.start()

    def renew_interval(self) -> float:
        """Several renewals per half lease, however short the lease."""
        return min(self.renew_s, self.lease_s / 4) if self.lease_s else self.renew_s

    def time_left(self) -> float:
        """Seconds until the RM may free the regions, by this run's clock."""
        return self.lease_s - (self._clock() - self._renewed_at)

    def _renew_loop(self) -> None:
        while not self._stop.wait(self.renew_interval()):
            reason = self.renew_once()
            if reason is not None:
                self.lost = reason
                logger.error("cxl tier: letting go of the regions: %s", reason)
                self._on_lost(reason)
                return

    def renew_once(self) -> str | None:
        """Renew the lease; why the run must let go, or None."""
        sent = self._clock()
        try:
            self.lease_s = self.source.node_renew(self.node_id, self.node_epoch)
            self._renewed_at = sent
            return None
        except NodeLeaseExpiredError:
            return "the RM no longer counts this run as the owner (its lease ended)"
        except StaleNodeEpochError:
            return (
                "the RM no longer counts this run as the owner (another run "
                "claimed the node)"
            )
        except (OSError, ConnectionError, TimeoutError, RuntimeError) as e:
            silent = self._clock() - self._renewed_at
            if silent >= self.lease_s / 2:
                return f"no lease renewal for {silent:.0f} s ({e})"
            logger.warning("cxl tier: lease renewal failed (%s); retrying", e)
            return None

    def stop(self, timeout: float | None = None) -> None:
        """Stop renewing the lease and watching holds. A request under way may
        outlast ``timeout``; it changes nothing."""
        self._stop.set()
        for thread in (self._thread, self._watcher):
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout)

    # ------------------------------------------------------------- the holds

    def _earlier_holds(self, region: NodeRegion) -> set[str]:
        """Holds of another run of this node (a remote agent's names none)."""
        return {
            h.holder
            for h in region.holders
            if h.node_epoch and h.node_epoch != self.node_epoch
        }

    def earlier_holds(self) -> dict[str, set[str]]:
        """Claimed pools an earlier run still holds, with those holders."""
        with self._lock:
            return {pool_id: set(held) for pool_id, held in self._earlier.items()}

    def watch_holds(
        self, on_clear: Callable[[str], None], interval_s: float = HOLDS_POLL_S
    ) -> None:
        """Ask NODE_HOLDS every ``interval_s`` while a pool waits for an
        earlier run's holds; ``on_clear(pool_id)``, from the watch thread,
        once a pool has none left."""
        if not self.earlier_holds():
            return
        self._watcher = threading.Thread(
            target=self._watch_loop,
            args=(on_clear, interval_s),
            name="maru-cxl-holds",
            daemon=True,
        )
        self._watcher.start()

    def _watch_loop(self, on_clear: Callable[[str], None], interval_s: float) -> None:
        failing = False
        while not self._stop.wait(interval_s):
            try:
                regions = self.source.node_holds(self.node_id, self.node_epoch)
            except StaleNodeEpochError:
                return  # the run is over; the lease thread lets go
            except Exception as e:
                if not failing:
                    logger.warning("cxl tier: NODE_HOLDS failed (%s); retrying", e)
                failing = True
                continue
            failing = False
            assert self.rm_domain is not None
            listed = {
                cxl_pool_id(self.rm_domain, r.handle.region_id): r for r in regions
            }
            cleared = []
            with self._lock:
                for pool_id in list(self._earlier):
                    region = listed.get(pool_id)
                    if region is None:
                        continue  # not listed as the node's: never reused
                    held = self._earlier_holds(region)
                    if held:
                        self._earlier[pool_id] = held
                    else:
                        del self._earlier[pool_id]
                        cleared.append(pool_id)
                done = not self._earlier
            for pool_id in cleared:
                if self._stop.is_set():
                    return
                on_clear(pool_id)
            if done:
                return

    def unhold(self, holder: str) -> None:
        """Drop ``holder``'s hold on every region. Dropping one that is gone
        does nothing."""
        for pool in self.pools:
            try:
                self.source.unhold(pool.region.handle, holder, holder)
            except Exception as e:  # the RM reaps it once it sees the exit
                logger.warning(
                    "cxl tier: could not drop %s's hold on %s: %s",
                    holder,
                    pool.pool_id,
                    e,
                )

    def drop_holds(self, incarnation: str) -> None:
        """A worker settled (ended, its GPU drain grace over): drop its holds
        of this run and of the earlier runs still listed."""
        names = {run_holder(incarnation, self.node_epoch)}
        with self._lock:
            for held in self._earlier.values():
                names |= {h for h in held if h.partition("@")[0] == incarnation}
        for name in sorted(names):
            self.unhold(name)

    def release(self) -> None:
        """Give every region back (leave). One still mapped stays out of reuse
        in the RM until its last hold goes; one the RM does not take back now
        is freed when the lease ends."""
        try:
            freed = self.source.node_release(self.node_id, self.node_epoch)
            logger.info("cxl tier: %d regions given back to the RM", freed)
        except Exception as e:
            logger.error(
                "cxl tier: could not give the regions back (%s); the RM frees "
                "them when the lease ends",
                e,
            )

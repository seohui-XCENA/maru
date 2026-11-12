# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageService: builds and runs the parts of one node for one run.

Start order: claim the socket, open the tiers in config order (DRAM pool,
SSD file), then claim the leased ones (the node's CXL regions from the RM),
NODE_OPEN, adopted ledgers, quarantine of the regions an earlier run still
holds, heartbeat, then accept on the UDS. A GPU node with [dram],
[cxl], [dram, cxl], [dram, ssd], [cxl, ssd] or [dram, cxl, ssd] is served;
other configs are refused. The top tier gives credit; each tier makes room by
demoting its pages to the tier below (write-back), the last one by dropping
its own. Workers never see the SSD pool: only this process reads and writes
its file, and it keeps the file's lock until all of its I/O has ended. A
worker that agreed fetch_v1 FETCHes SSD pages into the top tier
(FetchManager).

A run ends on its own when MaruServer restarted or ended it, when a result
stays unknown (both close with restart), or when the RM lease is lost (leave);
Node then starts a new run in the same process. When the RM says another run
claimed the node, the run leaves and no new run starts.
"""

import dataclasses
import itertools
import logging
import mmap
import threading
import time
import uuid
from collections.abc import Callable
from typing import TypeVar

from maru_common.node_protocol import Budgets
from maru_common.storage_types import (
    FETCH_CAPABILITY,
    POOL_KINDS,
    CloseMode,
    PoolDescriptor,
    run_holder,
)

from .config import NodeConfig, NodeConfigError, TierConfig
from .config_fields import NotServableError as NotServableError
from .drivers.cxl import (
    DEFAULT_RENEW_S,
    OWN_HOLDER,
    CxlRegionPool,
    CxlTier,
    RegionSource,
)
from .drivers.dram import DramPool, DramPoolError, DramTier
from .drivers.registry import TIERS
from .drivers.ssd import SsdPool, SsdTier
from .fetch_manager import FetchManager
from .local_pin import LocalPins
from .local_server import LocalServer
from .op_ledger import OP_WINDOW
from .peer_watcher import PeerWatcher
from .placement import DEFAULT_KEPT_BACKOFF_S, PlacementPolicy
from .pool_manager import PageState, PoolManager
from .ports import EpochChanged, ServerStopped
from .server_client import ServerClient
from .storage_manager import (
    DEFAULT_EVICT_INTERVAL_S,
    DEFAULT_GPU_DRAIN_GRACE_S,
    StorageManager,
)
from .tiers import L1Tier, L2Tier, Leased, Level, Tier, TierEnv
from .transfer_engine import DEFAULT_TRANSFER_BYTES_PER_S, TransferEngine

logger = logging.getLogger(__name__)


# How long a leave waits for the workers to unmap the CXL regions
DEFAULT_UNMAP_TIMEOUT_S = 30.0
# Pause between a run that ended and the next one, doubled up to the max for
# runs that end soon after they start
NEW_RUN_PAUSE_S = 1.0
NEW_RUN_PAUSE_MAX_S = 60.0
# A run at least this long resets the pause
NEW_RUN_RESET_S = 300.0
# How long a stopped node waits for a run's teardown
STOP_WAIT_S = 30.0
# A DRAM pool that does not fit is tried again, this often (doubled up to the
# max), and logged this often
DRAM_RETRY_S = 1.0
DRAM_RETRY_MAX_S = 30.0
DRAM_LOG_S = 30.0

T = TypeVar("T", bound=Tier)


def _served_tiers() -> tuple[list[str], ...]:
    """One or more served L1 stores in registry order, then at most one
    served L2 store; those without an L2 first."""
    l1 = [s.store for s in TIERS.values() if s.served and s.level is Level.L1]
    l2 = [s.store for s in TIERS.values() if s.served and s.level is Level.L2]
    tops = [
        list(c) for n in range(1, len(l1) + 1) for c in itertools.combinations(l1, n)
    ]
    belows: list[list[str]] = [[], *([s] for s in l2)]
    return tuple(top + below for below in belows for top in tops)


SERVED_TIERS = _served_tiers()


def check_servable(config: NodeConfig) -> None:
    """Refuse configs that cannot be served in full yet."""
    if config.role != "gpu":
        raise NotServableError("serving a memory node is not implemented yet")
    stores = [t.store for t in config.tiers]
    if stores not in SERVED_TIERS:
        served = ", ".join(f"[{', '.join(s)}]" for s in SERVED_TIERS)
        raise NotServableError(
            f"serving the tiers {stores} is not implemented yet: only {served} "
            "are served"
        )
    for t in config.tiers:
        TIERS[t.store].check_servable(t)
    if any(t.demotion == "through" for t in config.tiers):
        raise NotServableError(
            "write-through demotion is not implemented yet: use demotion back"
        )


def worker_budgets(
    config: NodeConfig, top_bytes: int, bounce: int, workers: int = 1
) -> Budgets:
    """The budgets a worker gets at ATTACH. With an L2 tier (SSD) its
    fetch_request_max_bytes is its fetch window: the pages it may hold
    fetched at once. Within the per-op cap, its share among ``workers``
    (the fetch_v1 workers attached, itself included) of the flights the
    node's in-flight cap holds (a page and its ``bounce`` each) and of the
    ``top_bytes`` really mapped for the top tier less its credit, commit
    pending and transfer reserve; one page at least."""
    budgets = config.budgets
    if all(t.is_memory for t in config.tiers):
        return budgets
    ps = config.page_size
    workers = max(1, workers)
    room = (
        top_bytes
        - budgets.credit_total_max_bytes
        - budgets.commit_pending_max_bytes
        - budgets.transfer_reserve_bytes.get(config.top.store, 0)
    )
    window = min(
        budgets.fetch_request_max_bytes,
        budgets.fetch_max_inflight_bytes // workers // (ps + bounce) * ps,
        room // workers,
    )
    return dataclasses.replace(
        budgets, fetch_request_max_bytes=max(ps, window // ps * ps)
    )


class StorageService:
    def __init__(
        self,
        config: NodeConfig,
        *,
        rpc_timeout_s: float = 2.0,
        gpu_drain_grace_s: float = DEFAULT_GPU_DRAIN_GRACE_S,
        evict_interval_s: float = DEFAULT_EVICT_INTERVAL_S,
        kept_backoff_s: float = DEFAULT_KEPT_BACKOFF_S,
        host_boot_id: str | None = None,
        node_epoch: str | None = None,
        op_window: int = OP_WINDOW,
        region_source: RegionSource | None = None,
        cxl_renew_s: float = DEFAULT_RENEW_S,
        unmap_timeout_s: float = DEFAULT_UNMAP_TIMEOUT_S,
        transfer_bytes_per_s: float = DEFAULT_TRANSFER_BYTES_PER_S,
        map_region: Callable[[CxlRegionPool], mmap.mmap] | None = None,
    ):
        """``transfer_bytes_per_s``: the demotion copy budget.
        ``map_region``: maps a CXL region for those copies (NODE_HOLD and the
        device by default)."""
        check_servable(config)
        self.config = config
        self.node_epoch = node_epoch or uuid.uuid4().hex
        # This process's RM hold on the regions it maps for those copies
        self.own_holder = run_holder(OWN_HOLDER, self.node_epoch)
        self.rpc_timeout_s = rpc_timeout_s
        self.gpu_drain_grace_s = gpu_drain_grace_s
        self.evict_interval_s = evict_interval_s
        self.kept_backoff_s = kept_backoff_s
        self.host_boot_id = host_boot_id
        self.op_window = op_window
        self.region_source = region_source
        self.cxl_renew_s = cxl_renew_s
        self.unmap_timeout_s = unmap_timeout_s
        self.transfer_bytes_per_s = transfer_bytes_per_s
        self.map_region = map_region
        self.transfers: TransferEngine | None = None
        # The run's tiers by store name, in the order they started
        self._tiers: dict[str, Tier] = {}
        # Why the run ended on its own, and whether a new run follows
        self.ended: str | None = None
        self.new_run = False
        # An earlier run of this process may still have its pool mapped
        self.later_run = False
        # Why tearing the run down failed: nothing may run after it
        self.teardown_failed: str | None = None
        self._interrupted = threading.Event()
        self._finished = threading.Event()
        # One stop at a time, and never while a start is half done
        self._stop_lock = threading.Lock()
        self._lifecycle = threading.Lock()
        self.pools: PoolManager | None = None
        self.server: ServerClient | None = None
        self.manager: StorageManager | None = None
        self.local: LocalServer | None = None
        # Pools taken over from the run before: adopted or claimed from the RM
        self.adopted: list[str] = []
        self._credit_tiers: set[str] = set()
        self._stopped = threading.Event()

    def _named(self, name: str, cls: type[T]) -> T | None:
        """The run's tier ``name``, if it is a ``cls``."""
        tier = self._tiers.get(name)
        return tier if isinstance(tier, cls) else None

    @property
    def _dram_tier(self) -> DramTier | None:
        return self._named("dram", DramTier)

    @property
    def _ssd_tier(self) -> SsdTier | None:
        return self._named("ssd", SsdTier)

    @property
    def cxl(self) -> CxlTier | None:
        """The cxl tier, once made."""
        return self._named("cxl", CxlTier)

    @property
    def dram(self) -> DramPool | None:
        """The run's DRAM pool, once created."""
        return self._dram_tier.pool if self._dram_tier is not None else None

    @property
    def ssd(self) -> SsdPool | None:
        """The SSD pool, once open."""
        return self._ssd_tier.pool if self._ssd_tier is not None else None

    def _env(self) -> TierEnv:
        return TierEnv(
            node_id=self.config.node_id,
            node_epoch=self.node_epoch,
            page_size=self.config.page_size,
            own_holder=self.own_holder,
            transfer_bytes_per_s=self.transfer_bytes_per_s,
            interrupted=self._interrupted,
            later_run=self.later_run,
            on_lost=self._lose_regions,
            renew_s=self.cxl_renew_s,
            region_source=self.region_source,
            map_region=self.map_region,
        )

    def _make(self, tier: TierConfig) -> Tier:
        """The tier object of ``tier``, made by its store's TierSpec; the run
        keeps it."""
        spec = TIERS[tier.store]
        assert spec.make is not None  # check_servable lets served stores only
        made = spec.make(tier, self._env())
        if spec.leased != isinstance(made, Leased):
            raise TypeError(
                f"store {tier.store}: its spec says leased={spec.leased}, "
                f"but it made a {type(made).__name__}"
            )
        self._tiers[tier.store] = made
        return made

    def start(self) -> None:
        """Bring the node up; on failure everything started is undone.
        ServerStopped if stopped meanwhile, or if the run ended during the
        start (``ended``)."""
        try:
            with self._lifecycle:
                self._start()
        except EpochChanged as e:
            self._end_run(
                f"MaruServer no longer knows the session ({e})",
                CloseMode.RESTART,
                new_run=True,
            )
            raise ServerStopped(str(e)) from e
        except BaseException:
            self.stop(CloseMode.RESTART)
            raise

    def _check_stopped(self) -> None:
        """After a blocking step: a stop asked meanwhile ends the start."""
        if self._stopped.is_set():
            raise ServerStopped()

    def _start(self) -> None:
        config = self.config
        top = config.top
        size, high, low = top.size, top.high_watermark, top.low_watermark
        if size is None or high is None or low is None:  # parsing sets them
            raise NodeConfigError(f"the {top.store} tier needs size and watermarks")
        if self._interrupted.is_set():
            raise ServerStopped()
        # Claim the socket first: a second start must fail before NODE_OPEN
        self.local = LocalServer(
            config.socket,
            None,
            node_id=config.node_id,
            node_epoch=self.node_epoch,
            server_address=config.server,
            page_size=config.page_size,
            budgets=config.budgets,  # the workers' are set before it serves
            pools=[],
            transfer_defaults=config.transfer_defaults(),
            socket_group=config.socket_group,
            capabilities=(
                frozenset({FETCH_CAPABILITY})
                if any(not t.is_memory for t in config.tiers)
                else frozenset()
            ),
        )
        self.local.claim()
        # Before NODE_OPEN, in config order. An SSD file's lock is free only
        # once every earlier run's I/O on it has ended: an earlier run of
        # this process drained and closed it, an earlier process exited
        for t in config.tiers:
            if not TIERS[t.store].leased:
                self._open(self._make(t))
        # Then the leased ones: only regions this run owns are announced
        for t in config.tiers:
            if TIERS[t.store].leased:
                leased = self._make(t)
                assert isinstance(leased, Leased)
                leased.open()
                leased.claim()
                if self._interrupted.is_set():
                    raise ServerStopped()
                leased.start_renewing()
                self._check_stopped()
        # In the order they started: the order pools are announced and logged
        tiers = self._tiers
        traits = config.traits()
        if any(t.demotes_to is not None for t in traits.values()):
            self.transfers = TransferEngine(self.transfer_bytes_per_s)
            self._map_for_transfers(self.transfers)
        self.pools = PoolManager(config.budgets, config.page_size)
        self._credit_tiers = {
            name for name, t in traits.items() if t.credit or t.credit_fallback
        }
        # A pool that ends with its run (DRAM) is never adopted
        for t in config.tiers:
            for info in tiers[t.store].pool_infos():
                if POOL_KINDS[info.kind].run_scoped:
                    self.pools.add_pool(
                        info, creditable=t.store in self._credit_tiers, tier=t.store
                    )
        self.server = ServerClient(
            config.server,
            config.node_id,
            self.node_epoch,
            rpc_timeout_s=self.rpc_timeout_s,
            host_boot_id=self.host_boot_id,
        )
        if self._interrupted.is_set():  # before interrupt() could stop it
            raise ServerStopped()
        placement = PlacementPolicy(high, low, kept_backoff_s=self.kept_backoff_s)
        for t in config.tiers[1:]:
            if t.high_watermark is not None and t.low_watermark is not None:
                placement.set_watermarks(t.store, t.high_watermark, t.low_watermark)
        self.manager = StorageManager(
            node_epoch=self.node_epoch,
            pool_manager=self.pools,
            placement=placement,
            local_pins=LocalPins(),
            metadata=self.server,
            watcher=PeerWatcher(),
            gpu_drain_grace_s=self.gpu_drain_grace_s,
            evict_interval_s=self.evict_interval_s,
            op_window=self.op_window,
            traits=traits,
            on_worker_settled=self._worker_settled,
            transfers=self.transfers,
            on_end=lambda reason: self._end_run(
                reason, CloseMode.RESTART, new_run=True
            ),
        )
        l2 = [t for t in tiers.values() if isinstance(t, L2Tier)]
        if l2:
            # Workers fetch L2 pages into the top tier
            self.manager.fetches = FetchManager(
                self.manager,
                sources=self.transfers.l2_pools() if self.transfers is not None else {},
                node_id=config.node_id,
                page_size=config.page_size,
                tier=top.store,
                request_max_bytes=config.budgets.fetch_request_max_bytes,
                inflight_max_bytes=config.budgets.fetch_max_inflight_bytes,
            )
        leases = [t for t in tiers.values() if isinstance(t, Leased)]
        # The tier of each pool this run opens: adopted ones are among them
        announced = [(n, i) for n, t in tiers.items() for i in t.pool_infos()]
        pool_tiers = {info.pool_id: name for name, info in announced}
        rm_domains = [d for t in leases for d in t.rm_domains()]
        opened = self.server.open(
            [info for _, info in announced], rm_domains=rm_domains
        )
        self._check_stopped()
        self.pools.set_epoch_seq(opened.epoch_seq)
        adopted_ids = {info.pool_id for info in opened.adopted}
        for info in opened.adopted:
            # Its committed pages come back READY, the rest stay quarantined.
            # MaruServer adopts the SSD pool only on the same boot and when
            # no other run opened the file since; the file lock shows its
            # earlier I/O ended, so its quarantine ends below once listed
            name = pool_tiers.get(info.pool_id)
            if name is None:
                raise RuntimeError(f"MaruServer adopted {info.pool_id}, not opened")
            self.pools.add_pool(
                info, creditable=False, state=PageState.QUARANTINED, tier=name
            )
            self.pools.quarantine_pool(info.pool_id)
            items = self.server.recover_list(info.pool_id)
            refs = self.pools.recovered(
                info.pool_id, [(i.key, i.location) for i in items]
            )
            for ref in refs:
                placement.on_ready(ref)
            self.adopted.append(info.pool_id)
            logger.info("adopted pool %s: %d committed pages", info.pool_id, len(refs))
        # The rest in config order. An SSD file not adopted (first run,
        # another boot, another MaruServer or a run on one in between) is
        # reused from scratch
        for t in config.tiers:
            tier = tiers[t.store]
            for info in tier.pool_infos():
                if info.pool_id in adopted_ids or POOL_KINDS[info.kind].run_scoped:
                    continue
                if isinstance(tier, Leased) and tier.taken_over(info.pool_id):
                    # The run before may have written it: quarantined as an
                    # adopted pool, with no committed page
                    self.pools.add_pool(
                        info,
                        creditable=False,
                        state=PageState.QUARANTINED,
                        tier=t.store,
                    )
                    self.pools.quarantine_pool(info.pool_id)
                    self.adopted.append(info.pool_id)
                else:
                    self.pools.add_pool(
                        info, creditable=t.store in self._credit_tiers, tier=t.store
                    )
        self.manager.start()
        # Quarantined while an earlier run holds the region in the RM
        earlier: dict[str, set[str]] = {}
        for lease in leases:
            earlier.update(lease.earlier_holds())
        for pool_id in self.adopted:
            if pool_id in earlier:
                logger.info(
                    "pool %s stays quarantined while earlier runs hold it: %s",
                    pool_id,
                    sorted(earlier[pool_id]),
                )
            else:
                self._end_quarantine(pool_id)
        for lease in leases:
            lease.watch_holds(self._end_quarantine)
        self.manager.start_heartbeat()
        self.local.manager = self.manager
        # Workers map the L1 pools; memfd indexes the fds sent with ATTACH
        attach: list[tuple[PoolDescriptor, int | None]] = []
        for tier in tiers.values():
            if isinstance(tier, L1Tier):
                for pool in tier.pools:
                    fd = pool.worker_fd()
                    memfd = (
                        None if fd is None else sum(f is not None for _, f in attach)
                    )
                    attach.append((pool.descriptor(memfd=memfd), fd))
        self.local.pools = attach
        if l2:
            # The window fits the pools fetches can use at ATTACH (the RM may
            # give less, a mapping may fail, a pool may be quarantined)
            pools, fetches = self.pools, self.manager.fetches

            def top_bytes() -> int:
                infos = [pools.pool_info(i) for i in fetches.destinations()]
                return sum(i.capacity for i in infos if i is not None)

            bounce = max(t.io_size(config.page_size) for t in l2)
            self.local.budgets = worker_budgets(config, top_bytes(), bounce)
            self.local.fetch_budgets = lambda workers: worker_budgets(
                config, top_bytes(), bounce, workers
            )
        self.local.start()
        logger.info(
            "StorageService %s serving node %s, pages of %d KiB",
            self.node_epoch,
            config.node_id,
            config.page_size >> 10,
        )
        for name, tier in tiers.items():
            if tier.pool_infos():
                logger.info("%s tier: %s", name, tier.describe(adopted_ids))

    def _create_dram(self, tier: TierConfig) -> DramPool:
        """The run's memfd: its DramTier made and opened."""
        dram = self._make(tier)
        assert isinstance(dram, DramTier)
        self._open(dram)
        return dram.pools[0]

    def _open(self, tier: Tier) -> None:
        """Open ``tier``. A DRAM pool that ran short of memory may fit once
        workers unmap an earlier run's pool: tried again with a backoff until
        it fits or the run is stopped, as is any DRAM failure of a later run
        in this process."""
        pause, logged = DRAM_RETRY_S, None
        while True:
            try:
                tier.open()
                return
            except DramPoolError as e:
                if not (e.short or self.later_run):
                    raise
                now = time.monotonic()
                if logged is None or now - logged >= DRAM_LOG_S:
                    logged = now
                    logger.warning(
                        "cannot allocate the dram pool yet; workers may still map "
                        "the pool of an earlier run. Trying again: %s",
                        e,
                    )
            if self._interrupted.wait(pause):
                raise ServerStopped("stopped while waiting for the dram pool")
            pause = min(pause * 2, DRAM_RETRY_MAX_S)

    def _map_for_transfers(self, engine: TransferEngine) -> None:
        """The node's own mappings of its L1 pools, for demotion copies, then
        the L2 tiers, in the order the tiers started. A pool it cannot map
        takes no demotions."""
        tiers = self._tiers.values()
        for tier in tiers:
            if isinstance(tier, L1Tier):
                for pool in tier.pools:
                    mapping = tier.map_for_transfers(pool)
                    if mapping is not None:
                        engine.add_pool(pool.pool_id, mapping)
        for tier in tiers:
            if isinstance(tier, L2Tier):
                engine.add_l2(tier)
        engine.start()

    def _end_quarantine(self, pool_id: str) -> None:
        """No earlier run holds the pool: its quarantined pages are free, and
        it gives credit if its tier does and MaruServer did not call it
        invalid. Never after a stop began."""
        assert self.pools is not None and self.manager is not None
        with self._stop_lock:
            if self._stopped.is_set():
                return
            tier = self.pools.tier_of(pool_id)
            creditable = tier in self._credit_tiers and not self.manager.is_invalid(
                pool_id
            )
            freed = self.pools.end_pool_quarantine(pool_id, creditable)
        logger.info(
            "pool %s: no earlier run holds it; %d quarantined pages are free",
            pool_id,
            freed,
        )

    def _worker_settled(self, incarnation: str) -> None:
        """Its GPU drain grace is over: its region holds can go."""
        if self.cxl is not None:
            self.cxl.worker_settled(incarnation)

    def interrupt(self) -> None:
        """Stop a start still waiting for the RM or MaruServer."""
        self._interrupted.set()
        if self.server is not None:
            self.server.stop()

    def _lose_regions(self, reason: str) -> None:
        """The lease may end: stop with leave so no location keeps pointing at
        the regions and no worker copies into them. A new run follows unless
        another run claimed the node."""
        assert self.cxl is not None
        self._end_run(reason, CloseMode.LEAVE, new_run=not self.cxl.claimed_elsewhere)

    def _end_run(self, reason: str, mode: CloseMode, new_run: bool) -> None:
        """The run ends on its own: stop with ``mode`` on a thread of its own
        (the caller may be a thread the stop joins). The first reason wins;
        none after a stop began."""
        with self._stop_lock:
            if self.ended is not None or self._stopped.is_set():
                return
            self.ended, self.new_run = reason, new_run
        logger.warning("run %s ends (%s): %s", self.node_epoch, mode.value, reason)
        threading.Thread(
            target=self.stop, args=(mode,), name="maru-run-end", daemon=True
        ).start()

    def stop(self, mode: CloseMode = CloseMode.RESTART) -> None:
        """Stop serving, then NODE_CLOSE (restart: back soon; leave: gone)."""
        with self._stop_lock:
            if self._stopped.is_set():
                return
            self._stopped.set()
        self.interrupt()
        try:
            with self._lifecycle:  # a start still running gives up first
                self._teardown(mode)
        except Exception as e:
            self.teardown_failed = f"{type(e).__name__}: {e}"
            logger.exception("tearing run %s down failed", self.node_epoch)
        finally:
            self._finished.set()

    def _teardown(self, mode: CloseMode) -> None:
        if self.transfers is not None:
            self.transfers.stop()  # the copy under way ends; no new one starts
        if self.cxl is not None:
            self.cxl.stop()
        if self.cxl is not None and mode == CloseMode.LEAVE:
            still = {p.pool_id for p in self.cxl.pools} - self._unmap_cxl()
            if still:
                # Their holds keep them out of reuse in the RM until unmapped
                logger.warning(
                    "cxl regions still mapped by a worker: %s", sorted(still)
                )
        if self.local is not None:
            self.local.stop()
        if self.server is not None:
            self.server.stop()
        if self.manager is not None:
            self.manager.stop()
            stats = self.manager.stats()
            logger.info(
                "this run removed pages: retire %s, demotion %s; credit came "
                "short (by cause, MiB): %s",
                stats["retire_results"] or "none",
                stats["demotions"] or "none",
                {k: v >> 20 for k, v in stats["credit_short_bytes"].items()} or "never",
            )
        kept: set[str] = set()
        if self.transfers is not None:
            kept = self.transfers.close()  # unmapped before the regions may go back
            if kept:
                logger.error(
                    "pools %s stay mapped: their hold and regions stay", sorted(kept)
                )
            elif self.cxl is not None:
                self.cxl.after_transfers_closed()
        if self.server is not None:
            if self.server.close_session(mode):
                logger.info("node session closed (%s)", mode.value)
            self.server.close()
        if self.cxl is not None and mode == CloseMode.LEAVE and not kept:
            self.cxl.release()
        if self.cxl is not None:
            self.cxl.close()  # its RM client
        if self._dram_tier is not None:
            self._dram_tier.close()
        if self._ssd_tier is not None:
            # Every I/O was drained above (transfers.stop); closing ends the
            # lock, so the next run may open the file
            self._ssd_tier.close()

    def _unmap_cxl(self) -> set[str]:
        """The CXL pools no worker can map any more, before they go back."""
        assert self.cxl is not None
        cxl = self.cxl
        # A pool an earlier run still holds stays held after this
        earlier = set(cxl.earlier_holds())
        if self.manager is None or self.local is None or not self.local.started:
            # No worker of this run mapped them
            return {p.pool_id for p in cxl.pools} - earlier
        # A new ATTACH maps none of them: its record exists before its reply;
        # and no credit comes from them any more
        self.local.pools = [(d, fd) for d, fd in self.local.pools if d.dax is None]
        for pool in cxl.pools:
            self.pools.disable_pool(pool.pool_id)
        timeout = self.unmap_timeout_s
        if self.ended is not None:  # the RM may free them once the lease ends
            timeout = min(timeout, cxl.time_left() / 2)
        done = self.manager.unmap_pools(
            [(p.pool_id, p.pool_generation) for p in cxl.pools], max(0.0, timeout)
        )
        return done - earlier

    def wait(self, timeout: float | None = None) -> bool:
        """Whether stopping has begun."""
        return self._stopped.wait(timeout)

    def wait_finished(self, timeout: float | None = None) -> bool:
        """Whether a stop has run to its end."""
        return self._finished.wait(timeout)


class Node:
    """Runs of the node's StorageService, one after another in this process:
    a run that ends on its own and asks for a new one is followed by a new
    run with a new node_epoch, after a pause that grows while runs end soon
    after they start. Workers re-attach to it as after a process restart."""

    def __init__(
        self,
        config: NodeConfig,
        make_service: Callable[[NodeConfig], StorageService] = StorageService,
        pause_s: float = NEW_RUN_PAUSE_S,
        max_pause_s: float = NEW_RUN_PAUSE_MAX_S,
    ):
        check_servable(config)
        self.config = config
        self._make_service = make_service
        self.pause_s = pause_s
        self.max_pause_s = max_pause_s
        self._stop = threading.Event()
        self.service: StorageService | None = None
        self.runs = 0
        # Why the last run ended on its own
        self.ended: str | None = None
        # A run's teardown failed: no run follows, the process must exit
        self.failed: str | None = None

    def stop(self) -> None:
        """End the current run with restart and start no other; any thread,
        a signal handler too (no lock)."""
        self._stop.set()
        service = self.service
        if service is not None:
            service.interrupt()

    def run(self) -> bool:
        """Serve until stop() (True), or until no run may follow (False):
        another run claimed the node, or a teardown failed (``failed``). A
        start that fails otherwise raises."""
        pause = self.pause_s
        while True:
            if self._stop.is_set():
                return True
            service = self._make_service(self.config)
            service.later_run = self.runs > 0
            self.service = service
            self.runs += 1
            if self._stop.is_set():  # stop() may have missed it
                service.interrupt()
            started = time.monotonic()
            try:
                service.start()
            except ServerStopped:
                pass  # stopped, or the run ended during its start
            except Exception as e:
                if self._claimed_elsewhere(service, str(e)):
                    return False
                if service.teardown_failed is not None:
                    self.failed = service.teardown_failed
                raise
            while not self._stop.wait(0.1) and not service.wait(0):
                pass
            if service.ended is None:
                # Stopped: torn down on a thread, waited for up to STOP_WAIT_S
                threading.Thread(
                    target=service.stop,
                    args=(CloseMode.RESTART,),
                    name="maru-run-stop",
                    daemon=True,
                ).start()
            if not self._finished(service):
                return True
            if service.teardown_failed is not None:
                self.failed = service.teardown_failed
                logger.error("no new run: the teardown failed (%s)", self.failed)
                return False
            if service.ended is None:
                return True
            self.ended = service.ended
            if not service.new_run or self._claimed_elsewhere(service, service.ended):
                return False
            if self._stop.is_set():
                return True
            if time.monotonic() - started >= NEW_RUN_RESET_S:
                pause = self.pause_s
            logger.warning(
                "starting a new run in %.1f s after: %s", pause, service.ended
            )
            if self._stop.wait(pause):
                return True
            pause = min(pause * 2, self.max_pause_s)

    def _finished(self, service: StorageService) -> bool:
        """Wait for the run's teardown; once stopped, for STOP_WAIT_S more."""
        deadline = None
        while not service.wait_finished(0.1):
            if self._stop.is_set():
                deadline = deadline or time.monotonic() + STOP_WAIT_S
                if time.monotonic() >= deadline:
                    logger.error("the run's teardown did not end; stopping anyway")
                    return False
        return True

    def _claimed_elsewhere(self, service: StorageService, reason: str) -> bool:
        """Another run claimed the node in the RM: never claim it back."""
        if service.cxl is None or not service.cxl.claimed_elsewhere:
            return False
        self.ended = f"another run claimed the node ({reason})"
        return True

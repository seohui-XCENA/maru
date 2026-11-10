# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageService: builds and runs the parts of one node.

Start order: claim the socket, DRAM pool, the node's CXL regions from the RM,
NODE_OPEN, adopted ledgers, old worker drain, heartbeat, then accept on the
UDS. A GPU node with [dram] or [dram, cxl] is served; other configs are
refused. CXL pools are registered and kept, but not used yet.
"""

import logging
import threading
import uuid

from maru_common.storage_types import CloseMode, RmDomain
from maru_shm.client import MaruShmClient

from .config import NodeConfig, NodeConfigError
from .drivers.cxl import DEFAULT_RENEW_S, CxlTier, RegionSource
from .drivers.dram import DramDriver, DramPool
from .local_pin import LocalPins
from .local_server import LocalServer
from .op_ledger import OP_WINDOW
from .peer_watcher import PeerWatcher
from .placement import DEFAULT_KEPT_BACKOFF_S, PlacementPolicy
from .pool_manager import PageState, PoolManager
from .ports import ServerStopped
from .server_client import ServerClient
from .storage_manager import (
    DEFAULT_EVICT_INTERVAL_S,
    DEFAULT_GPU_DRAIN_GRACE_S,
    StorageManager,
)

logger = logging.getLogger(__name__)

# How long a leave waits for the workers to unmap the CXL regions
DEFAULT_UNMAP_TIMEOUT_S = 30.0


class NotServableError(RuntimeError):
    """The config asks for something not supported yet."""


def check_servable(config: NodeConfig) -> None:
    """Refuse configs that cannot be served in full yet."""
    if config.role != "gpu":
        raise NotServableError("serving a memory node is not implemented yet")
    stores = [t.store for t in config.tiers]
    if stores not in (["dram"], ["dram", "cxl"]):
        raise NotServableError(
            f"serving the tiers {stores} is not implemented yet: only [dram] "
            "and [dram, cxl] are served"
        )
    cxl = config.tier("cxl")
    if cxl is not None and cxl.size is None:
        raise NotServableError("the cxl tier needs a size")


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
    ):
        check_servable(config)
        self.config = config
        self.node_epoch = node_epoch or uuid.uuid4().hex
        self.rpc_timeout_s = rpc_timeout_s
        self.gpu_drain_grace_s = gpu_drain_grace_s
        self.evict_interval_s = evict_interval_s
        self.kept_backoff_s = kept_backoff_s
        self.host_boot_id = host_boot_id
        self.op_window = op_window
        self.region_source = region_source
        self.cxl_renew_s = cxl_renew_s
        self.unmap_timeout_s = unmap_timeout_s
        self.dram: DramPool | None = None
        self.cxl: CxlTier | None = None
        # Why the run let go of its CXL regions and stopped, if it did
        self.lost: str | None = None
        self._interrupted = threading.Event()
        self._finished = threading.Event()
        # One stop at a time, and never while a start is half done
        self._stop_lock = threading.Lock()
        self._lifecycle = threading.Lock()
        self._rm_client: MaruShmClient | None = None
        self.pools: PoolManager | None = None
        self.server: ServerClient | None = None
        self.manager: StorageManager | None = None
        self.local: LocalServer | None = None
        self.adopted: list[str] = []
        self._stopped = threading.Event()

    def start(self) -> None:
        """Bring the node up; on failure everything started is undone."""
        try:
            with self._lifecycle:
                self._start()
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
        # Claim the socket first: a second start must fail before NODE_OPEN
        self.local = LocalServer(
            config.socket,
            None,
            node_id=config.node_id,
            node_epoch=self.node_epoch,
            server_address=config.server,
            page_size=config.page_size,
            budgets=config.budgets,
            budget_keys=config.budget_keys,
            pools=[],
            transfer_defaults=config.transfer_defaults(),
            socket_group=config.socket_group,
        )
        self.local.claim()
        self.dram = DramDriver().create(
            top,
            node_id=config.node_id,
            node_epoch=self.node_epoch,
            page_size=config.page_size,
        )
        cxl_tier = config.tier("cxl")
        if cxl_tier is not None:
            # Before NODE_OPEN: only regions this run owns are announced
            source = self.region_source
            if source is None:
                source = self._rm_client = MaruShmClient(address=cxl_tier.rm)
            self.cxl = CxlTier(
                source,
                cxl_tier,
                node_id=config.node_id,
                node_epoch=self.node_epoch,
                page_size=config.page_size,
                on_lost=self._lose_regions,
                renew_s=self.cxl_renew_s,
            )
            self.cxl.claim(self._interrupted)
            if self._interrupted.is_set():
                raise ServerStopped()
            self.cxl.start_renewing()
            self._check_stopped()
        self.pools = PoolManager(config.budgets, config.page_size)
        self.pools.add_pool(self.dram.info())
        self.server = ServerClient(
            config.server,
            config.node_id,
            self.node_epoch,
            rpc_timeout_s=self.rpc_timeout_s,
            host_boot_id=self.host_boot_id,
        )
        placement = PlacementPolicy(high, low, kept_backoff_s=self.kept_backoff_s)
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
        )
        cxl_pools = self.cxl.pools if self.cxl is not None else []
        rm_domains = []
        if self.cxl is not None and self.cxl.rm_domain is not None:
            devices = sorted({p.region.device_uuid for p in cxl_pools} - {""})
            rm_domains = [RmDomain(self.cxl.rm_domain, tuple(devices))]
        opened = self.server.open(
            [self.dram.info()] + [p.info() for p in cxl_pools], rm_domains=rm_domains
        )
        self._check_stopped()
        self.pools.set_epoch_seq(opened.epoch_seq)
        adopted_ids = {info.pool_id for info in opened.adopted}
        for info in opened.adopted:
            # Its committed pages come back READY; no credit from it yet
            self.pools.add_pool(info, creditable=False, state=PageState.QUARANTINED)
            items = self.server.recover_list(info.pool_id)
            refs = self.pools.recovered(
                info.pool_id, [(i.key, i.location) for i in items]
            )
            for ref in refs:
                placement.on_ready(ref)
            self.adopted.append(info.pool_id)
            logger.info(
                "adopted pool %s: %d committed pages, the rest quarantined until "
                "the old workers are confirmed",
                info.pool_id,
                len(refs),
            )
        for pool in cxl_pools:
            if pool.pool_id in adopted_ids:
                continue
            if pool.claimed:
                # The run before may still write it: held until its workers
                # are confirmed, like an adopted pool
                self.pools.add_pool(
                    pool.info(), creditable=False, state=PageState.QUARANTINED
                )
                self.adopted.append(pool.pool_id)
            else:
                self.pools.add_pool(pool.info(), creditable=False)
        self.manager.start()
        self.manager.workers.start_restart_drain(
            opened.old_workers, self.adopted, opened.previous_node_epoch
        )
        self.manager.start_heartbeat()
        self.local.manager = self.manager
        self.local.pools = [(self.dram.descriptor(memfd=0), self.dram.fd)] + [
            (pool.descriptor(), None) for pool in cxl_pools
        ]
        self.local.start()
        logger.info(
            "StorageService %s serving node %s: dram pool %s (%d MiB, pages of %d KiB)",
            self.node_epoch,
            config.node_id,
            self.dram.pool_id,
            self.dram.capacity >> 20,
            config.page_size >> 10,
        )
        if cxl_pools:
            logger.info(
                "cxl tier: %d regions (%d MiB) from RM %s, %d adopted",
                len(cxl_pools),
                sum(p.capacity for p in cxl_pools) >> 20,
                self.cxl.rm_domain if self.cxl else None,
                len(adopted_ids & {p.pool_id for p in cxl_pools}),
            )

    def interrupt(self) -> None:
        """Stop a start still waiting for the RM or MaruServer."""
        self._interrupted.set()
        if self.server is not None:
            self.server.stop()

    def _lose_regions(self, reason: str) -> None:
        """The lease may end: stop with leave so no location keeps pointing at
        the regions and no worker copies into them (spec 4.4). Runs on the
        lease thread, so the stop runs on its own."""
        self.lost = reason
        threading.Thread(
            target=self.stop, args=(CloseMode.LEAVE,), name="maru-cxl-lost"
        ).start()

    def stop(self, mode: CloseMode = CloseMode.RESTART) -> None:
        """Stop serving, then NODE_CLOSE (restart: back soon; leave: gone)."""
        with self._stop_lock:
            if self._stopped.is_set():
                return
            self._stopped.set()
        self.interrupt()
        with self._lifecycle:  # a start still running gives up first
            self._teardown(mode)
        self._finished.set()

    def _teardown(self, mode: CloseMode) -> None:
        if self.cxl is not None:
            self.cxl.stop_renewing(timeout=1.0)
        unmapped: set[str] = set()
        if self.cxl is not None and mode == CloseMode.LEAVE:
            unmapped = self._unmap_cxl()
        if self.local is not None:
            self.local.stop()
        if self.server is not None:
            self.server.stop()
        if self.manager is not None:
            self.manager.stop()
        if self.server is not None:
            if self.server.close_session(mode):
                logger.info("node session closed (%s)", mode.value)
            self.server.close()
        if self.cxl is not None and mode == CloseMode.LEAVE:
            self.cxl.release(unmapped)
        if self._rm_client is not None:
            self._rm_client.close()
        if self.dram is not None:
            self.dram.close()

    def _unmap_cxl(self) -> set[str]:
        """The CXL pools no worker can map any more, before they go back."""
        assert self.cxl is not None
        cxl = self.cxl
        if self.manager is None or self.local is None or not self.local.started:
            # No worker of this run mapped them; the run before may have
            return {p.pool_id for p in cxl.pools if not p.claimed}
        # A new ATTACH maps none of them: its record exists before its reply
        self.local.pools = [(d, fd) for d, fd in self.local.pools if d.dax is None]
        timeout = self.unmap_timeout_s
        if self.lost is not None:  # the RM may free them once the lease ends
            timeout = min(timeout, cxl.time_left() / 2)
        return self.manager.unmap_pools(
            [(p.pool_id, p.pool_generation) for p in cxl.pools],
            max(0.0, timeout),
            old_may_map=set(self.adopted),
        )

    def wait(self, timeout: float | None = None) -> bool:
        """Whether stopping has begun."""
        return self._stopped.wait(timeout)

    def wait_finished(self, timeout: float | None = None) -> bool:
        """Whether a stop has run to its end."""
        return self._finished.wait(timeout)

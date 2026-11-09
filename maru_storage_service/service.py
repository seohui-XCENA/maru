# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageService: builds and runs the parts of one node.

Start order: claim the socket, DRAM pool, NODE_OPEN, adopted ledgers, old
worker drain, heartbeat, then accept on the UDS. Only a GPU node with a
single dram tier is served so far; other configs are refused.
"""

import logging
import threading
import uuid

from maru_common.storage_types import CloseMode

from .config import NodeConfig, NodeConfigError
from .drivers.dram import DramDriver, DramPool
from .local_pin import LocalPins
from .local_server import LocalServer
from .op_ledger import OP_WINDOW
from .peer_watcher import PeerWatcher
from .placement import DEFAULT_KEPT_BACKOFF_S, PlacementPolicy
from .pool_manager import PageState, PoolManager
from .server_client import ServerClient
from .storage_manager import (
    DEFAULT_EVICT_INTERVAL_S,
    DEFAULT_GPU_DRAIN_GRACE_S,
    StorageManager,
)

logger = logging.getLogger(__name__)


class NotServableError(RuntimeError):
    """The config asks for something not supported yet."""


def check_servable(config: NodeConfig) -> None:
    """Refuse configs that cannot be served in full yet."""
    if config.role != "gpu":
        raise NotServableError("serving a memory node is not implemented yet")
    stores = [t.store for t in config.tiers]
    if stores != ["dram"]:
        raise NotServableError(
            f"serving the tiers {stores} is not implemented yet: only a single "
            "dram tier is served"
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
        self.dram: DramPool | None = None
        self.pools: PoolManager | None = None
        self.server: ServerClient | None = None
        self.manager: StorageManager | None = None
        self.local: LocalServer | None = None
        self.adopted: list[str] = []
        self._stopped = threading.Event()

    def start(self) -> None:
        """Bring the node up; on failure everything started is undone."""
        try:
            self._start()
        except BaseException:
            self.stop(CloseMode.RESTART)
            raise

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
        opened = self.server.open([self.dram.info()], rm_domains=[])
        self.pools.set_epoch_seq(opened.epoch_seq)
        for info in opened.adopted:
            # No CXL/SSD pool is asked for, but any adopted ledger is rebuilt;
            # no credit from them (they cannot be mapped yet)
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
        self.manager.start()
        self.manager.workers.start_restart_drain(
            opened.old_workers, self.adopted, opened.previous_node_epoch
        )
        self.manager.start_heartbeat()
        self.local.manager = self.manager
        self.local.pools = [(self.dram.descriptor(memfd=0), self.dram.fd)]
        self.local.start()
        logger.info(
            "StorageService %s serving node %s: dram pool %s (%d MiB, pages of %d KiB)",
            self.node_epoch,
            config.node_id,
            self.dram.pool_id,
            self.dram.capacity >> 20,
            config.page_size >> 10,
        )

    def stop(self, mode: CloseMode = CloseMode.RESTART) -> None:
        """Stop serving, then NODE_CLOSE (restart: back soon; leave: gone)."""
        if self._stopped.is_set():
            return
        self._stopped.set()
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
        if self.dram is not None:
            self.dram.close()

    def wait(self, timeout: float | None = None) -> bool:
        return self._stopped.wait(timeout)

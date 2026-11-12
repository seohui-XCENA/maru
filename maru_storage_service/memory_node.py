# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""A memory node run's NIC and endpoint.

Each run makes its own NIXL agent (no progress thread), exports its L1 pools
whole with it, and serves GPU nodes' remote tiers on its RemoteServer. The
agent ends after every pool was deregistered: a new run is a new agent, so
no connection of an earlier run reaches its pages.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

from maru_common.remote_protocol import RemotePool
from maru_common.storage_types import PoolInfo
from maru_remote.checked import require_checked_transport
from maru_remote.exporter import Export, NixlExporter
from maru_remote.progress import ConnectProgress
from maru_remote.region_map import load_key
from maru_remote.transport import NixlTransport

from .remote_server import RemoteServer, TcpRemoteServer
from .tiers import Exporter

if TYPE_CHECKING:
    from .service import StorageService


class PoolExporter(Exporter, Protocol):
    """An Exporter that tells where a registered pool is."""

    def export(self, pool_id: str) -> Export | None: ...


class RemoteParts:
    """The run's exporter and RemoteServer; ``close`` ends its NIXL agent."""

    def __init__(
        self,
        exporter: PoolExporter,
        server: RemoteServer,
        close: Callable[[], None] = lambda: None,
    ):
        self.exporter = exporter
        self.server = server
        self._close = close

    def remote_pools(self, infos: list[PoolInfo]) -> list[RemotePool]:
        """What peers need to reach each registered pool of ``infos``."""
        pools = []
        for info in infos:
            export = self.exporter.export(info.pool_id)
            if export is not None:
                pools.append(
                    RemotePool(
                        pool_id=info.pool_id,
                        kind=info.kind,
                        pool_generation=info.pool_generation,
                        capacity=info.capacity,
                        base=export.base,
                        metadata=export.metadata,
                    )
                )
        return pools

    def close(self) -> None:
        self._close()


def agent_name(node_id: str, node_epoch: str) -> str:
    return f"maru-mem-{node_id}-{node_epoch[:8]}"


class RunOps:
    """The run's RemoteOps: its PeerOps, set before the server is ready (no
    op is served before)."""

    def __init__(self) -> None:
        self.target: Any = None

    def __getattr__(self, name: str) -> Any:
        if self.target is None:
            raise RuntimeError("the memory node run is not ready")
        return getattr(self.target, name)


def make_remote_parts(service: StorageService) -> RemoteParts:
    """The NIXL agent of the run (UCX limited first), its exporter, and the
    RemoteServer on the configured address."""
    config = service.config
    rs = config.remote_server
    assert rs is not None  # parsing requires it on a memory node
    key = load_key(rs.key_file)
    name = agent_name(config.node_id, service.node_epoch)
    if service.make_transport is not None:
        transport = service.make_transport(name, rs.rdma_device)
    else:
        require_checked_transport(os.environ)
        transport = NixlTransport(name, ucx_device=rs.rdma_device)
    progress = ConnectProgress(transport)
    server = TcpRemoteServer(
        rs.listen,
        key,
        node_id=config.node_id,
        node_epoch=service.node_epoch,
        capabilities=service.capabilities(),
        page_size=config.page_size,
        agent_name=name,
        progress=progress,
        ops=service.remote_ops,
        stage_workers=rs.stage_workers,
        stage_queue=rs.stage_queue,
    )
    return RemoteParts(NixlExporter(transport), server, close=transport.close)

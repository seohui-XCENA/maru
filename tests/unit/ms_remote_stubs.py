# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Stand-ins for a memory node's Exporter and RemoteServer, without a NIC or
a network."""

import errno
import mmap

from maru_common.remote_protocol import RemotePool
from maru_storage_service.remote_server import RemoteServer


class StubExporter:
    """Records the registered pools: ``refused`` ones fail to attach,
    ``stuck`` ones to detach."""

    def __init__(self, refused=(), stuck=(), log: list[str] | None = None):
        self.refused = set(refused)
        self.stuck = set(stuck)
        self.log = [] if log is None else log
        self.registered: dict[str, int] = {}  # pool_id -> registered bytes

    def attach(self, pool_id: str, memory: mmap.mmap) -> None:
        if pool_id in self.refused:
            raise RuntimeError(f"the NIC refused {pool_id}")
        self.registered[pool_id] = len(memory)
        self.log.append(f"attach {pool_id}")

    def detach(self, pool_id: str) -> None:
        if pool_id in self.stuck:
            raise RuntimeError(f"cannot deregister {pool_id}")
        self.registered.pop(pool_id, None)
        self.log.append(f"detach {pool_id}")


class StubRemoteServer(RemoteServer):
    """Records its lifetime in ``log``; ``taken`` fails start as a bound
    address does."""

    def __init__(self, taken: bool = False, log: list[str] | None = None):
        self.taken = taken
        self.log = [] if log is None else log
        self.listening = self.serving = False

    def start(self) -> None:
        if self.taken:
            raise OSError(errno.EADDRINUSE, "address already in use")
        self.listening = True
        self.log.append("remote start")

    def ready(self, server_epoch: str, pools: list[RemotePool]) -> None:
        assert self.listening
        self.serving = True
        self.log.append("remote ready")

    def stop(self) -> None:
        if self.listening:
            self.log.append("remote stop")
        self.listening = self.serving = False

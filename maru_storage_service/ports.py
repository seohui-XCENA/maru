# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""What the StorageManager needs of MaruServer, worker connections and the
process watcher; ServerClient, LocalServer connections and PeerWatcher
implement them."""

from collections.abc import Callable
from typing import Protocol

from maru_common.node_protocol import Notify
from maru_common.protocol import (
    CommitItem,
    CommitResponse,
    NodeHeartbeatResponse,
    RetireResponse,
    WorkerExitResponse,
)
from maru_common.storage_types import ExitCause, KeyLocation, PoolUsage, WorkerRef


class ServerStopped(Exception):  # noqa: N818 - a state, not a failure
    """The client is stopping; the request was given up unresolved."""


class EpochChanged(Exception):  # noqa: N818 - a state, not a failure
    """MaruServer answered EPOCH: the node session is gone."""


class ReplyError(RuntimeError):
    """MaruServer answered OK with the wrong number of items; the op is resolved."""


class MetadataPort(Protocol):
    """The node session with MaruServer. Each op is resent until its result is
    known (OK, REJECTED or DISABLED); EpochChanged, ServerStopped and
    ReplyError end it otherwise."""

    def start_heartbeat(
        self,
        usage: Callable[[], list[PoolUsage]],
        on_response: Callable[[NodeHeartbeatResponse], None],
        on_epoch: Callable[[str], None],
    ) -> None: ...

    def commit(self, items: list[CommitItem]) -> CommitResponse: ...

    def retire(self, items: list[KeyLocation]) -> RetireResponse: ...

    def worker_exit(
        self, workers: list[WorkerRef], cause: ExitCause
    ) -> WorkerExitResponse: ...

    def stop(self) -> None:
        """Give up unresolved ops and stop the heartbeat."""
        ...


class WorkerEndpoint(Protocol):
    """A worker's connection, as used outside its own requests."""

    def close(self) -> None: ...

    def post_notify(self, notify: Notify) -> None: ...


class ProcessWatcher(Protocol):
    """Watches processes by (pid, pid_start) under a key and calls
    ``on_exit(key)`` once when one ends."""

    def start(self, on_exit: Callable[[str], None]) -> None: ...

    def stop(self) -> None: ...

    def watch(self, key: str, pid: int, pid_start: int) -> bool:
        """False if the process already ended."""
        ...

    def unwatch(self, key: str) -> None: ...

    def watching(self, key: str) -> bool: ...

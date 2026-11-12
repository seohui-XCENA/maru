# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Drivers: the resource behind each pool of a tier and what workers get of it.

They do not change the Catalog or the policy; transfers are run by the
TransferEngine.
"""

from enum import StrEnum
from typing import Protocol, runtime_checkable

from maru_common.storage_types import PoolDescriptor, PoolInfo

from ..config_fields import TierConfig


class PoolAccess(StrEnum):
    """How workers reach a pool's bytes."""

    MAPPED = "mapped"  # workers map the pool and copy themselves (DRAM, CXL)
    IO_ONLY = "io_only"  # only the TransferEngine moves its bytes (SSD, peer)


@runtime_checkable
class DriverPool(Protocol):
    """One pool's resource; closed once, when the StorageService stops."""

    @property
    def access(self) -> PoolAccess: ...

    def info(self) -> PoolInfo: ...

    def descriptor(self, memfd: int | None = None) -> PoolDescriptor:
        """How a worker maps a MAPPED pool (IO_ONLY pools are not handed to
        workers); ``memfd`` indexes the fds sent with the ATTACH reply."""
        ...

    def worker_fd(self) -> int | None:
        """The fd sent with ATTACH for workers to map, or None."""
        ...

    def close(self) -> None: ...


class TierDriver(Protocol):
    """Makes the pools of one tier."""

    def create(
        self, tier: TierConfig, *, node_id: str, node_epoch: str, page_size: int
    ) -> DriverPool:
        """A new pool for this run."""
        ...

    def open(self, info: PoolInfo) -> DriverPool:
        """A pool this run did not create, adopted from the run before. CXL
        regions come from the RM through CxlTier (claim, renew, release), which
        does not fit this interface yet."""
        ...

# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Drivers: the resource behind each pool of a tier and what workers get of it.

They do not change the Catalog or the policy; transfers are run by the
TransferEngine.
"""

from enum import StrEnum


class PoolAccess(StrEnum):
    """How workers reach a pool's bytes."""

    MAPPED = "mapped"  # workers map the pool and copy themselves (DRAM, CXL)
    IO_ONLY = "io_only"  # only the TransferEngine moves its bytes (SSD, peer)

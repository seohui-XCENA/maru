# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Peer driver: pools of another node reached over RDMA. Not implemented yet.

Its TierDriver opens a memory node's pool from its PoolInfo as an IO_ONLY
DriverPool (no descriptor, no fd); it creates none, since the memory node makes
its own pools. close() releases the connection and its registered memory.
"""

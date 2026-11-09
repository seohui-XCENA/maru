# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""TransferEngine: submits, completes, cancels and drains byte transfers.

CPU copies between tiers, RDMA and SSD I/O. It waits outside every metadata
lock. Not implemented yet.
"""

# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PoolManager: pages, credit and the ledger of every pool (wire spec 6.2).

Keeps the state of each page (FREE, CREDITED, TRANSFER, PENDING, READY,
RETIRING, QUARANTINED) and its holder while it is written, the bytes in each
state, the commit-pending cap and the transfer headroom. Region refill and
return for CXL pools live here too. Implemented from stage S2.
"""

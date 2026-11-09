# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageManager: runs stores, fetches, demotions and removals in order.

Keeps the op ledger so that a resent request gets the same result, turns
WRITE_DONE into batched COMMIT, and owns the cleanup of partial failures and
UNKNOWN results. A fetch is its responsibility from receipt until hand-off.
Implemented from stage S2.
"""

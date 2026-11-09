# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageService: the pool manager that runs on every node.

One StorageService runs on each GPU node and each memory node. It creates the
node pools, hands pages to workers as credit, commits finished writes to
MaruServer, chooses eviction and demotion candidates, moves bytes between
tiers and nodes, and takes space back once MaruServer approved the removal
and every access ended. MaruServer stays the authority on which KV exists.
"""

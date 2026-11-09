# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PlacementPolicy: first placement, demotion plans and LRU candidates.

Uses the access information MaruServer sends with heartbeats, since reads on
the reference path do not pass through the node. A candidate is not an
approval; it sends no RPC. Implemented from stage S2.
"""

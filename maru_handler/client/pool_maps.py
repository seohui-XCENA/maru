# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PoolMaps: mappings of node pools and their CUDA registration (stage S2).

Maps a DRAM pool from its memfd and page-locks it, maps CXL regions from
their RM handle, and checks that every location MaruServer gives falls inside
a mapped pool (#59).
"""

# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Memory management components for Maru Handler.

Provides:
- DaxMapper: Memory mapping via MaruShmClient (RPC mode, owns all mmap/munmap)
- MarufsMapper: Memory mapping via marufs VFS (fs mode, file-based mmap)
- OwnedRegionManager: RPC mode region management (DaxMapper-based)
- OwnedRegionManagerFs: fs/marufs mode region management (MarufsMapper-based)
- PagedMemoryAllocator: Fixed-size paged allocator (pure page management)
- Types: MappedRegion, MemoryInfo, OwnedRegion, OwnedRegionFs
"""

from .allocator import PagedMemoryAllocator
from .mapper import DaxMapper
from .marufs_mapper import MarufsMappedRegion, MarufsMapper
from .owned_region_manager import OwnedRegionManager
from .owned_region_manager_fs import OwnedRegionFs, OwnedRegionManagerFs
from .types import AllocHandle, MappedRegion, MemoryInfo, OwnedRegion

__all__ = [
    "AllocHandle",
    # RPC mode
    "DaxMapper",
    "MappedRegion",
    "OwnedRegion",
    "OwnedRegionManager",
    # fs/marufs mode
    "MarufsMappedRegion",
    "MarufsMapper",
    "OwnedRegionFs",
    "OwnedRegionManagerFs",
    # Shared
    "MemoryInfo",
    "PagedMemoryAllocator",
]

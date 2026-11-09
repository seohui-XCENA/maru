# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PoolMaps: node pool mappings and their CUDA registration.

Every location is checked against the mapped pools before a byte is touched
(#59).
"""

import logging
import mmap
import os
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from maru_common.storage_types import Location, PoolDescriptor, PoolInfo, PoolKind
from maru_handler.storage.cpu import CpuPool

logger = logging.getLogger(__name__)

# Pools a GPU copy may still write: mapped and registered until the process ends
_KEPT_UNTIL_EXIT: list["MappedPool"] = []


class LocationError(ValueError):
    """A location that does not fall inside a mapped pool (#59)."""


class PoolMapping(Protocol):
    """A pool mapped into this process: views of its bytes, CUDA registration,
    and the unmap once no view is held."""

    @property
    def cuda_registered(self) -> bool: ...

    def view(self, offset: int, length: int) -> memoryview: ...

    def page_lock(self) -> None:
        """Register with CUDA, best effort."""
        ...

    def close(self) -> None:
        """Unregister and unmap; RuntimeError while views are held."""
        ...


def map_dram_memfd(info: PoolInfo, fd: int) -> PoolMapping:
    """A DRAM pool's memfd mapped shared; the fd stays the caller's. Page-locked
    with the cpu backend's code (#128)."""
    size = os.fstat(fd).st_size
    if size < info.capacity:
        raise LocationError(
            f"pool {info.pool_id}: the memfd has {size} bytes, the pool {info.capacity}"
        )
    mapping = mmap.mmap(
        fd,
        info.capacity,
        flags=mmap.MAP_SHARED,
        prot=mmap.PROT_READ | mmap.PROT_WRITE,
    )
    return CpuPool(
        info.pool_id,
        info.capacity,
        info.page_size,
        medium="dram",
        mapping=mapping,
        shared=True,
    )


@dataclass(eq=False)
class MappedPool:
    info: PoolInfo
    node_epoch: str
    mapping: PoolMapping
    # Views handed out and not yet released (PEP 688 export count)
    views: int = 0
    removing: bool = False
    # Replaced while views were held: the last released view unmaps it
    retired: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)


class _PoolSlice:
    """Part of a mapped pool; its views are counted until released or freed."""

    def __init__(self, mapped: MappedPool, offset: int, length: int):
        self._mapped = mapped
        self._offset = offset
        self._length = length

    def __buffer__(self, flags: int) -> memoryview:
        mapped = self._mapped
        with mapped.lock:
            mapped.views += 1
        return mapped.mapping.view(self._offset, self._length)

    def __release_buffer__(self, view: memoryview) -> None:
        view.release()
        mapped = self._mapped
        with mapped.lock:
            mapped.views -= 1
            close = mapped.retired and mapped.views == 0
            if close:
                mapped.retired = False
        if close:
            mapped.mapping.close()


class PoolMaps:
    def __init__(
        self,
        pin_pools: bool = True,
        map_memfd: Callable[[PoolInfo, int], PoolMapping] = map_dram_memfd,
    ):
        self.pin_pools = pin_pools
        self._map_memfd = map_memfd
        self._lock = threading.Lock()
        self._pools: dict[str, MappedPool] = {}

    def add(self, descriptor: PoolDescriptor, fd: int, node_epoch: str) -> MappedPool:
        """Map a DRAM pool from its memfd; the descriptor is closed here."""
        info = descriptor.pool
        try:
            if info.kind != PoolKind.DRAM:
                raise LocationError(
                    f"pool {info.pool_id}: mapping {info.kind} pools comes later"
                )
            if (
                info.capacity <= 0
                or info.page_size <= 0
                or info.capacity % info.page_size
            ):
                raise LocationError(f"pool {info.pool_id}: bad capacity or page size")
            mapping = self._map_memfd(info, fd)
        finally:
            os.close(fd)
        if self.pin_pools:
            mapping.page_lock()
        mapped = MappedPool(info=info, node_epoch=node_epoch, mapping=mapping)
        with self._lock:
            old = self._pools.get(info.pool_id)
            self._pools[info.pool_id] = mapped
        if old is not None:
            _retire(old)
        logger.info(
            "mapped %s (%d MiB, page-locked: %s)",
            info.pool_id,
            info.capacity >> 20,
            mapping.cuda_registered,
        )
        return mapped

    def check(self, location: Location) -> MappedPool:
        """The mapped pool ``location`` lies in; LocationError otherwise."""
        mapped = self._pools.get(location.pool_id)
        if mapped is None:
            raise LocationError(f"pool {location.pool_id!r} is not mapped")
        info = mapped.info
        if location.kind != info.kind:
            raise LocationError(f"kind {location.kind} is not the pool's {info.kind}")
        if (
            location.offset < 0
            or location.length <= 0
            or location.offset % info.page_size
            or location.length > info.page_size
            or location.offset + location.length > info.capacity
        ):
            raise LocationError(
                f"offset {location.offset} length {location.length} is not inside "
                f"a page of {location.pool_id} ({info.capacity} bytes, pages of "
                f"{info.page_size})"
            )
        return mapped

    def view(self, location: Location, length: int | None = None) -> memoryview:
        """Checked bytes of ``location`` (the first ``length``)."""
        n = location.length if length is None else length
        if not 0 < n <= location.length:
            raise LocationError(f"length {n} is not within the location")
        with self._lock:
            mapped = self.check(location)
            if mapped.removing:
                raise LocationError(f"pool {location.pool_id!r} is being unmapped")
            # Counted before the caller sees it
            return memoryview(_PoolSlice(mapped, location.offset, n))

    def get(self, pool_id: str) -> MappedPool | None:
        with self._lock:
            return self._pools.get(pool_id)

    def pool_ids(self) -> list[str]:
        with self._lock:
            return list(self._pools)

    def remove(self, pool_id: str) -> bool:
        """Unregister and unmap; False (nothing touched) while views are held."""
        with self._lock:
            mapped = self._pools.get(pool_id)
            if mapped is None:
                return True
            with mapped.lock:
                if mapped.views:
                    return False
                mapped.removing = True
            del self._pools[pool_id]
        # No view is left, so the unmap after the CUDA unregister succeeds
        mapped.mapping.close()
        return True

    def close(self, keep: set[str] | frozenset[str] = frozenset()) -> None:
        """Unmap every pool but ``keep``, which stay mapped and registered
        until the process ends; RuntimeError if views are still held."""
        errors = []
        for pool_id in self.pool_ids():
            if pool_id in keep:
                with self._lock:
                    _KEPT_UNTIL_EXIT.append(self._pools.pop(pool_id))
            elif not self.remove(pool_id):
                errors.append(pool_id)
        if errors:
            raise RuntimeError(
                f"Release the buffers of {errors} before closing the handler"
            )

    def views(self, pool_id: str) -> int:
        mapped = self.get(pool_id)
        return mapped.views if mapped else 0


def _retire(mapped: MappedPool) -> None:
    """Unmap a replaced mapping now, or when its last view is released."""
    with mapped.lock:
        mapped.removing = True
        if mapped.views:
            mapped.retired = True
            return
    mapped.mapping.close()

# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""One locked, preallocated O_DIRECT file per SSD pool.

Only a new file is initialized. Completed bytes survive a process restart;
boot fencing and extent recovery belong to the caller, not this driver.
"""

import errno
import fcntl
import math
import os
import stat
import struct
import threading
import time
import uuid
from dataclasses import dataclass, field

from maru_common.storage_types import PoolDescriptor, PoolInfo, PoolKind

from ..config import TierConfig
from . import PoolAccess
from .ssd_alignment import (
    DioAlignment,
    SsdAlignmentError,
    aligned_buffer,
    buffer_address,
    direct_alignment,
    round_up,
)

_MAGIC = b"MARUSSD\0"
_VERSION = 1
_HEADER = struct.Struct("<8sI16sQQIIQQ")
DEFAULT_IO_TIMEOUT_S = 0.250
_MAX_REWINDS = 3


class SsdPoolError(RuntimeError):
    """The SSD pool cannot be opened as configured."""


def ssd_pool_id(node_id: str, path: str) -> str:
    return f"ssd:{node_id}:{os.path.realpath(path)}"


def _check_deadline(deadline: float) -> None:
    if time.monotonic() >= deadline:
        raise TimeoutError("SSD I/O deadline expired")


def _transfer(
    fd: int,
    offset: int,
    view: memoryview,
    alignment: DioAlignment,
    *,
    write: bool,
    deadline: float,
) -> None:
    """Finish this extent, retaining the original buffer for aligned retries."""
    cursor = high_water = rewinds = stalls = 0
    step = math.lcm(alignment.memory, alignment.offset)
    syscall = os.pwritev if write else os.preadv
    while cursor < len(view):
        _check_deadline(deadline)
        try:
            with view[cursor:] as tail:
                count = syscall(fd, [tail], offset + cursor)
        except OSError as e:
            if e.errno == errno.EINTR:
                continue
            raise
        _check_deadline(deadline)
        if count <= 0 or count > len(view) - cursor:
            raise OSError(errno.EIO, "SSD I/O made no progress or reached EOF")
        end = cursor + count
        stalls = stalls + 1 if end <= high_water else 0
        high_water = max(high_water, end)
        if stalls >= 2:
            raise OSError(errno.EIO, "SSD I/O repeatedly made no progress")
        if end == len(view):
            return
        cursor = end // step * step
        if cursor != end:
            rewinds += 1
            if rewinds > _MAX_REWINDS:
                raise OSError(errno.EIO, "SSD I/O repeatedly lost alignment")


@dataclass
class SsdPool:
    """A DriverPool. The executor owns each writable, aligned bounce buffer."""

    pool_id: str
    node_id: str
    path: str
    capacity: int
    page_size: int
    fd: int
    device_id: str
    alignment: DioAlignment
    header_size: int
    pool_generation: int = 1
    _cv: threading.Condition = field(default_factory=threading.Condition, repr=False)
    _active: int = field(default=0, init=False, repr=False)
    _closing: bool = field(default=False, init=False, repr=False)

    @property
    def access(self) -> PoolAccess:
        return PoolAccess.IO_ONLY

    def info(self) -> PoolInfo:
        return PoolInfo(
            pool_id=self.pool_id,
            kind=PoolKind.SSD,
            node_id=self.node_id,
            pool_generation=self.pool_generation,
            capacity=self.capacity,
            page_size=self.page_size,
            device_id=self.device_id,
            path=self.path,
        )

    def descriptor(self, memfd: int | None = None) -> PoolDescriptor:
        raise SsdPoolError("an IO_ONLY SSD pool has no worker descriptor")

    def worker_fd(self) -> int | None:
        return None

    def io_size(self, n: int) -> int:
        if not 0 < n <= self.page_size:
            raise ValueError("SSD I/O must fit one page")
        return round_up(n, self.alignment.offset)

    def read(
        self, page_offset: int, n: int, dst, *, deadline: float | None = None
    ) -> int:
        """Read rounded bytes into dst; only its first n bytes are logical data."""
        return self._io(page_offset, dst, n, write=False, deadline=deadline)

    def write(
        self, page_offset: int, src, n: int, *, deadline: float | None = None
    ) -> int:
        """Write n bytes and zero padding from the executor's bounce buffer."""
        return self._io(page_offset, src, n, write=True, deadline=deadline)

    def _io(
        self,
        page_offset: int,
        buffer,
        n: int,
        *,
        write: bool,
        deadline: float | None,
    ) -> int:
        size = self.io_size(n)
        if (
            page_offset < 0
            or page_offset % self.page_size
            or page_offset + self.page_size > self.capacity
        ):
            raise ValueError("SSD page offset is unaligned or outside the pool")
        if deadline is None:
            deadline = time.monotonic() + DEFAULT_IO_TIMEOUT_S
        if not math.isfinite(deadline):
            raise ValueError("SSD I/O deadline must be finite")
        with memoryview(buffer) as raw:
            if raw.readonly or not raw.c_contiguous:
                raise ValueError("SSD I/O needs a writable contiguous bounce buffer")
            with raw.cast("B") as view:
                if len(view) < size or buffer_address(view) % self.alignment.memory:
                    raise ValueError("SSD bounce buffer is too small or unaligned")
                with self._cv:
                    if self._closing or self.fd < 0:
                        raise SsdPoolError("SSD pool is closed")
                    self._active += 1
                    fd = self.fd
                try:
                    _check_deadline(deadline)
                    if write:
                        view[n:size] = b"\0" * (size - n)
                    with view[:size] as extent:
                        _transfer(
                            fd,
                            self.header_size + page_offset,
                            extent,
                            self.alignment,
                            write=write,
                            deadline=deadline,
                        )
                finally:
                    with self._cv:
                        self._active -= 1
                        self._cv.notify_all()
        return n

    def close(self) -> None:
        """Keep the data FD and its lock until every blocking call has ended."""
        with self._cv:
            self._closing = True
            while self._active:
                self._cv.wait()
            if self.fd >= 0:
                os.close(self.fd)
                self.fd = -1


def _open_pool(
    path: str, node_id: str, size: int, page_size: int, *, create: bool
) -> SsdPool:
    if page_size <= 0 or size < page_size:
        raise SsdPoolError(
            "the SSD tier needs a positive page size and at least one page"
        )
    capacity = size // page_size * page_size
    if not os.path.isabs(path):
        raise SsdPoolError("the SSD pool path must be absolute")
    path = os.path.realpath(path)
    try:
        if not stat.S_ISREG(os.stat(path).st_mode):
            raise SsdPoolError("the SSD pool must be a regular file")
    except FileNotFoundError:
        pass
    flags = os.O_RDWR | os.O_CLOEXEC | os.O_DIRECT | os.O_NOFOLLOW
    new = False
    if create:
        try:
            fd = os.open(path, flags | os.O_CREAT | os.O_EXCL, 0o600)
            new = True
        except FileExistsError:
            fd = os.open(path, flags)
    else:
        fd = os.open(path, flags)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise SsdPoolError("the SSD pool must be a regular file")
        alignment = direct_alignment(fd)
        if page_size % alignment.offset:
            raise SsdPoolError(
                "SSD page_size is not a multiple of direct I/O alignment"
            )
        header_size = round_up(max(4096, _HEADER.size), alignment.step)
        total_size = header_size + capacity
        if new:
            os.posix_fallocate(fd, 0, total_size)
        if os.fstat(fd).st_size != total_size:
            raise SsdPoolError("SSD file size does not match the configured pool")
        with aligned_buffer(header_size, alignment.memory) as header:
            deadline = time.monotonic() + DEFAULT_IO_TIMEOUT_S
            identity = uuid.uuid4().bytes if new else None
            if new:
                _HEADER.pack_into(
                    header,
                    0,
                    _MAGIC,
                    _VERSION,
                    identity,
                    page_size,
                    capacity,
                    alignment.memory,
                    alignment.offset,
                    header_size,
                    1,
                )
            _transfer(fd, 0, header, alignment, write=new, deadline=deadline)
            magic, version, identity, p, cap, mem, off, h, generation = (
                _HEADER.unpack_from(header)
            )
            if (magic, version, p, cap, mem, off, h, generation) != (
                _MAGIC,
                _VERSION,
                page_size,
                capacity,
                alignment.memory,
                alignment.offset,
                header_size,
                1,
            ) or not any(identity):
                raise SsdPoolError("SSD header does not match the configured pool")
        return SsdPool(
            pool_id=ssd_pool_id(node_id, path),
            node_id=node_id,
            path=path,
            capacity=capacity,
            page_size=page_size,
            fd=fd,
            device_id=str(uuid.UUID(bytes=identity)),
            alignment=alignment,
            header_size=header_size,
            pool_generation=generation,
        )
    except BaseException:
        os.close(fd)
        raise


def create_ssd_pool(node_id: str, path: str, size: int, page_size: int) -> SsdPool:
    """Create pool.data under path, or validate and reuse the existing file."""
    try:
        if not os.path.isabs(path):
            raise SsdPoolError("the SSD tier path must be an absolute directory")
        os.makedirs(path, mode=0o700, exist_ok=True)
        return _open_pool(
            os.path.join(path, "pool.data"), node_id, size, page_size, create=True
        )
    except (OSError, SsdAlignmentError) as e:
        raise SsdPoolError(f"cannot open SSD pool under {path}: {e}") from e


class SsdDriver:
    """A TierDriver for the SSD tier; node_epoch does not change file identity."""

    def create(
        self, tier: TierConfig, *, node_id: str, node_epoch: str, page_size: int
    ) -> SsdPool:
        if tier.size is None or tier.path is None:
            raise SsdPoolError("the SSD tier needs size and path")
        return create_ssd_pool(node_id, tier.path, tier.size, page_size)

    def open(self, info: PoolInfo) -> SsdPool:
        if info.kind != PoolKind.SSD or not info.path:
            raise SsdPoolError("expected SSD PoolInfo with a data file path")
        try:
            pool = _open_pool(
                info.path, info.node_id, info.capacity, info.page_size, create=False
            )
        except (OSError, SsdAlignmentError) as e:
            raise SsdPoolError(f"cannot reopen SSD pool {info.path}: {e}") from e
        if pool.info() != info:
            pool.close()
            raise SsdPoolError("SSD pool identity does not match PoolInfo")
        return pool

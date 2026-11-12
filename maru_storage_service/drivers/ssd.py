# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""One locked, preallocated O_DIRECT file per SSD pool.

A new file is built under a temporary name and linked into place, so an
interrupted init never blocks a later start. Its header names the node that
owns it (another node cannot open it) and counts the opens: each create()
of an existing file writes the count + 1, sent as pool_generation, so
MaruServer can tell whether another run opened the file in between.
Completed bytes survive a process restart; boot fencing and extent recovery
belong to the caller, not this driver.
"""

import contextlib
import errno
import fcntl
import math
import mmap
import os
import stat
import struct
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

from maru_common.storage_types import Location, PoolDescriptor, PoolInfo, PoolKind

from ..config import TierConfig
from ..page_table import PageRef
from ..tiers import IOResult, L2Tier, Span
from . import PoolAccess
from .io_executor import (
    DEFAULT_IO_QUEUE_SIZE,
    DEFAULT_TRANSFER_BYTES_PER_S,
    IO_MIN_BYTES_PER_S,
    IO_TIMEOUT_S,
    IOExecutor,
    IOTicket,
)
from .ssd_alignment import (
    DioAlignment,
    SsdAlignmentError,
    aligned_buffer,
    buffer_address,
    direct_alignment,
    round_up,
)

_MAGIC = b"MARUSSD\0"
_VERSION = 2
# The owning node_id is the last field (NUL padded): a file is never reused
# by another node, whose pages its Catalog locations would then read
_OWNER_BYTES = 256
_HEADER = struct.Struct(f"<8sI16sQQIIQQ{_OWNER_BYTES}s")
_GENERATION_MAX = (1 << 63) - 1
DEFAULT_IO_TIMEOUT_S = 0.250
# Header I/O runs once per start, off every request path
HEADER_IO_TIMEOUT_S = 10.0
_MAX_REWINDS = 3
# A new file is built here, then linked as pool.data
_INIT_SUFFIX = ".init"


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
    """Finish this extent, retaining the original buffer for aligned retries.
    An extent whose every byte moved is done, also past the deadline."""
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
        if count <= 0 or count > len(view) - cursor:
            raise OSError(errno.EIO, "SSD I/O made no progress or reached EOF")
        end = cursor + count
        if end == len(view):
            return
        _check_deadline(deadline)
        stalls = stalls + 1 if end <= high_water else 0
        high_water = max(high_water, end)
        if stalls >= 2:
            raise OSError(errno.EIO, "SSD I/O repeatedly made no progress")
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
        self,
        page_offset: int,
        n: int,
        dst,
        *,
        deadline: float | None = None,
        fd: int | None = None,
    ) -> int:
        """Read rounded bytes into dst; only its first n bytes are logical
        data. ``fd``: the caller's duplicate of the pool's fd, if any."""
        return self._io(page_offset, dst, n, write=False, deadline=deadline, fd=fd)

    def write(
        self,
        page_offset: int,
        src,
        n: int,
        *,
        deadline: float | None = None,
        fd: int | None = None,
    ) -> int:
        """Write n bytes and zero padding from the executor's bounce buffer."""
        return self._io(page_offset, src, n, write=True, deadline=deadline, fd=fd)

    def _io(
        self,
        page_offset: int,
        buffer,
        n: int,
        *,
        write: bool,
        deadline: float | None,
        fd: int | None,
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
                    if fd is None:
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


def _layout(fd: int, page_size: int) -> tuple[DioAlignment, int]:
    """The file's direct I/O alignment and header size."""
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        raise SsdPoolError("the SSD pool must be a regular file")
    alignment = direct_alignment(fd)
    if page_size % alignment.offset:
        raise SsdPoolError("SSD page_size is not a multiple of direct I/O alignment")
    return alignment, round_up(max(4096, _HEADER.size), alignment.step)


def _header_io(
    fd: int, header: memoryview, alignment: DioAlignment, *, write: bool
) -> None:
    deadline = time.monotonic() + HEADER_IO_TIMEOUT_S
    _transfer(fd, 0, header, alignment, write=write, deadline=deadline)


def _create_file(
    path: str, flags: int, owner: bytes, page_size: int, capacity: int
) -> int:
    """Build a new pool file under a temporary name, then link it as
    ``path``: a crash leaves no half-built pool.data. Returns its locked fd."""
    init = path + _INIT_SUFFIX
    fd = os.open(init, flags | os.O_CREAT, 0o600)
    try:
        # Held by a start building it now; a stale one is rebuilt
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        alignment, header_size = _layout(fd, page_size)
        os.ftruncate(fd, 0)
        os.posix_fallocate(fd, 0, header_size + capacity)
        with aligned_buffer(header_size, alignment.memory) as header:
            _HEADER.pack_into(
                header,
                0,
                _MAGIC,
                _VERSION,
                uuid.uuid4().bytes,
                page_size,
                capacity,
                alignment.memory,
                alignment.offset,
                header_size,
                1,
                owner,
            )
            _header_io(fd, header, alignment, write=True)
        # Never replaces a pool.data another start linked meanwhile
        os.link(init, path)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(init)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _open_pool(
    path: str, node_id: str, size: int, page_size: int, *, create: bool
) -> SsdPool:
    """``create``: build a missing file, and count this open in the header
    of an existing one."""
    if page_size <= 0 or size < page_size:
        raise SsdPoolError(
            "the SSD tier needs a positive page size and at least one page"
        )
    capacity = size // page_size * page_size
    owner = node_id.encode()
    if not owner or len(owner) > _OWNER_BYTES:
        raise SsdPoolError(f"node_id must be 1..{_OWNER_BYTES} bytes for an SSD pool")
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
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        if not create:
            raise
        fd = _create_file(path, flags, owner, page_size, capacity)
        new = True
    try:
        if not new:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        alignment, header_size = _layout(fd, page_size)
        if os.fstat(fd).st_size != header_size + capacity:
            raise SsdPoolError("SSD file size does not match the configured pool")
        with aligned_buffer(header_size, alignment.memory) as header:
            _header_io(fd, header, alignment, write=False)
            magic, version, identity, p, cap, mem, off, h, generation, stored = (
                _HEADER.unpack_from(header)
            )
            if (
                (magic, version, p, cap, mem, off, h)
                != (
                    _MAGIC,
                    _VERSION,
                    page_size,
                    capacity,
                    alignment.memory,
                    alignment.offset,
                    header_size,
                )
                or not 1 <= generation < _GENERATION_MAX
                or not any(identity)
            ):
                raise SsdPoolError("SSD header does not match the configured pool")
            stored = stored.rstrip(b"\0")
            if stored != owner:
                raise SsdPoolError(
                    f"the SSD file {path} belongs to node "
                    f"{stored.decode(errors='replace')!r}, not {node_id!r}: give "
                    "each node its own path, or remove the file to hand it over "
                    "(a new file gets a new UUID)"
                )
            if create and not new:
                # Counted before NODE_OPEN: a run on another MaruServer in
                # between makes the count jump by more than one
                generation += 1
                _HEADER.pack_into(
                    header,
                    0,
                    magic,
                    version,
                    identity,
                    p,
                    cap,
                    mem,
                    off,
                    h,
                    generation,
                    owner,
                )
                _header_io(fd, header, alignment, write=True)
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
    """Create pool.data under path, or validate and reuse the existing file
    (counting this open)."""
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
        """The pool exactly as ``info`` describes it; the open is not
        counted."""
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


class SsdTier(L2Tier):
    """One SSD pool and the I/O executor that moves its pages: reads first,
    writes paced to ``bytes_per_s``, through aligned bounce buffers. The
    owner closes the pool after drain()."""

    name = "ssd"

    def __init__(
        self,
        pool: SsdPool,
        *,
        run: str,
        bytes_per_s: float = DEFAULT_TRANSFER_BYTES_PER_S,
        max_queue: int = DEFAULT_IO_QUEUE_SIZE,
    ):
        self.pool = pool
        self.io = IOExecutor(run, max_queue=max_queue, bytes_per_s=bytes_per_s)
        self._lock = threading.Lock()
        self._memory: dict[str, mmap.mmap] = {}

    def pool_infos(self) -> list[PoolInfo]:
        return [self.pool.info()]

    def describe(self, adopted: set[str]) -> str:
        state = "adopted" if self.pool.pool_id in adopted else "empty"
        return f"{self.pool.path} ({self.pool.capacity >> 20} MiB), {state}"

    def close(self) -> None:
        self.pool.close()

    def io_size(self, nbytes: int) -> int:
        return self.pool.io_size(nbytes)

    def attach(self, pool_id: str, memory: mmap.mmap) -> None:
        with self._lock:
            self._memory[pool_id] = memory

    def detach(self, pool_id: str) -> None:
        with self._lock:
            self._memory.pop(pool_id, None)

    def submit_store(
        self,
        src: Span,
        dst: Location,
        *,
        refs: tuple[PageRef, PageRef],
        callback: Callable[[IOResult], None] | None = None,
        deadline: float | None = None,
    ) -> IOTicket:
        return self._submit(src, dst, True, refs, callback, deadline)

    def submit_load(
        self,
        src: Location,
        dst: Span,
        *,
        refs: tuple[PageRef, PageRef],
        callback: Callable[[IOResult], None] | None = None,
        deadline: float | None = None,
    ) -> IOTicket:
        return self._submit(dst, src, False, refs, callback, deadline)

    def drain(self) -> None:
        self.io.drain()

    def _submit(
        self,
        span: Span,
        page: Location,
        write: bool,
        refs: tuple[PageRef, PageRef],
        callback: Callable[[IOResult], None] | None,
        deadline: float | None,
    ) -> IOTicket:
        """Move ``span`` to (``write``) or from the pool page ``page``. By
        default the I/O gets IO_TIMEOUT_S plus its bytes at
        IO_MIN_BYTES_PER_S from its start."""
        pool, n, offset, page_offset = self.pool, span.nbytes, span.offset, page.offset
        with self._lock:
            mapping = self._memory.get(span.pool_id)
        if (
            mapping is None
            or page.pool_id != pool.pool_id
            or n <= 0
            or offset < 0
            or offset + n > len(mapping)
        ):
            raise ValueError(f"no SSD I/O for {span} and {page}")
        size = pool.io_size(n)

        def work(ticket: IOTicket) -> None:
            limit = deadline
            if limit is None:
                limit = time.monotonic() + IO_TIMEOUT_S + n / IO_MIN_BYTES_PER_S
            bounce = ticket.buffer
            assert bounce is not None
            # The ticket's own fd: valid until its callback returns
            if write:
                with memoryview(mapping) as view:
                    bounce[:n] = view[offset : offset + n]
                pool.write(page_offset, bounce, n, deadline=limit, fd=ticket.fd)
            else:
                pool.read(page_offset, n, bounce, deadline=limit, fd=ticket.fd)
                with memoryview(mapping) as view:
                    view[offset : offset + n] = bounce[:n]
            ticket._state.late = time.monotonic() > limit

        return self.io.submit(
            refs[0],
            refs[1],
            fd=pool.fd,
            nbytes=size,
            write=write,
            work=work,
            callback=callback,
            alignment=pool.alignment.memory,
            resources=(pool, mapping),
        )

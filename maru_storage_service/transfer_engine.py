# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""TransferEngine: copies page bytes between the node's mapped pools.

Copies run on the engine's own thread, outside every metadata lock, paced to
a bandwidth budget. A batch is answered once every copy in it has ended, one
result per copy; after stop() what has not started fails. Blocking I/O uses
a separate bounded executor, with one slot kept available for reads: it
moves pages between a mapped pool and a file pool (SSD) through a reused
bounce buffer.
"""

import collections
import ctypes
import logging
import mmap
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from maru_common.storage_types import Location

from .page_table import PageRef

if TYPE_CHECKING:
    from .drivers.ssd import SsdPool

logger = logging.getLogger(__name__)

# Provisional: leaves most of a CXL device's bandwidth to the workers' reads
DEFAULT_TRANSFER_BYTES_PER_S = 4 << 30
DEFAULT_IO_QUEUE_SIZE = 64
# A file I/O without a deadline gets this long from its start, plus its
# bytes at this rate; one that ends later is late
IO_TIMEOUT_S = 1.0
IO_MIN_BYTES_PER_S = 64 << 20
IO_THREADS = 2
# Reads go first, but a queued write waits at most this long, or this many
# reads, before the next free thread takes it
WRITE_MAX_WAIT_S = 0.05
WRITE_AFTER_READS = 8


class IOQueueFullError(RuntimeError):
    """The executor cannot accept another queued operation."""


@dataclass(eq=False)
class _Bounce:
    """A pre-faulted, aligned bounce buffer, reused across I/Os."""

    mapping: mmap.mmap
    offset: int
    size: int

    @classmethod
    def allocate(cls, size: int, alignment: int) -> "_Bounce":
        mapping = mmap.mmap(-1, size + alignment - 1)
        try:
            base = ctypes.addressof(ctypes.c_char.from_buffer(mapping))
            offset = -base % alignment
            ctypes.memset(base + offset, 0, size)  # fault it in once
        except BaseException:
            mapping.close()
            raise
        return cls(mapping, offset, size)

    def fits(self, size: int, alignment: int) -> bool:
        base = ctypes.addressof(ctypes.c_char.from_buffer(self.mapping))
        return self.size >= size and (base + self.offset) % alignment == 0


@dataclass
class _IOState:
    work: Callable[["IOTicket"], None] | None
    callback: Callable[["IOTicket"], None] | None
    resources: tuple[object, ...]
    fd: int
    alignment: int
    queued_at: float
    bounce: _Bounce | None = None
    buffer: memoryview | None = None
    done: threading.Event = field(default_factory=threading.Event)
    started: bool = False
    finishing: bool = False
    cancelled: bool = False
    ok: bool = False
    late: bool = False
    error: Exception | None = None


@dataclass(frozen=True, eq=False)
class IOTicket:
    """One run's exact pages and resources, retained through its callback.

    The duplicated ``fd`` is valid until the callback returns; ``buffer``
    (aligned, contents undefined) from the start of the work until then. A
    cancellation request does not make a running syscall complete. ``done``
    is set only after work, callback and resource cleanup have ended.
    """

    run: str
    src: PageRef
    dst: PageRef
    nbytes: int
    write: bool
    _owner: "IOExecutor" = field(repr=False)
    _state: _IOState = field(repr=False)

    @property
    def fd(self) -> int:
        return self._state.fd

    @property
    def buffer(self) -> memoryview | None:
        return self._state.buffer

    @property
    def done(self) -> threading.Event:
        return self._state.done

    @property
    def cancelled(self) -> bool:
        return self._state.cancelled

    @property
    def ok(self) -> bool:
        return self._state.ok

    @property
    def late(self) -> bool:
        """Every byte moved, but after the I/O's deadline."""
        return self._state.late

    @property
    def error(self) -> Exception | None:
        return self._state.error


class IOExecutor:
    """Two I/O threads for a single run; drain permanently closes submission.

    Work uses the ticket's fd and aligned bounce buffer. It must return only
    after all syscalls end, and raise on incomplete I/O. The caller acquires
    page I/O references before submit and releases them in the captured
    callback, using the ticket's run and exact generations. No work or callback
    runs under the executor lock. Callbacks must not call drain themselves.
    """

    def __init__(
        self,
        run: str,
        *,
        max_queue: int = DEFAULT_IO_QUEUE_SIZE,
        bytes_per_s: float = DEFAULT_TRANSFER_BYTES_PER_S,
    ):
        if max_queue <= 0 or bytes_per_s <= 0:
            raise ValueError("max_queue and bytes_per_s must be positive")
        self._run = run
        self._max_queue = max_queue
        self._bytes_per_s = bytes_per_s
        self._cv = threading.Condition()
        self._reads: collections.deque[IOTicket] = collections.deque()
        self._writes: collections.deque[IOTicket] = collections.deque()
        self._live: set[IOTicket] = set()
        self._threads: list[threading.Thread] = []
        self._stopping = False
        self._writing = False
        self._next_write_at = 0.0
        # Reads taken while a write waited
        self._reads_passed = 0
        # Free bounce buffers, at most one per thread
        self._bounces: list[_Bounce] = []

    def submit(
        self,
        src: PageRef,
        dst: PageRef,
        *,
        fd: int,
        nbytes: int,
        write: bool,
        work: Callable[[IOTicket], None],
        callback: Callable[[IOTicket], None] | None = None,
        alignment: int = mmap.PAGESIZE,
        resources: tuple[object, ...] = (),
    ) -> IOTicket:
        """Accept without waiting for queue space; reject without taking refs.

        ``nbytes`` is the driver's aligned I/O length, the bounce's size.
        ``resources`` retains driver and page-reference owners until completion.
        Threads are started only for accepted work.
        """
        if nbytes <= 0 or alignment <= 0:
            raise ValueError("nbytes and alignment must be positive")
        with self._cv:
            if self._stopping:
                raise RuntimeError("I/O executor is drained")
            if len(self._reads) + len(self._writes) >= self._max_queue:
                raise IOQueueFullError("I/O submit queue is full")
            owned_fd = os.dup(fd)
            try:
                if not self._threads:
                    try:
                        for i in range(IO_THREADS):
                            thread = threading.Thread(
                                target=self._loop, name=f"maru-io-{i}", daemon=True
                            )
                            thread.start()
                            self._threads.append(thread)
                    except Exception:
                        self._stopping = True
                        self._cv.notify_all()
                        raise
            except Exception:
                os.close(owned_fd)
                raise
            state = _IOState(
                work,
                callback,
                tuple(resources),
                owned_fd,
                alignment,
                time.monotonic(),
            )
            ticket = IOTicket(self._run, src, dst, nbytes, write, self, state)
            self._live.add(ticket)
            (self._writes if write else self._reads).append(ticket)
            self._cv.notify_all()
            return ticket

    def cancel(self, ticket: IOTicket) -> bool:
        """True if queued work was stopped; running work still needs draining."""
        if ticket._owner is not self:
            raise ValueError("ticket belongs to another executor")
        with self._cv:
            if ticket.done.is_set() or ticket._state.finishing:
                return False
            ticket._state.cancelled = True
            if ticket._state.started:
                return False
            queue = self._writes if ticket.write else self._reads
            if ticket not in queue:
                return False
            queue.remove(ticket)
            self._cv.notify_all()
        self._finish(ticket)
        return True

    def drain(self) -> None:
        """Stop submission, cancel queued work, and join work and callbacks."""
        if threading.current_thread() in self._threads:
            raise RuntimeError("cannot drain from an I/O worker")
        with self._cv:
            self._stopping = True
            queued = list(self._reads) + list(self._writes)
            self._reads.clear()
            self._writes.clear()
            for ticket in queued:
                ticket._state.cancelled = True
            self._cv.notify_all()
        for ticket in queued:
            self._finish(ticket)
        for thread in self._threads:
            thread.join()
        with self._cv:
            self._cv.wait_for(lambda: not self._live)
            bounces, self._bounces = self._bounces, []
        for bounce in bounces:
            bounce.mapping.close()

    def _next(self) -> IOTicket | None:
        """Caller holds the lock: the ticket to run now, if any. A paced or
        running write lets reads go; an aged write goes before them."""
        now = time.monotonic()
        if self._writes and not self._writing and now >= self._next_write_at:
            write = self._writes[0]
            aged = (
                now - write._state.queued_at >= WRITE_MAX_WAIT_S
                or self._reads_passed >= WRITE_AFTER_READS
            )
            if aged or not self._reads:
                self._writes.popleft()
                self._writing = True
                self._reads_passed = 0
                self._next_write_at = now + write.nbytes / self._bytes_per_s
                return write
        if self._reads:
            if self._writes:
                self._reads_passed += 1
            return self._reads.popleft()
        return None

    def _loop(self) -> None:
        while True:
            with self._cv:
                while True:
                    if self._stopping:
                        return
                    ticket = self._next()
                    if ticket is not None:
                        break
                    timeout = None
                    if self._writes and not self._writing:
                        timeout = max(0.0, self._next_write_at - time.monotonic())
                    self._cv.wait(timeout)
                ticket._state.started = True
            try:
                assert ticket._state.work is not None
                self._lend(ticket)
                ticket._state.work(ticket)
            except Exception as e:
                ticket._state.error = e
            self._finish(ticket)
            with self._cv:
                if ticket.write:
                    self._writing = False
                self._cv.notify_all()

    def _lend(self, ticket: IOTicket) -> None:
        """A free bounce that fits, else a new one (outside the lock)."""
        state = ticket._state
        with self._cv:
            bounce = next(
                (b for b in self._bounces if b.fits(ticket.nbytes, state.alignment)),
                None,
            )
            if bounce is not None:
                self._bounces.remove(bounce)
        if bounce is None:
            bounce = _Bounce.allocate(ticket.nbytes, state.alignment)
        state.bounce = bounce
        start = bounce.offset
        state.buffer = memoryview(bounce.mapping)[start : start + ticket.nbytes]

    def _give_back(self, bounce: _Bounce) -> None:
        """Keep the largest IO_THREADS bounces; close the rest."""
        with self._cv:
            self._bounces.append(bounce)
            self._bounces.sort(key=lambda b: b.size, reverse=True)
            extra = self._bounces[IO_THREADS:]
            del self._bounces[IO_THREADS:]
        for other in extra:
            other.mapping.close()

    def _finish(self, ticket: IOTicket) -> None:
        state = ticket._state
        with self._cv:
            state.finishing = True
            state.ok = state.started and not state.cancelled and state.error is None
        try:
            if state.callback is not None:
                state.callback(ticket)
        except Exception:
            logger.exception("I/O completion failed for run %s: %s", ticket.run, ticket)
        finally:
            state.work = state.callback = None
            try:
                try:
                    os.close(state.fd)
                finally:
                    state.fd = -1
                    if state.buffer is not None:
                        state.buffer.release()
                    if state.bounce is not None:
                        self._give_back(state.bounce)
                        state.bounce = None
            except Exception:
                logger.exception("I/O resource cleanup failed for run %s", ticket.run)
            finally:
                state.resources = ()
                with self._cv:
                    state.done.set()
                    self._live.remove(ticket)
                    self._cv.notify_all()


@dataclass(frozen=True)
class Copy:
    """``nbytes`` from the start of ``src`` to the start of ``dst``."""

    src: Location
    dst: Location
    nbytes: int


@dataclass(eq=False)
class _Batch:
    copies: list[Copy]
    results: list[bool] = field(default_factory=list)
    done: threading.Event = field(default_factory=threading.Event)


class TransferEngine:
    """Thread safe. The engine owns the mappings it is given and closes them."""

    def __init__(
        self,
        bytes_per_s: float = DEFAULT_TRANSFER_BYTES_PER_S,
        *,
        run: str = "",
        io_max_queue: int = DEFAULT_IO_QUEUE_SIZE,
    ):
        if bytes_per_s <= 0:
            raise ValueError(f"bytes_per_s must be positive, got {bytes_per_s}")
        self.bytes_per_s = bytes_per_s
        self.io = IOExecutor(run, max_queue=io_max_queue, bytes_per_s=bytes_per_s)
        # Held while a copy reads or writes a mapping
        self._lock = threading.Lock()
        self._maps: dict[str, mmap.mmap] = {}
        # IO_ONLY pools: only the I/O executor moves their pages
        self._files: dict[str, SsdPool] = {}
        self._cv = threading.Condition()
        self._queue: collections.deque[_Batch] = collections.deque()
        self._stopped = threading.Event()
        self._thread: threading.Thread | None = None
        # When the budget lets the next copy start
        self._next_at = 0.0
        self.copied_bytes = 0

    def add_pool(self, pool_id: str, mapping: mmap.mmap) -> None:
        with self._lock:
            self._maps[pool_id] = mapping

    def add_file_pool(self, pool_id: str, pool: "SsdPool") -> None:
        """An IO_ONLY pool; its owner closes it after stop()."""
        with self._lock:
            self._files[pool_id] = pool

    def has_pool(self, pool_id: str) -> bool:
        """Whether transfers reach the pool: mapped here, or a file pool."""
        with self._lock:
            return pool_id in self._maps or pool_id in self._files

    def submit_io(
        self,
        copy: Copy,
        src: PageRef,
        dst: PageRef,
        *,
        callback: Callable[[IOTicket], None] | None = None,
        deadline: float | None = None,
    ) -> IOTicket:
        """Move ``copy`` between a mapped page and a file pool page on the I/O
        executor: a write when ``copy.dst`` is in the file pool, else a read.
        ``src`` and ``dst`` are the pages; the caller holds their I/O
        references until ``callback``. ``deadline`` (monotonic) bounds the
        syscalls; by default the I/O gets IO_TIMEOUT_S plus its bytes at
        IO_MIN_BYTES_PER_S from its start. ``ticket.ok`` only once all bytes
        moved (``ticket.late`` if past the deadline). Raises if not accepted
        (e.g. IOQueueFullError, or RuntimeError once drained)."""
        n = copy.nbytes
        with self._lock:
            write = copy.dst.pool_id in self._files
            file = self._files.get(copy.dst.pool_id if write else copy.src.pool_id)
            mapping = self._maps.get(copy.src.pool_id if write else copy.dst.pool_id)
        page_offset = (copy.dst if write else copy.src).offset
        offset = (copy.src if write else copy.dst).offset
        if (
            file is None
            or mapping is None
            or not 0 < n <= min(copy.src.length, copy.dst.length)
            or offset < 0
            or offset + n > len(mapping)
        ):
            raise ValueError(f"no file I/O for {copy}")
        size = file.io_size(n)

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
                file.write(page_offset, bounce, n, deadline=limit, fd=ticket.fd)
            else:
                file.read(page_offset, n, bounce, deadline=limit, fd=ticket.fd)
                with memoryview(mapping) as view:
                    view[offset : offset + n] = bounce[:n]
            ticket._state.late = time.monotonic() > limit

        return self.io.submit(
            src,
            dst,
            fd=file.fd,
            nbytes=size,
            write=write,
            work=work,
            callback=callback,
            alignment=file.alignment.memory,
            resources=(file, mapping),
        )

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._loop, name="maru-transfer", daemon=True
        )
        self._thread.start()

    def run(self, copies: list[Copy]) -> list[bool]:
        """Copy and wait until every copy has ended; True for each that
        completed."""
        batch = _Batch(copies)
        with self._cv:
            if self._stopped.is_set() or self._thread is None:
                return [False] * len(copies)
            self._queue.append(batch)
            self._cv.notify()
        batch.done.wait()
        return batch.results

    def stop(self) -> None:
        """The copy under way ends; the rest fail. Answers every batch."""
        with self._cv:
            self._stopped.set()
            self._cv.notify_all()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join()
        self.io.drain()

    def close(self) -> None:
        """Stop, then unmap every pool (file pools stay open)."""
        self.stop()
        with self._lock:
            maps, self._maps = self._maps, {}
            self._files = {}
        for mapping in maps.values():
            mapping.close()

    def _loop(self) -> None:
        while True:
            with self._cv:
                while not self._queue and not self._stopped.is_set():
                    self._cv.wait()
                if not self._queue:
                    return
                batch = self._queue.popleft()
            for copy in batch.copies:
                try:
                    ok = self._pace(copy.nbytes) and self._copy(copy)
                except Exception:  # every copy gets its answer
                    logger.exception("transfer failed: %s", copy)
                    ok = False
                batch.results.append(ok)
            batch.done.set()

    def _pace(self, nbytes: int) -> bool:
        """Wait until the budget allows ``nbytes``; False if stopped meanwhile."""
        now = time.monotonic()
        start = max(now, self._next_at)
        if start > now and self._stopped.wait(start - now):
            return False
        self._next_at = start + nbytes / self.bytes_per_s
        return not self._stopped.is_set()

    def _copy(self, copy: Copy) -> bool:
        n = copy.nbytes
        if not 0 < n <= min(copy.src.length, copy.dst.length):
            logger.error("transfer of %d bytes does not fit %s", n, copy)
            return False
        with self._lock:
            src = self._maps.get(copy.src.pool_id)
            dst = self._maps.get(copy.dst.pool_id)
            if src is None or dst is None:
                logger.error("transfer between unmapped pools: %s", copy)
                return False
            s, d = copy.src.offset, copy.dst.offset
            try:
                if s < 0 or d < 0 or s + n > len(src) or d + n > len(dst):
                    logger.error("transfer outside its pools: %s", copy)
                    return False
                with memoryview(src) as sv, memoryview(dst) as dv:
                    dv[d : d + n] = sv[s : s + n]
            except (OSError, ValueError, BufferError) as e:
                logger.error("transfer failed: %s (%s)", copy, e)
                return False
            self.copied_bytes += n
        return True

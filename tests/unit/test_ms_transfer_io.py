# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Bounded I/O scheduling and resource lifetime, without an SSD driver."""

import ctypes
import errno
import mmap
import os
import threading
import time
import weakref
from dataclasses import FrozenInstanceError, dataclass, field

import pytest

from maru_common.storage_types import Location, PoolKind
from maru_storage_service.page_table import PageRef
from maru_storage_service.transfer_engine import (
    Copy,
    IOExecutor,
    IOQueueFullError,
    IOTicket,
    TransferEngine,
)

PAGE = mmap.PAGESIZE
WAIT = 5


@dataclass
class Call:
    entered: threading.Event = field(default_factory=threading.Event)
    release: threading.Event = field(default_factory=threading.Event)
    error: Exception | None = None


class FakeDriver:
    def __init__(self):
        self.calls: dict[int, Call] = {}
        self.order: list[int] = []
        self.data: dict[int, bytes] = {}
        self.lock = threading.Lock()
        self.writes = 0
        self.active = 0
        self.max_writes = 0
        self.max_active = 0

    def block(self, index: int) -> Call:
        call = self.calls[index] = Call()
        return call

    def read(self, page_offset: int, n: int, dst: memoryview) -> None:
        dst[:n] = self.data.get(page_offset, b"r" * n)

    def write(self, page_offset: int, src: memoryview, n: int) -> None:
        self.data[page_offset] = bytes(src[:n])

    def work(self, ticket: IOTicket) -> None:
        index = ticket.src.index
        call = self.calls.get(index)
        os.fstat(ticket.fd)
        assert not os.get_inheritable(ticket.fd)
        with self.lock:
            self.order.append(index)
            self.active += 1
            self.writes += ticket.write
            self.max_writes = max(self.max_writes, self.writes)
            self.max_active = max(self.max_active, self.active)
        try:
            if call is not None:
                call.entered.set()
                assert call.release.wait(WAIT), "fake syscall was not released"
                if call.error is not None:
                    raise call.error
            if ticket.write:
                self.write(index * PAGE, ticket.buffer, ticket.nbytes)
            else:
                self.read(index * PAGE, ticket.nbytes, ticket.buffer)
        finally:
            with self.lock:
                self.active -= 1
                self.writes -= ticket.write


@pytest.fixture
def io(tmp_path):
    driver = FakeDriver()
    executors = []
    with (tmp_path / "fake.data").open("w+b") as file:

        def make(run="run-1", **kwargs):
            executor = IOExecutor(run, bytes_per_s=1 << 40, **kwargs)
            executors.append(executor)
            return executor

        def submit(executor, index=0, *, write=False, **kwargs):
            work = kwargs.pop("work", driver.work)
            return executor.submit(
                PageRef("source", index, 100 + index),
                PageRef("destination", index, 200 + index),
                fd=file.fileno(),
                nbytes=PAGE,
                write=write,
                work=work,
                **kwargs,
            )

        yield driver, make, submit
        for call in driver.calls.values():
            call.release.set()
        for executor in executors:
            executor.drain()


def completed(ticket):
    assert ticket.done.wait(WAIT), "ticket did not complete"


def address_of(view) -> int:
    return ctypes.addressof(ctypes.c_char.from_buffer(view))


def test_bounce_is_aligned_reused_and_owned_through_callback(io, monkeypatch):
    driver, make, submit = io
    executor = make()
    driver.data[0] = b"v" * PAGE
    got = []
    allocate, mappings = mmap.mmap, []

    def counted(*args, **kwargs):
        mappings.append(allocate(*args, **kwargs))
        return mappings[-1]

    monkeypatch.setattr(mmap, "mmap", counted)

    def callback(ticket):
        os.fstat(ticket.fd)
        assert not ticket.done.is_set()
        got.append((ticket.ok, bytes(ticket.buffer), address_of(ticket.buffer)))

    read = submit(executor, callback=callback, alignment=2 * PAGE)
    completed(read)
    assert [g[:2] for g in got] == [(True, b"v" * PAGE)]
    assert read.fd == -1
    with pytest.raises(ValueError):
        bytes(read.buffer)

    def write(ticket):
        address = address_of(ticket.buffer)
        assert address % (2 * PAGE) == 0 and address == got[0][2]
        # the read's bounce again: what it holds is undefined
        assert bytes(ticket.buffer) == b"v" * PAGE
        ticket.buffer[:4] = b"tail"
        driver.work(ticket)

    padded = submit(
        executor,
        1,
        write=True,
        work=write,
        alignment=2 * PAGE,
    )
    completed(padded)
    assert padded.ok
    assert driver.data[PAGE] == b"tail" + b"v" * (PAGE - 4)
    # one bounce, faulted in once, for both
    assert len(mappings) == 1 and not mappings[0].closed
    executor.drain()
    assert mappings[0].closed


def test_a_queued_write_is_not_starved_by_reads(io):
    """Reads always queued, as under a steady fetch load: a write still
    starts within its wait bound."""
    driver, make, submit = io
    executor = make(max_queue=64)
    stop, started = threading.Event(), threading.Event()

    def read(ticket):
        time.sleep(0.01)

    def feed():
        while not stop.is_set():
            try:
                submit(executor, 0, work=read)
            except Exception:
                pass
            time.sleep(0.003)

    feeder = threading.Thread(target=feed, daemon=True)
    feeder.start()
    try:
        time.sleep(0.1)
        began = time.monotonic()
        write = submit(executor, 1, write=True, work=lambda t: started.set())
        assert started.wait(2), "the write never started"
        assert time.monotonic() - began < 0.5
    finally:
        stop.set()
        feeder.join(WAIT)
    completed(write)
    assert write.ok


def test_one_write_keeps_a_thread_for_reads(io):
    driver, make, submit = io
    executor = make()
    first, second, read = (driver.block(i) for i in range(3))
    w1 = submit(executor, 0, write=True)
    assert first.entered.wait(WAIT)
    w2 = submit(executor, 1, write=True)
    r = submit(executor, 2)
    assert read.entered.wait(WAIT)
    assert not second.entered.is_set()
    assert driver.max_active == 2
    read.release.set()
    completed(r)
    assert not second.entered.is_set()
    first.release.set()
    assert second.entered.wait(WAIT)
    second.release.set()
    completed(w1)
    completed(w2)
    assert w1.ok and w2.ok and r.ok
    assert driver.max_writes == 1


def test_queued_reads_run_before_queued_writes(io):
    driver, make, submit = io
    executor = make()
    first, second, write, read = (driver.block(i) for i in range(4))
    r1, r2 = submit(executor, 0), submit(executor, 1)
    assert first.entered.wait(WAIT) and second.entered.wait(WAIT)
    w = submit(executor, 2, write=True)
    r3 = submit(executor, 3)
    first.release.set()
    assert read.entered.wait(WAIT)
    assert not write.entered.is_set()
    assert driver.order == [0, 1, 3] or driver.order == [1, 0, 3]
    read.release.set()
    assert write.entered.wait(WAIT)
    second.release.set()
    write.release.set()
    for ticket in (r1, r2, r3, w):
        completed(ticket)
        assert ticket.ok


def test_bounded_queue_and_cancel_before_start(io):
    driver, make, submit = io
    executor = make(max_queue=2)
    first, second = driver.block(0), driver.block(1)
    running = [submit(executor, i) for i in range(2)]
    assert first.entered.wait(WAIT) and second.entered.wait(WAIT)
    callbacks = []
    queued = submit(executor, 2, callback=callbacks.append)
    another = submit(executor, 3, write=True)
    with pytest.raises(IOQueueFullError):
        submit(executor, 4)
    assert executor.cancel(queued)
    completed(queued)
    assert queued.cancelled and not queued.ok
    assert callbacks == [queued]
    assert not executor.cancel(queued)
    replacement = submit(executor, 4)
    executor.cancel(another)
    executor.cancel(replacement)
    first.release.set()
    second.release.set()
    for ticket in running:
        completed(ticket)
    assert set(driver.order) == {0, 1}


@pytest.mark.parametrize("write", [False, True])
def test_cancel_running_keeps_fd_bounce_and_refs_until_syscall_returns(io, write):
    driver, make, submit = io
    executor = make()
    call = driver.block(0)
    callbacks = []

    class Resource:
        pass

    resource = Resource()
    ref = weakref.ref(resource)

    def callback(ticket):
        assert ref() is not None
        os.fstat(ticket.fd)
        assert len(ticket.buffer) == PAGE
        assert not ticket.done.is_set()
        callbacks.append((ticket.run, ticket.src, ticket.dst, ticket.ok))

    ticket = submit(
        executor, write=write, callback=callback, resources=(resource, driver)
    )
    del resource
    assert call.entered.wait(WAIT)
    assert not executor.cancel(ticket)
    assert ticket.cancelled and not ticket.done.is_set()
    assert callbacks == [] and ref() is not None
    os.fstat(ticket.fd)
    assert len(ticket.buffer) == PAGE

    drained = threading.Event()
    drainer = threading.Thread(target=lambda: (executor.drain(), drained.set()))
    drainer.start()
    assert not drained.wait(0.05)
    assert ref() is not None and callbacks == []
    call.release.set()
    assert drained.wait(WAIT)
    drainer.join(WAIT)
    assert ticket.done.is_set() and ref() is None
    assert callbacks == [("run-1", ticket.src, ticket.dst, False)]


def test_drain_cancels_queue_and_waits_for_running_callback(io):
    driver, make, submit = io
    executor = make()
    call = driver.block(0)
    callback_entered, callback_release = threading.Event(), threading.Event()
    queued_callbacks = []

    def callback(ticket):
        callback_entered.set()
        assert callback_release.wait(WAIT)
        os.fstat(ticket.fd)

    running = submit(executor, 0, write=True, callback=callback)
    assert call.entered.wait(WAIT)
    queued = submit(executor, 1, write=True, callback=queued_callbacks.append)
    drainer = threading.Thread(target=executor.drain)
    drainer.start()
    try:
        completed(queued)
        assert queued.cancelled and queued_callbacks == [queued]
        with pytest.raises(RuntimeError, match="drained"):
            submit(executor, 2)
        call.release.set()
        assert callback_entered.wait(WAIT)
        assert not running.done.is_set() and drainer.is_alive()
        assert not executor.cancel(running)
        assert not running.cancelled
    finally:
        callback_release.set()
        call.release.set()
        drainer.join(WAIT)
    assert not drainer.is_alive()
    assert running.done.is_set() and running.ok
    assert driver.order == [0]
    executor.drain()


def test_drain_waits_for_callback_of_concurrently_cancelled_ticket(io):
    driver, make, submit = io
    executor = make()
    call = driver.block(0)
    entered, release = threading.Event(), threading.Event()

    def callback(ticket):
        entered.set()
        assert release.wait(WAIT)

    running = submit(executor, 0, write=True)
    assert call.entered.wait(WAIT)
    queued = submit(executor, 1, write=True, callback=callback)
    canceller = threading.Thread(target=lambda: executor.cancel(queued))
    canceller.start()
    assert entered.wait(WAIT)
    drainer = threading.Thread(target=executor.drain)
    drainer.start()
    try:
        call.release.set()
        completed(running)
        assert not queued.done.is_set() and drainer.is_alive()
    finally:
        release.set()
        canceller.join(WAIT)
        drainer.join(WAIT)
    assert not drainer.is_alive() and queued.done.is_set()


def test_callbacks_keep_their_submitted_run_and_generations(io):
    driver, make, submit = io
    old, new = make("old-run"), make("new-run")
    call = driver.block(0)
    got = []

    def callback(ticket):
        got.append((ticket.run, ticket.src, ticket.dst))

    old_ticket = submit(old, 0, callback=callback)
    assert call.entered.wait(WAIT)
    new_ticket = submit(new, 1, callback=callback)
    completed(new_ticket)
    with pytest.raises(FrozenInstanceError):
        old_ticket.run = "new-run"
    with pytest.raises(FrozenInstanceError):
        old_ticket.dst = new_ticket.dst
    with pytest.raises(ValueError, match="another executor"):
        new.cancel(old_ticket)
    call.release.set()
    completed(old_ticket)
    assert got == [
        ("new-run", PageRef("source", 1, 101), PageRef("destination", 1, 201)),
        ("old-run", PageRef("source", 0, 100), PageRef("destination", 0, 200)),
    ]


@pytest.mark.parametrize("code", [errno.EIO, errno.ENOSPC, errno.EDQUOT])
def test_io_failure_is_reported_after_return_and_executor_keeps_working(io, code):
    driver, make, submit = io
    executor = make()
    call = driver.block(0)
    call.error = OSError(code, "injected failure")
    callbacks = []
    failed = submit(executor, callback=callbacks.append, write=True)
    assert call.entered.wait(WAIT)
    assert not failed.done.is_set() and callbacks == []
    call.release.set()
    completed(failed)
    assert not failed.ok and failed.error is call.error
    assert callbacks == [failed]
    good = submit(executor, 1, write=True)
    completed(good)
    assert good.ok and good.error is None


def test_callback_failure_does_not_lose_completion_or_stop_executor(io, caplog):
    _, make, submit = io
    executor = make()

    def callback(ticket):
        raise RuntimeError("injected callback failure")

    ticket = submit(executor, callback=callback)
    completed(ticket)
    assert ticket.ok and ticket.fd == -1
    assert "injected callback failure" in caplog.text
    another = submit(executor, 1)
    completed(another)
    assert another.ok


def test_callbacks_allow_other_threads_to_submit_and_cancel(io):
    _, make, submit = io
    executor = make()
    nested_done = threading.Event()

    def callback(ticket):
        def nested():
            queued = submit(executor, 1)
            executor.cancel(queued)
            nested_done.set()

        thread = threading.Thread(target=nested)
        thread.start()
        assert nested_done.wait(WAIT)
        thread.join(WAIT)

    ticket = submit(executor, callback=callback)
    completed(ticket)
    assert ticket.ok and nested_done.is_set()


def test_write_pacing_does_not_occupy_the_read_slot(io):
    driver, _, submit = io
    executor = IOExecutor("paced", bytes_per_s=PAGE)
    try:
        first = submit(executor, 0, write=True)
        completed(first)
        second = submit(executor, 1, write=True)
        read = submit(executor, 2)
        completed(read)
        assert read.ok and driver.order == [0, 2]
        assert not second.done.is_set()
    finally:
        executor.drain()
    assert second.cancelled and not second.ok


def test_owned_fd_survives_original_close(io, tmp_path):
    driver, make, _ = io
    executor = make()
    call = driver.block(0)
    with (tmp_path / "original").open("w+b") as original:
        ticket = executor.submit(
            PageRef("s", 0, 1),
            PageRef("d", 0, 2),
            fd=original.fileno(),
            nbytes=PAGE,
            write=False,
            work=driver.work,
        )
        assert call.entered.wait(WAIT)
    os.fstat(ticket.fd)
    call.release.set()
    completed(ticket)
    assert ticket.ok


@pytest.mark.parametrize("kwargs", [{"max_queue": 0}, {"bytes_per_s": 0}])
def test_invalid_executor_limits_are_rejected(kwargs):
    with pytest.raises(ValueError):
        IOExecutor("run", **kwargs)


def test_submit_failure_releases_fd_and_bounce_before_accepting(io, monkeypatch):
    driver, make, submit = io
    executor = make()
    duplicate, allocate, start = os.dup, mmap.mmap, threading.Thread.start
    fds, mappings, callbacks = [], [], []

    def dup(fd):
        result = duplicate(fd)
        fds.append(result)
        return result

    def mapping(*args, **kwargs):
        result = allocate(*args, **kwargs)
        mappings.append(result)
        return result

    def start_thread(thread):
        if thread.name == "maru-io-1":
            raise RuntimeError("injected thread startup failure")
        start(thread)

    monkeypatch.setattr(os, "dup", dup)
    monkeypatch.setattr(mmap, "mmap", mapping)
    monkeypatch.setattr(threading.Thread, "start", start_thread)
    with pytest.raises(RuntimeError, match="startup failure"):
        submit(executor, callback=callbacks.append)
    executor.drain()
    assert driver.order == [] and callbacks == []
    assert all(mapping.closed for mapping in mappings)  # bounces come at start
    assert len(fds) == 1
    with pytest.raises(OSError, match="Bad file descriptor"):
        os.fstat(fds[0])
    with pytest.raises(RuntimeError, match="drained"):
        submit(executor)


def test_bounce_allocation_failure_fails_the_io_and_leaves_executor_usable(
    io, monkeypatch
):
    driver, make, submit = io
    executor = make()
    duplicate = os.dup
    fds, callbacks = [], []

    def dup(fd):
        result = duplicate(fd)
        fds.append(result)
        return result

    def allocate(*args):
        raise OSError(errno.ENOMEM, "injected allocation failure")

    with monkeypatch.context() as patch:
        patch.setattr(os, "dup", dup)
        patch.setattr(mmap, "mmap", allocate)
        failed = submit(executor, callback=callbacks.append)
        completed(failed)
        assert not failed.ok and failed.error.errno == errno.ENOMEM
        assert callbacks == [failed] and driver.order == []
        assert len(fds) == 1
        with pytest.raises(OSError, match="Bad file descriptor"):
            os.fstat(fds[0])
    ticket = submit(executor)
    completed(ticket)
    assert ticket.ok


def test_cpu_copy_still_reports_each_result_and_counts_bytes():
    engine = TransferEngine(1 << 40)
    src, dst = mmap.mmap(-1, PAGE), mmap.mmap(-1, PAGE)
    engine.add_pool("s", src)
    engine.add_pool("d", dst)
    source = Location("s", "s0", 1, 0, PAGE, PoolKind.DRAM)
    dest = Location("d", "d0", 2, 0, PAGE, PoolKind.CXL)
    missing = Location("missing", "x", 3, 0, PAGE, PoolKind.CXL)
    engine.start()
    try:
        src[:4] = b"copy"
        assert engine.run(
            [Copy(source, dest, 4), Copy(source, missing, 4), Copy(source, dest, 0)]
        ) == [True, False, False]
        assert dst[:4] == b"copy" and engine.copied_bytes == 4
        assert not engine.io._threads
        engine.stop()
        assert engine.run([Copy(source, dest, 4)]) == [False]
    finally:
        engine.close()
    assert src.closed and dst.closed


def test_engine_close_drains_io_before_closing_mappings(tmp_path):
    engine = TransferEngine(1 << 40, run="engine-run")
    mapping = mmap.mmap(-1, PAGE)
    engine.add_pool("mapped", mapping)
    entered, release = threading.Event(), threading.Event()
    closer = threading.Thread(target=engine.close)

    def work(ticket):
        entered.set()
        assert release.wait(WAIT)
        assert not mapping.closed
        mapping[:4] = b"safe"

    with (tmp_path / "engine.data").open("w+b") as file:
        ticket = engine.io.submit(
            PageRef("source", 0, 1),
            PageRef("mapped", 0, 2),
            fd=file.fileno(),
            nbytes=PAGE,
            write=False,
            work=work,
        )
        assert entered.wait(WAIT)
        closer.start()
        try:
            assert not ticket.done.wait(0.05)
            assert not mapping.closed
        finally:
            release.set()
            closer.join(WAIT)
            engine.close()
    assert mapping.closed and ticket.done.is_set() and ticket.ok

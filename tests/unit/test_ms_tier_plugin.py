# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""An L2 tier written against tiers.L2Tier alone moves pages through the
TransferEngine: bytes, result flags, and drain before unmap."""

import collections
import mmap
import threading
import time
from dataclasses import dataclass, field

import pytest

from maru_common.storage_types import Location, PoolInfo, PoolKind
from maru_storage_service.page_table import PageRef
from maru_storage_service.tiers import IOResult, L2Tier, Level
from maru_storage_service.transfer_engine import Copy, TransferEngine

PAGE = mmap.PAGESIZE
WAIT = 5
POOL = "ssd:n:/fake"


@dataclass(eq=False)
class FakeResult:
    run: str
    src: PageRef
    dst: PageRef
    ok: bool = False
    late: bool = False
    cancelled: bool = False
    error: Exception | None = None
    done: threading.Event = field(default_factory=threading.Event)


class FakeL2(L2Tier):
    """Pages in a dict, moved on the tier's own thread."""

    name = "fake"

    def __init__(self, run: str):
        self.run = run
        self.pages: dict[int, bytes] = {}
        self.memory: dict[str, mmap.mmap] = {}
        self.events: list[str] = []
        self.entered = threading.Event()
        self.gate = threading.Event()
        self.gate.set()
        self._cv = threading.Condition()
        self._queue: collections.deque = collections.deque()
        self._stopping = False
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def pool_infos(self) -> list[PoolInfo]:
        return [PoolInfo(POOL, PoolKind.SSD, "n", 1, 4 * PAGE, PAGE)]

    def describe(self, adopted: set[str]) -> str:
        return "fake"

    def io_size(self, nbytes: int) -> int:
        return nbytes

    def attach(self, pool_id: str, memory: mmap.mmap) -> None:
        self.memory[pool_id] = memory

    def detach(self, pool_id: str) -> None:
        self.events.append(f"detach {pool_id}")
        del self.memory[pool_id]

    def submit_store(self, src, dst, *, refs, callback=None, deadline=None):
        def work() -> None:
            view = self.memory[src.pool_id]
            self.pages[dst.offset] = bytes(view[src.offset : src.offset + src.nbytes])

        return self._submit(work, refs, callback, deadline)

    def submit_load(self, src, dst, *, refs, callback=None, deadline=None):
        def work() -> None:
            data = self.pages[src.offset][: dst.nbytes]
            self.memory[dst.pool_id][dst.offset : dst.offset + len(data)] = data

        return self._submit(work, refs, callback, deadline)

    def drain(self) -> None:
        with self._cv:
            self._stopping = True
            queued = list(self._queue)
            self._queue.clear()
            self._cv.notify_all()
        for _, result, callback, _ in queued:
            result.cancelled = True
            self._finish(result, callback)
        self._thread.join(WAIT)
        self.events.append("drained")

    def _submit(self, work, refs, callback, deadline) -> IOResult:
        result = FakeResult(self.run, *refs)
        with self._cv:
            if self._stopping:
                raise RuntimeError("drained")
            self._queue.append((work, result, callback, deadline))
            self._cv.notify_all()
        return result

    def _loop(self) -> None:
        while True:
            with self._cv:
                while not self._queue and not self._stopping:
                    self._cv.wait()
                if not self._queue:
                    return
                work, result, callback, deadline = self._queue.popleft()
            self.entered.set()
            assert self.gate.wait(WAIT)
            try:
                work()
                result.ok = True
                result.late = deadline is not None and time.monotonic() > deadline
            except Exception as e:
                result.error = e
            self._finish(result, callback)

    def _finish(self, result: FakeResult, callback) -> None:
        if callback is not None:
            callback(result)
        self.events.append("callback")
        result.done.set()


def loc(pool_id: str, offset: int, kind: PoolKind = PoolKind.DRAM) -> Location:
    return Location(pool_id, "a", 1, offset, PAGE, kind)


@pytest.fixture
def engine():
    engine = TransferEngine(1 << 40)
    memory = mmap.mmap(-1, 4 * PAGE)
    tier = FakeL2("run-1")
    engine.add_pool("mem", memory)
    engine.add_l2(tier)
    engine.start()
    yield engine, tier, memory
    engine.close()


def test_a_store_and_a_load_go_through_the_tier(engine):
    engine, tier, memory = engine
    assert tier.level is Level.L2 and tier.memory == {"mem": memory}
    assert engine.has_pool(POOL)
    memory[PAGE : PAGE + 5] = b"bytes"
    mem_ref, page_ref = PageRef("mem", 1, 1), PageRef(POOL, 2, 1)
    seen = []
    store = engine.submit_io(
        Copy(loc("mem", PAGE), loc(POOL, 2 * PAGE, PoolKind.SSD), 5),
        mem_ref,
        page_ref,
        callback=seen.append,
    )
    assert store.done.wait(WAIT) and store.ok and not store.late
    assert seen == [store] and (store.src, store.dst) == (mem_ref, page_ref)
    assert tier.pages[2 * PAGE] == b"bytes"
    # past its deadline but every byte moved: ok and late, the caller decides
    load = engine.submit_io(
        Copy(loc(POOL, 2 * PAGE, PoolKind.SSD), loc("mem", 3 * PAGE), 5),
        page_ref,
        PageRef("mem", 3, 1),
        deadline=time.monotonic() - 1,
    )
    assert load.done.wait(WAIT) and load.ok and load.late
    assert load.run == "run-1" and memory[3 * PAGE : 3 * PAGE + 5] == b"bytes"
    with pytest.raises(ValueError, match="no file I/O"):
        engine.submit_io(Copy(loc("mem", 0), loc("mem", PAGE), 5), mem_ref, mem_ref)


def test_close_drains_the_tier_before_it_detaches_and_unmaps(engine):
    engine, tier, memory = engine
    tier.gate.clear()
    page_ref = PageRef(POOL, 0, 1)
    running = engine.submit_io(
        Copy(loc("mem", 0), loc(POOL, 0, PoolKind.SSD), 5),
        PageRef("mem", 0, 1),
        page_ref,
    )
    assert tier.entered.wait(WAIT)
    queued = engine.submit_io(
        Copy(loc("mem", PAGE), loc(POOL, PAGE, PoolKind.SSD), 5),
        PageRef("mem", 1, 1),
        PageRef(POOL, 1, 1),
    )
    closer = threading.Thread(target=engine.close)
    closer.start()
    try:
        assert not running.done.wait(0.05)
        assert not memory.closed and "detach mem" not in tier.events
    finally:
        tier.gate.set()
        closer.join(WAIT)
    assert running.ok and queued.cancelled and not queued.ok
    assert tier.events == ["callback", "callback", "drained", "detach mem"]
    assert memory.closed and not engine.has_pool(POOL)
    with pytest.raises(ValueError):
        engine.submit_io(
            Copy(loc("mem", 0), loc(POOL, 0, PoolKind.SSD), 5),
            PageRef("mem", 0, 1),
            page_ref,
        )


class StuckL2(FakeL2):
    """A tier that cannot let go of one mapping."""

    def detach(self, pool_id: str) -> None:
        if pool_id == "stuck":
            raise OSError("still registered")
        super().detach(pool_id)


def test_a_mapping_a_tier_cannot_detach_stays_mapped_and_is_reported():
    engine = TransferEngine(1 << 40)
    tier = StuckL2("run")
    stuck, free = mmap.mmap(-1, PAGE), mmap.mmap(-1, PAGE)
    engine.add_pool("stuck", stuck)
    engine.add_pool("free", free)
    engine.add_l2(tier)
    assert engine.close() == {"stuck"}
    assert not stuck.closed
    assert free.closed


@dataclass(eq=True)
class Unhashable:
    """A dataclass with eq=True and no frozen: not hashable."""

    value: int = 0


class UnhashableL2(FakeL2, Unhashable):
    __hash__ = None  # type: ignore[assignment]


def test_tiers_need_not_be_hashable():
    engine = TransferEngine(1 << 40)
    tier = UnhashableL2("run")
    memory = mmap.mmap(-1, PAGE)
    engine.add_l2(tier)
    engine.add_pool("m", memory)
    assert engine.close() == set()
    assert "drained" in tier.events
    assert memory.closed

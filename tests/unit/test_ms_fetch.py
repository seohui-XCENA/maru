# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""FETCH of SSD pages into DRAM by the node, for workers that agreed
fetch_v1, against the real MaruServer: one read per key for every waiter,
a pin each, cancel and deadline before and after the handoff, the node's
quota, failed reads, and a load larger than DRAM. A worker here is a raw
UDS connection plus its MaruServer session; Events hold the reads."""

import errno
import mmap
import os
import shutil
import tempfile
import threading
import time
from types import SimpleNamespace

import pytest

from maru_common.config import MaruConfig
from maru_common.ms_rpc import PLACEHOLDER_OP
from maru_common.node_protocol import (
    FetchCancelRequest,
    FetchItem,
    FetchRequest,
    NodeMessageType,
    process_start_time,
)
from maru_common.storage_types import (
    FETCH_CAPABILITY,
    ItemOutcome,
    ItemResult,
    Key,
    Location,
    MissReason,
    PoolKind,
    Status,
)
from maru_handler import MaruHandler
from maru_handler.client import pool_maps
from maru_server.catalog import PoolState
from maru_server.rpc_async_server import RpcAsyncServer
from maru_server.server import MaruServer
from maru_storage_service.fetch_manager import CANCEL_MARKS, FLIGHT_WAITERS
from maru_storage_service.pool_manager import PageState
from maru_storage_service.ports import ReplyError
from tests.unit.ms_fakes import FakeWorker, Wire
from tests.unit.ms_node_helpers import (
    PAGE,
    RawWorker,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
)
from tests.unit.ms_node_helpers import wait_until as _wait_until
from tests.unit.test_ms_cxl_tier import FakeRm
from tests.unit.test_ms_demotion import SMALL_CREDIT, Regions, put, value_of
from tests.unit.test_ms_ssd_tier import _free_port, cxl_ssd, dram_ssd

NS = "ns"


@pytest.fixture
def sockdir():
    path = short_tmpdir()
    yield path
    remove_tree(path)


@pytest.fixture
def ssd_dir():
    path = tempfile.mkdtemp(prefix="maru-fetch-", dir="/var/tmp")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def real_server():
    server = MaruServer(enable_cxl=False, session_ttl_s=6.0, heartbeat_s=0.3)
    port = _free_port()
    rpc = RpcAsyncServer(server, host="127.0.0.1", port=port, num_workers=2)
    thread = threading.Thread(target=rpc.start, daemon=True)
    thread.start()
    time.sleep(0.2)
    yield server, f"tcp://127.0.0.1:{port}"
    rpc.stop()
    thread.join(5)
    server.close()


@pytest.fixture
def handlers():
    made = []

    def make(node):
        handler = MaruHandler(
            MaruConfig(
                maru_node=node.config.socket,
                engine_id="eng",
                cache_namespace=NS,
                chunk_size_bytes=PAGE,
                pool_size=PAGE,
                pin_pools=False,
                timeout_ms=300,
                auto_connect=False,
                store_timeout_s=10.0,
            )
        )
        handler.connect()
        made.append(handler)
        return handler

    yield make
    for handler in made:
        try:
            handler.close()
        except Exception:
            pass


def wait_until(pred, what: str = "", timeout: float = 15.0):
    return _wait_until(pred, timeout=timeout, what=what)


def make_node(url, sockdir, ssd_dir, **budgets):
    config = node_config(
        url,
        f"{sockdir}/node.sock",
        pages=16,
        high=0.5,
        low=0.0625,
        budgets=SMALL_CREDIT | budgets,
        ssd_path=ssd_dir,
        ssd_pages=64,
    )
    return start_node(config, host_boot_id="boot-1")


@pytest.fixture
def env(real_server, sockdir, ssd_dir, handlers):
    """A [dram, ssd] node whose first keys were moved to the SSD."""
    server, url = real_server
    made = []

    def make(keys: int = 12, **budgets):
        node = make_node(url, sockdir, ssd_dir, **budgets)
        made.append(node)
        h = handlers(node)
        names = [f"k{i}" for i in range(keys)]
        for name in names:
            assert put(h, name)
        wait_until(lambda: len(on_ssd(server, names)) >= keys // 2, "moved to SSD")
        h.close()
        return node, on_ssd(server, names)

    yield server, make
    for node in made:
        node.stop()


def miss(reason: MissReason | None = None) -> ItemOutcome:
    return ItemOutcome(ItemResult.MISS, reason=reason)


def locations(server, key: str) -> list[Location]:
    service = server.ms_service
    with service.lock:
        return [
            e.location
            for e in service.catalog.locations(Key(NS, key))
            if e.pool.state == PoolState.ACTIVE
        ]


def on_ssd(server, names: list[str]) -> list[str]:
    """Keys only on the SSD."""
    return [
        n
        for n in names
        if (locs := locations(server, n))
        and all(loc.kind == PoolKind.SSD for loc in locs)
    ]


def pins(server, key: str, location: Location) -> int:
    service = server.ms_service
    with service.lock:
        return service.pins.count(Key(NS, key), location)


class Loader:
    """A worker's loader: its UDS connection (fetch_v1) and its MaruServer
    session."""

    def __init__(self, node, server, tp_rank: int = 0):
        self.node = node
        self.service = server.ms_service
        self.raw = RawWorker(node.config.socket)
        reply = self.raw.attach(capabilities=[FETCH_CAPABILITY], tp_rank=tp_rank)
        assert reply.capabilities == [FETCH_CAPABILITY]
        self.session = FakeWorker(
            Wire(self.service),
            SimpleNamespace(node_id=node.config.node_id, epoch=node.node_epoch),
            tp_rank=tp_rank,
        )
        self.session.incarnation = self.session.origin = self.raw.incarnation
        self.session.pid, self.session.pid_start = (
            os.getpid(),
            process_start_time(os.getpid()),
        )
        assert self.session.open().status == Status.OK
        self.dram = None
        if node.dram is not None:  # its memfd comes first
            self.dram = mmap.mmap(
                self.raw.fds[0], node.dram.capacity, prot=mmap.PROT_READ
            )

    @property
    def token(self) -> str:
        return self.session.token

    def items(self, *names) -> list[FetchItem]:
        response = self.session.lookup(*[Key(NS, n) for n in names])
        out = []
        for name, outcome in zip(names, response.items, strict=True):
            assert outcome.result == ItemResult.FETCH, (name, outcome)
            out.append(FetchItem(key=Key(NS, name), plan=outcome.plan))
        return out

    def request(self, items, *, deadline_ms=5000, max_bytes=1 << 30):
        return self.raw.stamp(
            FetchRequest(
                op=PLACEHOLDER_OP,
                worker_session=self.token,
                items=items,
                max_bytes=max_bytes,
                deadline_ms=deadline_ms,
            )
        )

    def send(self, request):
        """The FETCH's future."""
        return self.raw.conn.submit(NodeMessageType.FETCH, request)

    def fetch(self, *names, **kwargs):
        request = self.request(self.items(*names), **kwargs)
        return self.send(request).result(10)

    def cancel(self, request):
        return self.raw.request(
            NodeMessageType.FETCH_CANCEL, FetchCancelRequest(target=request.op)
        )

    def read(self, location: Location) -> bytes:
        return self.dram[location.offset : location.offset + location.length]

    def unpin(self, *pin_ids):
        return self.session.unpin(*pin_ids)

    def close(self):
        if self.dram is not None:
            self.dram.close()
        self.raw.close()


class Reads:
    """Counts the node's SSD reads; ``hold()`` keeps them until ``release``."""

    def __init__(self, node, monkeypatch, error: Exception | None = None):
        self.calls = 0
        self.entered = threading.Event()
        self.gate = threading.Event()
        self.gate.set()
        read = node.ssd.read

        def counted(*args, **kwargs):
            self.calls += 1
            self.entered.set()
            assert self.gate.wait(10)
            if error is not None:
                raise error
            return read(*args, **kwargs)

        monkeypatch.setattr(node.ssd, "read", counted)

    def hold(self):
        self.gate.clear()
        self.entered.clear()
        return self

    def release(self):
        self.gate.set()


def settled(node) -> bool:
    fetches = node.manager.fetches
    return fetches.flights() == 0 and fetches.inflight_bytes == 0


def no_transfer_pages(node) -> bool:
    return node.pools.bytes_in(PageState.TRANSFER, node.dram.pool_id) == 0


# =============================================================================
# One read, a pin each
# =============================================================================


def test_two_workers_share_one_read_and_get_their_own_pins(env, monkeypatch):
    server, make = env
    node, keys = make()
    name = keys[0]
    (source,) = locations(server, name)
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    reads = Reads(node, monkeypatch).hold()
    first = a.send(a.request(a.items(name)))
    assert reads.entered.wait(5)
    second = b.send(b.request(b.items(name)))
    wait_until(lambda: node.manager.fetches.results["joined"] == 1, "joined")
    reads.release()
    one, two = first.result(10).items[0], second.result(10).items[0]
    assert (one.result, two.result) == (ItemResult.HIT, ItemResult.HIT)
    assert one.location == two.location and one.pin_id != two.pin_id
    assert reads.calls == 1
    copy = one.location
    assert copy.kind == PoolKind.DRAM and a.read(copy) == value_of(name)
    assert b.read(copy) == value_of(name)
    # the node pin went after the fan-out; the source pin with FETCH_COMPLETE
    wait_until(lambda: pins(server, name, copy) == 2, "the node pin released")
    assert pins(server, name, source) == 0
    # each UNPIN is its own
    a.unpin(one.pin_id)
    assert pins(server, name, copy) == 1
    b.unpin(two.pin_id)
    assert pins(server, name, copy) == 0
    wait_until(lambda: settled(node), "quota back")
    node.pools.check()
    a.close()
    b.close()


@pytest.mark.parametrize("how", ["cancel", "exit"])
def test_the_leader_leaving_does_not_hold_up_the_others(env, monkeypatch, how):
    server, make = env
    node, keys = make()
    name = keys[0]
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    reads = Reads(node, monkeypatch).hold()
    request = a.request(a.items(name))
    first = a.send(request)
    assert reads.entered.wait(5)
    second = b.send(b.request(b.items(name)))
    wait_until(lambda: node.manager.fetches.results["joined"] == 1, "joined")
    if how == "cancel":
        assert a.cancel(request).status == Status.OK
        answer = first.result(10)
        assert answer.items[0].result == ItemResult.MISS
        assert answer.items[0].reason is None
    else:
        a.session.close()  # its MaruServer session goes with the process
        node.manager.workers.on_process_exit(f"worker:{a.raw.incarnation}")
    reads.release()
    hit = second.result(10).items[0]
    assert hit.result == ItemResult.HIT and b.read(hit.location) == value_of(name)
    assert reads.calls == 1
    wait_until(lambda: pins(server, name, hit.location) == 1, "only b's pin")
    with server.ms_service.lock:
        assert server.ms_service.pins.held_by(a.token) == []
    b.unpin(hit.pin_id)
    wait_until(lambda: settled(node), "quota back")
    node.pools.check()
    a.close()
    b.close()


# =============================================================================
# Cancel and deadline, before and after the handoff
# =============================================================================


def test_a_cancel_before_the_handoff_cleans_up_and_keeps_the_copy(env, monkeypatch):
    server, make = env
    node, keys = make()
    name = keys[0]
    (source,) = locations(server, name)
    loader = Loader(node, server)
    reads = Reads(node, monkeypatch).hold()
    request = loader.request(loader.items(name))
    reply = loader.send(request)
    assert reads.entered.wait(5)
    assert loader.cancel(request).status == Status.OK
    assert reply.result(10).items[0] == miss()
    # the read under way ends; the copy stays as cache, with no pin
    reads.release()
    wait_until(lambda: settled(node), "the flight ended")
    wait_until(lambda: len(locations(server, name)) == 2, "the copy published")
    copy = next(c for c in locations(server, name) if c.kind == PoolKind.DRAM)
    wait_until(lambda: pins(server, name, copy) == 0, "no pin left")
    assert pins(server, name, source) == 0
    assert node.pools.page(node.pools.find(copy)).state == PageState.READY
    assert no_transfer_pages(node)
    # a resend is answered the same
    assert loader.send(request).result(10).items[0] == miss()
    node.pools.check()
    loader.close()


def test_a_cancel_after_the_handoff_leaves_the_pin_to_the_worker(env):
    server, make = env
    node, keys = make()
    name = keys[0]
    loader = Loader(node, server)
    request = loader.request(loader.items(name))
    hit = loader.send(request).result(10).items[0]
    assert hit.result == ItemResult.HIT
    assert loader.cancel(request).status == Status.OK
    wait_until(lambda: pins(server, name, hit.location) == 1, "the node pin gone")
    time.sleep(0.1)
    assert pins(server, name, hit.location) == 1  # the worker's stays
    assert loader.read(hit.location) == value_of(name)
    loader.unpin(hit.pin_id)
    assert pins(server, name, hit.location) == 0
    loader.close()


def test_a_cancel_that_comes_first_stops_its_fetch(env, monkeypatch):
    server, make = env
    node, keys = make()
    loader = Loader(node, server)
    reads = Reads(node, monkeypatch)
    request = loader.request(loader.items(keys[0]))
    assert loader.cancel(request).status == Status.OK
    assert loader.send(request).result(10).items[0] == miss()
    assert reads.calls == 0 and settled(node)
    # another worker's op cannot be cancelled
    other = Loader(node, server, tp_rank=1)
    assert other.cancel(request).status == Status.REJECTED
    loader.close()
    other.close()


def test_a_deadline_answers_timeout_and_the_read_still_lands(env, monkeypatch):
    server, make = env
    node, keys = make()
    name = keys[0]
    loader = Loader(node, server)
    reads = Reads(node, monkeypatch).hold()
    started = time.monotonic()
    reply = loader.send(loader.request(loader.items(name), deadline_ms=150))
    assert reply.result(10).items[0] == miss(MissReason.TIMEOUT)
    assert time.monotonic() - started < 2.0
    reads.release()
    wait_until(lambda: settled(node), "the flight ended")
    wait_until(lambda: len(locations(server, name)) == 2, "the copy published")
    with server.ms_service.lock:
        assert server.ms_service.pins.held_by(loader.token) == []
    assert node.manager.fetches.results["MISS:timeout"] == 1
    node.pools.check()
    loader.close()


# =============================================================================
# Quota and failures
# =============================================================================


def test_the_quota_holds_page_and_bounce_until_the_handoff(env, monkeypatch):
    server, make = env
    # room for one page and its bounce only
    node, keys = make(fetch_request_max_bytes=PAGE, fetch_max_inflight_bytes=2 * PAGE)
    loader = Loader(node, server)
    reads = Reads(node, monkeypatch).hold()
    first = loader.send(loader.request(loader.items(keys[0])))
    assert reads.entered.wait(5)
    bounce = node.ssd.io_size(len(value_of(keys[0])))
    fetches = node.manager.fetches
    assert fetches.inflight_bytes == PAGE + bounce
    # another key does not fit
    busy = loader.fetch(keys[1])
    assert busy.items[0] == miss(MissReason.BUSY)
    # one over the request cap is refused as a whole
    over = loader.fetch(keys[1], keys[2])
    assert over.status == Status.REJECTED and "cap" in over.error
    reads.release()
    hit = first.result(10).items[0]
    assert hit.result == ItemResult.HIT
    wait_until(lambda: settled(node), "quota back")
    assert loader.fetch(keys[1]).items[0].result == ItemResult.HIT
    loader.close()


@pytest.mark.parametrize("code", [errno.EIO, errno.EINVAL])
def test_a_failed_read_is_an_io_error_and_frees_all_it_held(env, monkeypatch, code):
    server, make = env
    node, keys = make()
    name = keys[0]
    (source,) = locations(server, name)
    loader = Loader(node, server)
    Reads(node, monkeypatch, error=OSError(code, os.strerror(code)))
    answer = loader.fetch(name)
    assert answer.items[0] == miss(MissReason.IO_ERROR)
    wait_until(lambda: settled(node), "quota back")
    wait_until(lambda: pins(server, name, source) == 0, "the source unpinned")
    assert locations(server, name) == [source]  # nothing published
    assert no_transfer_pages(node)
    node.pools.check()
    loader.close()


def test_a_resend_gets_the_same_answer_without_a_second_read(env, monkeypatch):
    server, make = env
    node, keys = make()
    loader = Loader(node, server)
    reads = Reads(node, monkeypatch)
    request = loader.request(loader.items(keys[0]))
    first = loader.send(request).result(10)
    assert first.items[0].result == ItemResult.HIT
    assert loader.send(request).result(10) == first
    assert reads.calls == 1
    loader.unpin(first.items[0].pin_id)
    loader.close()


def test_what_is_refused(env):
    server, make = env
    node, keys = make()
    loader = Loader(node, server)
    (item,) = loader.items(keys[0])
    for request, why in [
        (loader.request([item], deadline_ms=0), "deadline_ms"),
        (loader.request([item], max_bytes=PAGE - 1), "cap"),
        (loader.request([]), "items"),
    ]:
        answer = loader.send(request).result(10)
        assert answer.status == Status.REJECTED and why in answer.error
    # a plan of another node, or not on the SSD
    other = FetchItem(
        item.key,
        type(item.plan)(item.plan.source, "another-node", item.plan.bytes),
    )
    dram = FetchItem(
        item.key,
        type(item.plan)(
            Location(node.dram.pool_id, "a", 1, 0, PAGE, PoolKind.DRAM),
            item.plan.source_node,
            item.plan.bytes,
        ),
    )
    answer = loader.send(loader.request([other, dram])).result(10)
    assert [o.result for o in answer.items] == [ItemResult.REJECTED] * 2
    # a worker that did not agree fetch_v1
    plain = RawWorker(node.config.socket)
    assert plain.attach().capabilities == []
    stamped = plain.stamp(
        FetchRequest(PLACEHOLDER_OP, loader.token, [item], 1 << 30, 1000)
    )
    assert plain.request(NodeMessageType.FETCH, stamped).status == Status.DISABLED
    plain.close()
    loader.close()


def test_a_worker_exit_keeps_its_fetched_pins_through_the_gpu_grace(env):
    server, make = env
    node, keys = make()
    name = keys[0]
    loader = Loader(node, server)
    hit = loader.fetch(name).items[0]
    assert hit.result == ItemResult.HIT
    wait_until(lambda: pins(server, name, hit.location) == 1, "the node pin gone")
    node.manager.workers.on_process_exit(f"worker:{loader.raw.incarnation}")
    # its GPU may still read the copy: the pin stays for the grace
    time.sleep(0.1)
    assert pins(server, name, hit.location) == 1
    wait_until(lambda: pins(server, name, hit.location) == 0, "WORKER_EXIT")
    loader.close()


# =============================================================================
# Progress past the DRAM size
# =============================================================================


def test_a_load_larger_than_dram_goes_through_in_windows(env, monkeypatch):
    """40 keys on the SSD, 16 DRAM pages: windows of 4, each unpinned after
    its reads; eviction makes room by dropping fetched copies onto their SSD
    pages."""
    server, make = env
    node, keys = make(keys=40)
    all_names = [f"k{i}" for i in range(40)]
    wait_until(lambda: len(on_ssd(server, all_names)) >= 24, "on the SSD")
    names = on_ssd(server, all_names)
    assert len(names) * PAGE > node.dram.capacity
    writes = []
    write = node.ssd.write
    monkeypatch.setattr(
        node.ssd, "write", lambda *a, **k: writes.append(1) or write(*a, **k)
    )
    loader = Loader(node, server)
    for i in range(0, len(names), 4):
        window = names[i : i + 4]
        answer = loader.fetch(*window)
        assert [o.result for o in answer.items] == [ItemResult.HIT] * len(window), [
            (o.result, o.reason) for o in answer.items
        ]
        for name, outcome in zip(window, answer.items, strict=True):
            assert loader.read(outcome.location) == value_of(name)
        loader.unpin(*[o.pin_id for o in answer.items])
    fetches = node.manager.fetches
    assert fetches.results["reads"] == len(names)
    # fetched copies left by dropping them onto their SSD pages: no rewrite
    assert node.manager.move_results["RETIRED"] > 40
    assert len(writes) <= 40 - len(names)
    assert not fetches.results["MISS:busy"] and not fetches.results["MISS:timeout"]
    wait_until(lambda: settled(node), "quota back")
    node.pools.check()
    loader.close()


# =============================================================================
# Into CXL: [cxl, ssd] fetches into a region, [dram, cxl, ssd] into DRAM
# =============================================================================


@pytest.fixture
def regions(monkeypatch):
    stand_in = Regions()
    monkeypatch.setattr(pool_maps, "CxlRegionMapping", stand_in.worker_map)
    yield stand_in
    stand_in.close()


def start_cxl_node(config, regions):
    return start_node(
        config,
        host_boot_id="boot-1",
        region_source=FakeRm(),
        cxl_renew_s=0.05,
        map_region=regions.node_map,
    )


def region_bytes(node, regions, location: Location) -> bytes:
    """What a worker's mapping of the region holds there."""
    (pool,) = [p for p in node.cxl.pools if p.pool_id == location.pool_id]
    fd = regions.fds[pool.region.handle.region_id]
    return os.pread(fd, location.length, location.offset)


def test_a_cxl_top_tier_fetches_into_a_region(real_server, sockdir, ssd_dir, regions):
    server, url = real_server
    node = start_cxl_node(cxl_ssd(url, sockdir, ssd_dir), regions)
    try:
        by_pool = {p.pool_id: p.region.handle.region_id for p in node.cxl.pools}
        writer = RawWorker(node.config.socket)
        writer.attach()
        names = []
        while len(names) < 12:
            for credit in writer.credit(2 * PAGE).credits:
                name = f"k{len(names)}"
                fd = regions.fds[by_pool[credit.pool_id]]
                os.pwrite(fd, value_of(name), credit.offset)
                done = writer.write_done([credit], [name], len(value_of(name)))
                assert done.status == Status.OK
                names.append(name)
        writer.close()
        wait_until(lambda: len(on_ssd(server, names)) >= 3, "moved to the SSD")
        moved = on_ssd(server, names)[:3]
        loader = Loader(node, server)
        answer = loader.fetch(*moved)
        assert [o.result for o in answer.items] == [ItemResult.HIT] * 3
        for name, outcome in zip(moved, answer.items, strict=True):
            copy = outcome.location
            assert copy.kind == PoolKind.CXL and copy.pool_id in by_pool
            assert region_bytes(node, regions, copy) == value_of(name)
        loader.unpin(*[o.pin_id for o in answer.items])
        wait_until(lambda: settled(node), "quota back")
        node.pools.check()
        loader.close()
    finally:
        node.stop()


def test_dram_over_cxl_over_ssd_fetches_into_dram(
    real_server, sockdir, ssd_dir, regions, handlers
):
    server, url = real_server
    node = start_cxl_node(dram_ssd(url, sockdir, ssd_dir, cxl_pages=8), regions)
    try:
        h = handlers(node)
        names = [f"k{i}" for i in range(24)]
        for name in names:
            assert put(h, name)
        wait_until(lambda: len(on_ssd(server, names)) >= 3, "moved down to the SSD")
        h.close()
        moved = on_ssd(server, names)[:3]
        loader = Loader(node, server)
        answer = loader.fetch(*moved)
        assert [o.result for o in answer.items] == [ItemResult.HIT] * 3
        for name, outcome in zip(moved, answer.items, strict=True):
            assert outcome.location.kind == PoolKind.DRAM
            assert loader.read(outcome.location) == value_of(name)
        loader.unpin(*[o.pin_id for o in answer.items])
        wait_until(lambda: settled(node), "quota back")
        node.pools.check()
        loader.close()
    finally:
        node.stop()


# =============================================================================
# Review fixes: whose session, early cancels, waiters, deadlines, late plans
# =============================================================================


def node_pins(server, node) -> int:
    """Pins the node session holds (source and copy pins)."""
    service = server.ms_service
    with service.lock:
        return len(service.pins.held_by(node.server.session))


def test_a_worker_cannot_fetch_with_another_workers_session(env, monkeypatch):
    server, make = env
    node, keys = make()
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    reads = Reads(node, monkeypatch)
    forged = a.raw.stamp(
        FetchRequest(PLACEHOLDER_OP, b.token, a.items(keys[0]), 1 << 30, 5000)
    )
    answer = a.send(forged).result(10)
    assert answer.items[0].result == ItemResult.REJECTED
    assert reads.calls == 0  # refused before any read
    with server.ms_service.lock:
        assert server.ms_service.pins.held_by(b.token) == []
    # nor by joining another worker's read with a forged session
    reads.hold()
    first = a.send(a.request(a.items(keys[1])))
    assert reads.entered.wait(5)
    joined = b.raw.stamp(
        FetchRequest(PLACEHOLDER_OP, a.token, b.items(keys[1]), 1 << 30, 5000)
    )
    second = b.send(joined)
    wait_until(lambda: node.manager.fetches.results["joined"] == 1, "joined")
    reads.release()
    hit = first.result(10).items[0]
    assert hit.result == ItemResult.HIT
    assert second.result(10).items[0].result == ItemResult.REJECTED
    wait_until(lambda: pins(server, keys[1], hit.location) == 1, "only a's pin")
    a.unpin(hit.pin_id)
    wait_until(lambda: node_pins(server, node) == 0, "no node pin left")
    a.close()
    b.close()


def test_an_unknown_unpin_result_is_retried_then_ends_the_run(env, monkeypatch):
    server, make = env
    node, keys = make()
    loader = Loader(node, server)
    unpin = node.manager.metadata.unpin
    calls = []

    def flaky(ids):
        calls.append(list(ids))
        if len(calls) == 1:
            raise ReplyError("UNPIN: 0 results")
        return unpin(ids)

    monkeypatch.setattr(node.manager.metadata, "unpin", flaky)
    hit = loader.fetch(keys[0]).items[0]
    assert hit.result == ItemResult.HIT
    # the node pin is sent again, not dropped
    wait_until(lambda: pins(server, keys[0], hit.location) == 1, "node pin gone")
    assert len(calls) == 2 and calls[0] == calls[1]
    assert node.manager.ended is None
    loader.unpin(hit.pin_id)

    def broken(ids):
        raise ReplyError("UNPIN: 0 results")

    monkeypatch.setattr(node.manager.metadata, "unpin", broken)
    loader.fetch(keys[1])
    wait_until(lambda: node.manager.ended is not None, "the run ends")
    assert "UNPIN" in node.manager.ended
    loader.close()


def test_early_cancels_are_kept_until_acked(env, monkeypatch):
    server, make = env
    node, keys = make()
    loader = Loader(node, server)
    reads = Reads(node, monkeypatch)
    items = loader.items(keys[0])
    cancelled = [loader.request(items) for _ in range(CANCEL_MARKS)]
    for request in cancelled:
        assert loader.cancel(request).status == Status.OK
    # one more cancel, and a FETCH not cancelled, wait for acks
    assert loader.cancel(loader.request(items)).status == Status.BUSY
    assert loader.send(loader.request(items)).result(10).status == Status.BUSY
    # a cancelled FETCH that comes late never reads
    assert loader.send(cancelled[0]).result(10).items[0] == miss()
    assert reads.calls == 0
    for request in cancelled:
        loader.raw.origin.resolved(request.op.seq)
    hit = loader.fetch(keys[0]).items[0]  # its ack drops the marks
    assert hit.result == ItemResult.HIT and reads.calls == 1
    loader.unpin(hit.pin_id)
    loader.close()


def test_a_flight_takes_a_bounded_number_of_waiters(env, monkeypatch):
    server, make = env
    node, keys = make()
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    reads = Reads(node, monkeypatch).hold()
    first = a.send(a.request(a.items(keys[0])))
    assert reads.entered.wait(5)
    items = b.items(keys[0])
    fetches = node.manager.fetches
    # cancelled waiters leave the flight
    for i in range(FLIGHT_WAITERS + 6):
        request = b.request(items)
        reply = b.send(request)
        wait_until(lambda i=i: fetches.results["joined"] == i + 1, "joined")
        assert b.cancel(request).status == Status.OK
        assert reply.result(10).items[0] == miss()
    with fetches._lock:
        (flight,) = fetches._flights.values()
        assert flight.waiters == []
    waiting = [b.send(b.request(items)) for _ in range(FLIGHT_WAITERS)]
    over = b.send(b.request(items)).result(10).items[0]
    assert over == miss(MissReason.BUSY)
    reads.release()
    hits = [f.result(10).items[0] for f in [first, *waiting]]
    assert all(h.result == ItemResult.HIT for h in hits) and reads.calls == 1
    a.unpin(hits[0].pin_id)
    b.unpin(*[h.pin_id for h in hits[1:]])
    wait_until(lambda: pins(server, keys[0], hits[0].location) == 0, "pins gone")
    wait_until(lambda: node_pins(server, node) == 0, "no node pin left")
    a.close()
    b.close()


def test_each_waiter_waits_for_room_until_its_own_deadline(env, monkeypatch):
    server, make = env
    node, keys = make()
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    fetches = node.manager.fetches
    reserve = fetches._reserve
    short = threading.Event()
    monkeypatch.setattr(fetches, "_reserve", lambda count: short.set() or [])
    first = a.send(a.request(a.items(keys[0]), deadline_ms=150))
    assert short.wait(5)
    second = b.send(b.request(b.items(keys[0]), deadline_ms=5000))
    wait_until(lambda: fetches.results["joined"] == 1, "joined")
    assert first.result(5).items[0] == miss(MissReason.TIMEOUT)
    time.sleep(0.2)  # the flight goes on for b
    assert not second.done()
    monkeypatch.setattr(fetches, "_reserve", reserve)  # room now
    hit = second.result(5).items[0]
    assert hit.result == ItemResult.HIT
    b.unpin(hit.pin_id)
    wait_until(lambda: settled(node), "quota back")
    a.close()
    b.close()


def test_a_late_plan_uses_the_copy_published_before(env, monkeypatch):
    server, make = env
    node, keys = make()
    a, b = Loader(node, server), Loader(node, server, tp_rank=1)
    reads = Reads(node, monkeypatch)
    late = b.request(b.items(keys[0]))  # planned on the SSD, sent later
    first = a.fetch(keys[0]).items[0]
    assert first.result == ItemResult.HIT
    wait_until(lambda: settled(node), "the first flight ended")
    second = b.send(late).result(10).items[0]
    assert second.result == ItemResult.HIT and second.location == first.location
    assert reads.calls == 1
    assert node.manager.fetches.results["published_used"] == 1
    wait_until(lambda: node_pins(server, node) == 0, "the copy's node pin gone")
    assert pins(server, keys[0], first.location) == 2
    a.unpin(first.pin_id)
    b.unpin(second.pin_id)
    assert pins(server, keys[0], first.location) == 0
    node.pools.check()
    a.close()
    b.close()

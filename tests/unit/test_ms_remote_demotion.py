# SPDX-License-Identifier: Apache-2.0
"""Write-back demotion from a GPU node's DRAM to a memory node, on a real
MaruServer: RESERVE, RDMA WRITE (fake NIC), WRITE_DONE (the memory node
commits), FINALIZE_MOVE with no finalizer; a write that does not end DONE
keeps its source and settles later; no room there drops past the high
watermark."""

import threading
import time

import pytest

from maru_common.storage_types import Key, PoolKind
from maru_server.catalog import PoolState
from maru_server.rpc_async_server import RpcAsyncServer
from maru_server.server import MaruServer
from maru_storage_service.config import parse_node_config
from maru_storage_service.pool_manager import PageState
from tests.unit.ms_node_helpers import (
    PAGE,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)
from tests.unit.remote_fakes import key_file, reset_fake_agents
from tests.unit.test_ms_demotion import SMALL_CREDIT, put, value_of
from tests.unit.test_ms_remote_tier import fake_transport, free_port

NS = "ns"


@pytest.fixture(autouse=True)
def _fresh_agents():
    reset_fake_agents()
    yield
    reset_fake_agents()


@pytest.fixture
def ms():
    """A real MaruServer on a loopback port."""
    server = MaruServer(enable_cxl=False, session_ttl_s=6.0, heartbeat_s=0.3)
    port = free_port()
    rpc = RpcAsyncServer(server, host="127.0.0.1", port=port, num_workers=2)
    thread = threading.Thread(target=rpc.start, daemon=True)
    thread.start()
    time.sleep(0.2)
    yield server, f"tcp://127.0.0.1:{port}"
    rpc.stop()
    thread.join(5)
    server.close()


@pytest.fixture
def workers():
    """MaruHandlers attached to a node's socket."""
    from maru_common.config import MaruConfig
    from maru_handler import MaruHandler

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


def memory_config(url: str, port: int, pages: int = 32):
    return parse_node_config(
        {
            "node_id": "mem1",
            "role": "memory",
            "server": url,
            "page_size": PAGE,
            "remote_server": {"listen": f"127.0.0.1:{port}", "key_file": key_file()},
            "tiers": [{"store": "dram", "size": pages * PAGE, "hugepage": False}],
            "budgets": SMALL_CREDIT | {"transfer_reserve_bytes": PAGE},
        }
    )


def gpu_config(url: str, sockdir: str, port: int, *, timeout_s: float = 30):
    return parse_node_config(
        {
            "node_id": "gpu-test",
            "socket": f"{sockdir}/node.sock",
            "server": url,
            "page_size": PAGE,
            "tiers": [
                {
                    "store": "dram",
                    "size": 16 * PAGE,
                    "hugepage": False,
                    "high_watermark": 0.5,
                    "low_watermark": 0.25,
                },
                {
                    "store": "remote",
                    "url": [f"127.0.0.1:{port}"],
                    "timeout_s": timeout_s,
                    "key_file": key_file(),
                },
            ],
            "budgets": SMALL_CREDIT | {"transfer_reserve_bytes": PAGE},
        }
    )


class Pair:
    """A memory node and a GPU node [dram, remote] on one MaruServer."""

    def __init__(self, server, url, *, timeout_s=30, memory_pages=32):
        self.server = server
        self.sockdir = short_tmpdir()
        self.port = free_port()
        self.memory = start_node(
            memory_config(url, self.port, memory_pages), make_transport=fake_transport
        )
        self.gpu = start_node(
            gpu_config(url, self.sockdir, self.port, timeout_s=timeout_s),
            make_transport=fake_transport,
        )
        self.remote = self.gpu.tier("remote")
        wait_until(lambda: self.remote.ready("mem1"), what="remote ready")

    def close(self):
        self.gpu.stop()
        self.memory.stop()
        remove_tree(self.sockdir)

    def locations(self, key: str):
        service = self.server.ms_service
        with service.lock:
            return [
                e.location
                for e in service.catalog.locations(Key(NS, key))
                if e.pool.state == PoolState.ACTIVE
            ]

    def on_memory(self, key: str) -> bool:
        mem_pool = self.memory.tier("dram").pool.pool_id
        return [loc.pool_id for loc in self.locations(key)] == [mem_pool]


@pytest.fixture
def pair(ms):
    server, url = ms
    p = Pair(server, url)
    yield p
    p.close()


def test_a_full_dram_moves_its_pages_to_the_memory_node(pair, workers):
    h = workers(pair.gpu)
    keys = [f"k{i}" for i in range(12)]
    for key in keys:
        assert put(h, key)
    stats = pair.gpu.manager.move_results
    wait_until(lambda: stats["RETIRED"] >= 3, what="pages demoted")
    moved = [k for k in keys if pair.on_memory(k)]
    assert len(moved) >= 3
    mem = pair.memory
    for key in moved:
        (location,) = pair.locations(key)
        assert location.kind == PoolKind.DRAM
        ref = mem.pools.find(location)
        assert ref is not None and mem.pools.page(ref).state == PageState.READY
        data = mem.transfers._maps[location.pool_id]
        assert bytes(data[location.offset : location.offset + location.length]) == (
            value_of(key)
        )
    # A key is lost only when no room below dropped it
    lost = [k for k in keys if not pair.locations(k)]
    assert len(lost) <= stats["DROPPED"]
    assert stats["COMMIT:ADDED"] >= len(moved)
    wait_until(lambda: pair.gpu.manager._remote_writing == 0, what="writes ended")
    pair.gpu.pools.check()
    mem.pools.check()


def test_a_write_not_done_keeps_its_source_until_it_settles(ms, workers):
    server, url = ms
    p = Pair(server, url, timeout_s=0.3)
    try:
        transport = p.remote.transport
        transport.stall()
        h = workers(p.gpu)
        for i in range(10):
            assert put(h, f"k{i}")
        stats = p.gpu.manager.move_results
        wait_until(lambda: p.remote.kept_bytes > 0, what="a write kept")
        wait_until(lambda: stats["COPY_FAILED"] >= 1, what="its failure counted")
        # The sources stay in the GPU's DRAM (unless dropped for room); the
        # memory node quarantined the credits
        lost = [i for i in range(10) if not p.locations(f"k{i}")]
        assert len(lost) <= stats["DROPPED"]
        mem_quarantined = p.memory.peer_ops.quarantined
        wait_until(lambda: mem_quarantined["gpu-test"] > 0, what="memory quarantine")
        kept = p.remote.kept_bytes
        transport.stall(False)
        assert transport.finish_stalled() >= 1
        wait_until(lambda: p.remote.kept_bytes < kept, what="settled")
        # The late DONE committed the copy there; the next demotion only
        # finalizes onto it
        wait_until(lambda: mem_quarantined["gpu-test"] == 0, what="released")
        wait_until(
            lambda: any(p.on_memory(f"k{i}") for i in range(10)), what="moved later"
        )
    finally:
        p.remote.transport.stall(False)
        p.remote.transport.finish_stalled()
        p.close()


def test_no_room_on_the_memory_node_drops_past_the_high_watermark(ms, workers):
    server, url = ms
    p = Pair(server, url)
    try:
        # The memory node gives this peer no room: its quarantine cap is 0
        p.memory.peer_ops.quarantine_max = 0
        h = workers(p.gpu)
        for i in range(12):
            assert put(h, f"k{i}")
        stats = p.gpu.manager.move_results
        wait_until(lambda: stats["DROPPED"] >= 1, what="pages dropped")
        assert not any(p.on_memory(f"k{i}") for i in range(12))
    finally:
        p.close()


def test_the_remote_tier_takes_demotions(ms):
    server, url = ms
    p = Pair(server, url)
    try:
        traits = p.gpu.config.traits()
        assert traits["dram"].demotes_to == "remote"
        assert not traits["remote"].evicts and not traits["remote"].credit
        assert p.gpu.manager.remote == {"remote": p.remote}
        assert threading.active_count() > 0
    finally:
        p.close()

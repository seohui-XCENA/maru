# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Restarts with real processes: MaruServer killed (SIGKILL) and started again
on the same port, and a StorageService killed and started again. Workers are
separate processes (MaruHandler on maru_node); no RM needed."""

import hashlib
import json
import os
import signal
import subprocess
import sys
import time

import pytest

from maru_storage_service.pool_manager import PageState
from tests.integration.test_ms_s2_real_server import (
    RealMaruServer,
    _raw_config,
    subprocess_env,
)
from tests.unit.ms_node_helpers import (
    PAGE,
    WORKTREE,
    Agent,
    AgentError,
    listening,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)

pytestmark = pytest.mark.integration

WINDOW_S = 10.0
SMALL_CREDIT = {
    "credit_max_bytes": 2 * PAGE,
    "credit_total_max_bytes": 4 * PAGE,
    "credit_refill_low": 1 * PAGE,
    "credit_refill_high": 2 * PAGE,
    "commit_pending_max_bytes": 2 * PAGE,
}


@pytest.fixture
def workdir():
    path = short_tmpdir()
    yield path
    remove_tree(path)


@pytest.fixture
def servers(workdir):
    made = []

    def make(port=None):
        server = RealMaruServer(
            workdir, 6.0, 0.3, port=port, recovery_window_s=WINDOW_S
        )
        made.append(server)
        return server

    yield make
    for server in made:
        server.close()


@pytest.fixture
def nodes(workdir):
    made = []

    def make(server, **kwargs):
        node = start_node(
            node_config(server.url, f"{workdir}/node.sock", **kwargs),
            rpc_timeout_s=0.5,
        )
        made.append(node)
        return node

    yield make
    for node in made:
        node.stop()


@pytest.fixture
def agents(workdir):
    made = []

    def make(socket_path: str, engine: str = "eng", connect=True, **kwargs):
        agent = Agent(socket_path, engine=engine, log_dir=workdir, **kwargs)
        made.append(agent)
        if connect:
            agent.connect()
        return agent

    yield make
    for agent in made:
        if agent.proc.poll() is None:
            agent.proc.send_signal(signal.SIGCONT)
        agent.stop()


def page_value(key: str) -> bytes:
    """A whole page of bytes that depend on the key."""
    seed = hashlib.sha256(key.encode()).digest()
    return (seed * (PAGE // len(seed) + 1))[:PAGE]


def store(agent, key: str) -> bool:
    agent.call("wait_credit", bytes=PAGE)
    return agent.call("store", key=key, value=page_value(key).hex())


def try_store(agent, key: str) -> bool:
    """A store that is skipped when no credit is at hand."""
    try:
        agent.call("wait_credit", bytes=PAGE, wait_s=0.5)
    except AgentError:
        return False
    try:
        return agent.call("store", key=key, value=page_value(key).hex())
    except AgentError:
        return False


def readable(agent, key: str) -> bool:
    got = agent.call("retrieve", key=key, hold=False)
    assert got in (None, page_value(key).hex())
    return got is not None


def used_share(node) -> float:
    """Share of the DRAM pool not free: above high, eviction would run."""
    (pool_id,) = node.pools.pool_ids()
    capacity = node.pools.pool_info(pool_id).capacity
    return 1 - node.pools.bytes_in(PageState.FREE, pool_id) / capacity


def restart_server(server, servers):
    """SIGKILL MaruServer and start a new one on the same port."""
    port = server.port
    server.kill()
    return servers(port=port)


def test_maruserver_killed_during_reads_comes_back(servers, nodes, agents):
    server = servers()
    node = nodes(server)
    writer = agents(node.config.socket, "writer")
    reader = agents(node.config.socket, "reader", read_only=True)
    keys = [f"k{i}" for i in range(10)]
    for key in keys:
        assert store(writer, key)
    misses = 0
    for i in range(40):
        if i == 10:
            server = restart_server(server, servers)
        key = keys[i % len(keys)]
        if not readable(reader, key):
            misses += 1  # down, recovering or not re-registered yet
    wait_until(lambda: node.manager.recoveries == 1, timeout=30, what="recovery")
    for key in keys:
        wait_until(lambda key=key: readable(reader, key), timeout=30, what=key)
    stats = node.manager.stats()
    assert stats["last_reregistration"]["added"] == len(keys)
    assert stats["safe_state"] is None
    assert reader.call("state")["server_epoch"] == server.server_epoch
    counters = reader.call("state")["stats"]
    assert counters.get("retrieve_unavailable", 0) >= misses > 0
    assert store(writer, "after")
    assert readable(reader, "after")
    node.pools.check()


def test_a_pinned_page_never_changes_until_released(servers, nodes, agents):
    server = servers()
    node = nodes(server, high=0.5, low=0.25, budgets=SMALL_CREDIT)
    writer = agents(node.config.socket, "writer")
    reader = agents(node.config.socket, "reader", read_only=True)
    assert store(writer, "pinned")
    expected = page_value("pinned").hex()
    assert reader.call("retrieve", key="pinned", hold=True) == expected
    for i in range(4):
        assert store(writer, f"a{i}")
    server = restart_server(server, servers)
    wait_until(lambda: node.manager.recoveries == 1, timeout=30, what="recovery")
    # Writes go on past the high watermark; nothing old may be reused while
    # the reader may still read a page pinned at the ended MaruServer
    stored = 0
    for i in range(30):
        stored += try_store(writer, f"b{i}")  # skipped once the pool is full
        assert reader.call("held", key="pinned") == expected
    assert stored > 0
    assert used_share(node) > 0.5  # above the high watermark
    assert node.manager.workers.server_drain_open()
    waiting = node.manager.stats()["server_drain_waiting"]
    assert set(waiting) == {reader.incarnation}
    assert node.pools.bytes_in(PageState.RETIRING) == 0
    assert sum(node.manager.retire_results.values()) == 0
    reader.call("release", key="pinned")
    wait_until(
        lambda: not node.manager.workers.server_drain_open(), timeout=30, what="drain"
    )
    wait_until(
        lambda: sum(node.manager.retire_results.values()) > 0,
        timeout=30,
        what="eviction again",
    )
    assert store(writer, "later")
    assert readable(reader, "later")
    node.pools.check()


def test_an_unresponsive_worker_pauses_eviction_until_it_exits(servers, nodes, agents):
    server = servers()
    node = nodes(server, high=0.5, low=0.25, budgets=SMALL_CREDIT)
    writer = agents(node.config.socket, "writer")
    silent = agents(node.config.socket, "silent", read_only=True)
    for i in range(6):
        assert store(writer, f"a{i}")
    silent.proc.send_signal(signal.SIGSTOP)  # alive, never answers
    server = restart_server(server, servers)
    wait_until(lambda: node.manager.recoveries == 1, timeout=30, what="recovery")
    wait_until(
        lambda: (
            set(node.manager.stats()["server_drain_waiting"]) == {silent.incarnation}
        ),
        timeout=30,
        what="only the silent worker left",
    )
    for i in range(10):
        try_store(writer, f"b{i}")  # skipped once no page is free
    time.sleep(1.0)
    assert used_share(node) > 0.5  # above the high watermark
    assert sum(node.manager.retire_results.values()) == 0  # eviction paused
    assert node.manager.workers.server_drain_open()
    silent.kill()  # its exit is confirmed by the process watcher
    wait_until(
        lambda: not node.manager.workers.server_drain_open(), timeout=30, what="drain"
    )
    wait_until(
        lambda: sum(node.manager.retire_results.values()) > 0,
        timeout=30,
        what="eviction again",
    )
    assert node.pools.bytes_in(PageState.QUARANTINED) == 0
    assert store(writer, "later")


def _storage_cli(config_path: str, log_path: str) -> subprocess.Popen:
    with open(log_path, "ab") as log:
        return subprocess.Popen(
            [sys.executable, "-m", "maru_storage_service", "--config", config_path],
            env=subprocess_env(),
            cwd=WORKTREE,
            stdout=log,
            stderr=subprocess.STDOUT,
        )


def _write_config(path: str, config) -> None:
    with open(path, "w") as f:
        json.dump(_raw_config(config), f)


def test_storage_service_killed_and_back(servers, workdir, agents):
    server = servers()
    socket_path = f"{workdir}/node.sock"
    config = node_config(server.url, socket_path)
    path = os.path.join(workdir, "node.json")
    log_path = os.path.join(workdir, "maru-storage.log")
    _write_config(path, config)
    proc = _storage_cli(path, log_path)
    try:
        wait_until(lambda: listening(socket_path), timeout=30, what="listening")
        worker = agents(socket_path, "worker")
        assert store(worker, "before")
        old_epoch = worker.call("state")["node_epoch"]
        proc.kill()  # SIGKILL
        proc.wait(10)
        proc = _storage_cli(path, log_path)
        wait_until(
            lambda: worker.call("state")["node_epoch"] not in (None, old_epoch),
            timeout=30,
            what="attached to the new run",
        )
        assert not readable(worker, "before")  # DRAM of the old run is dropped
        assert store(worker, "after")
        assert readable(worker, "after")
        assert worker.call("state")["off"] is None

        # Started again with smaller pages: the worker's objects no longer fit
        old_epoch = worker.call("state")["node_epoch"]
        proc.kill()
        proc.wait(10)
        _write_config(path, node_config(server.url, socket_path, page_size=PAGE // 2))
        proc = _storage_cli(path, log_path)
        wait_until(lambda: listening(socket_path), timeout=30, what="listening")
        wait_until(lambda: worker.call("state")["off"], timeout=30, what="off")
        state = worker.call("state")
        assert "larger than the page size" in state["off"]
        with pytest.raises(AgentError, match="Maru is off"):
            worker.call("store", key="no", value=page_value("no").hex())
        assert worker.call("retrieve", key="after", hold=False) is None
        attaches = state["stats"].get("attach", 0)
        time.sleep(2.0)
        assert worker.call("state")["stats"].get("attach", 0) == attaches
        # a worker whose objects fit is served by the new run
        small = agents(socket_path, "small", chunk=PAGE // 2)
        small.call("wait_credit", bytes=PAGE // 2)
        assert small.call("store", key="s", value=(b"s" * 64).hex())
        assert small.call("retrieve", key="s", hold=False) == (b"s" * 64).hex()
    except BaseException:
        with open(log_path, "rb") as f:
            print("maru-storage output:\n" + f.read().decode(errors="replace")[-6000:])
        raise
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(10)

# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Write-back demotion of a [dram, cxl] node against the real MaruServer:
COMMIT with a finalizer and FINALIZE_MOVE end to end, with workers (separate
processes) reading while pages move. memfds stand in for the CXL regions; a
FakeRm gives them, so no RM is needed."""

import dataclasses
import os

import pytest

from maru_common.storage_types import Key, PoolKind
from maru_storage_service.pool_manager import PageState
from tests.integration.test_ms_s2_real_server import RealMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    Agent,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
)
from tests.unit.test_ms_cxl_tier import FakeRm
from tests.unit.test_ms_demotion import Regions

pytestmark = pytest.mark.integration


@pytest.fixture
def workdir():
    path = short_tmpdir()
    yield path
    remove_tree(path)


@pytest.fixture
def server(workdir):
    server = RealMaruServer(workdir, 6.0, 0.3)
    yield server
    server.close()


@pytest.fixture
def regions():
    stand_in = Regions()
    yield stand_in
    stand_in.close()


@pytest.fixture
def node(server, workdir, regions):
    config = node_config(
        server.url,
        f"{workdir}/node.sock",
        pages=16,
        high=0.5,
        low=0.0625,  # demote nearly everything once past half
        cxl_pages=32,
        region_pages=16,
        budgets={
            "credit_max_bytes": 2 * PAGE,
            "credit_total_max_bytes": 4 * PAGE,
            "credit_refill_low": PAGE,
            "credit_refill_high": 2 * PAGE,
            "commit_pending_max_bytes": 2 * PAGE,
        },
    )
    cxl = dataclasses.replace(config.tiers[1], rm="127.0.0.1:1")  # never a real RM
    node = start_node(
        dataclasses.replace(config, tiers=(config.tiers[0], cxl)),
        rpc_timeout_s=0.5,
        region_source=FakeRm(),
        cxl_renew_s=0.05,
        map_region=regions.node_map,
    )
    yield node
    node.stop()


@pytest.fixture
def agents(workdir, node, regions):
    made = []
    paths = {rid: f"/proc/{os.getpid()}/fd/{fd}" for rid, fd in regions.fds.items()}

    def make(engine: str):
        agent = Agent(
            node.config.socket, engine=engine, log_dir=workdir, cxl_paths=paths
        )
        made.append(agent)
        agent.connect()
        return agent

    yield make
    for agent in made:
        agent.stop()


def value(key: str) -> str:
    return (key * 64).encode().hex()


def store(agent, key: str) -> bool:
    agent.call("wait_credit", bytes=PAGE)
    return agent.call("store", key=key, value=value(key))


def read(agent, key: str) -> str | None:
    return agent.call("retrieve", key=key, hold=False)


def kinds_of(node, key: str) -> set[PoolKind]:
    """Where the node's ledger has the key READY (in step with the Catalog)."""
    out = set()
    for pool_id in node.pools.pool_ids():
        for ref in node.pools.pages_in(pool_id, PageState.READY):
            page = node.pools.page(ref)
            if page is not None and page.key == Key("ns", key):
                out.add(page.location.kind)
    return out


def test_pages_move_to_cxl_and_stay_readable_while_read(node, agents):
    w = agents("writer")
    r = agents("reader")
    assert set(w.call("state")["pools"]) == {node.tier("dram").pool.pool_id} | {
        p.pool_id for p in node.tier("cxl").pools
    }
    assert store(w, "k0")
    assert r.call("retrieve", key="k0") == value("k0")  # held: a pin in DRAM
    i = 1
    while not node.manager.move_results["KEPT:PINNED"]:
        assert store(w, f"k{i}")
        i += 1
        assert i < 100, node.manager.move_results
    # COMMIT with a finalizer, then FINALIZE_MOVE: the others moved, k0 has two places
    moves = node.manager.move_results
    assert moves["RETIRED"] > 0 and moves["COMMIT:ADDED"] >= moves["RETIRED"]
    assert kinds_of(node, "k0") == {PoolKind.DRAM, PoolKind.CXL}
    assert r.call("held", key="k0") == value("k0")  # the reader is undisturbed
    moved = [f"k{j}" for j in range(1, i) if kinds_of(node, f"k{j}") == {PoolKind.CXL}]
    assert moved
    for key in moved:  # read from CXL by a worker of another process
        assert read(r, key) == value(key)
    r.call("release", key="k0")
    # pressure again: its copy gets a finalizer again (COMMIT of the same
    # location)
    # and FINALIZE_MOVE removes the source
    added = node.manager.move_results["COMMIT:ADDED"]
    while kinds_of(node, "k0") != {PoolKind.CXL}:
        assert store(w, f"k{i}")
        i += 1
        assert i < 200, (node.manager.move_results, node.manager.retire_results)
    assert read(w, "k0") == value("k0")
    assert node.manager.move_results["COMMIT:ADDED"] > added
    assert not node.manager.move_results["REJECTED"]
    node.pools.check()

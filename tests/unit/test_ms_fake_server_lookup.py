# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""The fake MaruServer chooses lookup results as MaruServer does."""

import time

import pytest

from maru_common.storage_types import (
    ItemResult,
    Key,
    Location,
    MissReason,
    PoolKind,
    SessionRole,
)
from tests.unit.ms_fake_server import Entry, FakeMaruServer, NodeRec, WorkerRec

KEY = Key("ns", "k")


def location(pool_id: str, kind: PoolKind) -> Location:
    return Location(pool_id, "a", 1, 0, 4096, kind)


@pytest.fixture
def server():
    server = FakeMaruServer()
    for node_id in ("a", "b"):
        server.nodes[node_id] = NodeRec(node_id, "run", "s", 1, {}, time.monotonic())
    yield server
    server.close()


def worker() -> WorkerRec:
    return WorkerRec(
        "t", "inc", "a", "run", "e", 0, 0, 1, 1, "ns", SessionRole.WORKER, 0.0
    )


def add(server: FakeMaruServer, node_id: str, pool_id: str, kind: PoolKind) -> None:
    entry = Entry(KEY, location(pool_id, kind), 4096, node_id)
    server.catalog.setdefault(KEY, []).append(entry)


def test_another_nodes_memory_copy_is_not_a_fetch_source(server):
    add(server, "b", "dram:b:1", PoolKind.DRAM)
    assert server._find(KEY, worker()) == (ItemResult.MISS, MissReason.OFFLINE)


def test_the_own_ssd_copy_is_fetched_before_another_nodes_memory(server):
    add(server, "b", "dram:b:1", PoolKind.DRAM)
    add(server, "a", "ssd:a:/x", PoolKind.SSD)
    result, found = server._find(KEY, worker())
    assert result == ItemResult.FETCH and found.node_id == "a"

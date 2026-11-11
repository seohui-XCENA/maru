# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruClient across node runs: a credit of an ended run, and pools being
installed by an attach."""

import pytest

from maru_common.config import MaruConfig
from maru_common.storage_types import Location, PoolKind
from maru_handler.client import CreditUnavailableError
from maru_handler.client.maru_client import MaruClient
from maru_handler.client.pool_maps import PoolMaps
from maru_storage_service.drivers import dram

M = 1 << 20


class FakeMapping:
    cuda_registered = False

    def __init__(self):
        self.views = 0
        self.closed = False

    def view(self, offset, length):
        self.views += 1
        return memoryview(bytearray(length))

    def page_lock(self):
        pass

    def close(self):
        self.closed = True


@pytest.fixture
def pool():
    p = dram.create_dram_pool("n", "e", 2 * M, M, hugepage=False)
    yield p
    p.close()


@pytest.fixture
def client():
    c = MaruClient(
        MaruConfig(
            maru_node="/nonexistent/node.sock",
            engine_id="e",
            cache_namespace="ns",
            auto_connect=False,
            pin_pools=False,
        )
    )
    c.pools = PoolMaps(pin_pools=False)
    c._ensure = lambda data=True: None
    c._storage = type("Conn", (), {"closed": False})()
    c._writes = True
    return c


def test_a_credit_of_an_ended_run_gets_no_view_of_the_new_runs_pool(client, pool):
    old, new = FakeMapping(), FakeMapping()
    info = pool.info()
    client.pools._install(info, old, "e1")
    client.node_epoch = "e1"
    credit = Location(info.pool_id, "a1", 1, 0, M, PoolKind.DRAM)
    client.credits.reset("e1")
    assert client.credits.put([credit], "e1")
    # the run ends, its mapping is closed and the next run maps the same pool
    assert client.pools.remove(info.pool_id)
    assert client.pools.flush(5)
    client.pools._install(info, new, "e2")
    client.node_epoch = "e2"
    with pytest.raises(CreditUnavailableError):
        client.alloc(10)
    assert new.views == 0 and not client._handles
    assert client._counters()["store_skipped_stale_run"] == 1


def test_an_attach_in_progress_keeps_its_new_mappings(client, pool):
    info = pool.info()
    client.node_epoch = "e1"
    client.pools._install(info, FakeMapping(), "e2")  # installed, not switched yet
    client._attaching_epoch = "e2"
    client._unmap_unused_old_pools()  # a read released meanwhile
    assert client.pools.get(info.pool_id) is not None
    client._attaching_epoch = None
    client.node_epoch = "e2"
    client._unmap_unused_old_pools()
    assert client.pools.get(info.pool_id) is not None


def test_removing_leaves_a_mapping_installed_since(client, pool):
    info = pool.info()
    maps = client.pools
    first = maps._install(info, FakeMapping(), "e1")
    second = maps._install(info, FakeMapping(), "e2")
    assert maps.remove(info.pool_id, expected=first)  # nothing to do
    assert maps.get(info.pool_id) is second
    assert maps.remove(info.pool_id, expected=second)
    assert maps.get(info.pool_id) is None

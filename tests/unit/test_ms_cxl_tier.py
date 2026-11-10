# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""The cxl tier: claiming the node's regions from the RM, the lease, letting
go, and the StorageService registering them."""

import threading

import pytest

from maru_common.storage_types import CloseMode, PoolKind, Visibility
from maru_shm.client import (
    NodeClaim,
    NodeHostConflictError,
    NodeRegion,
    StaleNodeEpochError,
)
from maru_shm.types import MaruHandle
from maru_storage_service.config import TierConfig
from maru_storage_service.drivers import PoolAccess
from maru_storage_service.drivers.cxl import CxlTier, CxlTierError, cxl_pool_id
from maru_storage_service.pool_manager import PageState
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)

DOMAIN = "rm-test-id"
TIER = TierConfig(
    store="cxl",
    size=4 * PAGE,
    region_size=2 * PAGE,
    rm="127.0.0.1:1",
    visibility=Visibility.COHERENT,
)


class FakeRm:
    """The RM's node requests over an in-memory owner table."""

    def __init__(self, lease_s: float = 900.0):
        self.lease_s = lease_s
        self.regions: dict[str, list[NodeRegion]] = {}
        self.epochs: dict[str, str] = {}
        self.next_id = 1
        self.unreachable = 0  # requests that fail before one gets through
        self.capacity: int | None = None  # bytes it may still give
        self.renew_error: Exception | None = None
        self.released: list[tuple[str, str]] = []
        self.freed: list[int] = []
        self.calls: list[str] = []

    def _check(self, node_id: str, node_epoch: str) -> None:
        if self.epochs.get(node_id) != node_epoch:
            raise StaleNodeEpochError(f"node {node_id}: not the current run")

    def region(self, size: int) -> NodeRegion:
        rid = self.next_id
        self.next_id += 1
        return NodeRegion(
            handle=MaruHandle(
                region_id=rid, offset=rid << 30, length=size, auth_token=7
            ),
            requested_size=size,
            dax_path="/dev/dax9.0",
            device_uuid="dev-uuid",
        )

    def node_claim(self, node_id, node_epoch):
        self.calls.append("claim")
        if self.unreachable:
            self.unreachable -= 1
            raise ConnectionRefusedError("RM down")
        self.epochs[node_id] = node_epoch
        return NodeClaim(
            rm_domain=DOMAIN,
            lease_s=self.lease_s,
            regions=list(self.regions.get(node_id, [])),
        )

    def node_alloc(self, node_id, node_epoch, size, dax_path=""):
        self.calls.append("alloc")
        self._check(node_id, node_epoch)
        if self.capacity is not None:
            if self.capacity < size:
                raise RuntimeError("alloc failed (-28): no space")
            self.capacity -= size
        region = self.region(size)
        self.regions.setdefault(node_id, []).append(region)
        return region

    def node_free(self, node_id, node_epoch, handle):
        self._check(node_id, node_epoch)
        self.freed.append(handle.region_id)
        self.regions[node_id] = [
            r for r in self.regions[node_id] if r.handle.region_id != handle.region_id
        ]

    def node_renew(self, node_id, node_epoch):
        self.calls.append("renew")
        if self.renew_error is not None:
            raise self.renew_error
        self._check(node_id, node_epoch)
        return self.lease_s

    def node_release(self, node_id, node_epoch):
        self._check(node_id, node_epoch)
        self.released.append((node_id, node_epoch))
        freed = len(self.regions.pop(node_id, []))
        del self.epochs[node_id]
        return freed


def tier(rm: FakeRm, epoch: str = "e1", clock=None, on_lost=None) -> CxlTier:
    return CxlTier(
        rm,
        TIER,
        node_id="n",
        node_epoch=epoch,
        page_size=PAGE,
        on_lost=on_lost or (lambda reason: None),
        renew_s=0.01,
        **({"clock": clock} if clock else {}),
    )


# =============================================================================
# Claim
# =============================================================================


def test_a_first_claim_allocates_up_to_the_tier_size():
    rm = FakeRm()
    t = tier(rm)
    t.claim(threading.Event())
    assert t.rm_domain == DOMAIN
    assert [p.claimed for p in t.pools] == [False, False]
    assert sum(p.capacity for p in t.pools) == 4 * PAGE


def test_a_new_run_takes_the_regions_back_and_tops_up():
    rm = FakeRm()
    rm.regions["n"] = [rm.region(2 * PAGE)]
    t = tier(rm, epoch="e2")
    t.claim(threading.Event())
    assert [p.claimed for p in t.pools] == [True, False]
    assert t.pools[0].pool_id == cxl_pool_id(DOMAIN, 1)
    assert rm.epochs["n"] == "e2"


def test_a_region_on_an_unserved_device_is_kept_but_not_used():
    rm = FakeRm()
    region = rm.region(2 * PAGE)
    rm.regions["n"] = [
        NodeRegion(region.handle, region.requested_size, dax_path="", device_uuid="")
    ]
    t = tier(rm, epoch="e2")
    t.claim(threading.Event())
    assert all(not p.claimed for p in t.pools)  # two new ones fill the size
    assert len(rm.regions["n"]) == 3


def test_the_claim_waits_for_the_rm():
    rm = FakeRm()
    rm.unreachable = 2
    t = tier(rm)
    t.claim(threading.Event(), retry_s=0.0)
    assert rm.calls[:3] == ["claim"] * 3 and t.pools


def test_a_stop_ends_the_wait_without_pools():
    rm = FakeRm()
    rm.unreachable = 10**6
    stopped = threading.Event()
    stopped.set()
    t = tier(rm)
    t.claim(stopped)
    assert t.pools == [] and t.rm_domain is None


def test_a_node_of_another_host_is_refused():
    rm = FakeRm()
    rm.node_claim = lambda *a: (_ for _ in ()).throw(NodeHostConflictError("host"))
    with pytest.raises(CxlTierError, match="host"):
        tier(rm).claim(threading.Event())


def test_a_short_rm_goes_on_with_what_it_gave():
    rm = FakeRm()
    rm.capacity = 2 * PAGE
    t = tier(rm)
    t.claim(threading.Event())
    assert sum(p.capacity for p in t.pools) == 2 * PAGE


def test_the_pool_names_its_region_and_maps_by_handle():
    rm = FakeRm()
    t = tier(rm)
    t.claim(threading.Event())
    pool = t.pools[0]
    info = pool.info()
    assert (info.kind, info.rm_domain, info.region_id) == (PoolKind.CXL, DOMAIN, 1)
    assert (info.capacity, info.page_size, info.device_id) == (
        2 * PAGE,
        PAGE,
        "dev-uuid",
    )
    assert pool.access == PoolAccess.MAPPED and pool.worker_fd() is None
    dax = pool.descriptor().dax
    assert dax is not None and dax.rm_domain == DOMAIN and dax.handle.region_id == 1


# =============================================================================
# Lease
# =============================================================================


def test_renewal_failures_let_go_only_after_half_the_lease():
    now = [0.0]
    rm = FakeRm(lease_s=100.0)
    t = tier(rm, clock=lambda: now[0])
    t.claim(threading.Event())
    rm.renew_error = ConnectionRefusedError("RM down")
    now[0] = 49.0
    assert t.renew_once() is None
    now[0] = 50.0
    assert "no lease renewal" in (t.renew_once() or "")
    rm.renew_error = None
    now[0] = 51.0
    assert t.renew_once() is None  # it got through: the clock restarts
    rm.renew_error = ConnectionRefusedError("RM down")
    now[0] = 100.0
    assert t.renew_once() is None


def test_a_stale_epoch_lets_go_at_once():
    rm = FakeRm()
    t = tier(rm)
    t.claim(threading.Event())
    rm.epochs["n"] = "e2"  # another run claimed the node
    assert "no longer counts this run" in (t.renew_once() or "")


def test_the_renew_thread_reports_once():
    rm = FakeRm()
    lost = []
    t = tier(rm, on_lost=lost.append)
    t.claim(threading.Event())
    t.start_renewing()
    rm.epochs["n"] = "e2"
    wait_until(lambda: lost, what="lost")
    t.stop_renewing()
    assert len(lost) == 1 and t.lost == lost[0]


# =============================================================================
# StorageService
# =============================================================================


@pytest.fixture
def fake():
    server = FakeMaruServer(heartbeat_s=0.1)
    yield server
    server.close()


@pytest.fixture
def sockdir():
    path = short_tmpdir()
    yield path
    remove_tree(path)


def cxl_node(fake, sockdir, rm, **kwargs):
    config = node_config(fake.url, f"{sockdir}/node.sock", cxl_pages=4, region_pages=2)
    return start_node(config, region_source=rm, cxl_renew_s=0.02, **kwargs)


def test_the_regions_are_registered_and_kept_off_credit(fake, sockdir):
    rm = FakeRm()
    node = cxl_node(fake, sockdir, rm)
    try:
        ids = [p.pool_id for p in node.cxl.pools]
        assert set(ids) <= set(fake.nodes["gpu-test"].pools)
        for pool_id in ids:
            assert not node.pools.creditable(pool_id)
            assert node.pools.bytes_in(PageState.FREE, pool_id) == 2 * PAGE
    finally:
        node.stop()
    assert rm.released == []  # restart keeps the regions


def test_claimed_regions_wait_for_the_old_workers(fake, sockdir):
    rm = FakeRm()
    rm.regions["gpu-test"] = [rm.region(2 * PAGE)]
    node = cxl_node(fake, sockdir, rm)
    try:
        claimed = node.cxl.pools[0].pool_id
        assert claimed in node.adopted
        # No old worker to wait for: the quarantine is released at once
        wait_until(
            lambda: node.pools.bytes_in(PageState.FREE, claimed) == 2 * PAGE,
            what="quarantine released",
        )
    finally:
        node.stop()


def test_leave_gives_the_regions_back(fake, sockdir):
    rm = FakeRm()
    node = cxl_node(fake, sockdir, rm)
    epoch = node.node_epoch
    node.stop(CloseMode.LEAVE)
    assert rm.released == [("gpu-test", epoch)]


def test_a_lost_lease_stops_the_run_with_leave(fake, sockdir):
    rm = FakeRm()
    node = cxl_node(fake, sockdir, rm)
    rm.epochs["gpu-test"] = "someone-else"
    assert node.wait_finished(5.0)
    assert node.lost and "no longer counts this run" in node.lost
    assert not fake.nodes["gpu-test"].online  # closed with leave


# =============================================================================
# Unmapping before the regions go back (leave)
# =============================================================================


def test_leave_frees_only_the_regions_every_worker_unmapped(fake, sockdir):
    from maru_common.node_protocol import NodeMessageType, UnmapAckRequest
    from maru_common.storage_types import NotifyKind
    from tests.unit.ms_node_helpers import RawWorker

    rm = FakeRm()
    node = cxl_node(fake, sockdir, rm, unmap_timeout_s=0.5)
    worker = RawWorker(f"{sockdir}/node.sock")
    worker.attach()
    first, second = node.cxl.pools
    stopper = threading.Thread(target=node.stop, args=(CloseMode.LEAVE,))
    stopper.start()
    notices = wait_until(
        lambda: (
            [n for n in worker.notices if n.kind == NotifyKind.POOL_REMOVE][:2]
            if len([n for n in worker.notices if n.kind == NotifyKind.POOL_REMOVE]) >= 2
            else None
        ),
        what="pool_remove notices",
    )
    assert {n.pool_id for n in notices} == {first.pool_id, second.pool_id}
    worker.request(
        NodeMessageType.UNMAP_ACK,
        UnmapAckRequest(pool_id=first.pool_id, pool_generation=1),
    )
    stopper.join(10)
    worker.close()
    assert rm.freed == [first.region.handle.region_id]
    assert rm.released == []  # the other one waits for the lease


def test_a_worker_maps_a_cxl_descriptor_through_its_rm():
    from maru_handler.client.pool_maps import PoolMaps
    from maru_storage_service.drivers.cxl import CxlRegionPool

    rm = FakeRm()
    pool = CxlRegionPool(
        node_id="n",
        rm_domain=DOMAIN,
        region=rm.region(2 * PAGE),
        page_size=PAGE,
        visibility=Visibility.COHERENT,
        rm_address="127.0.0.1:9851",
    )
    seen = []

    class Mapping:
        cuda_registered = False

        def view(self, offset, length):
            return memoryview(bytearray(length))

        def page_lock(self):
            pass

        def close(self):
            seen.append("closed")

    def map_dax(info, dax):
        seen.append((info.pool_id, dax.rm_address, dax.handle.region_id))
        return Mapping()

    maps = PoolMaps(pin_pools=False, map_dax=map_dax)
    maps.add_dax(pool.descriptor(), "e1")
    assert seen == [(pool.pool_id, "127.0.0.1:9851", 1)]
    assert maps.get(pool.pool_id) is not None
    assert maps.remove(pool.pool_id) and seen[-1] == "closed"


def test_a_worker_whose_connection_dropped_keeps_its_regions(fake, sockdir):
    """It may still map them: only an UNMAP_ACK or its end confirms."""
    from tests.unit.ms_node_helpers import RawWorker

    rm = FakeRm()
    node = cxl_node(fake, sockdir, rm, unmap_timeout_s=0.3)
    worker = RawWorker(f"{sockdir}/node.sock")
    worker.attach()
    worker.conn.close()  # the process lives on
    wait_until(
        lambda: all(c is None for c in node.manager.workers.mapping().values()),
        what="connection lost",
    )
    node.stop(CloseMode.LEAVE)
    assert rm.freed == [] and rm.released == []


def test_renewals_come_several_times_per_half_lease():
    rm = FakeRm(lease_s=8.0)
    t = tier(rm)
    t.renew_s = 10.0
    t.claim(threading.Event())
    assert t.renew_interval() == 2.0
    rm.lease_s = 900.0
    t.lease_s = 900.0
    assert t.renew_interval() == 10.0


def test_a_lease_lost_during_the_start_stops_it(fake, sockdir):
    rm = FakeRm()
    fake.pause()  # NODE_OPEN waits
    config = node_config(fake.url, f"{sockdir}/node.sock", cxl_pages=4, region_pages=2)
    from maru_storage_service.ports import ServerStopped
    from maru_storage_service.service import StorageService

    service = StorageService(
        config, rpc_timeout_s=0.3, region_source=rm, cxl_renew_s=0.02
    )
    starter = threading.Thread(target=lambda: _start_quietly(service, ServerStopped))
    starter.start()
    wait_until(lambda: "renew" in rm.calls, what="renewing")
    rm.epochs["gpu-test"] = "someone-else"
    starter.join(10)
    fake.resume()
    assert not starter.is_alive()
    assert service.wait_finished(5.0) and service.lost
    assert service.manager is None or service.local is None or not service.local.started


def _start_quietly(service, stopped_error) -> None:
    try:
        service.start()
    except stopped_error:
        pass

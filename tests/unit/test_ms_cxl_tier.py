# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""The cxl tier: claiming the node's regions from the RM, the lease, letting
go, and the StorageService registering them."""

import threading
import time

import pytest

from maru_common.storage_types import CloseMode, PoolKind, Visibility
from maru_shm.types import (
    MaruHandle,
    NodeClaim,
    NodeHostConflictError,
    NodeRegion,
    RegionHolder,
    StaleNodeEpochError,
)
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
        # region_id -> holder -> its record; the RM keeps them across runs
        self.holds: dict[int, dict[str, RegionHolder]] = {}
        self.unholds: list[tuple[int, str]] = []

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
        regions = [
            NodeRegion(
                r.handle,
                r.requested_size,
                r.dax_path,
                r.device_uuid,
                holders=list(self.holds.get(r.handle.region_id, {}).values()),
            )
            for r in self.regions.get(node_id, [])
        ]
        return NodeClaim(rm_domain=DOMAIN, lease_s=self.lease_s, regions=regions)

    def hold(self, region: NodeRegion, holder: str, epoch: str, pid: int = 0, **who):
        """A worker of run ``epoch`` holds ``region`` (set up by a test)."""
        import os
        import socket

        from maru_common.node_protocol import process_start_time
        from maru_storage_service.service import _local_identity

        _, boot, pid_ns = _local_identity()
        record = RegionHolder(
            holder=holder,
            node_epoch=epoch,
            host=who.get("host", socket.gethostname()),
            boot_id=who.get("boot_id", boot),
            pid=pid or os.getpid(),
            pid_start=who.get(
                "pid_start", 0 if pid else process_start_time(os.getpid())
            ),
            pid_ns=who.get("pid_ns", pid_ns),
        )
        self.holds.setdefault(region.handle.region_id, {})[holder] = record

    def unhold(self, handle, holder, instance):
        self.unholds.append((handle.region_id, holder))
        self.holds.get(handle.region_id, {}).pop(holder, None)

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


def anon_region(pool):
    """Stands in for the region's device in the node's own mapping."""
    import mmap

    return mmap.mmap(-1, pool.region.handle.length)


def cxl_node(fake, sockdir, rm, **kwargs):
    config = node_config(fake.url, f"{sockdir}/node.sock", cxl_pages=4, region_pages=2)
    kwargs.setdefault("map_region", anon_region)
    return start_node(config, region_source=rm, cxl_renew_s=0.02, **kwargs)


@pytest.mark.parametrize("fallback", [True, False])
def test_the_regions_are_registered_as_fallback_credit(fake, sockdir, fallback):
    rm = FakeRm()
    config = node_config(
        fake.url,
        f"{sockdir}/node.sock",
        cxl_pages=4,
        region_pages=2,
        cxl_fallback=fallback,
    )
    node = start_node(
        config, region_source=rm, cxl_renew_s=0.02, map_region=anon_region
    )
    try:
        ids = [p.pool_id for p in node.cxl.pools]
        assert set(ids) <= set(fake.nodes["gpu-test"].pools)
        for pool_id in ids:
            assert node.pools.creditable(pool_id) is fallback
            assert node.pools.bytes_in(PageState.FREE, pool_id) == 2 * PAGE
        expected = [PoolKind.DRAM, PoolKind.CXL] if fallback else [PoolKind.DRAM]
        assert node.manager.credit_kinds == expected
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
        config,
        rpc_timeout_s=0.3,
        region_source=rm,
        cxl_renew_s=0.02,
        map_region=anon_region,
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


# =============================================================================
# Credit from the cxl tier
# =============================================================================


def _two_pool_manager(free_a: int, free_b: int, reserve: int = 0):
    from maru_common.node_protocol import Budgets
    from maru_common.storage_types import PoolInfo
    from maru_storage_service.pool_manager import PoolManager

    pm = PoolManager(
        Budgets(
            credit_max_bytes=64 * PAGE,
            credit_total_max_bytes=64 * PAGE,
            credit_refill_low=PAGE,
            credit_refill_high=4 * PAGE,
            commit_pending_max_bytes=64 * PAGE,
            transfer_reserve_bytes={"cxl": reserve},
        ),
        PAGE,
    )
    for name, pages in (("a", free_a), ("b", free_b)):
        pm.add_pool(
            PoolInfo(
                pool_id=cxl_pool_id(DOMAIN, ord(name)),
                kind=PoolKind.CXL,
                node_id="n",
                pool_generation=1,
                capacity=pages * PAGE,
                page_size=PAGE,
            )
        )
    pm.set_epoch_seq(1)
    return pm


def test_each_page_comes_from_the_pool_with_the_most_room():
    pm = _two_pool_manager(2, 6)
    a, b = cxl_pool_id(DOMAIN, ord("a")), cxl_pool_id(DOMAIN, ord("b"))
    got = pm.credit("w", 3 * PAGE, PoolKind.CXL)
    assert [c.pool_id for c in got] == [b, b, b]
    # b has 3 left and a 2: b, then a on the tie, then the rest
    got = pm.credit("w", 5 * PAGE, PoolKind.CXL)
    assert [c.pool_id for c in got] == [b, a, b, a, b]
    pm.check()


def test_the_transfer_reserve_is_the_tiers_over_all_its_pools():
    pm = _two_pool_manager(4, 4, reserve=6 * PAGE)
    got = pm.credit("w", 8 * PAGE, PoolKind.CXL)
    assert len(got) == 2  # 8 free in the tier, 6 kept for transfers


def cxl_only_config(
    fake, sockdir, pages: int = 16, region_pages: int = 8, high=0.9, low=0.8
):
    from maru_storage_service.config import parse_node_config

    return parse_node_config(
        {
            "node_id": "gpu-test",
            "socket": f"{sockdir}/node.sock",
            "server": fake.url,
            "page_size": PAGE,
            "tiers": [
                {
                    "store": "cxl",
                    "size": pages * PAGE,
                    "region_size": region_pages * PAGE,
                    "high_watermark": high,
                    "low_watermark": low,
                }
            ],
            "budgets": {
                "credit_max_bytes": 4 * PAGE,
                "credit_total_max_bytes": 8 * PAGE,
                "credit_refill_low": PAGE,
                "credit_refill_high": 4 * PAGE,
                "commit_pending_max_bytes": 4 * PAGE,
                "transfer_reserve_bytes": PAGE,
            },
        }
    )


def test_a_cxl_only_node_gives_credit_from_its_regions(fake, sockdir):
    from tests.unit.ms_node_helpers import RawWorker

    rm = FakeRm()
    node = start_node(
        cxl_only_config(fake, sockdir), region_source=rm, cxl_renew_s=0.02
    )
    try:
        assert node.dram is None
        worker = RawWorker(f"{sockdir}/node.sock")
        response = worker.attach()
        assert {d.pool.pool_id for d in response.pools} == {
            p.pool_id for p in node.cxl.pools
        }
        credits = worker.credit(2 * PAGE).credits
        assert credits and all(c.kind == PoolKind.CXL for c in credits)
        assert worker.write_done(credits[:1], ["k1"]).status.value == "OK"
        worker.close()
    finally:
        node.stop()


def test_pressure_on_a_cxl_only_node_retires_its_pages(fake, sockdir):
    from tests.unit.ms_node_helpers import RawWorker

    rm = FakeRm()
    node = start_node(
        cxl_only_config(fake, sockdir), region_source=rm, cxl_renew_s=0.02
    )
    try:
        worker = RawWorker(f"{sockdir}/node.sock")
        worker.attach()
        written = 0
        for _ in range(20):
            credits = worker.credit(4 * PAGE).credits
            if not credits:
                continue
            keys = [f"k{written + i}" for i in range(len(credits))]
            assert worker.write_done(credits, keys).status.value == "OK"
            written += len(credits)
            if node.manager.retire_results:
                break
        wait_until(lambda: node.manager.retire_results, what="a retire")
        assert written >= 8
        worker.close()
    finally:
        node.stop()


def test_a_fuller_region_is_no_pressure_while_the_tier_has_room(fake, sockdir):
    from tests.unit.ms_node_helpers import RawWorker

    rm = FakeRm()
    # 8 regions of 4 pages; 12 pages put 2 in some regions (50%, past the
    # 40% high watermark) while the tier is 37.5% used
    config = cxl_only_config(fake, sockdir, pages=32, region_pages=4, high=0.4, low=0.3)
    node = start_node(config, region_source=rm, cxl_renew_s=0.02)
    try:
        worker = RawWorker(f"{sockdir}/node.sock")
        worker.attach()
        written = 0
        while written < 12:
            credits = worker.credit(4 * PAGE).credits
            keys = [f"k{written + i}" for i in range(len(credits))]
            assert worker.write_done(credits, keys).status.value == "OK"
            written += len(credits)
            wait_until(
                lambda n=written: node.pools.bytes_in(PageState.READY) == n * PAGE,
                what="READY",
            )
        per_region = [
            node.pools.bytes_in(PageState.READY, p.pool_id) // PAGE
            for p in node.cxl.pools
        ]
        assert max(per_region) == 2 and sum(per_region) == 12
        time.sleep(0.3)  # eviction passes run on every commit and on a timer
        assert not node.manager.retire_results
        worker.close()
    finally:
        node.stop()


@pytest.mark.parametrize("fallback", [True, False])
def test_credit_dram_lacks_comes_from_cxl(fake, sockdir, fallback):
    from tests.unit.ms_node_helpers import RawWorker

    config = node_config(
        fake.url,
        f"{sockdir}/node.sock",
        pages=8,
        high=1.0,  # nothing leaves DRAM in this test
        low=0.9,
        cxl_pages=8,
        region_pages=4,
        cxl_fallback=fallback,
        budgets={
            "credit_total_max_bytes": 4 * PAGE,
            "commit_pending_max_bytes": 2 * PAGE,
        },
    )
    node = start_node(
        config, region_source=FakeRm(), cxl_renew_s=0.02, map_region=anon_region
    )
    try:
        worker = RawWorker(f"{sockdir}/node.sock")
        worker.attach()
        first = worker.credit(4 * PAGE).credits
        assert [c.kind for c in first] == [PoolKind.DRAM] * 4
        assert (
            worker.write_done(first, [f"k{i}" for i in range(4)]).status.value == "OK"
        )
        wait_until(lambda: node.pools.bytes_in(PageState.READY) == 4 * PAGE)
        # DRAM has 4 free, less its reserve of 1
        second = worker.credit(4 * PAGE).credits
        expected = [PoolKind.DRAM] * 3 + ([PoolKind.CXL] if fallback else [])
        assert [c.kind for c in second] == expected
        if fallback:
            keys = [f"c{i}" for i in range(4)]
            assert worker.write_done(second, keys).status.value == "OK"
            wait_until(lambda: node.pools.bytes_in(PageState.READY) == 8 * PAGE)
            cxl_ready = sum(
                node.pools.bytes_in(PageState.READY, p.pool_id) for p in node.cxl.pools
            )
            assert cxl_ready == PAGE
        worker.close()
    finally:
        node.stop()


def test_leave_gives_no_more_cxl_credit(fake, sockdir):
    from maru_common.storage_types import CloseMode

    node = cxl_node(fake, sockdir, FakeRm())
    ids = [p.pool_id for p in node.cxl.pools]
    assert all(node.pools.creditable(p) for p in ids)
    node.stop(CloseMode.LEAVE)
    assert not any(node.pools.creditable(p) for p in ids)


class GoneWatcher:
    """Every watched process has already ended."""

    def watch(self, key, pid, pid_start) -> bool:
        return False

    def start(self, callback) -> None:
        pass

    def stop(self) -> None:
        pass


def test_an_old_worker_seen_ending_releases_after_the_gpu_grace():
    """Its GPU copies may still land: the adopted pool is freed, and gives
    credit, only after the drain grace."""
    import time

    from maru_common.storage_types import PoolInfo, WorkerRef
    from maru_storage_service.op_ledger import OpLedger
    from maru_storage_service.worker_registry import WorkerRegistry

    pm = _two_pool_manager(2, 2)
    adopted = cxl_pool_id(DOMAIN, ord("a"))
    other = PoolInfo(
        pool_id=adopted + "-q",
        kind=PoolKind.CXL,
        node_id="n",
        pool_generation=1,
        capacity=2 * PAGE,
        page_size=PAGE,
    )
    pm.add_pool(other, creditable=False, state=PageState.QUARANTINED)
    registry = WorkerRegistry(
        node_epoch="e2",
        pools=pm,
        ledger=OpLedger(16),
        watcher=GoneWatcher(),
        on_exit=lambda ref: None,
        gpu_drain_grace_s=0.5,
    )
    ref = WorkerRef("eng", 0, 0, pid=1 << 22, pid_start=1, incarnation="old")
    started = time.monotonic()
    registry.start_restart_drain(
        [ref], [other.pool_id], "e1", credit_after=frozenset({other.pool_id})
    )
    assert pm.bytes_in(PageState.QUARANTINED, other.pool_id) == 2 * PAGE
    assert not pm.creditable(other.pool_id)
    wait_until(
        lambda: pm.bytes_in(PageState.FREE, other.pool_id) == 2 * PAGE,
        what="released",
    )
    assert time.monotonic() - started >= 0.5
    assert pm.creditable(other.pool_id)
    registry.stop()


# =============================================================================
# Holds: who maps a region, across restarts (Codex P1 on CXL reuse)
# =============================================================================


def _dead_pid() -> int:
    import subprocess
    import sys

    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    return proc.pid


def test_leave_gives_back_every_region_and_holds_keep_mapped_ones(fake, sockdir):
    """A worker that does not unmap in time keeps its hold, so the RM keeps
    the region out of reuse; the node still gives every region back."""
    from maru_common.node_protocol import NodeMessageType, UnmapAckRequest
    from maru_common.storage_types import NotifyKind
    from tests.unit.ms_node_helpers import RawWorker

    rm = FakeRm()
    node = cxl_node(fake, sockdir, rm, unmap_timeout_s=0.3)
    worker = RawWorker(f"{sockdir}/node.sock")
    worker.attach()
    first, second = node.cxl.pools
    rm.hold(second.region, worker.incarnation, node.node_epoch)
    stopper = threading.Thread(target=node.stop, args=(CloseMode.LEAVE,))
    stopper.start()
    wait_until(
        lambda: (
            len([n for n in worker.notices if n.kind == NotifyKind.POOL_REMOVE]) >= 2
        ),
        what="pool_remove notices",
    )
    worker.request(
        NodeMessageType.UNMAP_ACK,
        UnmapAckRequest(pool_id=first.pool_id, pool_generation=1),
    )
    stopper.join(10)
    worker.close()
    assert rm.released == [("gpu-test", node.node_epoch)]
    assert worker.incarnation in rm.holds[second.region.handle.region_id]


def test_an_ended_worker_counts_as_unmapped_only_after_the_gpu_grace():
    import time

    from maru_storage_service.op_ledger import OpLedger
    from maru_storage_service.worker_registry import WorkerRegistry

    registry = WorkerRegistry(
        node_epoch="e1",
        pools=_two_pool_manager(1, 1),
        ledger=OpLedger(16),
        watcher=GoneWatcher(),
        on_exit=lambda ref: None,
        gpu_drain_grace_s=0.4,
    )
    with registry._lock:
        registry._settles_at["w"] = time.monotonic() + 0.4  # ended just now
    assert not registry.settled("w")
    wait_until(lambda: registry.settled("w"), what="grace over")
    registry.stop()


def test_claimed_regions_without_holders_are_free_at_once(fake, sockdir):
    """No worker maps a region without holding it: none held, none to wait for."""
    rm = FakeRm()
    rm.regions["gpu-test"] = [rm.region(2 * PAGE)]
    node = cxl_node(fake, sockdir, rm, gpu_drain_grace_s=30.0)
    try:
        claimed = node.cxl.pools[0].pool_id
        wait_until(
            lambda: node.pools.bytes_in(PageState.FREE, claimed) == 2 * PAGE,
            what="released",
        )
    finally:
        node.stop()


def test_a_live_holder_the_node_cannot_watch_keeps_the_pages_quarantined(fake, sockdir):
    """MaruServer names no old worker, but the RM's hold names one on
    another host: no time lets its pages go, only its DRAIN_ACK."""
    import time

    rm = FakeRm()
    region = rm.region(2 * PAGE)
    rm.regions["gpu-test"] = [region]
    rm.hold(region, "old-worker", "e0", host="elsewhere")
    node = cxl_node(fake, sockdir, rm, gpu_drain_grace_s=0.2)
    try:
        claimed = node.cxl.pools[0].pool_id
        time.sleep(0.6)
        assert node.pools.bytes_in(PageState.QUARANTINED, claimed) == 2 * PAGE
        assert not node.pools.creditable(claimed)
        assert rm.unholds == []
    finally:
        node.stop()


def test_a_holder_seen_ending_frees_the_pages_and_its_hold_after_the_grace(
    fake, sockdir
):
    import time

    rm = FakeRm()
    region = rm.region(2 * PAGE)
    rm.regions["gpu-test"] = [region]
    rm.hold(region, "old-worker", "e0", pid=_dead_pid())
    started = time.monotonic()
    node = cxl_node(fake, sockdir, rm, gpu_drain_grace_s=0.5)
    try:
        claimed = node.cxl.pools[0].pool_id
        wait_until(
            lambda: node.pools.bytes_in(PageState.FREE, claimed) == 2 * PAGE,
            what="released",
        )
        assert time.monotonic() - started >= 0.5
        wait_until(lambda: rm.unholds, what="hold dropped for it")
        # dropped on every region of the node; dropping one it lacks is a no-op
        assert (region.handle.region_id, "old-worker") in rm.unholds
        assert "old-worker" not in rm.holds[region.handle.region_id]
    finally:
        node.stop()


def test_a_drained_old_worker_frees_the_pages_and_keeps_its_hold(fake, sockdir):
    """DRAIN_ACK ends its old work; it may go on mapping the region in this
    run, so its RM hold stays."""
    from maru_common.node_protocol import DrainAckRequest, NodeMessageType
    from maru_common.storage_types import DrainKind, NotifyKind
    from tests.unit.ms_node_helpers import RawWorker

    rm = FakeRm()
    region = rm.region(2 * PAGE)
    rm.regions["gpu-test"] = [region]
    worker = RawWorker(f"{sockdir}/node.sock")
    rm.hold(region, worker.incarnation, "e0")  # this live process
    node = cxl_node(fake, sockdir, rm, gpu_drain_grace_s=30.0)
    try:
        claimed = node.cxl.pools[0].pool_id
        assert node.pools.bytes_in(PageState.QUARANTINED, claimed) == 2 * PAGE
        worker.attach()
        notice = wait_until(
            lambda: next(
                (n for n in worker.notices if n.kind == NotifyKind.DRAIN), None
            ),
            what="node drain notice",
        )
        assert notice.drain_kind == DrainKind.NODE and notice.epoch == "e0"
        worker.request(
            NodeMessageType.DRAIN_ACK,
            DrainAckRequest(kind=DrainKind.NODE, epoch="e0"),
        )
        wait_until(
            lambda: node.pools.bytes_in(PageState.FREE, claimed) == 2 * PAGE,
            what="released",
        )
        assert rm.unholds == []
        worker.close()
    finally:
        node.stop()


def test_a_cxl_only_restart_gives_credit_from_claimed_regions_once_released(
    fake, sockdir
):
    from tests.unit.ms_node_helpers import RawWorker

    rm = FakeRm()
    rm.regions["gpu-test"] = [rm.region(8 * PAGE), rm.region(8 * PAGE)]
    rm.hold(rm.regions["gpu-test"][0], "old-worker", "e0", pid=_dead_pid())
    node = start_node(
        cxl_only_config(fake, sockdir),
        region_source=rm,
        cxl_renew_s=0.02,
        gpu_drain_grace_s=1.0,
    )
    try:
        assert all(p.claimed for p in node.cxl.pools)  # nothing new to allocate
        worker = RawWorker(f"{sockdir}/node.sock")
        worker.attach()
        assert worker.credit(PAGE).credits == []  # all quarantined still
        wait_until(lambda: worker.credit(PAGE).credits, what="credit after release")
        worker.close()
    finally:
        node.stop()


# =============================================================================
# The worker's side: hold before mapping, unhold after a clean unmap
# =============================================================================


class HoldingRm:
    """The RM client a worker's CXL mapping uses."""

    def __init__(self, path: str):
        self.path = path
        self.calls: list[tuple] = []
        self.refuse: Exception | None = None

    def node_hold(self, node_id, node_epoch, handle, holder, **who):
        self.calls.append(("hold", handle.region_id, holder, node_epoch))
        if self.refuse is not None:
            raise self.refuse
        from types import SimpleNamespace

        return SimpleNamespace(dax_path=self.path)

    def get_access(self, handle):
        from types import SimpleNamespace

        self.calls.append(("access", handle.region_id))
        return SimpleNamespace(dax_path=self.path)

    def unhold(self, handle, holder, instance):
        self.calls.append(("unhold", handle.region_id, holder))


@pytest.fixture
def holding_rm(monkeypatch):
    import os

    from maru_handler.client import pool_maps

    fd = os.memfd_create("fake-cxl")
    os.ftruncate(fd, 8 * PAGE)
    rm = HoldingRm(f"/proc/self/fd/{fd}")
    monkeypatch.setattr(pool_maps, "_rm_client", lambda address: rm)
    monkeypatch.setattr(pool_maps, "_HOLDS", pool_maps.RegionHolds())
    yield rm
    os.close(fd)


def _cxl_descriptor(region_pages: int = 2, epoch: str = "e1"):
    from maru_common.storage_types import DaxMapping, PoolDescriptor, PoolInfo

    handle = MaruHandle(region_id=5, offset=0, length=region_pages * PAGE, auth_token=7)
    info = PoolInfo(
        pool_id=cxl_pool_id(DOMAIN, 5),
        kind=PoolKind.CXL,
        node_id="n",
        pool_generation=1,
        capacity=region_pages * PAGE,
        page_size=PAGE,
    )
    return PoolDescriptor(
        pool=info, dax=DaxMapping(DOMAIN, handle, "rm:1", node_id="n", node_epoch=epoch)
    )


def test_a_worker_holds_a_region_before_mapping_and_unholds_after(holding_rm):
    from maru_handler.client.pool_maps import PoolMaps, RegionHolder

    maps = PoolMaps(pin_pools=False, holder=RegionHolder.this_process("w1"))
    descriptor = _cxl_descriptor()
    maps.add_dax(descriptor, "e1")
    assert holding_rm.calls == [("hold", 5, "w1", "e1")]
    maps.add_dax(descriptor, "e2")  # the next run maps it again: no new hold
    assert holding_rm.calls[-1] == ("access", 5)
    assert maps.remove(descriptor.pool.pool_id)
    assert holding_rm.calls[-1] == ("unhold", 5, "w1")


def test_a_refused_hold_maps_nothing(holding_rm):
    from maru_handler.client.pool_maps import PoolMaps, RegionHolder

    holding_rm.refuse = StaleNodeEpochError("an older run")
    maps = PoolMaps(pin_pools=False, holder=RegionHolder.this_process("w1"))
    with pytest.raises(StaleNodeEpochError):
        maps.add_dax(_cxl_descriptor(), "e1")
    assert maps.pool_ids() == []
    assert [c[0] for c in holding_rm.calls] == ["hold"]


def test_a_mapping_still_in_use_keeps_its_hold(holding_rm):
    from maru_common.storage_types import Location
    from maru_handler.client.pool_maps import PoolMaps, RegionHolder

    maps = PoolMaps(pin_pools=False, holder=RegionHolder.this_process("w1"))
    descriptor = _cxl_descriptor()
    maps.add_dax(descriptor, "e1")
    view = maps.view(Location(descriptor.pool.pool_id, "a", 1, 0, PAGE, PoolKind.CXL))
    assert not maps.remove(descriptor.pool.pool_id)  # a view is held
    assert holding_rm.calls == [("hold", 5, "w1", "e1")]
    view.release()
    assert maps.remove(descriptor.pool.pool_id)
    assert holding_rm.calls[-1] == ("unhold", 5, "w1")


def test_one_rm_under_two_addresses_is_one_hold(holding_rm):
    """The RM keys holds by region and holder; so does the worker, by the RM's
    id, not by the address it was reached at."""
    import dataclasses

    from maru_handler.client.pool_maps import PoolMaps, RegionHolder

    maps = PoolMaps(pin_pools=False, holder=RegionHolder.this_process("w1"))
    first = _cxl_descriptor(epoch="e1")
    maps.add_dax(first, "e1")
    second = dataclasses.replace(
        first, dax=dataclasses.replace(first.dax, rm_address="other:1", node_epoch="e2")
    )
    maps.add_dax(second, "e2")  # retires the first mapping
    assert [c[0] for c in holding_rm.calls] == ["hold", "access"]
    assert maps.remove(first.pool.pool_id)
    assert holding_rm.calls[-1] == ("unhold", 5, "w1")


def test_a_failed_second_mapping_does_not_keep_a_count(holding_rm):
    from maru_handler.client.pool_maps import PoolMaps, RegionHolder

    maps = PoolMaps(pin_pools=False, holder=RegionHolder.this_process("w1"))
    descriptor = _cxl_descriptor()
    maps.add_dax(descriptor, "e1")
    holding_rm.get_access = lambda handle: (_ for _ in ()).throw(OSError("RM down"))
    with pytest.raises(OSError):
        maps.add_dax(descriptor, "e2")
    assert maps.remove(descriptor.pool.pool_id)
    assert holding_rm.calls[-1] == ("unhold", 5, "w1")


def test_a_holder_from_before_this_hosts_reboot_is_gone_at_once(fake, sockdir):
    rm = FakeRm()
    region = rm.region(2 * PAGE)
    rm.regions["gpu-test"] = [region]
    rm.hold(region, "old-worker", "e0", boot_id="an-earlier-boot")
    node = cxl_node(fake, sockdir, rm, gpu_drain_grace_s=30.0)
    try:
        claimed = node.cxl.pools[0].pool_id
        wait_until(
            lambda: node.pools.bytes_in(PageState.QUARANTINED, claimed) == 0,
            what="released",
        )
        assert (region.handle.region_id, "old-worker") in rm.unholds
    finally:
        node.stop()


def test_a_holder_of_another_pid_namespace_drains_by_incarnation(fake, sockdir):
    """Its pid is its own namespace's: it attaches again by incarnation, and
    its DRAIN_ACK confirms it."""
    from maru_common.node_protocol import DrainAckRequest, NodeMessageType
    from maru_common.storage_types import DrainKind
    from tests.unit.ms_node_helpers import RawWorker

    rm = FakeRm()
    region = rm.region(2 * PAGE)
    rm.regions["gpu-test"] = [region]
    worker = RawWorker(f"{sockdir}/node.sock")
    rm.hold(region, worker.incarnation, "e0", pid=7, pid_start=1, pid_ns=1)
    node = cxl_node(fake, sockdir, rm, gpu_drain_grace_s=30.0)
    try:
        claimed = node.cxl.pools[0].pool_id
        response = worker.attach()
        assert response.status.value == "OK", response.error
        worker.request(
            NodeMessageType.DRAIN_ACK, DrainAckRequest(kind=DrainKind.NODE, epoch="e0")
        )
        wait_until(
            lambda: node.pools.bytes_in(PageState.QUARANTINED, claimed) == 0,
            what="released",
        )
        worker.close()
    finally:
        node.stop()


def test_a_forgotten_detached_worker_counts_as_settled():
    from maru_storage_service.op_ledger import OpLedger
    from maru_storage_service.worker_registry import WorkerRegistry

    registry = WorkerRegistry(
        node_epoch="e1",
        pools=_two_pool_manager(1, 1),
        ledger=OpLedger(16),
        watcher=GoneWatcher(),
        on_exit=lambda ref: None,
        gpu_drain_grace_s=30.0,
    )
    with registry._lock:
        registry._settles_at["w"] = 0.0  # what forgetting a detached one records
    assert registry.settled("w")
    registry.stop()

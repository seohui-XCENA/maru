# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Catalog and pins through MsService with fake nodes and workers: lookup
order, commit, retire, finalize_move, source pins, fetch_complete,
preservation, unpin."""

from dataclasses import replace

import pytest

from maru_common.storage_types import (
    KeptCause,
    MissReason,
    SessionRole,
    Status,
)
from maru_server.catalog import PoolState
from tests.unit.ms_fakes import (
    PAGE,
    FakeClock,
    FakeNode,
    FakeWorker,
    Wire,
    key,
    make_service,
    policy_service,
)


@pytest.fixture
def env():
    clock = FakeClock()
    service = make_service(clock)
    wire = Wire(service)
    return clock, service, wire


def open_node(wire, node_id, ssd=False, cxl=False):
    node = FakeNode(wire, node_id)
    pools = [node.dram()] + ([node.ssd()] if ssd else [])
    if cxl:
        pools.append(node.cxl(region_id=hash(node_id) % 1000))
    assert node.open(pools=pools).status == Status.OK
    return node


def results(response):
    return [item.result for item in response.items]


class TestLookupOrder:
    def test_several_locations_are_chosen_in_cost_order(self, env):
        clock, service, wire = env
        a = open_node(wire, "a", ssd=True, cxl=True)
        b = open_node(wire, "b", ssd=True, cxl=True)
        k = key("k")
        a_dram = a.loc(a.dram(), 1)
        a_cxl = a.loc(a.cxl(region_id=hash("a") % 1000), 1)
        a_ssd = a.loc(a.ssd(), 1)
        b_dram = b.loc(b.dram(), 1)
        b_ssd = b.loc(b.ssd(), 1)
        # committed in reverse order of cost
        assert results(b.commit((k, b_ssd))) == ["ADDED"]
        assert results(a.commit((k, a_ssd))) == ["ADDED"]
        assert results(b.commit((k, b_dram))) == ["ADDED"]
        assert results(a.commit((k, a_cxl))) == ["ADDED"]
        assert results(a.commit((k, a_dram))) == ["ADDED"]
        worker = FakeWorker(wire, a)
        assert worker.open().status == Status.OK
        probe = FakeWorker(wire, a, role=SessionRole.PROBE)
        assert probe.open().status == Status.OK

        expected = [
            ("HIT", a_dram, a),  # node DRAM
            ("HIT", a_cxl, a),  # node CXL
            ("FETCH", b_dram, b),  # memory node DRAM
            ("FETCH", a_ssd, a),  # node SSD
            ("FETCH", b_ssd, b),  # memory node SSD
        ]
        for result, location, owner in expected:
            response = worker.lookup(k)
            item = response.items[0]
            assert item.result == result
            if result == "HIT":
                assert item.location == location and item.pin_id
                worker.unpin(item.pin_id)
            else:
                assert item.plan.source == location
                assert item.plan.source_node == owner.node_id
                assert item.plan.bytes == location.length
                assert item.pin_id is None
            # PROBE gives the same answer without pin or location
            hint = probe.probe(k).items[0]
            assert hint.result == result
            assert hint.location is None and hint.pin_id is None and hint.plan is None
            # retire the chosen one; the next one is chosen after it
            assert results(owner.retire((k, location))) == ["RETIRED"]
        miss = worker.lookup(k).items[0]
        assert miss.result == "MISS" and miss.reason == MissReason.ABSENT

    def test_fetch_plan_names_source_node(self, env):
        _, _, wire = env
        a = open_node(wire, "a")
        b = open_node(wire, "b")
        k = key("k")
        b.commit((k, b.loc(b.dram(), 3), 100))
        worker = FakeWorker(wire, a)
        worker.open()
        item = worker.lookup(k).items[0]
        assert item.result == "FETCH"
        assert item.plan.source_node == "b" and item.plan.bytes == 100

    def test_probe_session_gives_no_pins(self, env):
        _, service, wire = env
        a = open_node(wire, "a")
        k = key("k")
        a.commit((k, a.loc(a.dram(), 1)))
        probe = FakeWorker(wire, a, role=SessionRole.PROBE)
        probe.open()
        assert probe.probe(k).items[0].result == "HIT"
        assert len(service.pins) == 0
        response = probe.lookup(k)
        assert response.status == Status.REJECTED
        assert probe.unpin("x").status == Status.REJECTED
        assert probe.heartbeat().status == Status.OK
        assert probe.close().status == Status.OK


class TestMissReasons:
    def test_absent_vs_offline(self, env):
        clock, _, wire = env
        a = open_node(wire, "a")
        b = open_node(wire, "b")
        k = key("k")
        b.commit((k, b.loc(b.dram(), 1)))
        worker = FakeWorker(wire, a)
        worker.open()
        assert worker.lookup(key("other")).items[0].reason == MissReason.ABSENT
        assert worker.lookup(k).items[0].result == "FETCH"
        # b's session expires: its StorageService cannot send the bytes
        clock.advance(20)
        a.heartbeat()
        worker.heartbeat()
        clock.advance(15)
        item = worker.lookup(k).items[0]
        assert item.result == "MISS" and item.reason == MissReason.OFFLINE
        assert worker.probe(k).items[0].reason == MissReason.OFFLINE

    def test_fetch_needs_the_requesters_storage_service_too(self, env):
        clock, _, wire = env
        a = open_node(wire, "a", ssd=True)
        b = open_node(wire, "b")
        k_remote, k_ssd, k_dram = key("remote"), key("ssd"), key("dram")
        b.commit((k_remote, b.loc(b.dram(), 1)))
        a.commit((k_ssd, a.loc(a.ssd(), 1)))
        a.commit((k_dram, a.loc(a.dram(), 2)))
        worker = FakeWorker(wire, a)
        worker.open()
        assert results(worker.lookup(k_remote, k_ssd, k_dram)) == [
            "FETCH",
            "FETCH",
            "HIT",
        ]
        # a's session expires while b stays up
        clock.advance(20)
        b.heartbeat()
        worker.heartbeat()
        clock.advance(15)
        response = worker.lookup(k_remote, k_ssd, k_dram)
        assert results(response) == ["MISS", "MISS", "HIT"]
        assert [i.reason for i in response.items[:2]] == [MissReason.OFFLINE] * 2

    def test_restart_close_hides_dram_but_not_cxl(self, env):
        _, service, wire = env
        a = open_node(wire, "a", cxl=True)
        k_dram, k_cxl = key("d"), key("c")
        a.commit((k_dram, a.loc(a.dram(), 1)))
        a.commit((k_cxl, a.loc(a.cxl(region_id=hash("a") % 1000), 1)))
        worker = FakeWorker(wire, a)
        worker.open()
        assert results(worker.lookup(k_dram, k_cxl)) == ["HIT", "HIT"]
        assert a.close("restart").status == Status.OK
        response = worker.lookup(k_dram, k_cxl)
        assert results(response) == ["MISS", "HIT"]
        assert response.items[0].reason == MissReason.OFFLINE
        # the closed session's requests get EPOCH
        assert a.heartbeat().status == Status.EPOCH


class TestCommit:
    def test_resend_same_op_gives_same_result(self, env):
        _, service, wire = env
        a = open_node(wire, "a")
        from maru_common.protocol import CommitItem, CommitRequest, MessageType

        loc = a.loc(a.dram(), 1)
        request = a.build(
            CommitRequest,
            **a.envelope(),
            items=[CommitItem(key("k"), loc, PAGE, finalizer=True)],
        )
        first = wire.call(MessageType.COMMIT, request)
        assert results(first) == ["ADDED"]
        again = wire.call(MessageType.COMMIT, request)
        assert again == first
        assert len(service.pins) == 1

    def test_duplicate_and_rejections(self, env):
        _, service, wire = env
        a = open_node(wire, "a", ssd=True)
        b = open_node(wire, "b")
        k = key("k")
        first = a.loc(a.dram(), 1)
        assert results(a.commit((k, first))) == ["ADDED"]
        # same key, same pool: DUPLICATE with the location already there
        response = a.commit((k, a.loc(a.dram(), 2)))
        assert results(response) == ["DUPLICATE"]
        assert response.items[0].location == first
        # same key, another pool: one more location
        assert results(a.commit((k, a.loc(a.ssd(), 1)))) == ["ADDED"]
        assert len(service.catalog.locations(k)) == 2
        # a pool of another node
        assert results(a.commit((key("x"), b.loc(b.dram(), 5)))) == ["REJECTED"]
        # a page that already holds another key's location
        taken = a.loc(a.dram(), 1)
        assert results(a.commit((key("y"), taken))) == ["REJECTED"]
        # a generation of another run (not this epoch_seq)
        bad = a.loc(a.dram(), 9)
        from dataclasses import replace

        old_run = replace(bad, generation=bad.generation - (1 << 40))
        assert results(a.commit((key("z"), old_run))) == ["REJECTED"]
        # beyond the pool, wrong kind, bad logical bytes
        beyond = replace(a.loc(a.dram(), 10), offset=a.dram().capacity)
        wrong_kind = replace(a.loc(a.dram(), 11), kind="ssd")
        assert results(a.commit((key("p"), beyond), (key("q"), wrong_kind))) == [
            "REJECTED",
            "REJECTED",
        ]
        assert results(a.commit((key("r"), a.loc(a.dram(), 12), 0))) == ["REJECTED"]
        # an unknown pool
        unknown = replace(a.loc(a.dram(), 13), pool_id="dram:a:other")
        assert results(a.commit((key("s"), unknown))) == ["REJECTED"]

    def test_same_location_again_is_added_not_duplicate(self, env):
        _, service, wire = env
        a = open_node(wire, "a")
        k = key("k")
        loc = a.loc(a.dram(), 1)
        first = a.commit((k, loc), finalizer=True)
        # a new op naming the very same location: it is in, not a duplicate,
        # and the request gets a finalizer of its own
        again = a.commit((k, loc), finalizer=True)
        assert results(again) == ["ADDED"]
        assert again.items[0].finalizer_id != first.items[0].finalizer_id
        assert service.pins.count(k, loc) == 2

    def test_overlapping_bytes_are_rejected(self, env):
        _, _, wire = env
        a = open_node(wire, "a")
        first = a.loc(a.dram(), 1, length=2 * PAGE)  # pages 1 and 2
        assert results(a.commit((key("a"), first))) == ["ADDED"]
        from dataclasses import replace

        inside = replace(a.loc(a.dram(), 9), offset=first.offset + PAGE + 10, length=8)
        assert results(a.commit((key("b"), inside))) == ["REJECTED"]
        before = replace(a.loc(a.dram(), 8), offset=first.offset - 8, length=16)
        assert results(a.commit((key("c"), before))) == ["REJECTED"]
        after = replace(a.loc(a.dram(), 7), offset=first.offset + 2 * PAGE, length=8)
        assert results(a.commit((key("d"), after))) == ["ADDED"]

    def test_a_finalizer_protects_a_demotion_target(self, env):
        _, service, wire = env
        a = open_node(wire, "a", ssd=True)
        k = key("k")
        dest = a.loc(a.ssd(), 1)
        response = a.commit((k, dest), finalizer=True)
        assert results(response) == ["ADDED"] and response.items[0].finalizer_id
        assert results(a.retire((k, dest))) == ["KEPT"]

    def test_batch_over_limit_is_rejected(self, env):
        _, _, wire = env
        a = open_node(wire, "a")
        items = [(key(f"k{i}"), a.loc(a.dram(), i)) for i in range(1025)]
        assert a.commit(*items).status == Status.REJECTED


class TestRetire:
    def test_kept_pinned_while_pinned_then_retired(self, env):
        _, _, wire = env
        a = open_node(wire, "a")
        k = key("k")
        loc = a.loc(a.dram(), 1)
        a.commit((k, loc))
        worker = FakeWorker(wire, a)
        worker.open()
        pin_id = worker.lookup(k).items[0].pin_id
        response = a.retire((k, loc))
        assert results(response) == ["KEPT"]
        assert response.items[0].cause == KeptCause.PINNED
        assert worker.unpin(pin_id).items[0].result == "OK"
        assert results(a.retire((k, loc))) == ["RETIRED"]
        assert worker.lookup(k).items[0].reason == MissReason.ABSENT

    def test_kept_moved_names_the_occupant_of_the_page(self, env):
        _, _, wire = env
        a = open_node(wire, "a")
        k = key("k")
        loc = a.loc(a.dram(), 1)
        a.commit((k, loc))
        # the same key with another generation on the page
        stale = replace(loc, generation=loc.generation + 1)
        response = a.retire((k, stale))
        assert results(response) == ["KEPT"]
        item = response.items[0]
        assert item.cause == KeptCause.MOVED
        assert (item.key, item.location) == (k, loc)
        # another key on the page, by allocation or by overlapping bytes
        for other in (
            replace(loc, generation=loc.generation + 2),
            replace(loc, allocation_id="x", offset=loc.offset + 1, length=8),
        ):
            item = a.retire((key("j"), other)).items[0]
            assert item.cause == KeptCause.MOVED
            assert (item.key, item.location) == (k, loc)

    def test_retired_when_the_page_is_free_even_if_the_key_is_elsewhere(self, env):
        _, service, wire = env
        a = open_node(wire, "a")
        k = key("k")
        loc = a.loc(a.dram(), 1)
        a.commit((k, loc))
        elsewhere = a.loc(a.dram(), 5)  # a page nothing published holds
        assert results(a.retire((k, elsewhere))) == ["RETIRED"]
        assert [e.location for e in service.catalog.locations(k)] == [loc]

    def test_kept_invalid_for_a_pool_that_is_not_the_nodes(self, env):
        _, _, wire = env
        a = open_node(wire, "a")
        b = open_node(wire, "b")
        k = key("k")
        loc = b.loc(b.dram(), 1)
        b.commit((k, loc))
        response = a.retire((k, loc))
        assert response.items[0].cause == KeptCause.INVALID

    def test_preserved_location_is_kept_pinned(self, tmp_path):
        service, wire, preserve = policy_service(tmp_path)
        a = open_node(wire, "a")
        k = key("k")
        loc = a.loc(a.dram(), 1)
        a.commit((k, loc))
        version = a.heartbeat().policy_version
        preserve(k)
        assert a.heartbeat().policy_version == version + 1
        response = a.retire((k, loc))
        assert response.items[0].cause == KeptCause.PINNED
        preserve()
        assert results(a.retire((k, loc))) == ["RETIRED"]

    def test_a_location_already_gone_is_retired(self, env):
        _, _, wire = env
        a = open_node(wire, "a")
        k = key("k")
        loc = a.loc(a.dram(), 1)
        assert results(a.retire((k, loc))) == ["RETIRED"]


def test_a_runs_finalizers_go_with_it(env):
    """No message releases a finalizer by itself: one that FINALIZE_MOVE did
    not release goes when the next run of the node opens."""
    _, service, wire = env
    a = open_node(wire, "a", ssd=True)
    k = key("k")
    dest = a.loc(a.ssd(), 1)
    finalizer = a.commit((k, dest), finalizer=True).items[0].finalizer_id
    assert results(a.retire((k, dest))) == ["KEPT"]
    a.restart()
    assert a.open(pools=[a.dram(), a.ssd()]).status == Status.OK
    assert service.pins.get(finalizer) is None
    assert results(a.retire((k, dest))) == ["RETIRED"]


class TestFinalizeMove:
    def _setup(self, wire):
        a = open_node(wire, "a", ssd=True)
        k1, k2 = key("k1"), key("k2")
        src1, src2 = a.loc(a.dram(), 1), a.loc(a.dram(), 2)
        a.commit((k1, src1), (k2, src2))
        dst1, dst2 = a.loc(a.ssd(), 1), a.loc(a.ssd(), 2)
        response = a.commit((k1, dst1), (k2, dst2), finalizer=True)
        fin1, fin2 = (item.finalizer_id for item in response.items)
        return a, (k1, src1, dst1, fin1), (k2, src2, dst2, fin2)

    def test_partial_failure_keeps_the_source(self, env):
        _, service, wire = env
        a, one, two = self._setup(wire)
        worker = FakeWorker(wire, a)
        worker.open()
        # a reader pins the second source
        reader_pin = worker.lookup(two[0]).items[0].pin_id
        response = a.finalize_move(one, two)
        assert results(response) == ["RETIRED", "KEPT"]
        assert response.items[1].cause == KeptCause.PINNED
        # both finalizers are released whatever the result
        assert service.pins.get(one[3]) is None and service.pins.get(two[3]) is None
        locations = {e.location for e in service.catalog.locations(two[0])}
        # the kept source stays and the target is one more location
        assert locations == {two[1], two[2]}
        assert {e.location for e in service.catalog.locations(one[0])} == {one[2]}
        worker.unpin(reader_pin)
        assert worker.lookup(two[0]).items[0].location == two[1]

    def test_a_released_finalizer_keeps_the_source(self, env):
        _, service, wire = env
        a, one, two = self._setup(wire)
        with service.lock:
            service.pins.unpin(one[3])  # the finalizer is gone
        response = a.finalize_move(one, two)
        assert results(response) == ["REJECTED", "RETIRED"]
        assert {e.location for e in service.catalog.locations(one[0])} == {
            one[1],
            one[2],
        }

    def test_a_finalizer_of_another_location_is_rejected(self, env):
        _, service, wire = env
        a, one, two = self._setup(wire)
        crossed = (one[0], one[1], one[2], two[3])
        response = a.finalize_move(crossed)
        assert results(response) == ["REJECTED"]
        assert service.catalog.entry_exact(one[0], one[1]) is not None
        # it was this node's finalizer: released anyway
        assert service.pins.get(two[3]) is None

    def test_source_gone_is_moved_only_when_the_page_is_occupied(self, env):
        _, service, wire = env
        a, one, two = self._setup(wire)
        k, src, dest, finalizer = one
        assert results(a.retire((k, src))) == ["RETIRED"]
        # another key now holds the source page
        occupant = replace(src, generation=src.generation + 1)
        a.commit((key("z"), occupant))
        item = a.finalize_move((k, src, dest, finalizer)).items[0]
        assert item.cause == KeptCause.MOVED
        assert (item.key, item.location) == (key("z"), occupant)
        # a free source page is RETIRED (the move is done)
        k2, src2, dest2, fin2 = two
        a.retire((k2, src2))
        assert results(a.finalize_move((k2, src2, dest2, fin2))) == ["RETIRED"]

    def test_a_finalizer_of_another_node_is_not_released(self, env):
        _, service, wire = env
        a, one, _ = self._setup(wire)
        b = open_node(wire, "b", ssd=True)
        kb = key("kb")
        b_dest = b.loc(b.ssd(), 1)
        b_finalizer = b.commit((kb, b_dest), finalizer=True).items[0].finalizer_id
        response = a.finalize_move((one[0], one[1], one[2], b_finalizer))
        assert results(response) == ["REJECTED"]
        assert service.pins.get(b_finalizer) is not None
        assert service.catalog.entry_exact(one[0], one[1]) is not None

    def test_target_no_longer_in_the_catalog_is_rejected(self, env):
        _, service, wire = env
        a, one, _ = self._setup(wire)
        k, src, dest, finalizer = one
        with service.lock:
            service.pins.unpin(finalizer)
        assert results(a.retire((k, dest))) == ["RETIRED"]
        # no finalizer on it can exist; name a fresh source pin of k instead
        fresh = a.source_pin((k, src)).items[0].pin_id
        response = a.finalize_move((k, src, dest, fresh))
        assert results(response) == ["REJECTED"]
        assert service.catalog.entry_exact(k, src) is not None
        assert service.pins.get(fresh) is not None  # not a finalizer: kept

    def test_a_source_pin_is_not_a_finalizer(self, env):
        _, service, wire = env
        a, one, _ = self._setup(wire)
        k, src, dest, finalizer = one
        source_pin = a.source_pin((k, dest)).items[0].pin_id
        response = a.finalize_move((k, src, dest, source_pin))
        assert results(response) == ["REJECTED"]
        assert service.catalog.entry_exact(k, src) is not None
        assert service.pins.get(source_pin) is not None
        assert service.pins.get(finalizer) is not None

    def test_target_in_an_invalid_pool_is_kept_invalid(self, env):
        _, service, wire = env
        a, one, _ = self._setup(wire)
        k, src, dest, finalizer = one
        service.catalog.pool(dest.pool_id).state = PoolState.HIDDEN
        item = a.finalize_move((k, src, dest, finalizer)).items[0]
        assert (item.result, item.cause) == ("KEPT", KeptCause.INVALID)

    def test_same_pool_is_rejected(self, env):
        _, _, wire = env
        a, one, _ = self._setup(wire)
        response = a.finalize_move((one[0], one[1], one[1], one[3]))
        assert results(response) == ["REJECTED"]


class TestFetch:
    def _setup(self, wire):
        a = open_node(wire, "a", ssd=True)
        b = open_node(wire, "b", ssd=True)
        worker = FakeWorker(wire, a)
        worker.open()
        k = key("k")
        source = b.loc(b.ssd(), 1)
        b.commit((k, source))
        return a, b, worker, k, source

    def test_source_pin_and_fetch_complete(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        plan = worker.lookup(k).items[0]
        assert plan.result == "FETCH" and plan.plan.source == source
        pinned = a.source_pin((k, plan.plan.source))
        assert results(pinned) == ["HIT"]
        source_pin = pinned.items[0].pin_id
        # the source pin keeps the source from its own node's retire
        assert b.retire((k, source)).items[0].cause == KeptCause.PINNED
        copy = a.loc(a.dram(), 1)
        done = a.fetch_complete(worker.token, (k, source_pin, copy))
        assert results(done) == ["HIT"]
        assert done.items[0].location == copy and done.items[0].pin_id
        # the source pin went with it
        assert service.pins.get(source_pin) is None
        assert results(b.retire((k, source))) == ["RETIRED"]
        # the copy is a location of its own, pinned for the worker
        assert a.retire((k, copy)).items[0].cause == KeptCause.PINNED
        other = FakeWorker(wire, a, tp_rank=1)
        other.open()
        assert other.lookup(k).items[0].location == copy
        worker.unpin(done.items[0].pin_id)

    def test_copy_for_a_replaced_run_or_orphan_session_is_only_cache(self, env):
        from maru_common.storage_types import CloseMode

        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        # the worker's session belongs to a replaced run of node a
        a.restart()
        assert a.open(pools=[a.dram(), a.ssd(), a.cxl()]).status == Status.OK
        pin = a.source_pin((k, source)).items[0].pin_id
        copy = a.loc(a.dram(), 1)
        done = a.fetch_complete(worker.token, (k, pin, copy))
        assert results(done) == ["ADDED"] and done.items[0].pin_id is None
        assert not service.pins.held_by(worker.token)
        assert copy in [e.location for e in service.catalog.locations(k)]
        # an orphan: it reads node a's CXL, and node a leaves and comes back
        a.commit((key("c"), a.loc(a.cxl(), 1)))
        reader = FakeWorker(wire, a)
        reader.open()
        reader.lookup(key("c"))
        a.close(CloseMode.LEAVE)
        assert service.registry.known_worker(reader.token).orphan
        a.restart()
        assert a.open(pools=[a.dram(), a.ssd()]).status == Status.OK
        k2, source2 = key("k2"), b.loc(b.ssd(), 2)
        b.commit((k2, source2))
        pin = a.source_pin((k2, source2)).items[0].pin_id
        done = a.fetch_complete(reader.token, (k2, pin, a.loc(a.dram(), 2)))
        assert results(done) == ["ADDED"] and done.items[0].pin_id is None
        assert all(p.location.kind == "cxl" for p in service.pins.held_by(reader.token))

    def test_existing_copy_in_node_dram_is_pinned_instead(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        first_pin = a.source_pin((k, source)).items[0].pin_id
        copy = a.loc(a.dram(), 1)
        a.fetch_complete(worker.token, (k, first_pin, copy))
        # a second fetch of the same key finishes with another page
        second_pin = a.source_pin((k, source)).items[0].pin_id
        done = a.fetch_complete(worker.token, (k, second_pin, a.loc(a.dram(), 2)))
        assert results(done) == ["HIT"]
        assert done.items[0].location == copy
        assert service.pins.get(second_pin) is None
        assert len(service.catalog.locations(k)) == 2  # source and one copy

    def test_storage_service_unpins_a_copy_pin_it_made(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        pin = a.source_pin((k, source)).items[0].pin_id
        copy_pin = a.fetch_complete(worker.token, (k, pin, a.loc(a.dram(), 1)))
        copy_pin = copy_pin.items[0].pin_id
        # another node may not release it
        assert b.unpin(copy_pin).items[0].result == "REJECTED"
        assert service.pins.get(copy_pin) is not None
        # the StorageService that made it may (the worker cancelled)
        assert a.unpin(copy_pin).items[0].result == "OK"
        assert service.pins.get(copy_pin) is None
        # and UNPIN stays OK for an id that is gone
        assert a.unpin(copy_pin).items[0].result == "OK"

    def test_fetch_complete_rejections(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        # a source pin that is not there
        done = a.fetch_complete(worker.token, (k, "nope", a.loc(a.dram(), 1)))
        assert results(done) == ["REJECTED"]
        assert service.catalog.entry_exact(k, source) is not None
        # a copy on an SSD (not mapped by workers); the source pin goes
        pin = a.source_pin((k, source)).items[0].pin_id
        done = a.fetch_complete(worker.token, (k, pin, a.loc(a.ssd(), 3)))
        assert results(done) == ["REJECTED"]
        assert service.pins.get(pin) is None
        # a copy overlapping a live location
        taken = a.loc(a.dram(), 5)
        a.commit((key("other"), taken))
        pin = a.source_pin((k, source)).items[0].pin_id
        overlap = replace(a.loc(a.dram(), 6), offset=taken.offset)
        done = a.fetch_complete(worker.token, (k, pin, overlap))
        assert results(done) == ["REJECTED"] and service.pins.get(pin) is None
        # a worker of another node: every item REJECTED, source pins released
        b_worker = FakeWorker(wire, b)
        b_worker.open()
        pin = a.source_pin((k, source)).items[0].pin_id
        done = a.fetch_complete(b_worker.token, (k, pin, a.loc(a.dram(), 4)))
        assert done.status == Status.OK and results(done) == ["REJECTED"]
        assert service.pins.get(pin) is None
        assert service.catalog.entry_exact(k, source) is not None

    def test_a_finalizer_is_not_a_source_pin(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        dest = a.loc(a.ssd(), 1)
        finalizer = a.commit((k, dest), finalizer=True).items[0].finalizer_id
        done = a.fetch_complete(worker.token, (k, finalizer, a.loc(a.dram(), 1)))
        assert results(done) == ["REJECTED"]
        assert service.pins.get(finalizer) is not None

    def test_worker_gone_before_the_first_run_keeps_the_copy_as_cache(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        pin = a.source_pin((k, source)).items[0].pin_id
        token = worker.token
        worker.close()
        copy = a.loc(a.dram(), 1)
        done = a.fetch_complete(token, (k, pin, copy))
        assert done.status == Status.OK and results(done) == ["ADDED"]
        assert done.items[0].pin_id is None
        assert service.pins.get(pin) is None
        assert service.pins.count(k, copy) == 0
        assert results(a.retire((k, copy))) == ["RETIRED"]
        # with a DRAM copy there already, the new page is not needed
        a.commit((k, a.loc(a.dram(), 2)))
        pin = a.source_pin((k, source)).items[0].pin_id
        done = a.fetch_complete(token, (k, pin, a.loc(a.dram(), 3)))
        assert results(done) == ["DUPLICATE"] and service.pins.get(pin) is None

    def test_two_fetches_of_the_same_copy_get_their_own_pins(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        copy = a.loc(a.dram(), 1)
        pin = a.source_pin((k, source)).items[0].pin_id
        first = a.fetch_complete(worker.token, (k, pin, copy))
        # another request naming the same copy (its source pin is gone now)
        again = a.fetch_complete(worker.token, (k, pin, copy))
        assert results(again) == ["HIT"] and again.items[0].location == copy
        assert again.items[0].pin_id != first.items[0].pin_id
        assert service.pins.count(k, copy) == 2
        # the first reader's UNPIN leaves the second reader protected
        worker.unpin(first.items[0].pin_id)
        assert a.retire((k, copy)).items[0].cause == KeptCause.PINNED

    def test_a_failing_pin_leaves_no_copy_and_releases_the_source(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        pin = a.source_pin((k, source)).items[0].pin_id
        copy = a.loc(a.dram(), 1)
        original = service.pins.pin

        def failing(*args, **kwargs):
            if kwargs.get("creator"):
                raise RuntimeError("boom")
            return original(*args, **kwargs)

        service.pins.pin = failing
        done = a.fetch_complete(worker.token, (k, pin, copy))
        service.pins.pin = original
        assert results(done) == ["REJECTED"]
        assert service.catalog.entry_exact(k, copy) is None
        assert service.pins.get(pin) is None

    def test_source_pin_misses(self, env):
        clock, _, wire = env
        a, b, worker, k, source = self._setup(wire)
        from dataclasses import replace

        gone = replace(source, generation=source.generation + 5)
        response = a.source_pin((k, gone))
        assert results(response) == ["MISS"]
        assert response.items[0].reason == MissReason.ABSENT
        clock.advance(20)
        a.heartbeat()
        clock.advance(15)
        response = a.source_pin((k, source))
        assert response.items[0].reason == MissReason.OFFLINE

    def test_fetch_complete_resend_after_the_worker_left_replays(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        from maru_common.protocol import (
            FetchCompleteItem,
            FetchCompleteRequest,
            MessageType,
        )

        pin = a.source_pin((k, source)).items[0].pin_id
        copy = a.loc(a.dram(), 1)
        request = a.build(
            FetchCompleteRequest,
            **a.envelope(),
            worker_session=worker.token,
            items=[FetchCompleteItem(k, pin, copy, copy.length)],
        )
        first = wire.call(MessageType.FETCH_COMPLETE, request)
        assert results(first) == ["HIT"]
        worker.close()
        # the reply was lost; the resend gets the same result, not REJECTED
        assert wire.call(MessageType.FETCH_COMPLETE, request) == first


class TestHoldCopy:
    """FETCH_COMPLETE with hold_copy: the node's pin on the copy comes with
    the worker's, so a shared fetch's copy outlives its first worker."""

    _setup = TestFetch._setup

    def _first(self, a, worker, k, source, copy):
        pin = a.source_pin((k, source)).items[0].pin_id
        return a.fetch_complete(worker.token, (k, pin, copy, True)).items[0]

    def test_both_pins_at_once(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        copy = a.loc(a.dram(), 1)
        item = self._first(a, worker, k, source, copy)
        assert item.result == "HIT" and item.location == copy
        assert item.pin_id and item.node_pin_id and item.pin_id != item.node_pin_id
        node_pin = service.pins.get(item.node_pin_id)
        assert (node_pin.holder, node_pin.holder_kind) == (a.token, "node")
        assert node_pin.location == copy
        assert service.pins.count(k, copy) == 2
        # the source pin went as without hold_copy
        assert service.pins.held_by(a.token) == [node_pin]

    def test_the_node_pin_blocks_removal_until_the_node_unpins(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        copy = a.loc(a.dram(), 1)
        item = self._first(a, worker, k, source, copy)
        worker.unpin(item.pin_id)
        assert a.retire((k, copy)).items[0].cause == KeptCause.PINNED
        dest = a.loc(a.ssd(), 1)
        finalizer = a.commit((k, dest), finalizer=True).items[0].finalizer_id
        moved = a.finalize_move((k, copy, dest, finalizer)).items[0]
        assert moved.cause == KeptCause.PINNED
        # only the node session may release it
        assert results(b.unpin(item.node_pin_id)) == ["REJECTED"]
        assert results(worker.unpin(item.node_pin_id)) == ["REJECTED"]
        assert results(a.unpin(item.node_pin_id)) == ["OK"]
        assert results(a.retire((k, copy))) == ["RETIRED"]

    def test_other_waiters_pin_the_held_copy_after_the_first_worker_exits(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        copy = a.loc(a.dram(), 1)
        first = self._first(a, worker, k, source, copy)
        other = FakeWorker(wire, a, tp_rank=1)
        other.open()
        assert results(a.worker_exit(worker.ref())) == ["OK"]
        assert service.pins.get(first.pin_id) is None
        assert a.retire((k, copy)).items[0].cause == KeptCause.PINNED
        # a waiter names the exact copy: its own pin, no node pin, no read
        again = a.fetch_complete(other.token, (k, "", copy)).items[0]
        assert again.result == "HIT" and again.location == copy
        assert again.pin_id and again.node_pin_id is None
        assert a.unpin(first.node_pin_id).items[0].result == "OK"
        assert a.retire((k, copy)).items[0].cause == KeptCause.PINNED
        other.unpin(again.pin_id)
        assert results(a.retire((k, copy))) == ["RETIRED"]

    def test_a_cancel_before_handoff_unpins_both_and_keeps_the_cache(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        copy = a.loc(a.dram(), 1)
        item = self._first(a, worker, k, source, copy)
        # the node made both pins, so it may release both
        assert results(a.unpin(item.pin_id, item.node_pin_id)) == ["OK", "OK"]
        assert service.pins.count(k, copy) == 0
        assert worker.lookup(k).items[0].location == copy
        worker.unpin(service.pins.held_by(worker.token)[0].pin_id)
        assert results(a.retire((k, copy))) == ["RETIRED"]

    def test_with_no_worker_the_copy_is_named_and_held(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        token = worker.token
        worker.close()
        copy = a.loc(a.dram(), 1)
        pin = a.source_pin((k, source)).items[0].pin_id
        item = a.fetch_complete(token, (k, pin, copy, True)).items[0]
        assert item.result == "ADDED" and item.pin_id is None
        assert item.location == copy and item.node_pin_id
        assert service.pins.count(k, copy) == 1
        assert a.retire((k, copy)).items[0].cause == KeptCause.PINNED
        # with a copy there already, that copy is the one held
        pin = a.source_pin((k, source)).items[0].pin_id
        late = a.fetch_complete(token, (k, pin, a.loc(a.dram(), 2), True)).items[0]
        assert late.result == "DUPLICATE" and late.location == copy
        assert service.pins.get(late.node_pin_id).location == copy
        assert service.pins.count(k, copy) == 2

    def test_the_pins_go_on_the_copy_chosen(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        copy = a.loc(a.dram(), 1)
        a.commit((k, copy))
        item = self._first(a, worker, k, source, a.loc(a.dram(), 2))
        assert item.result == "HIT" and item.location == copy
        assert service.pins.get(item.node_pin_id).location == copy
        assert service.pins.count(k, copy) == 2
        assert len(service.catalog.locations(k)) == 2  # source and one copy

    def test_a_resend_replays_the_node_pin(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        from maru_common.protocol import (
            FetchCompleteItem,
            FetchCompleteRequest,
            MessageType,
        )

        pin = a.source_pin((k, source)).items[0].pin_id
        copy = a.loc(a.dram(), 1)
        request = a.build(
            FetchCompleteRequest,
            **a.envelope(),
            worker_session=worker.token,
            items=[FetchCompleteItem(k, pin, copy, copy.length, hold_copy=True)],
        )
        first = wire.call(MessageType.FETCH_COMPLETE, request)
        assert first.items[0].node_pin_id
        worker.close()
        # the reply was lost: same pins back, none made again
        assert wire.call(MessageType.FETCH_COMPLETE, request) == first
        assert service.pins.count(k, copy) == 1  # the worker's went at close
        assert service.pins.get(first.items[0].node_pin_id) is not None
        # the same op with another hold_copy is another request
        changed = replace(request, items=[FetchCompleteItem(k, pin, copy, copy.length)])
        assert wire.call(MessageType.FETCH_COMPLETE, changed).status == "REJECTED"

    def test_a_failing_worker_pin_leaves_neither_pin(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        original = service.pins.pin

        def failing(*args, **kwargs):
            if kwargs.get("creator"):
                raise RuntimeError("boom")
            return original(*args, **kwargs)

        service.pins.pin = failing
        copy = a.loc(a.dram(), 1)
        item = self._first(a, worker, k, source, copy)
        service.pins.pin = original
        assert item.result == "REJECTED" and item.node_pin_id is None
        assert service.catalog.entry_exact(k, copy) is None
        assert len(service.pins) == 0

    def test_no_node_pin_when_the_item_fails(self, env):
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        done = a.fetch_complete(worker.token, (k, "nope", a.loc(a.dram(), 1), True))
        assert results(done) == ["REJECTED"] and done.items[0].node_pin_id is None
        b_worker = FakeWorker(wire, b)
        b_worker.open()
        pin = a.source_pin((k, source)).items[0].pin_id
        done = a.fetch_complete(b_worker.token, (k, pin, a.loc(a.dram(), 2), True))
        assert results(done) == ["REJECTED"] and done.items[0].node_pin_id is None
        assert len(service.pins) == 0

    def test_a_new_node_run_releases_it(self, env):
        # on CXL too, whose pins are kept apart
        _, service, wire = env
        a, b, worker, k, source = self._setup(wire)
        a.restart()
        assert a.open(pools=[a.dram(), a.ssd(), a.cxl()]).status == Status.OK
        worker = FakeWorker(wire, a)
        worker.open()
        item = self._first(a, worker, k, source, a.loc(a.cxl(), 1))
        assert item.result == "HIT" and item.node_pin_id
        worker.close()
        a.restart()
        assert a.open(pools=[a.dram(), a.ssd(), a.cxl()]).status == Status.OK
        assert service.pins.get(item.node_pin_id) is None


class TestUnpin:
    def test_unknown_is_ok_foreign_is_rejected(self, env):
        _, service, wire = env
        a = open_node(wire, "a")
        k = key("k")
        a.commit((k, a.loc(a.dram(), 1)))
        w1, w2 = FakeWorker(wire, a), FakeWorker(wire, a, tp_rank=1)
        w1.open()
        w2.open()
        pin = w1.lookup(k).items[0].pin_id
        response = w2.unpin(pin, "unknown")
        assert results(response) == ["REJECTED", "OK"]
        assert service.pins.get(pin) is not None
        assert results(w1.unpin(pin, pin)) == ["OK", "OK"]
        assert service.pins.get(pin) is None

    def test_several_pins_on_one_location(self, env):
        _, service, wire = env
        a = open_node(wire, "a")
        k = key("k")
        loc = a.loc(a.dram(), 1)
        a.commit((k, loc))
        w = FakeWorker(wire, a)
        w.open()
        pins = [item.pin_id for item in w.lookup(k, k).items]
        assert len(set(pins)) == 2
        assert service.pins.count(k, loc) == 2
        w.unpin(pins[0])
        assert a.retire((k, loc)).items[0].cause == KeptCause.PINNED
        w.unpin(pins[1])
        assert results(a.retire((k, loc))) == ["RETIRED"]

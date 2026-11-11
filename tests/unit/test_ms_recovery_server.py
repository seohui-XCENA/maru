# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruServer restart: the recovery window, NODE_OPEN resume and
NODE_REREGISTER, through MsService."""

import json
import logging
import uuid
from dataclasses import replace

import pytest

from maru_common.protocol import MessageType, NodeReregisterRequest, ReregisterItem
from maru_common.storage_types import (
    MAX_BATCH,
    CloseMode,
    ItemResult,
    KeptCause,
    MissReason,
    OpRef,
    Status,
    make_generation,
)
from maru_server.ms_service import DEFAULT_RECOVERY_WINDOW_S, MsService
from tests.unit.ms_fakes import (
    PAGE,
    FakeClock,
    FakeNode,
    FakeWorker,
    Wire,
    admin_delete,
    key,
    make_service,
)

WINDOW = 20.0  # within the session TTL: sessions stay open across it


@pytest.fixture
def env():
    clock = FakeClock()
    service = make_service(clock, recovery_window_s=WINDOW)
    return clock, service, Wire(service)


def results(response):
    return [item.result for item in response.items]


def resumed(wire, node_id="n1", epoch_seq=7) -> FakeNode:
    """A node run that was serving before this MaruServer started."""
    node = FakeNode(wire, node_id)
    response = node.open(resume=epoch_seq)
    assert response.status == Status.OK, response.error
    return node


def test_the_default_window_is_thirty_seconds():
    assert DEFAULT_RECOVERY_WINDOW_S == 30.0
    service = MsService(server_epoch="e", clock=FakeClock())
    assert service.recovery_window_s == 30.0 and service.recovering()
    for bad in (-1, float("inf"), float("nan")):
        with pytest.raises(ValueError):
            MsService(server_epoch="e", recovery_window_s=bad)


class TestRecoveryWindow:
    def test_absent_keys_recover_until_the_window_ends(self, env):
        clock, service, wire = env
        node = resumed(wire)
        worker = FakeWorker(wire, node)
        assert worker.open().recovering
        probe = FakeWorker(wire, node, role="probe")
        assert probe.open().recovering
        k = key("absent")
        assert results(worker.lookup(k)) == [ItemResult.RECOVERING]
        assert results(probe.probe(k)) == [ItemResult.RECOVERING]
        assert service.stats()["recovering"] is True
        clock.advance(WINDOW)
        lookup = worker.lookup(k)
        assert results(lookup) == [ItemResult.MISS]
        assert lookup.items[0].reason == MissReason.ABSENT
        assert results(probe.probe(k)) == [ItemResult.MISS]
        assert not worker.open().recovering
        assert service.stats()["recovering"] is False

    def test_node_open_reports_the_window(self, env):
        clock, _, wire = env
        assert FakeNode(wire, "a").open().recovering
        clock.advance(WINDOW + 1)
        assert not FakeNode(wire, "b").open().recovering

    def test_re_registered_keys_hit_during_the_window(self, env):
        _, _, wire = env
        node = resumed(wire)
        k = key("k")
        loc = node.loc(node.dram(), 3)
        assert results(node.reregister((k, loc))) == [ItemResult.ADDED]
        worker = FakeWorker(wire, node)
        worker.open()
        lookup = worker.lookup(k, key("other"))
        assert results(lookup) == [ItemResult.HIT, ItemResult.RECOVERING]
        assert lookup.items[0].location == loc and lookup.items[0].pin_id

    def test_an_offline_key_stays_miss_offline(self, env):
        _, _, wire = env
        a, b = resumed(wire, "a"), resumed(wire, "b")
        k = key("k")
        a.reregister((k, a.loc(a.dram(), 0)), last=True)
        a.close(CloseMode.RESTART)  # its DRAM is hidden: present but offline
        worker = FakeWorker(wire, b)
        worker.open()
        lookup = worker.lookup(k)
        assert results(lookup) == [ItemResult.MISS]
        assert lookup.items[0].reason == MissReason.OFFLINE

    def test_the_maintenance_pass_logs_the_end_once(self, env, caplog):
        clock, service, _ = env
        log = logging.getLogger("maru_server.ms_service")
        log.addHandler(caplog.handler)
        try:
            caplog.set_level("INFO", logger="maru_server.ms_service")
            service.maintain()
            assert "recovery window over" not in caplog.text
            clock.advance(WINDOW)
            service.maintain()
            service.maintain()
        finally:
            log.removeHandler(caplog.handler)
        assert caplog.text.count("recovery window over") == 1


class TestAdminDelete:
    def test_an_absent_key_is_offline_during_the_window(self, env):
        clock, _, wire = env
        node = resumed(wire)
        k = key("doomed")
        during = admin_delete(wire, k)
        assert results(during) == [ItemResult.MISS]
        assert during.items[0].reason == MissReason.OFFLINE  # not known absent
        # a re-registered key is blocked as usual
        node.reregister((k, node.loc(node.dram(), 0)), last=True)
        assert results(admin_delete(wire, k)) == [ItemResult.OK]
        clock.advance(WINDOW)
        after = admin_delete(wire, key("never"))
        assert after.items[0].reason == MissReason.ABSENT


def test_refused_opens_carry_the_server_epoch(env):
    _, service, wire = env
    node = FakeNode(wire, "n1")
    refused = node.open(resume=0)  # out of range
    assert refused.status == Status.REJECTED
    assert refused.server_epoch == service.server_epoch
    worker = FakeWorker(wire, node)
    early = worker.open(node_epoch=node.epoch)
    assert early.status == Status.EPOCH
    assert early.server_epoch == service.server_epoch


class TestWorkerOpenBeforeTheNode:
    def test_a_node_not_resumed_yet_is_named_as_such(self, env):
        _, _, wire = env
        node = FakeNode(wire, "n1")  # resumes later
        worker = FakeWorker(wire, node)
        early = worker.open(node_epoch=node.epoch)
        assert early.status == Status.EPOCH
        assert "not opened its session here yet" in early.error
        assert "attach" not in early.error
        node.open(resume=2)
        assert worker.open().status == Status.OK
        node.restart()  # a new process: the old run is named as replaced
        node.open()
        stale = worker.open(node_epoch=worker.node_epoch)
        assert stale.status == Status.EPOCH and "attach" in stale.error


class TestResume:
    def test_a_resumed_run_keeps_its_epoch_seq(self, env):
        _, service, wire = env
        node = resumed(wire, epoch_seq=9)
        assert node.last_open.epoch_seq == 9
        assert node.last_open.adopted == []
        # new pages of the run carry the same epoch_seq
        k = key("new")
        assert results(node.commit((k, node.loc(node.dram(), 1)))) == [ItemResult.ADDED]
        assert service.stats()["reregistering_nodes"] == 1

    def test_a_resend_of_the_same_resume_renews_the_session(self, env):
        _, _, wire = env
        node = resumed(wire, epoch_seq=4)
        k = key("k")
        loc = node.loc(node.dram(), 0)
        node.reregister((k, loc))
        worker = FakeWorker(wire, node)
        worker.open()
        pin = worker.lookup(k).items[0].pin_id
        first = node.token
        again = node.open(resume=4)  # the reply was lost
        assert again.status == Status.OK and again.epoch_seq == 4
        assert node.token != first
        assert results(node.retire((k, loc))) == [ItemResult.KEPT]  # pin kept
        assert results(worker.unpin(pin)) == [ItemResult.OK]
        assert results(node.retire((k, loc))) == [ItemResult.RETIRED]

    def test_another_epoch_seq_for_the_same_run_is_rejected(self, env):
        _, _, wire = env
        node = resumed(wire, epoch_seq=4)
        response = node.open(resume=5)
        assert response.status == Status.REJECTED and "epoch_seq 4" in response.error

    def test_resume_for_a_node_with_another_run_is_rejected(self, env, caplog):
        _, _, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        late = FakeNode(wire, "n1")  # an older process of the same node
        log = logging.getLogger("maru_server.node_registry")
        log.addHandler(caplog.handler)
        try:
            response = late.open(resume=3)
        finally:
            log.removeHandler(caplog.handler)
        assert response.status == Status.REJECTED and "another run" in response.error
        assert late.epoch in caplog.text and node.epoch in caplog.text  # both named

    def test_an_ended_run_cannot_resume(self, env):
        _, _, wire = env
        node = resumed(wire, epoch_seq=4)
        old_epoch = node.epoch
        node.restart()
        assert node.open().epoch_seq == 5  # the next run counts on from it
        node.epoch = old_epoch
        response = node.open(resume=4)
        assert response.status == Status.REJECTED and "has ended" in response.error


class TestReregister:
    def test_items_are_added_duplicated_or_rejected(self, env):
        _, _, wire = env
        node = resumed(wire)
        a, b, c = key("a"), key("b"), key("c")
        loc_a = node.loc(node.dram(), 0)
        other_a = node.loc(node.dram(), 1)
        later = replace(
            node.loc(node.dram(), 2),
            generation=make_generation(node.epoch_seq + 1, 1),
        )
        foreign = replace(node.loc(node.dram(), 3), pool_id="dram:n9:x")
        response = node.reregister(
            (a, loc_a),
            (a, other_a),  # the key has a location in this pool already
            (b, later),  # a page no run up to this one gave
            (c, foreign),
            (c, node.loc(node.dram(), 4), 0),  # logical_bytes 0
            (c, replace(loc_a, allocation_id="z")),  # overlaps a's page
        )
        assert results(response) == [
            ItemResult.ADDED,
            ItemResult.DUPLICATE,
            ItemResult.REJECTED,
            ItemResult.REJECTED,
            ItemResult.REJECTED,
            ItemResult.REJECTED,
        ]
        assert response.items[1].location == loc_a
        # the very same location again is ADDED, not a duplicate
        assert results(node.reregister((a, loc_a))) == [ItemResult.ADDED]

    def test_a_page_of_an_earlier_run_in_a_kept_pool(self, env):
        _, _, wire = env
        node = FakeNode(wire, "n1")
        ssd = node.ssd()
        node.open(pools=[node.dram(), ssd])
        k = key("on-ssd")
        loc = node.loc(ssd, 0)  # given by run 1
        node.commit((k, loc))
        node.restart()
        node.open(pools=[node.dram(), ssd])  # run 2 keeps the SSD pool
        assert node.epoch_seq == 2
        # MaruServer restarts: a new service; run 2 resumes and sends it again
        restarted = make_service(FakeClock(), recovery_window_s=WINDOW)
        node.wire = Wire(restarted)
        assert node.open(pools=[node.dram(), ssd], resume=2).status == Status.OK
        assert results(node.reregister((k, loc))) == [ItemResult.ADDED]
        worker = FakeWorker(node.wire, node)
        worker.open()
        assert results(worker.probe(k)) == [ItemResult.FETCH]  # SSD: read via node
        # the DRAM pool is new in run 2: a page of run 1 cannot be in it
        old_dram = replace(node.loc(node.dram(), 1), generation=loc.generation)
        assert results(node.reregister((key("x"), old_dram))) == [ItemResult.REJECTED]

    def test_only_a_resumed_run_re_registers(self, env):
        _, _, wire = env
        node = FakeNode(wire, "n1")
        node.open()
        response = node.reregister((key("k"), node.loc(node.dram(), 0)), last=True)
        assert response.status == Status.REJECTED and "resumed" in response.error

    def test_op_rules_are_those_of_commit(self, env):
        _, service, wire = env
        node = resumed(wire)
        k = key("k")
        request = node.build(
            NodeReregisterRequest,
            **node.envelope(),
            items=[ReregisterItem(k, node.loc(node.dram(), 0), PAGE)],
            last=False,
        )
        first = wire.call(MessageType.NODE_REREGISTER, request)
        service.catalog.remove_location(service.catalog.locations(k)[0])
        replay = wire.call(MessageType.NODE_REREGISTER, request)
        assert results(replay) == results(first) == [ItemResult.ADDED]
        assert service.catalog.locations(k) == []  # replayed, not run again
        conflict = replace(request, last=True)
        conflict = replace(conflict, op=replace(request.op, digest=b"\1" * 16))
        bad = wire.call(MessageType.NODE_REREGISTER, conflict)
        assert bad.status == Status.REJECTED
        node.ack = request.op.seq
        node.reregister()  # acks seq 1
        stale = wire.call(
            MessageType.NODE_REREGISTER,
            replace(request, op=replace(request.op, ack=0)),
        )
        assert results(stale) == [ItemResult.STALE]

    def test_last_ends_the_node_s_re_registration(self, env):
        _, service, wire = env
        a, b = resumed(wire, "a"), resumed(wire, "b")
        assert service.stats()["reregistering_nodes"] == 2
        assert node_record(service, "a").reregister_done is False
        a.reregister((key("k"), a.loc(a.dram(), 0)))
        empty = a.reregister(last=True)
        assert empty.status == Status.OK and empty.items == []
        record = node_record(service, "a")
        assert record.reregister_done and record.reregistered == 1
        assert service.stats()["reregistering_nodes"] == 1
        b.reregister(last=True)
        assert service.stats()["reregistering_nodes"] == 0

    def test_a_late_node_re_registers_after_the_window(self, env):
        clock, _, wire = env
        clock.advance(WINDOW * 3)
        node = resumed(wire)
        k = key("late")
        node.reregister((k, node.loc(node.dram(), 0)), last=True)
        worker = FakeWorker(wire, node)
        worker.open()
        assert results(worker.lookup(k, key("never"))) == [
            ItemResult.HIT,
            ItemResult.MISS,
        ]

    def test_session_checks(self, env):
        _, service, wire = env
        node = resumed(wire)
        worker = FakeWorker(wire, node)
        worker.open()
        op = OpRef(node.epoch, 1, b"\0" * 16, 0)
        as_worker = wire.call(
            MessageType.NODE_REREGISTER,
            NodeReregisterRequest(
                session=worker.token,
                server_epoch=service.server_epoch,
                op=op,
                items=[],
                last=True,
            ),
        )
        assert as_worker.status == Status.REJECTED
        node.server_epoch = uuid.uuid4().hex  # an earlier MaruServer's
        assert node.reregister(last=True).status == Status.EPOCH
        node.server_epoch = None
        items = [(key(f"k{i}"), node.loc(node.dram(), i)) for i in range(MAX_BATCH + 1)]
        assert node.reregister(*items).status == Status.REJECTED

    def test_re_registered_locations_carry_no_pin(self, env):
        _, _, wire = env
        node = resumed(wire)
        k = key("k")
        loc = node.loc(node.dram(), 0)
        node.reregister((k, loc))
        assert results(node.retire((k, loc))) == [ItemResult.RETIRED]


class TestWritesDuringReregistration:
    def test_same_pool_keeps_one_location(self, env):
        _, _, wire = env
        node = resumed(wire)
        k = key("k")
        old, new = node.loc(node.dram(), 0), node.loc(node.dram(), 1)
        assert results(node.commit((k, new))) == [ItemResult.ADDED]
        late = node.reregister((k, old))
        assert results(late) == [ItemResult.DUPLICATE]
        assert late.items[0].location == new
        # the other order: the new write is the duplicate
        j = key("j")
        old_j, new_j = node.loc(node.dram(), 2), node.loc(node.dram(), 3)
        node.reregister((j, old_j))
        dup = node.commit((j, new_j))
        assert results(dup) == [ItemResult.DUPLICATE]
        assert dup.items[0].location == old_j

    def test_two_nodes_give_a_key_two_locations(self, env):
        _, service, wire = env
        a, b = resumed(wire, "a"), resumed(wire, "b", epoch_seq=2)
        k = key("k")
        on_a, on_b = a.loc(a.dram(), 0), b.loc(b.dram(), 0)
        b.commit((k, on_b))  # a new write on b while a re-registers
        a.reregister((k, on_a), last=True)
        assert sorted(e.location.pool_id for e in service.catalog.locations(k)) == [
            on_a.pool_id,
            on_b.pool_id,
        ]
        reader_a, reader_b = FakeWorker(wire, a), FakeWorker(wire, b)
        reader_a.open()
        reader_b.open()
        assert reader_a.lookup(k).items[0].location == on_a  # own DRAM first
        assert reader_b.lookup(k).items[0].location == on_b


def test_policy_is_read_again_at_start(tmp_path):
    path = tmp_path / "policy.json"
    k = key("keep")
    path.write_text(json.dumps({"preserve": [{"namespace": "ns", "key": "keep"}]}))
    clock = FakeClock()
    service = make_service(clock, policy_file=str(path), recovery_window_s=WINDOW)
    wire = Wire(service)
    node = resumed(wire)
    loc = node.loc(node.dram(), 0)
    node.reregister((k, loc), last=True)
    kept = node.retire((k, loc))
    assert results(kept) == [ItemResult.KEPT]
    assert kept.items[0].cause == KeptCause.PINNED


def node_record(service, node_id):
    return service.registry.nodes[node_id]

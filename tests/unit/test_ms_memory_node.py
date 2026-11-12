# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Memory-node negotiation, FETCH eligibility and remote move finalization."""

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from maru_common.protocol import (
    FinalizeMoveRequest,
    MessageType,
    MoveItem,
    SourcePinRequest,
)
from maru_common.storage_types import (
    MEMORY_CAPABILITY,
    REMOTE_CAPABILITY,
    STAGE_CAPABILITY,
    CloseMode,
    KeptCause,
    KeyLocation,
    PoolKind,
    SessionRole,
    Status,
)
from tests.unit.ms_fakes import (
    PAGE,
    FakeClock,
    FakeNode,
    FakeWorker,
    Wire,
    key,
    make_service,
)


@pytest.fixture
def env():
    clock = FakeClock()
    service = make_service(clock)
    return clock, service, Wire(service)


def open_node(node, capabilities=(), *, cxl=False):
    pools = [node.dram(), node.ssd()] + ([node.cxl()] if cxl else [])
    request = replace(node.open_request(pools=pools), capabilities=list(capabilities))
    response = node.wire.call(MessageType.NODE_OPEN, request)
    assert response.status == Status.OK
    node.token, node.epoch_seq = response.node_session, response.epoch_seq
    return response


def bound_pin(node, worker, k, source):
    return node.send(
        MessageType.SOURCE_PIN,
        node.build(
            SourcePinRequest,
            **node.envelope(),
            worker_session=worker.token,
            worker_incarnation=worker.incarnation,
            items=[KeyLocation(k, source)],
        ),
    ).items[0]


def setup_move(
    wire,
    kind=PoolKind.DRAM,
    requester_caps=(REMOTE_CAPABILITY,),
    owner_caps=(MEMORY_CAPABILITY,),
):
    gpu, memory = FakeNode(wire, "gpu"), FakeNode(wire, "memory")
    open_node(gpu, requester_caps)
    open_node(memory, owner_caps, cxl=True)
    k = key("move")
    source = gpu.loc(gpu.dram(), 1)
    dest = memory.loc(getattr(memory, kind.value)(), 1)
    assert gpu.commit((k, source)).items[0].result == "ADDED"
    assert memory.commit((k, dest)).items[0].result == "ADDED"
    return gpu, memory, k, source, dest


@pytest.mark.parametrize(
    "requested, accepted",
    [
        ([], []),
        (["unknown_v1"], []),
        ([REMOTE_CAPABILITY], [REMOTE_CAPABILITY]),
        ([MEMORY_CAPABILITY, STAGE_CAPABILITY], [MEMORY_CAPABILITY, STAGE_CAPABILITY]),
        (
            [
                "unknown_v1",
                REMOTE_CAPABILITY,
                STAGE_CAPABILITY,
                REMOTE_CAPABILITY,
                MEMORY_CAPABILITY,
            ],
            [REMOTE_CAPABILITY, STAGE_CAPABILITY, MEMORY_CAPABILITY],
        ),
    ],
)
def test_node_open_stores_and_returns_only_accepted_capabilities(
    env, requested, accepted
):
    _, service, wire = env
    node = FakeNode(wire, "gpu")
    response = open_node(node, requested)
    assert response.capabilities == accepted
    assert service.registry.nodes[node.node_id].session.capabilities == tuple(accepted)
    assert service.registry.capabilities_of(node.node_id) == tuple(accepted)
    assert service.registry.capabilities_of("unknown-node") == ()


def test_same_epoch_reopen_renegotiates_capabilities(env):
    clock, service, wire = env
    node = FakeNode(wire, "gpu")
    first = open_node(node, [REMOTE_CAPABILITY, "unknown_v1"])
    clock.advance(30)
    assert service.registry.capabilities_of(node.node_id) == ()
    again = open_node(node, [REMOTE_CAPABILITY, STAGE_CAPABILITY, "unknown_v2"])
    assert again.epoch_seq == first.epoch_seq
    assert again.capabilities == [REMOTE_CAPABILITY, STAGE_CAPABILITY]
    assert service.registry.node_session(first.node_session, clock()) is None
    assert service.registry.capabilities_of(node.node_id) == tuple(again.capabilities)
    node.restart()
    assert open_node(node).capabilities == []
    assert service.registry.capabilities_of(node.node_id) == ()


@pytest.mark.parametrize("mode", list(CloseMode))
def test_closed_node_has_no_live_capabilities(env, mode):
    _, service, wire = env
    node = FakeNode(wire, "memory")
    open_node(node, [MEMORY_CAPABILITY])
    assert node.close(mode).status == Status.OK
    assert service.registry.capabilities_of(node.node_id) == ()


@pytest.mark.parametrize("kind", list(PoolKind))
@pytest.mark.parametrize(
    "requester_caps",
    [
        (),
        (REMOTE_CAPABILITY,),
        (STAGE_CAPABILITY,),
        (REMOTE_CAPABILITY, STAGE_CAPABILITY),
    ],
)
@pytest.mark.parametrize(
    "owner_caps",
    [
        (),
        (MEMORY_CAPABILITY,),
        (MEMORY_CAPABILITY, STAGE_CAPABILITY),
        (REMOTE_CAPABILITY, STAGE_CAPABILITY),
    ],
)
def test_memory_fetch_capability_table(env, kind, requester_caps, owner_caps):
    _, service, wire = env
    gpu, memory, k, own, source = setup_move(wire, kind, requester_caps, owner_caps)
    # Leave just the remote copy for lookup and worker-bound SOURCE_PIN.
    assert gpu.retire((k, own)).items[0].result == "RETIRED"
    pool = service.catalog.pool(source.pool_id)
    expected = (
        REMOTE_CAPABILITY in requester_caps
        and MEMORY_CAPABILITY in owner_caps
        and (
            kind != PoolKind.SSD
            or (STAGE_CAPABILITY in requester_caps and STAGE_CAPABILITY in owner_caps)
        )
    )
    assert service.registry.can_fetch(gpu.node_id, pool) == expected
    worker = FakeWorker(wire, gpu)
    assert worker.open().status == Status.OK
    probe = FakeWorker(wire, gpu, role=SessionRole.PROBE)
    assert probe.open().status == Status.OK
    item = worker.lookup(k).items[0]
    assert item.result == ("FETCH" if expected else "MISS")
    assert probe.probe(k).items[0].result == item.result
    if expected:
        assert item.plan.source == source
        assert item.plan.source_node == memory.node_id
    else:
        assert item.plan is None
    pinned = bound_pin(gpu, worker, k, source)
    assert pinned.result == ("HIT" if expected else "REJECTED")
    if expected:
        assert service.pins.get(pinned.pin_id).location == source


@pytest.mark.parametrize("kind", list(PoolKind))
@pytest.mark.parametrize("state", ["expired", "restart", "leave", "hidden", "removing"])
def test_memory_fetch_requires_online_owner_and_active_pool(env, kind, state):
    clock, service, wire = env
    caps = (REMOTE_CAPABILITY, STAGE_CAPABILITY)
    gpu, memory, k, own, source = setup_move(
        wire, kind, caps, (MEMORY_CAPABILITY, STAGE_CAPABILITY)
    )
    gpu.retire((k, own))
    worker = FakeWorker(wire, gpu)
    worker.open()
    pool = service.catalog.pool(source.pool_id)
    if state == "expired":
        clock.advance(20)
        gpu.heartbeat()
        worker.heartbeat()
        clock.advance(10)
    elif state in ("restart", "leave"):
        memory.close(CloseMode(state))
    else:
        with service.lock:
            if state == "hidden":
                service.catalog.hide_pool(pool)
            else:
                service.catalog.remove_pool(pool)
    assert not service.registry.can_fetch(gpu.node_id, pool)
    assert worker.lookup(k).items[0].result == "MISS"
    assert bound_pin(gpu, worker, k, source).result != "HIT"
    assert not service.pins.count(k, source)


def test_own_ssd_and_mapped_hit_keep_their_existing_behavior(env):
    _, service, wire = env
    gpu, _, k, own, dest = setup_move(wire)
    worker = FakeWorker(wire, gpu)
    worker.open()
    hit = worker.lookup(k).items[0]
    assert hit.result == "HIT" and hit.location == own
    assert service.registry.can_fetch(
        gpu.node_id, service.catalog.pool(gpu.ssd().pool_id)
    )
    worker.unpin(hit.pin_id)
    gpu.retire((k, own))
    ssd = gpu.loc(gpu.ssd(), 1)
    gpu.commit((k, ssd))
    # The existing cost order prefers remote DRAM over own SSD for FETCH.
    assert worker.lookup(k).items[0].plan.source == dest


@pytest.mark.parametrize("kind", list(PoolKind))
@pytest.mark.parametrize("remote", [False, True])
@pytest.mark.parametrize("memory_node", [False, True])
def test_empty_finalizer_capability_table(env, kind, remote, memory_node):
    _, service, wire = env
    gpu, _, k, source, dest = setup_move(
        wire,
        kind,
        (REMOTE_CAPABILITY,) if remote else (),
        (MEMORY_CAPABILITY,) if memory_node else (),
    )
    item = gpu.finalize_move((k, source, dest, "")).items[0]
    accepted = remote and memory_node
    assert item.result == ("RETIRED" if accepted else "REJECTED")
    assert (service.catalog.entry_exact(k, source) is None) == accepted
    assert service.catalog.entry_exact(k, dest) is not None
    assert len(service.pins) == 0


@pytest.mark.parametrize(
    "field, value",
    [
        ("allocation_id", "other"),
        ("generation", 1),
        ("offset", 0),
        ("length", PAGE // 2),
        ("kind", PoolKind.SSD),
    ],
)
def test_empty_finalizer_requires_the_exact_ready_destination(env, field, value):
    _, service, wire = env
    gpu, _, k, source, dest = setup_move(wire)
    wrong = replace(dest, **{field: value})
    assert gpu.finalize_move((k, source, wrong, "")).items[0].result == "REJECTED"
    assert service.catalog.entry_exact(k, source) is not None
    assert service.catalog.entry_exact(k, dest) is not None


@pytest.mark.parametrize(
    "state",
    [
        "uncommitted",
        "removed",
        "wrong-key",
        "hidden",
        "removing",
        "swept",
        "expired",
        "restart",
        "leave",
    ],
)
def test_empty_finalizer_rejects_unavailable_destination_and_keeps_source(env, state):
    clock, service, wire = env
    gpu, memory, k, source, dest = setup_move(wire)
    if state == "uncommitted":
        dest = memory.loc(memory.dram(), 2)
    elif state == "removed":
        assert memory.retire((k, dest)).items[0].result == "RETIRED"
    elif state == "wrong-key":
        memory.retire((k, dest))
        memory.commit((key("other"), dest))
    elif state in ("hidden", "removing", "swept"):
        with service.lock:
            pool = service.catalog.pool(dest.pool_id)
            if state == "hidden":
                service.catalog.hide_pool(pool)
            else:
                service.catalog.remove_pool(pool)
                if state == "swept":
                    service.catalog.sweep(100)
                    assert service.catalog.pool(dest.pool_id) is None
    elif state == "expired":
        clock.advance(20)
        gpu.heartbeat()
        clock.advance(10)
    else:
        memory.close(CloseMode(state))
    assert gpu.finalize_move((k, source, dest, "")).items[0].result == "REJECTED"
    assert service.catalog.entry_exact(k, source) is not None


def test_empty_finalizer_rejects_a_foreign_source(env):
    _, service, wire = env
    gpu, memory, k, own, dest = setup_move(wire)
    other = FakeNode(wire, "other")
    open_node(other)
    foreign = other.loc(other.dram(), 1)
    other.commit((k, foreign))
    item = gpu.finalize_move((k, foreign, dest, "")).items[0]
    assert (item.result, item.cause, item.location) == (
        "KEPT",
        KeptCause.INVALID,
        foreign,
    )
    assert {e.location for e in service.catalog.locations(k)} == {own, foreign, dest}
    assert service.registry.capabilities_of(memory.node_id) == (MEMORY_CAPABILITY,)


def test_remote_move_keeps_a_pinned_source_then_retries_without_finalizer(env):
    _, service, wire = env
    gpu, _, k, source, dest = setup_move(wire)
    worker = FakeWorker(wire, gpu)
    worker.open()
    pin = worker.lookup(k).items[0].pin_id
    item = gpu.finalize_move((k, source, dest, "")).items[0]
    assert (item.result, item.cause) == ("KEPT", KeptCause.PINNED)
    assert service.catalog.entry_exact(k, source) is not None
    worker.unpin(pin)
    assert gpu.finalize_move((k, source, dest, "")).items[0].result == "RETIRED"
    assert {e.location for e in service.catalog.locations(k)} == {dest}


def test_remote_move_uses_duplicate_location_exactly(env):
    _, service, wire = env
    gpu, memory, k, source, dest = setup_move(wire)
    offered = memory.loc(memory.dram(), 2)
    duplicate = memory.commit((k, offered)).items[0]
    assert duplicate.result == "DUPLICATE" and duplicate.location == dest
    assert gpu.finalize_move((k, source, offered, "")).items[0].result == "REJECTED"
    assert service.catalog.entry_exact(k, source) is not None
    assert (
        gpu.finalize_move((k, source, duplicate.location, "")).items[0].result
        == "RETIRED"
    )


def test_remote_move_does_not_bypass_a_nonempty_finalizer(env):
    _, service, wire = env
    gpu, memory, k, source, dest = setup_move(wire)
    pin = memory.commit((k, dest), finalizer=True).items[0].finalizer_id
    for finalizer in ("unknown", pin):
        assert (
            gpu.finalize_move((k, source, dest, finalizer)).items[0].result
            == "REJECTED"
        )
        assert service.catalog.entry_exact(k, source) is not None
    assert service.pins.get(pin) is not None


def test_local_destination_still_requires_its_session_finalizer(env):
    _, service, wire = env
    gpu, _, k, source, _ = setup_move(wire)
    dest = gpu.loc(gpu.ssd(), 1)
    gpu.commit((k, dest))
    assert gpu.finalize_move((k, source, dest, "")).items[0].result == "REJECTED"
    assert service.catalog.entry_exact(k, source) is not None
    pin = gpu.commit((k, dest), finalizer=True).items[0].finalizer_id
    assert gpu.finalize_move((k, source, dest, pin)).items[0].result == "RETIRED"
    assert service.pins.get(pin) is None


def test_remote_move_replays_after_destination_retired(env):
    _, service, wire = env
    gpu, memory, k, source, dest = setup_move(wire)
    request = gpu.build(
        FinalizeMoveRequest, **gpu.envelope(), items=[MoveItem(k, source, dest, "")]
    )
    first = wire.call(MessageType.FINALIZE_MOVE, request)
    assert first.items[0].result == "RETIRED"
    assert memory.retire((k, dest)).items[0].result == "RETIRED"
    assert wire.call(MessageType.FINALIZE_MOVE, request) == first
    assert service.catalog.locations(k) == []


def test_destination_removed_while_finalize_waits_for_lock_keeps_source(env):
    _, service, wire = env
    gpu, memory, k, source, dest = setup_move(wire)
    started = threading.Event()

    def finalize():
        started.set()
        return gpu.finalize_move((k, source, dest, "")).items[0]

    with ThreadPoolExecutor(max_workers=1) as executor:
        with service.lock:
            pending = executor.submit(finalize)
            assert started.wait(5)
            assert service.catalog.retire(memory.node_id, k, dest).result == "RETIRED"
        assert pending.result(timeout=5).result == "REJECTED"
    assert service.catalog.entry_exact(k, source) is not None
    assert service.catalog.entry_exact(k, dest) is None

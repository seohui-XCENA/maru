# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Fetch RPC replay and the inactive parked-client hook."""

import dataclasses
import socket
import threading
import time
from concurrent.futures import Future, TimeoutError
from types import SimpleNamespace

import pytest

from maru_common.config import MaruConfig
from maru_common.ms_rpc import PLACEHOLDER_OP, OpOrigin, RpcUnknownError
from maru_common.node_protocol import (
    NODE_MESSAGE_CLASSES,
    FetchCancelResponse,
    FetchItem,
    FetchRequest,
    FetchResponse,
    NodeMessageType,
    recv_frame,
    send_frame,
)
from maru_common.protocol import (
    CommitItem,
    FetchCompleteItem,
    FetchCompleteResponse,
    HandshakeResponse,
    MessageType,
    NodeOpenResponse,
    SourcePinResponse,
    UnpinResponse,
)
from maru_common.serializer import Serializer
from maru_common.storage_types import (
    FETCH_CAPABILITY,
    MAX_BATCH,
    FetchPlan,
    ItemOutcome,
    ItemResult,
    Key,
    KeyLocation,
    Location,
    MissReason,
    PoolKind,
    Status,
    StorageUnavailableError,
)
from maru_handler.client.maru_client import MaruClient
from maru_handler.client.server_conn import ServerConn
from maru_handler.client.storage_conn import OpTracker, StorageConn
from maru_storage_service.ports import EpochChanged, ReplyError, ServerStopped
from maru_storage_service.server_client import ServerClient
from tests.unit.ms_fakes import PAGE, FakeNode, FakeWorker, Wire, key, make_service
from tests.unit.ms_node_helpers import wait_until

SOURCE = Location("ssd:n:/tmp/pool.data", "a0", 1, 0, PAGE, PoolKind.SSD)
COPY = Location("dram:n:run", "a1", 2, PAGE, PAGE, PoolKind.DRAM)
ITEMS = [FetchItem(Key("ns", "k"), FetchPlan(SOURCE, "n", PAGE))]
HIT = FetchResponse(Status.OK, items=[ItemOutcome(ItemResult.HIT, COPY, "worker-pin")])


@pytest.fixture
def metadata(monkeypatch):
    monkeypatch.setattr("maru_storage_service.server_client.RESEND_PAUSE_S", 0)
    service = make_service()
    wire = Wire(service)
    node = FakeNode(wire, "n")
    client = ServerClient("tcp://127.0.0.1:1", "n", node.epoch, host_boot_id=node.boot)
    client._rpc.call = wire.call
    response = client.open([node.dram(), node.ssd()])
    node.token, node.epoch_seq = response.node_session, response.epoch_seq
    yield client, service, wire, node
    client.close()


@pytest.mark.parametrize("hold_copy", [False, True])
def test_metadata_replays_applied_fetch_rpcs_without_duplicate_pins(
    metadata, hold_copy
):
    client, service, wire, node = metadata
    worker = FakeWorker(wire, node)
    assert worker.open().status == Status.OK
    k = key("k")
    source, copy = node.loc(node.ssd(), 0), node.loc(node.dram(), 1)
    client.commit([CommitItem(k, source, PAGE)])
    calls, applied = {}, {}

    def lose_reply(code, request):
        calls.setdefault(code, []).append(request)
        response = wire.call(code, request)
        if code not in applied:
            applied[code] = response
            raise RpcUnknownError("applied; reply lost")
        assert response == applied[code]
        return response

    client._rpc.call = lose_reply
    pin = client.source_pin([KeyLocation(k, source)]).items[0].pin_id
    assert pin and len(service.pins) == 1
    outcome = client.fetch_complete(
        worker.token, [FetchCompleteItem(k, pin, copy, PAGE, hold_copy=hold_copy)]
    ).items[0]
    assert outcome.location == copy and outcome.pin_id
    assert bool(outcome.node_pin_id) is hold_copy
    assert len(service.pins) == 1 + hold_copy
    for code in (MessageType.SOURCE_PIN, MessageType.FETCH_COMPLETE):
        first, replay = calls[code]
        assert first.op == replay.op and first.server_epoch == replay.server_epoch
    assert calls[MessageType.FETCH_COMPLETE][0].items[0].hold_copy is hold_copy
    pins = [outcome.pin_id] + ([outcome.node_pin_id] if hold_copy else [])
    assert client.unpin(pins).status == Status.OK
    assert len(service.pins) == 0 and not client.op_origin.outstanding
    assert len(calls[MessageType.UNPIN]) == 2
    assert calls[MessageType.UNPIN][0] == calls[MessageType.UNPIN][1]
    assert not hasattr(calls[MessageType.UNPIN][0], "op")


def test_fetch_complete_returns_node_pin_when_worker_has_gone(metadata):
    client, service, wire, node = metadata
    k, copy = key("k"), node.loc(node.dram(), 1)
    # An existing exact copy is also a valid completion source.
    client.commit([CommitItem(k, copy, PAGE)])
    pin = client.source_pin([KeyLocation(k, copy)]).items[0].pin_id
    outcome = client.fetch_complete(
        "gone-worker", [FetchCompleteItem(k, pin, copy, PAGE, hold_copy=True)]
    ).items[0]
    assert outcome.location == copy and outcome.pin_id is None and outcome.node_pin_id
    assert len(service.pins) == 1
    assert client.unpin([outcome.node_pin_id]).status == Status.OK
    assert len(service.pins) == 0


def invoke(client, code):
    if code == MessageType.SOURCE_PIN:
        return client.source_pin([KeyLocation(Key("ns", "k"), SOURCE)])
    if code == MessageType.FETCH_COMPLETE:
        return client.fetch_complete(
            "worker", [FetchCompleteItem(Key("ns", "k"), "p", COPY, PAGE, True)]
        )
    return client.unpin(["p"])


RESPONSES = {
    MessageType.SOURCE_PIN: SourcePinResponse,
    MessageType.FETCH_COMPLETE: FetchCompleteResponse,
    MessageType.UNPIN: UnpinResponse,
}


@pytest.mark.parametrize("code", RESPONSES)
@pytest.mark.parametrize("restart", [False, True])
def test_fetch_rpcs_renew_only_within_the_same_server_epoch(metadata, code, restart):
    client, _, _, _ = metadata
    old_epoch, old_session = client.server_epoch, client.session
    calls = []

    def call(got, request):
        if got == MessageType.HANDSHAKE:
            return HandshakeResponse(
                success=True, server_epoch="new" if restart else old_epoch
            )
        if got == MessageType.NODE_OPEN:
            return dataclasses.replace(client_open, node_session="renewed")
        assert got == code
        calls.append(request)
        if len(calls) == 1:
            raise RpcUnknownError("unknown")
        if len(calls) == 2:
            return RESPONSES[code](Status.EPOCH)
        return RESPONSES[code](Status.OK, items=[ItemOutcome(ItemResult.OK)])

    client_open = NodeOpenResponse(
        Status.OK,
        node_session=old_session,
        server_epoch=old_epoch,
        epoch_seq=client.epoch_seq,
        heartbeat_s=1,
        session_ttl_s=30,
    )
    client._rpc.call = call
    if restart:
        with pytest.raises(EpochChanged):
            invoke(client, code)
        assert client.epoch_lost and len(calls) == 2
    else:
        assert invoke(client, code).status == Status.OK
        assert [r.session for r in calls] == [old_session, old_session, "renewed"]
        if code != MessageType.UNPIN:
            assert calls[0].op == calls[1].op == calls[2].op
    assert all(r.server_epoch == old_epoch for r in calls)
    assert not client.op_origin.outstanding


@pytest.mark.parametrize("code", RESPONSES)
def test_fetch_rpcs_check_reply_count_and_stop_unknown_ops(metadata, code):
    client, _, _, _ = metadata
    client._rpc.call = lambda *_: RESPONSES[code](Status.OK, items=[])
    with pytest.raises(ReplyError):
        invoke(client, code)
    assert not client.op_origin.outstanding

    def stop_unknown(*_):
        client.stop()
        raise RpcUnknownError("still unknown")

    client._rpc.call = stop_unknown
    with pytest.raises(ServerStopped):
        invoke(client, code)
    assert bool(client.op_origin.outstanding) is (code != MessageType.UNPIN)


def test_storage_conn_matches_cancel_and_late_fetch_replies():
    conn = StorageConn("unused", on_notify=lambda *_: None, on_lost=lambda *_: None)
    local, peer = socket.socketpair()
    peer.settimeout(2)
    conn._sock = local
    thread = threading.Thread(target=conn._receive_loop, daemon=True)
    thread.start()
    serializer = Serializer(NODE_MESSAGE_CLASSES)
    request = OpOrigin("inc").stamp(
        FetchRequest(PLACEHOLDER_OP, "worker", ITEMS, PAGE, 250)
    )
    try:
        fetch = conn.fetch(request)
        with pytest.raises(TimeoutError):
            fetch.result(0.001)
        cancel = conn.fetch_cancel(request.op)
        first, _ = recv_frame(peer, max_fds=0)
        second, _ = recv_frame(peer, max_fds=0)
        fetch_header, sent = serializer.decode_request(first)
        cancel_header, sent_cancel = serializer.decode_request(second)
        assert sent == request and sent_cancel.target == request.op
        send_frame(
            peer,
            serializer.encode_response(cancel_header, FetchCancelResponse(Status.OK)),
        )
        assert cancel.result(2).status == Status.OK and not fetch.done()
        send_frame(peer, serializer.encode_response(fetch_header, HIT))
        assert fetch.result(2) == HIT
    finally:
        conn.close()
        peer.close()
        thread.join(2)
    assert not thread.is_alive()


class FetchConn:
    closed = False

    def __init__(self):
        self.requests = []
        self.replies = []
        self.cancels = []

    def fetch(self, request):
        future = Future()
        self.requests.append(request)
        self.replies.append(future)
        return future

    def fetch_cancel(self, target):
        self.cancels.append(target)
        future = Future()
        future.set_result(FetchCancelResponse(Status.OK))
        return future


@pytest.fixture
def client():
    c = MaruClient(
        MaruConfig(
            maru_node="/nonexistent/node.sock",
            engine_id="engine",
            cache_namespace="ns",
            auto_connect=False,
            pin_pools=False,
        )
    )
    unpins = []
    c._server = SimpleNamespace(
        session="worker",
        server_epoch="server",
        session_node_epoch="run",
        unpin=lambda ids: unpins.extend(ids),
        unpin_pending=lambda ids: ids & set(unpins),
    )
    c.node_epoch = "run"
    c._ops = OpTracker(c.identity.incarnation, "run")
    c._storage = FetchConn()
    c.attach_info = SimpleNamespace(capabilities=[FETCH_CAPABILITY])
    c._post = lambda task: task()
    yield c, unpins
    with c._lock:
        c._end_fetch_scopes()
    c.pools.close()


def parked(c, window=8, **config):
    """Retrieves on this thread fetch, within ``window`` pages."""
    c.config = dataclasses.replace(c.config, fetch_loads=True, **config)
    c.attach_info.budgets = SimpleNamespace(fetch_request_max_bytes=window * PAGE)
    c.attach_info.page_size = PAGE
    c._fetch_local.request_id = "parked"


def answer(c, *responses):
    """The node answers the next FETCHes at once."""
    queued, fetch = list(responses), c._storage.fetch

    def answered(request):
        future = fetch(request)
        if queued:
            future.set_result(queued.pop(0))
        return future

    c._storage.fetch = answered


def looked_up(c, *outcomes, node_epoch="run"):
    c._server.lookup_and_pin = lambda _keys: (list(outcomes), "server", node_epoch)


PLANNED = ItemOutcome(ItemResult.FETCH, plan=ITEMS[0].plan)


@pytest.mark.parametrize("cancelled", [False, True])
def test_unknown_fetch_replays_same_op_and_cancelled_hit_is_unpinned(client, cancelled):
    c, unpins = client
    scope = c._new_fetch_scope("request", deadline_ms=5000)
    result = c._fetch(scope, ITEMS, PAGE)
    first = c._storage.requests[0]
    assert c._ops.unresolved()[0][0] == NodeMessageType.FETCH
    c._storage.replies[0].set_exception(StorageUnavailableError("UDS broke"))
    if cancelled:
        c._cancel_fetch_scope(scope)
        assert result.result().items[0].result == ItemResult.MISS
    c._storage = FetchConn()
    c._resend_unresolved()
    replay = c._storage.requests[0]
    assert replay.op == first.op and replay.deadline_ms <= first.deadline_ms
    assert scope.request_id == "request"
    if cancelled:
        assert c._storage.cancels == [first.op]
    c._storage.replies[0].set_result(HIT)
    assert unpins == (["worker-pin"] if cancelled else [])
    assert not c._ops.unresolved() and not scope.pending
    if not cancelled:
        assert result.result() == HIT
        c._cancel_fetch_scope(scope)
        assert not unpins  # A delivered read belongs to its caller.


def test_fetch_deadline_keeps_unknown_op_until_late_pin_cleanup(client):
    c, unpins = client
    scope = c._new_fetch_scope("deadline", deadline_ms=30)
    result = c._fetch(scope, ITEMS, PAGE)
    assert result.result(2).items[0].reason == MissReason.TIMEOUT
    assert scope.cancelled and c._ops.unresolved()
    c._storage.replies[0].set_result(HIT)
    assert unpins == ["worker-pin"] and not c._ops.unresolved()
    assert c.stats["fetch_scope_timeout"] == c.stats["fetch_late_hit"] == 1
    with c._lock:
        assert c._fetch_used() == 1  # the slot stays while the UNPIN is queued
        unpins.clear()  # confirmed
        assert c._fetch_used() == 0


def test_fetch_cancel_is_scoped_and_late_reply_never_maps_bytes(client, monkeypatch):
    c, unpins = client
    monkeypatch.setattr(
        c.pools, "view", lambda *_a, **_kw: pytest.fail("mapped late HIT")
    )
    a = c._new_fetch_scope("a", deadline_ms=5000)
    b = c._new_fetch_scope("b", deadline_ms=5000)
    first, second = c._fetch(a, ITEMS, PAGE), c._fetch(b, ITEMS, PAGE)
    c._cancel_fetch_scope(a)
    c._cancel_fetch_scope(a)
    assert first.done() and not second.done() and len(c._storage.cancels) == 1
    c._storage.replies[0].set_result(HIT)
    c._storage.replies[1].set_result(
        dataclasses.replace(
            HIT,
            items=[dataclasses.replace(HIT.items[0], pin_id="other-pin")],
        )
    )
    assert unpins == ["worker-pin"] and second.result().items[0].pin_id == "other-pin"


def test_server_epoch_change_drops_queued_and_unknown_fetches(client):
    c, unpins = client
    queued = []
    c._post = queued.append
    scope = c._new_fetch_scope("old", deadline_ms=5000)
    result = c._fetch(scope, ITEMS, PAGE)
    c._server_epoch_changed("server", "new")
    c._server.server_epoch = "new"
    for task in queued:
        task()
    c._resend_unresolved()
    assert result.result().items[0].result == ItemResult.MISS
    assert not c._storage.requests and not c._ops.unresolved() and not unpins


def test_server_epoch_change_does_not_replay_a_sent_unknown_fetch(client):
    c, unpins = client
    result = c._fetch(c._new_fetch_scope("old", deadline_ms=5000), ITEMS, PAGE)
    old = c._storage
    old.replies[0].set_exception(StorageUnavailableError("lost reply"))
    c._server_epoch_changed("server", "new")
    c._server.server_epoch = "new"
    c._storage = FetchConn()
    c._resend_unresolved()
    assert result.result().items[0].result == ItemResult.MISS
    assert not c._storage.requests and not c._ops.origin.outstanding and not unpins


def test_fetch_malformed_reply_cleans_up_all_returned_pins(client):
    c, unpins = client
    result = c._fetch(c._new_fetch_scope("malformed", deadline_ms=5000), ITEMS, PAGE)
    c._storage.replies[0].set_result(
        FetchResponse(
            Status.OK,
            items=[HIT.items[0], dataclasses.replace(HIT.items[0], pin_id="second")],
        )
    )
    with pytest.raises(StorageUnavailableError, match="bad FETCH reply"):
        result.result()
    assert unpins == ["worker-pin", "second"] and not c._ops.unresolved()


def test_fetch_unknown_records_are_bounded_and_cancel_retains_them(client, monkeypatch):
    c, _ = client
    monkeypatch.setattr("maru_handler.client.maru_client.MAX_UNRESOLVED_FETCHES", 1)
    first = c._new_fetch_scope("first", deadline_ms=5000)
    c._fetch(first, ITEMS, PAGE)
    c._cancel_fetch_scope(first)
    result = c._fetch(c._new_fetch_scope("second"), ITEMS, PAGE)
    assert result.result().status == Status.BUSY
    assert len(c._ops.unresolved()) == len(c._storage.requests) == 1


def test_a_busy_fetch_misses_at_once_and_its_op_still_resolves(client, monkeypatch):
    c, unpins = client
    monkeypatch.setattr("maru_handler.client.maru_client.BUSY_RESEND_S", 0.001)
    scope = c._new_fetch_scope("busy", deadline_ms=5000)
    result = c._fetch(scope, ITEMS, PAGE)
    first = c._storage.requests[0]
    c._storage.replies[0].set_result(FetchResponse(Status.BUSY))
    # The load misses now, not at its deadline; the node hears the cancel
    assert result.result(0).items[0].reason == MissReason.BUSY
    assert scope.cancelled and c.stats["fetch_scope_busy"] == 1
    assert c._storage.cancels == [first.op]
    wait_until(lambda: len(c._storage.requests) == 2, what="same-op resend")
    resent = c._storage.requests[1]
    assert resent.deadline_ms <= first.deadline_ms  # the time left
    assert dataclasses.replace(resent, deadline_ms=first.deadline_ms) == first
    assert c._ops.unresolved()
    c._storage.replies[1].set_result(HIT)  # late: unpinned, never read
    assert unpins == ["worker-pin"] and not c._ops.unresolved() and not scope.pending


def test_a_resent_fetch_carries_the_time_left(client):
    c, _ = client
    scope = c._new_fetch_scope("left", deadline_ms=5000)
    c._fetch(scope, ITEMS, PAGE)
    first = c._storage.requests[0]
    c._storage.replies[0].set_exception(StorageUnavailableError("UDS broke"))
    scope.deadline = time.monotonic() + 1.0
    c._storage = FetchConn()
    c._resend_unresolved()
    resent = c._storage.requests[0]
    assert resent.op == first.op and 900 < resent.deadline_ms <= 1000
    # Past it: 1 ms behind its cancel, so the node answers and the op resolves
    c._storage.replies[0].set_exception(StorageUnavailableError("UDS broke"))
    scope.deadline = time.monotonic() - 1.0
    c._cancel_fetch_scope(scope, MissReason.TIMEOUT)
    c._storage = FetchConn()
    c._resend_unresolved()
    assert c._storage.cancels == [first.op]
    resent = c._storage.requests[0]
    assert resent.op == first.op and resent.deadline_ms == 1
    c._storage.replies[0].set_result(
        FetchResponse(Status.OK, items=[ItemOutcome(ItemResult.MISS)])
    )
    assert not c._ops.unresolved() and not scope.pending


def test_a_busy_fetch_cancel_is_sent_again_while_its_op_is_open(client, monkeypatch):
    c, _ = client
    monkeypatch.setattr("maru_handler.client.maru_client.BUSY_RESEND_S", 0.001)
    conn = c._storage
    answers = [Status.BUSY, Status.OK]

    def cancel(target):
        conn.cancels.append(target)
        future = Future()
        future.set_result(FetchCancelResponse(answers.pop(0)))
        return future

    conn.fetch_cancel = cancel
    scope = c._new_fetch_scope("cancel", deadline_ms=5000)
    c._fetch(scope, ITEMS, PAGE)
    c._cancel_fetch_scope(scope)
    wait_until(lambda: len(conn.cancels) == 2, what="cancel resent")
    assert conn.cancels == [conn.requests[0].op] * 2 and not answers


def test_a_large_fetch_gets_a_longer_deadline(client):
    c, _ = client
    parked(c, window=64, fetch_deadline_ms=250, fetch_deadline_per_page_ms=10)
    for keys, longest in ((3, 250), (40, 400)):
        looked_up(c, *[PLANNED] * keys)
        answer(c, FetchResponse(Status.OK, items=[ItemOutcome(ItemResult.MISS)] * keys))
        assert c._retrieve_chunk([f"k{i}" for i in range(keys)]) == [None] * keys
        assert longest - 10 < c._storage.requests[-1].deadline_ms <= longest
    c.config = dataclasses.replace(c.config, fetch_deadline_per_page_ms=10_000)
    answer(c, FetchResponse(Status.OK, items=[ItemOutcome(ItemResult.MISS)] * 40))
    c._retrieve_chunk([f"k{i}" for i in range(40)])
    assert 59_000 < c._storage.requests[-1].deadline_ms <= 60_000  # the node's cap


def test_an_unset_per_page_deadline_follows_the_node_page(client):
    c, _ = client
    assert c.config.fetch_deadline_per_page_ms is None
    with pytest.raises(ValueError, match="fetch_deadline_per_page_ms"):
        dataclasses.replace(c.config, fetch_deadline_per_page_ms=-1)
    page = 32 << 20
    parked(c, window=64, fetch_deadline_ms=1)
    c.attach_info.page_size = page
    c.attach_info.budgets.fetch_request_max_bytes = 64 * page
    # 32 MiB at 2 GB/s is 16.8 ms a page; a set value wins
    for per_page, longest in ((None, 51), (10, 30)):
        c.config = dataclasses.replace(c.config, fetch_deadline_per_page_ms=per_page)
        looked_up(c, *[PLANNED] * 3)
        answer(c, FetchResponse(Status.OK, items=[ItemOutcome(ItemResult.MISS)] * 3))
        assert c._retrieve_chunk(["a", "b", "c"]) == [None] * 3
        assert longest - 10 < c._storage.requests[-1].deadline_ms <= longest


def test_a_loader_waits_briefly_for_its_unpins_to_be_answered(client, monkeypatch):
    c, _ = client
    parked(c, window=2)
    # Long enough that only the answered UNPIN can end the wait in time
    monkeypatch.setattr("maru_handler.client.maru_client.FETCH_UNPIN_WAIT_S", 30.0)
    server = ServerConn(
        "tcp://127.0.0.1:1",
        c.identity,
        node_id="n",
        node_epoch="run",
        pid=1,
        pid_start=1,
        timeout_s=1,
    )
    server.session, server.server_epoch = "worker", "server"
    server.on_unpinned.append(c._unpins_answered)
    server._rpc.call = lambda _code, _request: UnpinResponse(
        Status.OK, items=[ItemOutcome(ItemResult.OK)]
    )
    c._server = server
    server.unpin(["p"])  # queued: its slot is held
    c._fetch_unpinning.add("p")
    checked, pending = threading.Event(), server.unpin_pending

    def spy(ids):
        found = pending(ids)
        checked.set()
        return found

    server.unpin_pending = spy
    got = []

    def loader():
        with c.fetch_scope("r"):
            got.append(c.retrieve_capacity())

    thread = threading.Thread(target=loader)
    try:
        thread.start()
        assert checked.wait(2)
        assert server._flush_unpins()
        thread.join(5)
        assert not thread.is_alive() and got == [2]
    finally:
        server._rpc.close()


def test_capacity_is_unbounded_without_the_node_connection(client, monkeypatch):
    c, unpins = client
    parked(c, window=2)
    monkeypatch.setattr("maru_handler.client.maru_client.FETCH_UNPIN_WAIT_S", 0)
    unpins.append("p")  # an UNPIN not answered: its slot is held
    c._fetch_unpinning.add("p")
    assert c.retrieve_capacity() == 1
    c._storage.closed = True  # nothing frees until the node is back
    assert c.retrieve_capacity() is None


def test_fetched_hits_and_scope_timeouts_count_apart(client, monkeypatch):
    c, unpins = client
    parked(c)
    monkeypatch.setattr(c.pools, "view", lambda *_a, **_kw: memoryview(bytearray(PAGE)))
    looked_up(c, PLANNED, ItemOutcome(ItemResult.HIT, COPY, "memory-pin"))
    answer(c, HIT)
    reads = c._retrieve_chunk(["fetched", "memory"])
    assert all(reads)
    assert c.stats["retrieve_hit"] == 1  # the memory hit only
    assert c.stats["retrieve_fetch_hit"] == c.stats["fetch_hit"] == 1
    c.release_retrieved(reads)
    c.config = dataclasses.replace(c.config, fetch_deadline_ms=1)
    looked_up(c, PLANNED, PLANNED)
    assert c._retrieve_chunk(["late", "later"]) == [None, None]  # never answered
    assert c.stats["fetch_scope_timeout"] == 1 and c.stats["fetch_timeout"] == 2


def test_a_failure_mid_fetch_reply_unpins_the_pins_not_taken(client, monkeypatch):
    c, unpins = client
    parked(c)
    monkeypatch.setattr(c.pools, "view", lambda *_a, **_kw: memoryview(bytearray(PAGE)))
    looked_up(c, PLANNED, PLANNED)
    pins = [ItemOutcome(ItemResult.HIT, COPY, p) for p in ("p1", "p2")]
    answer(c, FetchResponse(Status.OK, items=pins))
    take, calls = c._take_outcome, []

    def flaky(*args, **kwargs):
        calls.append(args[1])
        if len(calls) == 2:
            raise RuntimeError("mapping failed")
        return take(*args, **kwargs)

    monkeypatch.setattr(c, "_take_outcome", flaky)
    with pytest.raises(RuntimeError):
        c._retrieve_chunk(["a", "b"])
    # p2 goes back from the reply, p1 with its released read: each once
    assert unpins == ["p2", "p1"] and not c._reads
    with c._lock:
        assert c._fetch_unpinning == {"p1", "p2"} and c._fetch_reserved == 0


def test_a_lookup_of_another_node_run_is_not_fetched(client):
    c, unpins = client
    parked(c)
    looked_up(c, PLANNED, node_epoch="old-run")
    assert c._retrieve_chunk(["k"]) == [None]
    assert not c._storage.requests and not unpins
    assert c.stats["retrieve_fetch"] == 1


def test_fetch_socket_send_leaves_the_client_lock_available(client):
    c, _ = client
    queued = []
    c._post = queued.append
    entered, release = threading.Event(), threading.Event()
    original = c._storage.fetch

    def blocked_send(request):
        entered.set()
        assert release.wait(2)
        return original(request)

    c._storage.fetch = blocked_send
    c._fetch(c._new_fetch_scope("parked", deadline_ms=5000), ITEMS, PAGE)
    sender = threading.Thread(target=queued.pop(0))
    sender.start()
    try:
        assert entered.wait(2)
        acquired = c._lock.acquire(timeout=0.2)
        if acquired:
            c._lock.release()
        assert acquired, "foreground work waited on the fetch socket send"
    finally:
        release.set()
        sender.join(2)
    assert not sender.is_alive()


def test_foreground_fetch_is_an_immediate_miss_without_storage_rpc(client):
    c, unpins = client
    c._server.lookup_and_pin = lambda _keys: (
        [ItemOutcome(ItemResult.FETCH, plan=ITEMS[0].plan)],
        "server",
        "run",
    )
    assert c._retrieve_chunk(["k"]) == [None]
    assert not c._storage.requests and not unpins
    assert c.stats["retrieve_fetch"] == 1
    assert FETCH_CAPABILITY not in c._attach_request().capabilities
    c.attach_info.capabilities = []
    assert (
        c._fetch(c._new_fetch_scope("disabled"), ITEMS, PAGE).result().status
        == Status.DISABLED
    )
    assert not c._storage.requests


def test_fetch_reasons_do_not_count_as_not_found(client):
    c, _ = client
    for prefix in ("retrieve", "probe"):
        for reason in (*MissReason, None):
            c._count_miss(prefix, ItemOutcome(ItemResult.MISS, reason=reason))
        for suffix in (
            "not_found",
            "unavailable_offline",
            "busy",
            "timeout",
            "io_error",
            "miss",
        ):
            assert c.stats[f"{prefix}_{suffix}"] == 1


def test_fetch_rejects_invalid_input_before_stamping_an_op(client):
    c, _ = client
    scope = c._new_fetch_scope("invalid")
    for items, budget in (([], PAGE), (ITEMS * (MAX_BATCH + 1), PAGE), (ITEMS, 0)):
        with pytest.raises(ValueError):
            c._fetch(scope, items, budget)
    assert not c._ops.origin.outstanding and not c._storage.requests

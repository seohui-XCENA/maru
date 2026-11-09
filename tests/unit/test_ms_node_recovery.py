# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""A StorageService and its workers across a MaruServer restart (the fake
MaruServer loses its state): resume, re-registration, the server drain and
what waits for it."""

import dataclasses
import logging
import threading
import time

import pytest

from maru_common.config import MaruConfig
from maru_common.node_protocol import DrainAckRequest, NodeMessageType, Notify
from maru_common.protocol import MessageType, NodeHeartbeatResponse
from maru_common.storage_types import (
    BlockRequest,
    DrainKind,
    ExitCause,
    ItemOutcome,
    ItemResult,
    Key,
    KeyLocation,
    Location,
    NotifyKind,
    PoolKind,
    Status,
    make_generation,
)
from maru_handler import MaruHandler
from maru_handler.client import CreditUnavailableError
from maru_storage_service.pool_manager import PageState
from maru_storage_service.ports import EpochChanged, StaleOp
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import (
    PAGE,
    RawWorker,
    node_config,
    remove_tree,
    short_tmpdir,
    start_node,
    wait_until,
)

NS = "ns"


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


@pytest.fixture
def nodes(fake, sockdir):
    started = []

    def make(**kwargs):
        options = {k: kwargs.pop(k) for k in list(kwargs) if k in ("rpc_timeout_s",)}
        service = start_node(
            node_config(fake.url, f"{sockdir}/node.sock", **kwargs), **options
        )
        started.append(service)
        return service

    yield make
    for service in started:
        service.stop()


@pytest.fixture
def handlers():
    made = []

    def make(node, engine="eng", store_timeout_s=10.0):
        handler = MaruHandler(
            MaruConfig(
                maru_node=node.config.socket,
                engine_id=engine,
                cache_namespace=NS,
                chunk_size_bytes=PAGE,
                pool_size=PAGE,
                pin_pools=False,
                timeout_ms=300,
                auto_connect=False,
                store_timeout_s=store_timeout_s,
            )
        )
        handler.connect()
        made.append(handler)
        return handler

    yield make
    for handler in made:
        try:
            handler.close()
        except Exception:
            pass


@pytest.fixture
def raws():
    made = []

    def make(node):
        raw = RawWorker(node.config.socket)
        raw.attach()
        made.append(raw)
        return raw

    yield make
    for raw in made:
        raw.close()


SMALL_CREDIT = {
    "credit_max_bytes": 2 * PAGE,
    "credit_total_max_bytes": 2 * PAGE,
    "credit_refill_low": 1 * PAGE,
    "credit_refill_high": 2 * PAGE,
    "commit_pending_max_bytes": 2 * PAGE,
}


def put(handler, key: str, value: bytes | None = None) -> bool:
    value = value if value is not None else key.encode()
    handle = handler.alloc(len(value))
    handle.buf[:] = value
    return handler.store(key, handle)


def wait_credit(handler, pages: int = 1):
    wait_until(lambda: handler._cpu.credits.unused_bytes >= pages * PAGE, what="credit")


def read(handler, key: str, timeout: float = 10.0) -> bytes:
    """The bytes of ``key`` once the worker can read it again."""

    def attempt():
        lease = handler.retrieve(key)
        if lease is None:
            return None
        try:
            return bytes(lease.view)
        finally:
            handler.release_retrieved([lease])

    return wait_until(attempt, timeout=timeout, what=f"a hit on {key}")


def restart(fake, node, window: float = 5.0) -> str:
    """Restart the fake MaruServer and wait for the node to re-register;
    returns the ended server epoch."""
    old = fake.server_epoch
    recoveries = node.manager.recoveries
    fake.restart(recovery_window_s=window)
    wait_until(
        lambda: node.manager.recoveries > recoveries, timeout=15, what="recovery"
    )
    return old


def written(raw, fake, count: int) -> list[str]:
    """``count`` keys written and committed by a raw worker."""
    names = []
    while len(names) < count:
        credits = raw.credit(min(4, count - len(names)) * PAGE).credits
        if not credits:
            time.sleep(0.01)
            continue
        batch = [f"k{len(names) + i}" for i in range(len(credits))]
        raw.write_done(credits, batch)
        names += batch
    wait_until(lambda: len(fake.keys()) == count, what="commits")
    return names


def page_ref(node, key: str):
    """The READY page of ``key``."""
    for ref, page_key, _ in node.pools.ready_pages():
        if page_key == Key(NS, key):
            return ref
    raise AssertionError(f"no READY page of {key}")


def drain_notices(raw, epoch: str) -> list:
    return [
        n
        for n in raw.notices
        if n.kind == NotifyKind.DRAIN
        and n.drain_kind == DrainKind.SERVER
        and n.epoch == epoch
    ]


class TestReregistration:
    def test_hits_come_back_after_a_restart(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        for name in ("a", "b", "c"):
            assert put(h, name)
        epoch_seq = node.server.epoch_seq
        old = restart(fake, node)
        assert node.server.server_epoch == fake.server_epoch != old
        assert node.server.epoch_seq == epoch_seq and node.server.resumes == 1
        (resume,) = [r for r in fake.requests(MessageType.NODE_OPEN) if r.resume]
        assert resume.resume == epoch_seq and resume.node_epoch == node.node_epoch
        assert {k.key for k in fake.keys()} == {"a", "b", "c"}
        assert fake.reregistered["gpu-test"] == [3, True]
        assert node.manager.safe_state is None and not node.manager.recovering
        assert node.manager.last_reregistration["added"] == 3
        for name in ("a", "b", "c"):
            assert read(h, name) == name.encode()
        assert h._cpu.stats["attach"] == 1  # the UDS stayed up
        wait_credit(h)
        assert put(h, "after")  # new writes commit to the new MaruServer
        assert read(h, "after") == b"after"
        node.pools.check()

    def test_the_last_batch_is_marked_and_batches_are_bounded(
        self, fake, nodes, raws, monkeypatch
    ):
        from maru_storage_service import storage_manager

        node = nodes(pages=16)
        raw = raws(node)
        written(raw, fake, 7)
        monkeypatch.setattr(storage_manager, "MAX_BATCH", 3)
        restart(fake, node)
        batches = fake.requests(MessageType.NODE_REREGISTER)
        assert [len(b.items) for b in batches] == [3, 3, 1]
        assert [b.last for b in batches] == [False, False, True]
        assert len(fake.keys()) == 7

    def test_an_empty_node_sends_one_empty_last_batch(self, fake, nodes):
        node = nodes()
        restart(fake, node)
        (batch,) = fake.requests(MessageType.NODE_REREGISTER)
        assert batch.items == [] and batch.last
        assert fake.reregistered["gpu-test"] == [0, True]

    def test_pending_pages_commit_after_it_with_new_ops(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node, store_timeout_s=0.3)
        assert put(h, "ready")
        wait_credit(h)
        fake.pause(hold=False)  # MaruServer is gone: requests are lost
        assert put(h, "pending") is False  # its COMMIT result is unknown
        assert node.pools.bytes_in(PageState.PENDING) == PAGE
        old_seq = max(node.server.op_origin.outstanding)
        fake.restart(recovery_window_s=5)
        fake.resume()
        wait_until(lambda: node.manager.recoveries == 1, timeout=15, what="recovery")
        wait_until(lambda: Key(NS, "pending") in fake.keys(), what="commit")
        log = [code for code, _ in fake.log]
        last = max(i for i, c in enumerate(log) if c == MessageType.NODE_REREGISTER)
        first_commit = log.index(MessageType.COMMIT, last)
        assert first_commit > last  # committed after the last batch
        (commit,) = [
            r
            for r in fake.requests(MessageType.COMMIT)
            if r.server_epoch == fake.server_epoch
        ]
        assert commit.op.seq > old_seq  # a new op, not the unknown one
        assert old_seq not in node.server.op_origin.outstanding
        assert read(h, "pending") == b"pending"
        assert read(h, "ready") == b"ready"

    def test_a_lost_reply_is_resent_with_the_same_op(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        assert put(h, "k")
        fake.drop_responses(MessageType.NODE_REREGISTER)
        fake.restart(recovery_window_s=5)
        wait_until(lambda: node.manager.recoveries == 1, timeout=15, what="recovery")
        batches = fake.requests(MessageType.NODE_REREGISTER)
        assert len(batches) == 2 and batches[0].op == batches[1].op
        assert fake.executed[MessageType.NODE_REREGISTER] == 1
        assert fake.replayed[MessageType.NODE_REREGISTER] == 1
        assert read(h, "k") == b"k"

    def test_a_refused_resume_enters_the_safe_state(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node, store_timeout_s=0.2)
        assert put(h, "k")
        fake.fail_requests(MessageType.NODE_OPEN, Status.REJECTED)
        fake.restart()
        wait_until(lambda: node.manager.safe_state, what="safe state")
        assert "resume" in node.manager.safe_state
        assert node.pools.credit_stopped
        assert node.manager.recoveries == 0

    def test_dropped_items_are_held_until_the_drain(self, fake, nodes, raws):
        node = nodes()
        raw = raws(node)
        written(raw, fake, 2)
        other = fake.locations(Key(NS, "k1"))[0]

        def duplicate_first(response):
            first = ItemOutcome(ItemResult.DUPLICATE, location=other)
            return dataclasses.replace(response, items=[first, *response.items[1:]])

        fake.rewrite_responses(MessageType.NODE_REREGISTER, duplicate_first)
        old = restart(fake, node)
        # its old readers are not known: no reuse before the drain
        assert node.pools.bytes_in(PageState.READY) == PAGE
        assert node.pools.bytes_in(PageState.QUARANTINED) == PAGE
        raw.request(NodeMessageType.DRAIN_ACK, DrainAckRequest(DrainKind.SERVER, old))
        wait_until(
            lambda: node.pools.bytes_in(PageState.QUARANTINED) == 0, what="freed"
        )
        assert not node.manager.workers.server_drain_open()
        node.pools.check()


class TestServerDrain:
    def test_old_credit_waits_for_its_worker_s_drain_ack(self, fake, nodes, raws):
        node = nodes()
        raw = raws(node)
        held = raw.credit(2 * PAGE).credits
        assert len(held) == 2
        old = restart(fake, node)
        wait_until(lambda: drain_notices(raw, old), what="NOTIFY drain")
        assert node.pools.bytes_in(PageState.QUARANTINED) == 2 * PAGE
        assert node.pools.held_bytes(raw.incarnation) == 0
        # a write into old credit is refused: the page is quarantined
        refused = raw.write_done(held[:1], ["late"])
        assert [i.result for i in refused.items] == [ItemResult.REJECTED]
        assert node.manager.stats()["server_drain_waiting"] == {raw.incarnation: [old]}
        # new credit is of the new epoch and is not held back
        fresh = raw.credit(PAGE).credits
        assert fresh and all(c not in held for c in fresh)
        ack = raw.request(
            NodeMessageType.DRAIN_ACK, DrainAckRequest(DrainKind.SERVER, old)
        )
        assert ack.status == Status.OK
        wait_until(lambda: not node.manager.workers.server_drain_open(), what="done")
        assert node.pools.bytes_in(PageState.QUARANTINED) == 0
        assert node.manager.stats()["server_drain_waiting"] == {}
        node.pools.check()

    def test_eviction_waits_for_every_worker(self, fake, nodes, raws, handlers):
        node = nodes(high=0.5, low=0.25, budgets=SMALL_CREDIT)
        h = handlers(node)
        raw = raws(node)  # alive and attached, never answers the drain
        for i in range(6):
            wait_credit(h)
            assert put(h, f"k{i}")
        assert not fake.requests(MessageType.RETIRE)
        restart(fake, node)
        wait_until(lambda: node.manager.workers.server_drain_open(), what="drain")
        # the client drains on its own; the raw worker never does
        wait_until(
            lambda: (
                set(node.manager.workers.server_drain_pending()) == {raw.incarnation}
            ),
            what="only the silent worker left",
        )
        retires = len(fake.requests(MessageType.RETIRE))
        for i in range(6, 11):
            wait_credit(h)
            assert put(h, f"k{i}")  # above the high watermark now
        time.sleep(0.5)
        assert len(fake.requests(MessageType.RETIRE)) == retires  # paused
        assert node.pools.bytes_in(PageState.RETIRING) == 0
        raw.close()  # the UDS breaks; the process lives: still waiting
        time.sleep(0.3)
        assert node.manager.workers.server_drain_open()
        # its confirmed exit ends the wait (a DETACH-free end of the record)
        node.manager.workers.on_process_exit(f"worker:{raw.incarnation}")
        wait_until(lambda: not node.manager.workers.server_drain_open(), what="done")
        wait_until(
            lambda: len(fake.requests(MessageType.RETIRE)) > retires,
            what="eviction again",
        )

    def test_a_worker_drains_once_its_old_reads_and_writes_end(
        self, fake, nodes, handlers
    ):
        node = nodes()
        h = handlers(node)
        client = h._cpu
        assert put(h, "r")
        lease = h.retrieve("r")  # pinned in the old server epoch
        wait_credit(h)
        writing = h.alloc(4)  # a copy into old credit is under way
        old = restart(fake, node)
        assert client.pins.outstanding(server_epoch=old) == 0  # dropped
        time.sleep(0.3)
        assert (
            client.identity.incarnation in node.manager.workers.server_drain_pending()
        )
        writing.buf[:] = b"late"
        assert h.store("late", writing) is False  # its page was quarantined
        time.sleep(0.2)
        assert node.manager.workers.server_drain_open()  # the read goes on
        assert bytes(lease.view) == b"r"
        unpins = len(fake.requests(MessageType.UNPIN))
        h.release_retrieved([lease])
        wait_until(lambda: not node.manager.workers.server_drain_open(), what="drain")
        time.sleep(0.2)
        assert len(fake.requests(MessageType.UNPIN)) == unpins  # gone with it
        assert read(h, "r") == b"r"

    def test_alloc_racing_the_drain_does_not_write_old_credit(
        self, fake, nodes, handlers
    ):
        node = nodes()
        h = handlers(node)
        client = h._cpu
        wait_credit(h)
        take = client.credits.take

        def take_then_drain(wait_s=0.0):
            taken = take(wait_s)
            client.credits.new_era()  # a server drain slips in here
            return taken

        client.credits.take = take_then_drain
        with pytest.raises(CreditUnavailableError):
            h.alloc(4)
        client.credits.take = take
        assert not client._handles

    def test_read_racing_the_drain_is_not_handed_out(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        client = h._cpu
        assert put(h, "k")
        server = client.server_conn
        lookup = server.lookup_and_pin

        def lookup_then_drain(keys):
            answered = lookup(keys)
            with client._lock:
                client._ended_epochs.add(answered[1])
            return answered

        server.lookup_and_pin = lookup_then_drain
        assert h.retrieve("k") is None
        server.lookup_and_pin = lookup
        assert not client._reads and len(client.pins) == 0

    def test_a_second_restart_during_the_drain(self, fake, nodes, raws, handlers):
        node = nodes()
        h = handlers(node)
        raw = raws(node)
        assert put(h, "k")
        first = restart(fake, node)
        second = restart(fake, node)
        assert second != first
        pending = node.manager.workers.server_drain_pending()
        assert pending[raw.incarnation] == [first, second]
        assert read(h, "k") == b"k"
        assert fake.reregistered["gpu-test"] == [1, True]
        # one DRAIN_ACK of the later epoch covers the earlier one
        raw.request(
            NodeMessageType.DRAIN_ACK, DrainAckRequest(DrainKind.SERVER, second)
        )
        wait_until(lambda: not node.manager.workers.server_drain_open(), what="done")
        assert node.manager.workers.server_drains_done == 2

    def test_a_second_restart_before_the_resume(self, fake, nodes, raws):
        node = nodes()
        raw = raws(node)
        first = fake.server_epoch
        fake.fail_requests(MessageType.NODE_OPEN, Status.BUSY, count=1000)
        fake.restart()
        wait_until(lambda: drain_notices(raw, first), what="drain")
        fake.restart(recovery_window_s=5)  # again, before the node resumed
        with fake._lock:
            fake._fail[int(MessageType.NODE_OPEN)].clear()
        wait_until(lambda: node.manager.recoveries == 1, timeout=15, what="resumed")
        assert node.server.server_epoch == fake.server_epoch
        # the node's session only knew the first epoch: one drain
        assert node.manager.workers.server_drain_pending() == {raw.incarnation: [first]}

    def test_the_drain_is_asked_again_after_a_reattach(self, fake, nodes, raws):
        node = nodes()
        raw = raws(node)
        incarnation = raw.incarnation
        raw.close()  # UDS lost; the record stays until the process exits
        old = restart(fake, node)
        again = RawWorker(node.config.socket, incarnation=incarnation)
        try:
            again.attach()
            wait_until(lambda: drain_notices(again, old), what="drain on reattach")
            again.request(
                NodeMessageType.DRAIN_ACK, DrainAckRequest(DrainKind.SERVER, old)
            )
            wait_until(
                lambda: not node.manager.workers.server_drain_open(), what="done"
            )
        finally:
            again.close()


class TestRemovalAcrossTheRestart:
    def test_unknown_retire_result_is_not_reregistered(
        self, fake, nodes, handlers, raws
    ):
        node = nodes(high=0.5, low=0.25, budgets=SMALL_CREDIT, rpc_timeout_s=0.2)
        h = handlers(node)
        silent = raws(node)  # keeps the drain open
        fake.delay(MessageType.RETIRE, 60.0)  # RETIRE results unknown
        for i in range(9):
            wait_credit(h)
            assert put(h, f"k{i}")
        wait_until(lambda: fake.requests(MessageType.RETIRE), what="a RETIRE")
        retired = {i.key.key for i in fake.requests(MessageType.RETIRE)[0].items}
        fake.delay(MessageType.RETIRE, 0.0)
        old = restart(fake, node)
        kept = {k.key for k in fake.keys()}
        assert kept and not (kept & retired)  # those were not sent again
        # dropped, and held back while the drain is open
        transitions = node.pools.transitions
        assert transitions[(PageState.READY, PageState.QUARANTINED)] == len(retired)
        assert transitions[(PageState.READY, PageState.FREE)] == 0
        assert node.pools.bytes_in(PageState.QUARANTINED) >= len(retired) * PAGE
        silent.request(
            NodeMessageType.DRAIN_ACK, DrainAckRequest(DrainKind.SERVER, old)
        )
        wait_until(lambda: not node.manager.workers.server_drain_open(), what="done")
        node.pools.check()
        assert old != fake.server_epoch

    def test_a_deleted_page_is_not_reregistered(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        assert put(h, "gone")
        assert put(h, "stays")
        fake.drop_requests(MessageType.RETIRE, 1000)  # the delete never ends
        gone = Key(NS, "gone")
        item = KeyLocation(gone, fake.locations(gone)[0])
        fake.queue_block("gpu-test", BlockRequest("b1", (item,), "delete"))
        wait_until(lambda: node.manager._central, what="blocked")
        with fake._lock:
            fake._drop_requests.clear()
        restart(fake, node)
        assert {k.key for k in fake.keys()} == {"stays"}


class TestNodeParts:
    def test_local_pins_are_gated_on_a_live_session(self, fake, nodes):
        node = nodes()
        manager = node.manager
        wait_until(lambda: node.server.last_heartbeat_ok > 0, what="a heartbeat")
        assert manager.local_pins_open()
        node.server.last_heartbeat_ok = time.monotonic() - node.server.session_ttl_s
        assert not manager.local_pins_open()  # node_stale_s passed
        node.server.last_heartbeat_ok = time.monotonic()
        manager.server_lost("test")
        assert not manager.local_pins_open()
        wait_until(lambda: not manager.recovering, timeout=15, what="recovered")
        assert manager.local_pins_open()

    def test_an_op_of_an_ended_server_epoch_is_never_resent(self, fake, nodes):
        node = nodes()
        client = node.server
        epoch = client.server_epoch
        location = Location(
            pool_id=node.pools.pool_ids()[0],
            allocation_id="x",
            generation=make_generation(client.epoch_seq, 0),
            offset=0,
            length=PAGE,
            kind=PoolKind.DRAM,
        )
        errors = []

        def retire():
            try:
                client.retire([KeyLocation(Key(NS, "x"), location)])
            except (EpochChanged, StaleOp) as e:
                errors.append(e)

        fake.pause(hold=False)  # unanswered: resent with the same op
        thread = threading.Thread(target=retire)
        thread.start()
        time.sleep(0.5)
        fake.restart(recovery_window_s=5)
        fake.resume()
        thread.join(15)
        assert errors, "the RETIRE did not end"
        sent = fake.requests(MessageType.RETIRE)
        assert all(r.server_epoch == epoch for r in sent)  # never to the new one
        assert fake.executed[MessageType.RETIRE] == 0
        wait_until(lambda: node.manager.recoveries == 1, timeout=15, what="recovery")


class TestWorkerSide:
    def test_recovering_keys_are_a_miss_counted_apart(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        assert put(h, "k")
        restart(fake, node, window=1.0)
        assert read(h, "k") == b"k"
        assert h.retrieve("never") is None
        assert h.batch_exists(["never"]) == [False]
        stats = h.get_stats()["maru_node"]["counters"]
        assert stats["retrieve_unavailable_recovering"] == 1
        assert stats["probe_unavailable_recovering"] == 1
        assert "retrieve_not_found" not in stats
        time.sleep(1.0)  # the window is over: absent now
        assert h.retrieve("never") is None
        stats = h.get_stats()["maru_node"]["counters"]
        assert stats["retrieve_not_found"] == 1
        assert stats["retrieve_unavailable_recovering"] == 1

    def test_the_node_s_epoch_notice_reopens_the_session_at_once(
        self, fake, nodes, handlers
    ):
        node = nodes()
        h = handlers(node)
        server = h._cpu.server_conn
        server.heartbeat_s = 60.0  # it would not notice by itself for long
        assert put(h, "k")
        restart(fake, node)
        wait_until(lambda: server.server_epoch == fake.server_epoch, what="reopened")

    def test_a_refused_reattach_turns_maru_off(self, fake, nodes, handlers):
        first = nodes()
        h = handlers(first)
        client = h._cpu
        assert put(h, "k")
        attempts = []
        attach = client._attach

        def counted(timeout_s):
            attempts.append(timeout_s)
            return attach(timeout_s)

        client._attach = counted
        first.stop()
        nodes(page_size=PAGE // 2)  # objects no longer fit its pages
        wait_until(lambda: client.off is not None, what="off")
        assert "larger than the page size" in str(client.off)
        with pytest.raises(CreditUnavailableError):
            h.alloc(4)
        assert h.retrieve("k") is None
        assert h.batch_exists(["k"]) == [False]
        tried = len(attempts)
        time.sleep(1.0)
        assert len(attempts) == tried  # no retries
        stats = h.get_stats()["maru_node"]["counters"]
        assert stats["store_skipped_off"] == 1 and stats["retrieve_off"] == 1

    def test_maru_off_ends_its_maruserver_session(self, fake, nodes, handlers):
        first = nodes()
        h = handlers(first)
        client = h._cpu
        assert put(h, "k")
        lease = h.retrieve("k")  # a read still held when Maru turns off
        server = client.server_conn
        first.stop()
        nodes(page_size=PAGE // 2)
        wait_until(lambda: client.off is not None, what="off")
        time.sleep(0.3)
        assert client.server_conn is server  # kept while the read goes on
        assert not fake.requests(MessageType.WORKER_CLOSE)
        h.release_retrieved([lease])
        wait_until(lambda: client.server_conn is None, what="session ended")
        wait_until(lambda: fake.requests(MessageType.WORKER_CLOSE), what="goodbye")
        threads = {t.name for t in threading.enumerate() if t.is_alive()}
        wait_until(
            lambda: (
                not any(
                    t.is_alive()
                    and t.name in ("maru-credit-refill", "maru-worker-heartbeat")
                    for t in client._threads + [server._thread]
                )
            ),
            what="threads stopped",
        )
        assert threads
        # a MaruServer restart brings no new session or RPC from it
        sent = len([r for _, r in fake.log if getattr(r, "incarnation", None)])
        fake.restart(recovery_window_s=5)
        time.sleep(1.5)
        opens = [
            r
            for r in fake.requests(MessageType.WORKER_OPEN)
            if r.incarnation == client.identity.incarnation
        ]
        assert len(opens) == 1  # only the first one
        assert sent == len([r for _, r in fake.log if getattr(r, "incarnation", None)])

    def test_credit_asked_before_a_drain_goes_back(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        client = h._cpu
        wait_credit(h)
        credit = node.manager.credit
        armed = threading.Event()
        late = []

        def drain_while_answering(rec, decoded):
            response = credit(rec, decoded)
            if armed.is_set() and response.credits and not late:
                # the drain notice is handled before this reply arrives
                client._on_notify(
                    Notify(
                        kind=NotifyKind.DRAIN, drain_kind=DrainKind.SERVER, epoch="e"
                    )
                )
                late.extend(response.credits)
            return response

        node.manager.credit = drain_while_answering
        returned = []
        give_back = client._return_credits

        def record(credits):
            returned.extend(credits)
            give_back(credits)

        client._return_credits = record
        armed.set()
        give_back(client.credits.take_all())  # the refill asks for more now
        wait_until(lambda: late, what="a CREDIT answered after the drain")
        wait_until(lambda: all(c in returned for c in late), what="given back")
        cached = client.credits.take_all()
        assert not any(c in cached for c in late)
        wait_until(
            lambda: all(
                (ref := node.pools.find(c)) is None
                or node.pools.page(ref).holder is None
                for c in late
            ),
            what="free at the node",
        )

    def test_old_credit_freed_or_left_unsent_goes_back(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        client = h._cpu
        wait_credit(h, 2)
        freed = h.alloc(4)
        unsent = h.alloc(4)
        client._on_notify(
            Notify(kind=NotifyKind.DRAIN, drain_kind=DrainKind.SERVER, epoch="e")
        )
        returned = []
        client._return_credits = returned.extend
        h.free(freed)  # not put back in the cache: an old era
        assert returned == [freed.location]
        client._storage = None  # the UDS is gone when it would be stored
        unsent.buf[:] = b"late"
        assert h.store("late", unsent) is False
        assert returned == [freed.location, unsent.location]
        assert unsent.location not in client.credits.take_all()

    def test_a_refused_open_on_a_lookup_polls_the_session_back(
        self, fake, nodes, handlers
    ):
        node = nodes()
        h = handlers(node)
        client = h._cpu
        server = client.server_conn
        server.heartbeat_s = 60.0
        assert put(h, "k")
        time.sleep(0.2)  # the loop waits out its long beat now
        server.poke = lambda server_epoch=None: None  # no help from the node
        fake.fail_requests(MessageType.NODE_OPEN, Status.BUSY, count=1000)
        fake.restart(recovery_window_s=5)
        rpcs = len(fake.log)
        wait_until(lambda: h.retrieve("k") is None and server._refused, what="refused")
        for _ in range(10):
            assert h.retrieve("k") is None  # no RPC each
        assert len(fake.log) - rpcs < 10
        with fake._lock:
            fake._fail[int(MessageType.NODE_OPEN)].clear()
        wait_until(lambda: server.server_epoch == fake.server_epoch, timeout=5)
        assert read(h, "k") == b"k"

    def test_a_lookup_of_the_ended_server_is_not_sent_to_the_new_one(
        self, fake, nodes, handlers
    ):
        from maru_common.ms_rpc import PLACEHOLDER_OP
        from maru_common.protocol import LookupAndPinRequest

        node = nodes()
        h = handlers(node)
        assert put(h, "k")
        server = h._cpu.server_conn
        origin = server._origin
        request = origin.stamp(
            LookupAndPinRequest(
                session=server.session,
                server_epoch=server.server_epoch,
                op=PLACEHOLDER_OP,
                items=[Key(NS, "k")],
            )
        )
        restart(fake, node)
        wait_until(lambda: server.server_epoch == fake.server_epoch, what="reopened")
        assert server._send_lookup(origin, request) is None
        assert not [
            r
            for r in fake.requests(MessageType.LOOKUP_AND_PIN)
            if r.server_epoch == fake.server_epoch and r.op.seq == request.op.seq
        ]

    def test_a_worker_ahead_of_its_resuming_node_logs_no_warning(
        self, fake, nodes, handlers, caplog
    ):
        node = nodes()
        h = handlers(node)
        server = h._cpu.server_conn
        assert put(h, "k")
        fake.fail_requests(MessageType.NODE_OPEN, Status.BUSY, count=1000)
        log = logging.getLogger("maru_handler.client.server_conn")
        log.addHandler(caplog.handler)
        try:
            caplog.set_level("DEBUG", logger=log.name)
            fake.restart(recovery_window_s=5)
            wait_until(lambda: server._refused, what="refused while resuming")
            time.sleep(0.6)
            with fake._lock:
                fake._fail[int(MessageType.NODE_OPEN)].clear()
            wait_until(lambda: server.server_epoch == fake.server_epoch, timeout=5)
        finally:
            log.removeHandler(caplog.handler)
        records = [
            r for r in caplog.records if r.name == "maru_handler.client.server_conn"
        ]
        assert not [
            r
            for r in records
            if r.levelname == "WARNING" and "refused" in r.getMessage()
        ]
        assert any("resum" in r.getMessage() for r in records), [
            (r.levelname, r.getMessage()) for r in records
        ]

    def test_a_server_seen_down_is_polled_at_once(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        server = h._cpu.server_conn
        server.heartbeat_s = 60.0  # a slow beat must not delay the poll
        assert put(h, "k")
        time.sleep(0.2)  # the loop waits out its long beat now
        fake.pause(hold=False)
        assert h.retrieve("k") is None  # unanswered: down
        assert server.down
        fake.restart(recovery_window_s=5)
        fake.resume()
        wait_until(lambda: not server.down, timeout=5, what="polled")
        assert read(h, "k") == b"k"

    def test_a_probe_session_reopens_on_the_node_s_notice(self, fake, nodes):
        node = nodes()
        probe = MaruHandler(
            MaruConfig(
                maru_node=node.config.socket,
                engine_id="sched",
                cache_namespace=NS,
                chunk_size_bytes=PAGE,
                timeout_ms=300,
                auto_connect=False,
                metadata_only=True,
            )
        )
        probe.connect()
        try:
            server = probe._cpu.server_conn
            poked = []
            poke = server.poke

            def record(server_epoch=None):
                poked.append(server_epoch)
                poke(server_epoch)

            server.poke = record
            restart(fake, node)
            wait_until(lambda: fake.server_epoch in poked, what="the notice")
            wait_until(
                lambda: server.server_epoch == fake.server_epoch, what="reopened"
            )
        finally:
            probe.close()


class TestRacesAcrossTheRestart:
    def test_a_stale_worker_exit_starts_no_second_recovery(
        self, fake, nodes, raws, handlers
    ):
        node = nodes()
        h = handlers(node)
        assert put(h, "gone", b"GONE" * 8)
        assert put(h, "stays")
        silent = raws(node)  # keeps the server drain open
        victim = raws(node)
        # its WORKER_EXIT goes unanswered across the restart
        fake.drop_requests(MessageType.WORKER_EXIT, 100000)
        node.manager.workers.on_process_exit(f"worker:{victim.incarnation}")
        wait_until(lambda: node.server.resends > 0, what="WORKER_EXIT resent")
        old = restart(fake, node)
        # a delete at the new MaruServer waits for the drain
        gone = Key(NS, "gone")
        loc = fake.locations(gone)[0]
        fake.queue_block("gpu-test", BlockRequest("b1", (KeyLocation(gone, loc),), "x"))
        wait_until(lambda: node.manager._central, what="blocked")
        with fake._lock:
            fake._drop_requests.clear()
        # the exit is reported again with a new op, without another recovery
        wait_until(
            lambda: any(
                r.incarnation == victim.incarnation for r, _ in fake.worker_exits
            ),
            what="WORKER_EXIT at the new MaruServer",
        )
        time.sleep(0.3)
        assert node.manager.recoveries == 1 and node.server.resumes == 1
        assert node.pools.find(loc) is not None  # still READY, still blocked
        silent.request(
            NodeMessageType.DRAIN_ACK, DrainAckRequest(DrainKind.SERVER, old)
        )
        wait_until(lambda: gone not in fake.keys(), what="the delete done")
        assert h.retrieve("gone") is None
        lease = h.retrieve("stays")
        assert bytes(lease.view) == b"stays"
        h.release_retrieved([lease])

    def test_an_old_op_never_reaches_the_new_server(self, fake):
        from maru_common.storage_types import PoolInfo, WorkerRef
        from maru_storage_service.server_client import ServerClient

        client = ServerClient(
            fake.url, "solo", "run-1", rpc_timeout_s=0.2, host_boot_id="boot"
        )
        dram = PoolInfo(
            pool_id="dram:solo:run-1",
            kind=PoolKind.DRAM,
            node_id="solo",
            pool_generation=1,
            capacity=4 * PAGE,
            page_size=PAGE,
        )
        try:
            client.open([dram])
            old = client.server_epoch
            client._renew = lambda failed_session: None  # HANDSHAKEs time out
            ref = WorkerRef("e", 0, 0, 1, 1, "gone-worker", None)
            errors = []

            def report():
                try:
                    client.worker_exit([ref], ExitCause.EXIT)
                except StaleOp as e:
                    errors.append(e)

            fake.pause(hold=False)  # the old MaruServer is gone
            thread = threading.Thread(target=report)
            thread.start()
            wait_until(lambda: client.resends > 0, what="resent")
            fake.restart(recovery_window_s=5)
            fake.resume()
            client.resume()  # the session goes on at the new MaruServer
            thread.join(15)
            assert errors, "the old op was not dropped"
            new = client.server_epoch
            assert new != old
            assert all(
                r.server_epoch == old for r in fake.requests(MessageType.WORKER_EXIT)
            )
            assert fake.executed[MessageType.WORKER_EXIT] == 0
        finally:
            client.close()

    def test_a_restart_at_the_end_of_recovery_is_not_lost(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        assert put(h, "k")
        client = node.server
        reregister = client.reregister
        state = {"n": 0}

        def restart_after_the_last_batch(items, last):
            response = reregister(items, last)
            state["n"] += 1
            if state["n"] == 1:
                fake.restart(recovery_window_s=5)
                wait_until(lambda: client.epoch_lost, timeout=5, what="seen")
            return response

        client.reregister = restart_after_the_last_batch
        fake.restart(recovery_window_s=5)
        wait_until(lambda: node.manager.recoveries >= 2, timeout=15, what="again")
        wait_until(lambda: not node.manager.recovering, timeout=15, what="done")
        assert client.server_epoch == fake.server_epoch
        assert Key(NS, "k") in fake.keys()  # re-registered at the latest one
        assert read(h, "k") == b"k"

    def test_a_restart_between_handshake_and_renewal_is_a_restart(
        self, fake, nodes, raws, handlers
    ):
        from maru_common.protocol import HandshakeResponse

        node = nodes()
        h = handlers(node)
        assert put(h, "k")
        silent = raws(node)
        held = silent.credit(2 * PAGE).credits
        assert held
        client = node.server
        old = client.server_epoch
        call = client._rpc.call
        state = {"once": False}

        def old_server_answers_first(code, request, *a, **kw):
            if code == MessageType.HANDSHAKE and not state["once"]:
                state["once"] = True  # the old one answers, then is gone
                return HandshakeResponse(success=True, server_epoch=old)
            return call(code, request, *a, **kw)

        client._rpc.call = old_server_answers_first
        fake.restart(recovery_window_s=5)
        wait_until(lambda: node.manager.recoveries >= 1, timeout=15, what="recovery")
        assert client.server_epoch != old
        renewals = [
            r
            for r in fake.requests(MessageType.NODE_OPEN)
            if r.resume == client.epoch_seq
        ]
        assert renewals  # the run was taken back, not opened anew
        assert node.manager.workers.server_drain_open()
        assert silent.incarnation in node.manager.workers.server_drain_pending()
        assert node.pools.held_bytes(silent.incarnation) == 0  # quarantined
        assert Key(NS, "k") in fake.keys()

    def test_an_exit_racing_the_drain_start_keeps_its_grace(self, fake, nodes, raws):
        node = nodes()
        raw = raws(node)
        assert len(raw.credit(2 * PAGE).credits) == 2
        pools = node.pools
        registry = node.manager.workers
        quarantine_holder = pools.quarantine_holder
        started = threading.Event()

        def drain_starts_meanwhile(holder):
            # a restart seen on another thread while the exit is handled
            thread = threading.Thread(
                target=lambda: (
                    started.set(),
                    registry.start_server_drain(node.server.server_epoch),
                )
            )
            thread.start()
            started.wait(5)
            time.sleep(0.1)
            return quarantine_holder(holder)

        pools.quarantine_holder = drain_starts_meanwhile
        registry.on_process_exit(f"worker:{raw.incarnation}")
        assert pools.bytes_in(PageState.QUARANTINED) == 2 * PAGE  # in its grace
        wait_until(
            lambda: pools.bytes_in(PageState.QUARANTINED) == 0, what="grace over"
        )

    def test_a_delete_during_reregistration_is_kept(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        assert put(h, "gone")
        client = node.server
        manager = node.manager
        reregister = client.reregister
        gone = Key(NS, "gone")

        def delete_arrives(items, last):
            response = reregister(items, last)
            # its delete reaches the node before this result is applied
            block = BlockRequest(
                "b1", (KeyLocation(gone, fake.locations(gone)[0]),), "delete"
            )
            fake.queue_block("gpu-test", block)
            manager.on_heartbeat(NodeHeartbeatResponse(Status.OK, blocks=[block]))
            assert manager._central
            return response

        client.reregister = delete_arrives
        fake.restart(recovery_window_s=5)
        wait_until(lambda: manager.recoveries >= 1, timeout=15, what="recovery")
        wait_until(lambda: gone not in fake.keys(), what="the delete done")

    def test_a_settling_page_reregistered_is_evictable(self, fake, nodes, raws):
        node = nodes()
        silent = raws(node)  # keeps the first drain open: no settle RETIRE
        writer = raws(node)
        first = restart(fake, node)
        # a COMMIT whose reply has the wrong item count: the page settles
        fake.rewrite_responses(
            MessageType.COMMIT, lambda r: dataclasses.replace(r, items=[])
        )
        credits = writer.credit(PAGE, session="s").credits
        writer.write_done(credits, ["k"], timeout=10)
        wait_until(lambda: node.manager._settle, what="settling")
        (ref,) = node.manager._settle
        second = restart(fake, node)  # re-registered: ADDED
        assert ref not in node.manager._settle
        assert ref in node.manager.placement._lru.get(ref.pool_id, {})
        for worker in (silent, writer):
            for epoch in (first, second):
                worker.request(
                    NodeMessageType.DRAIN_ACK, DrainAckRequest(DrainKind.SERVER, epoch)
                )
        wait_until(lambda: not node.manager.workers.server_drain_open(), what="done")
        assert node.pools.page(ref).state == PageState.READY

    def test_a_same_epoch_resume_keeps_the_catalog(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        assert put(h, "k")
        manager = node.manager
        manager._hold_unknown([page_ref(node, "k")])  # its RETIRE result unknown
        assert put(h, "moved")
        manager.placement.hold(page_ref(node, "moved"))  # held for another cause
        node.server._epoch_lost.set()
        manager.server_lost("test: refused, epoch unchanged")
        wait_until(lambda: manager.recoveries == 1, timeout=15, what="resumed")
        assert not manager.recovering
        assert not fake.requests(MessageType.NODE_REREGISTER)  # nothing to send
        assert not manager.workers.server_drain_open()
        assert manager.stats()["held_after_resume"] == 1  # stays held, counted
        assert read(h, "k") == b"k"

    def test_a_failure_while_sorting_pages_sorts_them_again(
        self, fake, nodes, handlers
    ):
        node = nodes()
        h = handlers(node)
        assert put(h, "gone")
        assert put(h, "stays")
        fake.drop_requests(MessageType.RETIRE, 1000)  # the delete never ends
        gone = Key(NS, "gone")
        item = KeyLocation(gone, fake.locations(gone)[0])
        fake.queue_block("gpu-test", BlockRequest("b1", (item,), "delete"))
        wait_until(lambda: node.manager._central, what="blocked")
        with fake._lock:
            fake._drop_requests.clear()
        manager = node.manager
        drop_page = manager._drop_page
        failed = []

        def fail_once(ref):
            if not failed:
                failed.append(ref)
                raise RuntimeError("injected failure before any batch")
            drop_page(ref)

        manager._drop_page = fail_once
        restart(fake, node)
        assert failed and node.server.resumes == 2  # tried again
        assert {k.key for k in fake.keys()} == {"stays"}  # the delete holds

    def test_a_retry_after_a_failure_part_way_sends_every_page(
        self, fake, nodes, raws, monkeypatch
    ):
        from maru_storage_service import storage_manager

        node = nodes(pages=16)
        raw = raws(node)
        names = written(raw, fake, 3)
        monkeypatch.setattr(storage_manager, "MAX_BATCH", 1)
        client = node.server
        reregister = client.reregister
        state = {"n": 0}

        def fail_the_second_batch(items, last):
            state["n"] += 1
            if state["n"] == 2:
                raise RuntimeError("injected failure part way")
            return reregister(items, last)

        client.reregister = fail_the_second_batch
        fake.restart(recovery_window_s=5)
        wait_until(lambda: node.manager.recoveries >= 1, timeout=15, what="recovery")
        wait_until(lambda: not node.manager.recovering, timeout=15, what="done")
        assert {k.key for k in fake.keys()} == set(names)
        assert fake.reregistered["gpu-test"][1]  # `last` reached MaruServer
        assert node.server.resumes == 2


def test_a_loss_before_the_heartbeat_starts_keeps_the_catalog(fake, nodes, monkeypatch):
    from maru_storage_service.storage_manager import StorageManager

    start_heartbeat = StorageManager.start_heartbeat

    def lost_first(self):
        # the session is lost (renewal refused, same MaruServer) before the
        # heartbeat starts
        self.metadata._epoch_lost.set()
        self.server_lost("test")
        wait_until(lambda: self.recoveries == 1, timeout=15, what="resumed")
        start_heartbeat(self)

    monkeypatch.setattr(StorageManager, "start_heartbeat", lost_first)
    node = nodes()
    assert node.manager.recoveries == 1
    assert not fake.requests(MessageType.NODE_REREGISTER)  # its Catalog stands
    assert node.manager.safe_state is None and not node.manager.recovering


class TestDrainNotices:
    def test_a_reattached_worker_gets_each_open_drain(self, fake, nodes, raws):
        node = nodes()
        raw = raws(node)
        incarnation = raw.incarnation
        raw.close()  # UDS lost; the record stays until the process exits
        first = restart(fake, node)
        second = restart(fake, node)
        again = RawWorker(node.config.socket, incarnation=incarnation)
        try:
            again.attach()
            wait_until(lambda: drain_notices(again, second), what="notices")
            epochs = [
                n.epoch
                for n in again.notices
                if n.kind == NotifyKind.DRAIN and n.drain_kind == DrainKind.SERVER
            ]
            assert epochs == [first, second]  # oldest first
        finally:
            again.close()

    def test_the_client_ends_epochs_in_notice_order(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        client = h._cpu
        drains = []
        client._drain = lambda notify, era=None: drains.append(
            (notify.epoch, set(client._ended_epochs))
        )
        for epoch in ("e-1", "e-2"):
            client._on_notify(
                Notify(kind=NotifyKind.DRAIN, drain_kind=DrainKind.SERVER, epoch=epoch)
            )
        # each is marked ended on the notice thread, before its waiter runs
        assert {"e-1", "e-2"} <= client._ended_epochs
        wait_until(lambda: len(drains) == 2, what="waiters")
        assert all(epoch in ended for epoch, ended in drains)

    def test_the_same_drain_again_keeps_new_credit(self, fake, nodes, handlers):
        node = nodes()
        h = handlers(node)
        client = h._cpu
        drain = Notify(kind=NotifyKind.DRAIN, drain_kind=DrainKind.SERVER, epoch="e")
        client._on_notify(drain)
        era = client.credits.era
        wait_credit(h)
        client._on_notify(drain)  # sent again after a re-attach
        assert client.credits.era == era
        assert client.credits.unused_bytes > 0


class TestOldWorkersAndTheServerDrain:
    def test_an_old_worker_not_back_is_waited_for(self, fake, nodes):
        import subprocess
        import sys

        from maru_common.ms_rpc import MsRpcClient
        from maru_common.node_protocol import process_start_time
        from maru_common.protocol import WorkerOpenRequest
        from maru_common.storage_types import PoolInfo, SessionRole

        sleeper = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"]
        )
        try:
            first = nodes()
            rpc = MsRpcClient(fake.url, timeout_s=2)
            ssd = PoolInfo(
                pool_id="ssd:gpu-test:/data",
                kind=PoolKind.SSD,
                node_id="gpu-test",
                pool_generation=1,
                capacity=4 * PAGE,
                page_size=PAGE,
                path="/data",
            )
            opened = rpc.call(
                MessageType.WORKER_OPEN,
                WorkerOpenRequest(
                    node_id="gpu-test",
                    node_epoch=first.node_epoch,
                    engine_id="old",
                    dp_index=0,
                    tp_rank=0,
                    pid=sleeper.pid,
                    pid_start=process_start_time(sleeper.pid),
                    incarnation="old-worker",
                    namespace=NS,
                    role=SessionRole.WORKER,
                    protocol="ms1",
                ),
            )
            rpc.close()
            assert opened.status == Status.OK
            first.stop()
            fake.set_adopted("gpu-test", [(ssd, [])])  # a pool it may read
            node = nodes()  # the old worker has not attached again
            assert node.adopted == [ssd.pool_id]
            restart(fake, node)
            pending = node.manager.workers.server_drain_pending()
            assert "old-worker" in pending
            sleeper.kill()
            sleeper.wait()
            wait_until(
                lambda: not node.manager.workers.server_drain_open(), what="exit"
            )
        finally:
            sleeper.kill()
            sleeper.wait()

    def test_a_worker_refused_by_the_new_run_keeps_no_drain_open(
        self, fake, nodes, handlers
    ):
        first = nodes()
        h = handlers(first)
        client = h._cpu
        assert put(h, "k")
        first.stop()
        second = nodes(page_size=PAGE // 2)  # refuses h: Maru is off there
        wait_until(lambda: client.off is not None, what="off")
        assert second.adopted == []  # nothing of the old run it could read
        restart(fake, second)
        wait_until(
            lambda: not second.manager.workers.server_drain_open(), what="no wait"
        )


class TestMaruOffRaces:
    def test_a_lookup_in_flight_when_maru_turns_off_is_not_handed_out(
        self, fake, nodes, handlers
    ):
        from maru_handler.client.storage_conn import AttachRefusedError

        node = nodes()
        h = handlers(node)
        client = h._cpu
        assert put(h, "k")
        server = client.server_conn
        lookup = server.lookup_and_pin

        def lookup_then_off(keys):
            answered = lookup(keys)  # pinned at MaruServer
            client._turn_off(AttachRefusedError("refused"))
            wait_until(lambda: fake.requests(MessageType.WORKER_CLOSE), what="close")
            return answered

        server.lookup_and_pin = lookup_then_off
        assert h.retrieve("k") is None  # its pin went with WORKER_CLOSE
        assert not client._reads and len(client.pins) == 0
        assert h.get_stats()["maru_node"]["counters"]["retrieve_off"] == 1

    def test_an_alloc_racing_maru_off_writes_nothing(self, fake, nodes, handlers):
        from maru_handler.client.storage_conn import AttachRefusedError

        node = nodes()
        h = handlers(node)
        client = h._cpu
        wait_credit(h)
        take = client.credits.take
        returned = []
        client._return_credits = returned.extend

        def take_then_off(wait_s=0.0):
            taken = take(wait_s)
            client._turn_off(AttachRefusedError("refused"))
            return taken

        client.credits.take = take_then_off
        with pytest.raises(CreditUnavailableError):
            h.alloc(4)
        assert not client._handles and len(returned) == 1
        wait_until(lambda: client.server_conn is None, what="session ended")


class TestResumingNode:
    def test_a_node_that_does_not_resume_is_warned_about_once(
        self, fake, nodes, handlers, caplog
    ):
        node = nodes()
        h = handlers(node)
        server = h._cpu.server_conn
        assert put(h, "k")
        server.session_ttl_s = 0.5  # warn after this long
        fake.fail_requests(MessageType.NODE_OPEN, Status.BUSY, count=100000)
        log = logging.getLogger("maru_handler.client.server_conn")
        log.addHandler(caplog.handler)
        try:
            caplog.set_level("INFO", logger=log.name)
            fake.restart(recovery_window_s=5)
            wait_until(lambda: server._refused, what="waiting for the node")
            time.sleep(2.0)  # polls go on
        finally:
            log.removeHandler(caplog.handler)
        warnings = [
            r
            for r in caplog.records
            if r.levelname == "WARNING" and "has not resumed" in r.getMessage()
        ]
        assert len(warnings) == 1
        opens = [
            r
            for r in fake.requests(MessageType.WORKER_OPEN)
            if r.incarnation == h._cpu.identity.incarnation
        ]
        assert len(opens) >= 3  # still polled

# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruClient across node runs: a credit of an ended run, and pools being
installed by an attach."""

import threading
from concurrent.futures import Future
from types import SimpleNamespace

import pytest

from maru_common.config import MaruConfig
from maru_common.node_protocol import AttachResponse, Budgets, NodeMessageType, Notify
from maru_common.storage_types import (
    DaxMapping,
    Location,
    NotifyKind,
    PoolDescriptor,
    PoolInfo,
    PoolKind,
    Status,
)
from maru_handler.client import CreditUnavailableError, maru_client
from maru_handler.client.maru_client import MaruClient
from maru_handler.client.pool_maps import PoolMaps
from maru_shm.types import MaruHandle
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


class Busy(SimpleNamespace):
    __hash__ = object.__hash__


def test_a_pool_remove_of_an_ended_run_leaves_the_new_runs_mapping(client, pool):
    class Conn:
        closed = False

        def __init__(self):
            self.sent = []

        def submit(self, code, request):
            self.sent.append(code)

    old_conn, new_conn = Conn(), Conn()
    info = pool.info()
    client.pools._install(info, FakeMapping(), "e1")
    client.node_epoch, client._storage = "e1", old_conn
    busy = Busy(location=Location(info.pool_id, "a", 1, 0, M, PoolKind.DRAM))
    client._handles.add(busy)
    started = threading.Event()
    real = client.credits.remove_pool
    client.credits.remove_pool = lambda pool_id: (started.set(), real(pool_id))[1]
    notify = SimpleNamespace(pool_id=info.pool_id, pool_generation=1)
    thread = threading.Thread(
        target=client._pool_remove,
        args=(notify, "e1", old_conn, client.pools.get(info.pool_id)),
    )
    thread.start()
    assert started.wait(5)
    # a new run maps the same pool_id while the notice waits on the handle
    new = FakeMapping()
    client.pools._install(info, new, "e2")
    with client._lock:
        client.node_epoch, client._storage = "e2", new_conn
        client._handles.discard(busy)
        client._cv.notify_all()
    thread.join(5)
    assert not thread.is_alive()
    assert client.pools.get(info.pool_id) is not None and not new.closed
    assert not new_conn.sent and not old_conn.sent


CXL = PoolInfo("cxl-domain-1", PoolKind.CXL, "node", 1, 2 * M, M)


class AttachConn:
    def __init__(self):
        self.closed = False
        self.sent = []

    def start_notifications(self):
        pass

    def submit(self, code, request):
        self.sent.append(code)
        future = Future()
        future.set_result(None)
        return future


def attach_response(epoch, *, pools=True):
    dax = DaxMapping(
        "domain", MaruHandle(1, 0, 2 * M, 42), node_id="node", node_epoch=epoch
    )
    return AttachResponse(
        status=Status.OK,
        node_id="node",
        node_epoch=epoch,
        page_size=M,
        budgets=Budgets(4 * M, 8 * M, M, 4 * M, 4 * M),
        pools=[PoolDescriptor(CXL, dax=dax)] if pools else [],
    )


@pytest.fixture
def attached(client):
    client.pools = PoolMaps(pin_pools=False, map_dax=lambda info, dax: FakeMapping())
    client._take_attach(AttachConn(), attach_response("e1"), [], first=True)
    yield client
    client.pools.close()


def notice():
    return Notify(kind=NotifyKind.POOL_REMOVE, pool_id=CXL.pool_id, pool_generation=1)


def test_a_delayed_notice_is_not_taken_for_the_new_runs(attached, monkeypatch):
    client = attached
    started = []

    class Deferred:
        def __init__(self, target, args, **kwargs):
            started.append((target, args))

        def start(self):
            pass

    monkeypatch.setattr(maru_client.threading, "Thread", Deferred)
    client._on_notify(notice())  # arrives on the e1 connection
    monkeypatch.undo()
    old_conn = client._storage
    old_conn.closed = True
    client._on_lost(old_conn)
    client._unmap_on_loss()
    assert client.pools.flush(5)
    new_conn = AttachConn()
    client._take_attach(new_conn, attach_response("e2"), [], first=False)
    new = client.pools.get(CXL.pool_id)
    assert new is not None and new.node_epoch == "e2"
    target, args = started[0]
    target(*args)  # the thread runs late
    assert client.pools.get(CXL.pool_id) is new and not new.mapping.closed
    assert not new_conn.sent


def test_no_ack_while_a_detached_mapping_is_still_closing(attached, monkeypatch):
    client = attached
    monkeypatch.setattr(maru_client, "CLOSE_TIMEOUT_S", 0.2)
    entered, resume = threading.Event(), threading.Event()
    old = client.pools.get(CXL.pool_id)
    real_close = old.mapping.close

    def blocked_close():
        entered.set()
        assert resume.wait(5)
        real_close()

    old.mapping.close = blocked_close
    old_conn = client._storage
    old_conn.closed = True
    client._on_lost(old_conn)
    client._unmap_on_loss()
    try:
        assert entered.wait(5)
        conn = AttachConn()
        client._take_attach(conn, attach_response("e1", pools=False), [], first=False)
        args = (notice(), client.node_epoch, client._storage, None)
        client._pool_remove(*args)
        assert not conn.sent  # the detached mapping is not closed yet
        resume.set()
        assert client.pools.flush(5)
        client._pool_remove(*args)
        assert conn.sent == [NodeMessageType.UNMAP_ACK]
    finally:
        resume.set()

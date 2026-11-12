# SPDX-License-Identifier: Apache-2.0
"""The GPU node's remote tier against a real memory node (RemoteServer,
PeerOps, StorageManager) over loopback TCP and the fake NIC: connect and
probe, RESERVE, posted WRITE/READ, WRITE_DONE, kept and settled transfers,
the quarantine cap, drain, reconnects, a new memory run, and lost ops."""

import dataclasses
import mmap
import socket
import threading

import pytest

from maru_common.remote_protocol import TransferEnd, Written
from maru_common.storage_types import (
    REMOTE_CAPABILITY,
    STAGE_CAPABILITY,
    ItemResult,
    Key,
    Location,
    PoolKind,
)
from maru_remote.transport import buffer_address
from maru_storage_service.config import parse_node_config
from maru_storage_service.drivers.peer import NixlRemoteTier
from maru_storage_service.page_table import PageRef
from maru_storage_service.pool_manager import PageState
from maru_storage_service.tiers import Span, TierEnv
from tests.unit.ms_fake_server import FakeMaruServer
from tests.unit.ms_node_helpers import remove_tree, short_tmpdir, start_node, wait_until
from tests.unit.remote_fakes import FakeNixlAgent, key_file, reset_fake_agents
from tests.unit.remote_transport_fakes import FakeTransport
from tests.unit.test_ms_memory_role import PAGE, memory_raw

LOCAL = "dram:gpu1:e1"


def fake_transport(name: str, device: str) -> FakeTransport:
    return FakeTransport(name, ucx_device=device)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(autouse=True)
def _fresh_agents():
    reset_fake_agents()
    yield
    reset_fake_agents()


@pytest.fixture
def fake():
    server = FakeMaruServer()
    yield server
    server.close()


class Memory:
    """A memory node [dram] serving on loopback."""

    def __init__(self, fake, sockdir: str):
        self.fake, self.sockdir = fake, sockdir
        self.port = free_port()
        self.node = None
        self.start()

    def start(self):
        raw = memory_raw(self.fake.url, self.sockdir)
        raw["remote_server"]["listen"] = f"127.0.0.1:{self.port}"
        raw["remote_server"]["key_file"] = key_file()
        self.node = start_node(parse_node_config(raw), make_transport=fake_transport)
        return self.node

    @property
    def url(self) -> str:
        return f"127.0.0.1:{self.port}"

    def pool(self, location: Location) -> mmap.mmap:
        return self.node.transfers._maps[location.pool_id]

    def state(self, location: Location):
        ref = self.node.pools.find(location)
        return None if ref is None else self.node.pools.page(ref).state


@pytest.fixture
def memory(fake):
    sockdir = short_tmpdir()
    mem = Memory(fake, sockdir)
    yield mem
    mem.node.stop()
    remove_tree(sockdir)


def make_tier(urls, *, timeout_s=30, cap="1G", epoch="e1") -> NixlRemoteTier:
    config = parse_node_config(
        {
            "node_id": "gpu1",
            "server": "tcp://127.0.0.1:1",
            "page_size": PAGE,
            "tiers": [
                {"store": "dram", "size": 8 * PAGE, "hugepage": False},
                {
                    "store": "remote",
                    "url": list(urls),
                    "timeout_s": timeout_s,
                    "key_file": key_file(),
                    "quarantine_max": cap,
                },
            ],
            "budgets": {
                "credit_max_bytes": 2 * PAGE,
                "credit_total_max_bytes": 2 * PAGE,
                "credit_refill_low": PAGE,
                "credit_refill_high": 2 * PAGE,
                "commit_pending_max_bytes": 2 * PAGE,
                "transfer_reserve_bytes": PAGE,
            },
        }
    )
    env = TierEnv(
        node_id="gpu1",
        node_epoch=epoch,
        page_size=PAGE,
        own_holder="own",
        transfer_bytes_per_s=1 << 40,
        interrupted=threading.Event(),
        later_run=False,
        on_lost=lambda reason: None,
        renew_s=1.0,
        make_transport=fake_transport,
    )
    return NixlRemoteTier(config.tiers[1], env)


class Gpu:
    """A remote tier with a local L1 pool of 8 pages."""

    def __init__(self, fake, urls, **kwargs):
        self.tier = make_tier(urls, **kwargs)
        self.tier.open()
        self.local = mmap.mmap(-1, 8 * PAGE)
        self.tier.attach(LOCAL, self.local)
        self.tier.opened(fake.server_epoch, [REMOTE_CAPABILITY])

    def wait_ready(self, node_id="mem1"):
        wait_until(lambda: self.tier.ready(node_id), what="ready")

    def close(self):
        self.tier.stop()
        try:
            self.tier.drain()
            self.tier.detach(LOCAL)
        except RuntimeError:
            pass
        self.tier.close()


@pytest.fixture
def gpu(fake, memory):
    g = Gpu(fake, [memory.url])
    g.wait_ready()
    yield g
    g.close()


def run(fn, *args, **kwargs):
    """Submit and wait for the first end."""
    ended = threading.Event()
    result = fn(*args, callback=lambda r: ended.set(), **kwargs)
    assert ended.wait(10)
    return result


def store(gpu, page: int, credit: Location, fill: int, **kwargs):
    gpu.local[page * PAGE : (page + 1) * PAGE] = bytes([fill]) * PAGE
    return run(
        gpu.tier.submit_store,
        Span(LOCAL, page * PAGE, PAGE),
        credit,
        refs=(PageRef(LOCAL, page, 1), None),
        **kwargs,
    )


def test_it_connects_probes_and_learns_the_pools(gpu, memory):
    dram = memory.node.tier("dram").pool.pool_id
    assert gpu.tier.owner(dram) == "mem1"
    assert gpu.tier.owner("dram:other:1") is None
    assert gpu.tier.capabilities() == [REMOTE_CAPABILITY, STAGE_CAPABILITY]


def test_a_written_page_is_committed_on_the_memory_node(gpu, memory):
    [credit] = gpu.tier.reserve([Key("ns", "k")])
    assert credit is not None and memory.state(credit) == PageState.CREDITED
    result = store(gpu, 2, credit, 0x5A)
    assert result.ok and not result.kept and not result.late
    assert bytes(memory.pool(credit)[credit.offset : credit.offset + PAGE]) == (
        bytes([0x5A]) * PAGE
    )
    [outcome] = gpu.tier.write_done(
        [Written(Key("ns", "k"), credit, PAGE, TransferEnd.DONE)]
    )
    assert outcome.result == ItemResult.ADDED
    assert memory.state(credit) == PageState.READY
    assert memory.fake.locations(Key("ns", "k"))


def test_a_page_is_read_back(gpu, memory):
    [credit] = gpu.tier.reserve([Key("ns", "r")])
    store(gpu, 0, credit, 0x33)
    gpu.local[PAGE : 2 * PAGE] = bytes(PAGE)
    result = run(
        gpu.tier.submit_load,
        credit,
        Span(LOCAL, PAGE, PAGE),
        refs=(None, PageRef(LOCAL, 1, 1)),
    )
    assert result.ok
    assert bytes(gpu.local[PAGE : 2 * PAGE]) == bytes([0x33]) * PAGE


def test_reserve_spreads_over_memory_nodes_by_room(fake, memory):
    sockdir = short_tmpdir()
    other_fake = fake  # the same MaruServer: both nodes in one Catalog
    raw = memory_raw(other_fake.url, sockdir)
    raw["node_id"] = "mem2"
    port = free_port()
    raw["remote_server"].update(listen=f"127.0.0.1:{port}", key_file=key_file())
    mem2 = start_node(parse_node_config(raw), make_transport=fake_transport)
    g = Gpu(fake, [memory.url, f"127.0.0.1:{port}"])
    try:
        g.wait_ready("mem1")
        g.wait_ready("mem2")
        credits = g.tier.reserve([Key("ns", str(i)) for i in range(4)])
        assert all(c is not None for c in credits)
        owners = {g.tier.owner(c.pool_id) for c in credits}
        assert owners == {"mem1", "mem2"}  # each gives 2 (its peer cap)
        assert g.tier.reserve([Key("ns", "x")]) == [None]  # both at their cap
    finally:
        g.close()
        mem2.stop()
        remove_tree(sockdir)


def test_a_transfer_past_timeout_is_kept_then_settled(fake, memory):
    g = Gpu(fake, [memory.url], timeout_s=0.2)
    try:
        g.wait_ready()
        [credit] = g.tier.reserve([Key("ns", "slow")])
        g.tier.transport.stall()
        settled = threading.Event()
        result = store(g, 3, credit, 0x11, settled=lambda r: settled.set())
        assert not result.ok and result.kept
        assert isinstance(result.error, TimeoutError)
        assert g.tier.kept_bytes == PAGE
        with pytest.raises(RuntimeError, match="stays registered"):
            g.tier.detach(LOCAL)
        g.tier.transport.stall(False)
        assert g.tier.transport.finish_stalled() == 1
        assert settled.wait(10)
        assert g.tier.kept_bytes == 0 and result.kept and result.phase == "settled"
    finally:
        g.close()


def test_an_err_transfer_stays_kept_until_the_run_ends(fake, memory):
    g = Gpu(fake, [memory.url])
    try:
        g.wait_ready()
        [credit] = g.tier.reserve([Key("ns", "err")])
        g.tier.transport.fail_next()
        settled = []
        result = store(g, 4, credit, 0x22, settled=settled.append)
        assert not result.ok and result.kept and isinstance(result.error, RuntimeError)
        assert g.tier.kept_bytes == PAGE and settled == []
        # The released handle no longer blocks deregistration
        assert result.state.released
    finally:
        g.close()


def test_the_quarantine_cap_stops_remote_transfers(fake, memory):
    g = Gpu(fake, [memory.url], cap=PAGE)
    try:
        g.wait_ready()
        a, b = g.tier.reserve([Key("ns", "a"), Key("ns", "b")])
        g.tier.transport.fail_next()
        store(g, 0, a, 1)
        assert not g.tier.ready("mem1")
        assert g.tier.reserve([Key("ns", "c")]) == [None]
        with pytest.raises(RuntimeError, match="cap"):
            store(g, 1, b, 2)
    finally:
        g.close()


def test_drain_ends_every_running_transfer(fake, memory):
    g = Gpu(fake, [memory.url], timeout_s=0.2)
    try:
        g.wait_ready()
        [credit] = g.tier.reserve([Key("ns", "d")])
        g.tier.transport.stall()
        ended = []
        g.tier.submit_store(
            Span(LOCAL, 0, PAGE),
            credit,
            refs=(PageRef(LOCAL, 0, 1), None),
            callback=ended.append,
        )
        g.tier.drain()
        assert len(ended) == 1 and ended[0].kept  # it ran past timeout_s
        with pytest.raises(RuntimeError, match="drained"):
            store(g, 1, credit, 1)
    finally:
        g.tier.transport.stall(False)
        g.close()


def test_a_lost_connection_is_made_again_and_replays_ops(fake, memory, gpu):
    [credit] = gpu.tier.reserve([Key("ns", "re")])
    store(gpu, 0, credit, 0x44)
    node = gpu.tier._nodes[0]
    node.conn.close()  # the TCP connection drops; RDMA lives on
    wait_until(lambda: gpu.tier.ready("mem1") and node.conn is not None)
    [outcome] = gpu.tier.write_done(
        [Written(Key("ns", "re"), credit, PAGE, TransferEnd.DONE)]
    )
    # The memory node quarantined the credit when the connection went; the
    # same run's DONE releases it
    assert outcome.result == ItemResult.ADDED
    assert memory.state(credit) == PageState.READY


def test_a_new_memory_run_drops_what_the_old_one_gave(fake, memory, gpu):
    [credit] = gpu.tier.reserve([Key("ns", "old")])
    old_epoch = gpu.tier._nodes[0].node_epoch
    memory.node.stop()
    memory.start()  # a new run on the same address
    wait_until(
        lambda: gpu.tier._nodes[0].node_epoch != old_epoch and gpu.tier.ready("mem1")
    )
    assert gpu.tier.owner(credit.pool_id) is None
    with pytest.raises(RuntimeError, match="no ready memory node"):
        store(gpu, 0, credit, 1)
    [fresh] = gpu.tier.reserve([Key("ns", "new")])
    assert store(gpu, 1, fresh, 2).ok


def test_a_reserve_whose_answer_is_lost_is_canceled(fake, memory, gpu, monkeypatch):
    node = gpu.tier._nodes[0]
    real = node.conn.submit
    dropped = threading.Event()

    def lose_reserve(code, request, callback):
        if code == 0x10 and not dropped.is_set():
            dropped.set()
            seq = real(code, request, lambda _r: None)  # its answer goes nowhere
            node.conn.close()
            return seq
        return real(code, request, callback)

    monkeypatch.setattr(node.conn, "submit", lose_reserve)
    assert gpu.tier.reserve([Key("ns", "lost")]) == [None]
    wait_until(lambda: gpu.tier.ready("mem1") and not node.lost_reserves)
    pools = memory.node.pools
    wait_until(
        lambda: (
            pools.held_bytes(gpu.tier.origin) == 0
            and pools.bytes_in(PageState.QUARANTINED) == 0
        ),
        what="the lost credits went back",
    )


def test_cancel_and_drop_go_to_the_owner(gpu, memory):
    c1, c2 = gpu.tier.reserve([Key("ns", "c1"), Key("ns", "c2")])
    gpu.tier.cancel([c1])
    wait_until(lambda: memory.state(c1) is None, what="canceled")
    store(gpu, 0, c2, 9)
    [outcome] = gpu.tier.write_done(
        [Written(Key("ns", "c2"), c2, PAGE, TransferEnd.DONE)]
    )
    assert outcome.result == ItemResult.ADDED
    committed = memory.fake.locations(Key("ns", "c2"))[0]
    gpu.tier.drop(Key("ns", "c2"), committed)
    wait_until(lambda: memory.fake.locations(Key("ns", "c2")) == [], what="dropped")


def test_a_refused_registration_turns_the_tier_off(fake, memory):
    tier = make_tier([memory.url])
    tier.open()
    FakeNixlAgent.registry[tier.agent].close()  # the NIC refuses
    tier.attach(LOCAL, mmap.mmap(-1, 8 * PAGE))
    assert tier.capabilities() == []
    tier.opened(fake.server_epoch, [REMOTE_CAPABILITY])
    assert tier._threads == [] and not tier.ready("mem1")
    tier.close()


def test_no_remote_v1_accepted_means_no_connection(fake, memory):
    tier = make_tier([memory.url])
    tier.open()
    tier.attach(LOCAL, mmap.mmap(-1, 8 * PAGE))
    tier.opened(fake.server_epoch, [])
    assert tier._threads == []
    tier.detach(LOCAL)
    tier.close()


def test_the_local_mapping_must_be_registered(gpu, memory):
    [credit] = gpu.tier.reserve([Key("ns", "u")])
    with pytest.raises(ValueError, match="no remote I/O"):
        gpu.tier.submit_store(
            Span("dram:not-attached:1", 0, PAGE), credit, refs=(None, None)
        )
    assert buffer_address(gpu.local)  # the attached one is still fine
    assert store(gpu, 0, credit, 1).ok


def test_location_kind_is_the_memory_nodes(gpu, memory):
    [credit] = gpu.tier.reserve([Key("ns", "kind")])
    assert credit.kind == PoolKind.DRAM


def test_open_limits_ucx_before_the_agent(monkeypatch):
    import maru_storage_service.drivers.peer as peer

    monkeypatch.delenv("UCX_TLS", raising=False)
    monkeypatch.delenv("UCX_RCACHE_ENABLE", raising=False)
    made = []

    def transport(name, *, ucx_device=""):
        import os

        assert os.environ["UCX_TLS"] and os.environ["UCX_RCACHE_ENABLE"] == "n"
        made.append(FakeTransport(name, ucx_device=ucx_device))
        return made[-1]

    monkeypatch.setattr(peer, "NixlTransport", transport)
    tier = make_tier(["127.0.0.1:1"])
    tier.env = dataclasses.replace(tier.env, make_transport=None)
    tier.open()
    assert [t.agent.name for t in made] == [tier.agent]
    tier.close()


@pytest.fixture
def quick(monkeypatch):
    import maru_storage_service.drivers.peer as peer

    monkeypatch.setattr(peer, "PING_S", 0.1)
    monkeypatch.setattr(peer, "RECONNECT_S", 0.1)


def test_a_failed_connection_is_made_anew_once_released(quick, fake, memory):
    g = Gpu(fake, [memory.url])
    try:
        g.wait_ready()
        a, b = g.tier.reserve([Key("ns", "a"), Key("ns", "b")])
        g.tier.transport.fail_next()
        assert store(g, 0, a, 1).kept
        assert not g.tier.ready("mem1")  # no transfer goes there meanwhile
        g.wait_ready()  # the failed peer removed and loaded again
        assert store(g, 1, b, 2).ok
    finally:
        g.close()


def test_kept_pages_count_whole(fake, memory):
    g = Gpu(fake, [memory.url], timeout_s=0.1)
    try:
        g.wait_ready()
        [credit] = g.tier.reserve([Key("ns", "small")])
        g.tier.transport.stall()
        result = run(
            g.tier.submit_store,
            Span(LOCAL, 0, 1000),
            credit,
            refs=(PageRef(LOCAL, 0, 1), None),
        )
        assert result.kept and g.tier.kept_bytes == PAGE
    finally:
        g.tier.transport.stall(False)
        g.tier.transport.finish_stalled()
        g.close()


def test_running_transfers_count_toward_the_cap(fake, memory):
    g = Gpu(fake, [memory.url], cap=2 * PAGE)
    try:
        g.wait_ready()
        a, b = g.tier.reserve([Key("ns", "a"), Key("ns", "b")])
        g.tier.transport.stall()
        for i, credit in enumerate((a, b)):
            g.tier.submit_store(
                Span(LOCAL, i * PAGE, PAGE), credit, refs=(PageRef(LOCAL, i, 1), None)
            )
        # Two pages in flight: a third could leave three kept, past the cap
        assert not g.tier.ready("mem1")
        with pytest.raises(RuntimeError, match="cap"):
            g.tier.submit_store(
                Span(LOCAL, 2 * PAGE, PAGE), a, refs=(PageRef(LOCAL, 2, 1), None)
            )
        g.tier.transport.stall(False)
        assert g.tier.transport.finish_stalled() == 2
        wait_until(lambda: g.tier.ready("mem1"), what="room again")
    finally:
        g.tier.transport.stall(False)
        g.close()


def test_the_old_runs_peer_goes_once_released(quick, fake, memory):
    g = Gpu(fake, [memory.url])
    try:
        g.wait_ready()
        node = g.tier._nodes[0]
        old_peer, old_epoch = node.peer, node.node_epoch
        memory.node.stop()
        memory.start()
        wait_until(lambda: node.node_epoch != old_epoch and g.tier.ready("mem1"))
        wait_until(
            lambda: not g.tier.transport.has_peer(old_peer), what="old peer removed"
        )
        assert g.tier.transport.has_peer(node.peer)
    finally:
        g.close()


def test_a_transfer_to_an_ended_memory_run_is_released_and_kept(fake, memory):
    g = Gpu(fake, [memory.url], timeout_s=30)
    try:
        g.wait_ready()
        [credit] = g.tier.reserve([Key("ns", "gone")])
        g.tier.transport.stall()
        ended = threading.Event()
        result = g.tier.submit_store(
            Span(LOCAL, 0, PAGE),
            credit,
            refs=(PageRef(LOCAL, 0, 1), None),
            callback=lambda r: ended.set(),
        )
        # The memory node's process ends: NIXL reports the peer disconnected
        FakeNixlAgent.registry[memory.node.remote.server.agent_name].close()
        assert ended.wait(10)  # long before timeout_s
        assert result.kept and not result.ok and result.state.released
        assert g.tier.kept_bytes == PAGE
        g.tier.detach(LOCAL)  # no handle holds this memory any more
    finally:
        g.tier.transport.stall(False)
        g.close()


def test_written_and_canceled_credits_give_room_back(gpu, memory):
    node = gpu.tier._nodes[0]
    a, b = gpu.tier.reserve([Key("ns", "a"), Key("ns", "b")])
    assert node.room == 0  # the peer's credit cap is 2 pages
    assert gpu.tier.reserve([Key("ns", "c")]) == [None]
    store(gpu, 0, a, 1)
    gpu.tier.write_done([Written(Key("ns", "a"), a, PAGE, TransferEnd.DONE)])
    assert node.room == PAGE
    gpu.tier.cancel([b])
    wait_until(lambda: node.room == 2 * PAGE, what="canceled credit back")
    assert gpu.tier.reserve([Key("ns", "d")]) != [None]


def test_a_credit_of_an_ended_connection_is_never_written(quick, fake, memory):
    g = Gpu(fake, [memory.url])
    try:
        g.wait_ready()
        node = g.tier._nodes[0]
        [credit] = g.tier.reserve([Key("ns", "old-conn")])
        gen = node.conn_gen
        node.conn.close()  # the same memory run, a new connection
        wait_until(lambda: node.conn_gen > gen and g.tier.ready("mem1"))
        with pytest.raises(RuntimeError, match="no credit of this connection"):
            store(g, 0, credit, 1)
        g.tier.cancel([credit])  # never posted: it goes back
        wait_until(lambda: memory.state(credit) is None, what="credit freed")
    finally:
        g.close()


def test_a_settle_waits_for_the_end_callback(fake, memory):
    g = Gpu(fake, [memory.url], timeout_s=0.1)
    try:
        g.wait_ready()
        [credit] = g.tier.reserve([Key("ns", "race")])
        g.tier.transport.stall()
        in_end, release = threading.Event(), threading.Event()
        order = []

        def end(result):
            in_end.set()
            release.wait(10)
            order.append(("end", result.kept))

        def settled(result):
            order.append(("settled", result.kept))

        g.tier.submit_store(
            Span(LOCAL, 0, PAGE),
            credit,
            refs=(PageRef(LOCAL, 0, 1), None),
            callback=end,
            settled=settled,
        )
        assert in_end.wait(10)  # kept past timeout_s, its callback running
        g.tier.transport.stall(False)
        g.tier.transport.finish_stalled()  # DONE now, while the end callback runs
        release.set()
        wait_until(lambda: len(order) == 2, what="both reported")
        assert order == [("end", True), ("settled", True)]
    finally:
        g.tier.transport.stall(False)
        g.close()


def test_an_abandoned_reserve_stays_outstanding_until_canceled(
    quick, fake, memory, monkeypatch
):
    import maru_storage_service.drivers.peer as peer

    g = Gpu(fake, [memory.url])
    try:
        g.wait_ready()
        monkeypatch.setattr(peer, "RPC_TIMEOUT_S", 0.2)
        ops = memory.node.peer_ops
        real = ops.reserve
        gate = threading.Event()

        def slow(origin, request, done):
            gate.wait(5)  # answered after the GPU gave up
            real(origin, request, done)

        monkeypatch.setattr(ops, "reserve", slow)
        node = g.tier._nodes[0]
        assert g.tier.reserve([Key("ns", "slow")]) == [None]
        seq = max(node.ops.outstanding)
        gate.set()
        monkeypatch.setattr(ops, "reserve", real)
        # The CANCEL by its seq frees the credits; then the seq is resolved
        wait_until(
            lambda: (
                memory.node.pools.held_bytes(g.tier.origin) == 0
                and seq not in node.ops.outstanding
            ),
            what="canceled and resolved",
        )
    finally:
        g.close()


def test_writes_per_memory_node_are_bounded(gpu, memory):
    import maru_storage_service.drivers.peer as peer

    node = gpu.tier._nodes[0]
    node.writes = peer.IO_DEPTH_PER_NODE
    assert gpu.tier.reserve([Key("ns", "deep")]) == [None]
    node.writes = 0
    assert gpu.tier.reserve([Key("ns", "deep")]) != [None]


def test_drain_waits_for_an_end_callback_running_elsewhere(fake, memory):
    g = Gpu(fake, [memory.url], timeout_s=0.1)
    try:
        g.wait_ready()
        [credit] = g.tier.reserve([Key("ns", "busy")])
        g.tier.transport.stall()
        in_end, release, ended = threading.Event(), threading.Event(), []

        def end(result):  # the poller runs it past timeout_s
            in_end.set()
            release.wait(10)
            ended.append(result)

        g.tier.submit_store(
            Span(LOCAL, 0, PAGE),
            credit,
            refs=(PageRef(LOCAL, 0, 1), None),
            callback=end,
        )
        assert in_end.wait(10)
        drained = threading.Event()
        threading.Thread(target=lambda: (g.tier.drain(), drained.set())).start()
        assert not drained.wait(0.3)  # its callback has not returned yet
        release.set()
        assert drained.wait(10) and len(ended) == 1
    finally:
        g.tier.transport.stall(False)
        g.tier.transport.finish_stalled()
        g.close()


def test_a_lost_connection_is_told_at_once_and_made_again(fake, memory, gpu):
    lost = []
    gpu.tier.on_lost(lost.append)
    old_epoch = gpu.tier._nodes[0].node_epoch
    memory.node.stop()  # its end of the stream closes
    wait_until(lambda: lost == ["mem1"], timeout=2, what="told")
    assert not gpu.tier.ready("mem1")
    assert gpu.tier.reserve([Key("ns", "x")]) == [None]
    memory.start()
    wait_until(
        lambda: gpu.tier.ready("mem1") and gpu.tier._nodes[0].node_epoch != old_epoch,
        timeout=15,
        what="connected to the new run",
    )
    [credit] = gpu.tier.reserve([Key("ns", "y")])
    assert store(gpu, 0, credit, 3).ok

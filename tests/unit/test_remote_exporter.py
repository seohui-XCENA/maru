# SPDX-License-Identifier: Apache-2.0
"""NixlExporter: whole-pool NIC registration per run, and the TransferEngine
deregistering a pool before it unmaps it."""

import mmap

import pytest

from maru_common.storage_types import Location, PoolKind
from maru_remote.exporter import Export, NixlExporter
from maru_remote.transport import TransferState, buffer_address
from maru_storage_service.tiers import Exporter
from maru_storage_service.transfer_engine import Copy, TransferEngine
from tests.unit.remote_fakes import reset_fake_agents
from tests.unit.remote_transport_fakes import FakeTransport, mapped

N = 1 << 16
PAGE = 4096


@pytest.fixture(autouse=True)
def _fresh_agents():
    reset_fake_agents()
    yield
    reset_fake_agents()


@pytest.fixture
def mem():
    return FakeTransport("mem")


@pytest.fixture
def exporter(mem):
    return NixlExporter(mem)


def test_it_is_an_exporter(exporter):
    assert isinstance(exporter, Exporter)


def test_attach_registers_the_whole_mapping(mem, exporter):
    pool, addr = mapped(N)
    exporter.attach("dram:m:1", pool)
    export = exporter.export("dram:m:1")
    assert export == Export("dram:m:1", addr, N, export.metadata)
    assert mem.agent.registered == [(addr, N)]
    assert export.metadata == mem.agent.get_partial_agent_metadata(
        [(addr, N, mem.agent.key_of(addr, N))], inc_conn_info=True
    )
    assert exporter.exports() == [export]


def test_a_peer_reaches_any_page_by_base_and_metadata(mem, exporter):
    pool, _ = mapped(N, 9)
    exporter.attach("dram:m:1", pool)
    export = exporter.export("dram:m:1")
    gpu = FakeTransport("gpu")
    local, local_addr = mapped(PAGE, 7)
    gpu.register(local_addr, PAGE)
    peer = gpu.add_peer(export.metadata)
    offset = 3 * PAGE
    write = gpu.post("WRITE", peer, [(local_addr, export.base + offset, PAGE)])
    assert write.poll_state() is TransferState.DONE
    assert bytes(pool[offset : offset + PAGE]) == bytes([7]) * PAGE
    assert bytes(pool[:offset]) == bytes([9]) * offset


def test_detach_fences_late_rdma(mem, exporter):
    pool, _ = mapped(N, 9)
    exporter.attach("dram:m:1", pool)
    export = exporter.export("dram:m:1")
    gpu = FakeTransport("gpu")
    local, local_addr = mapped(PAGE, 7)
    gpu.register(local_addr, PAGE)
    peer = gpu.add_peer(export.metadata)
    gpu.stall()
    pending = gpu.post("WRITE", peer, [(local_addr, export.base, PAGE)])
    assert gpu.fail_stalled(late=True) == 1
    assert pending.poll_state() is TransferState.ERR
    exporter.detach("dram:m:1")
    assert mem.agent.registered == []
    assert gpu.deliver_late() == 0
    assert bytes(pool) == bytes([9]) * N
    assert exporter.export("dram:m:1") is None


def test_the_same_pool_twice_is_refused(mem, exporter):
    pool, _ = mapped(N)
    exporter.attach("dram:m:1", pool)
    with pytest.raises(ValueError, match="already registered"):
        exporter.attach("dram:m:1", pool)
    assert len(mem.agent.registered) == 1


def test_a_refused_registration_leaves_nothing(mem, exporter):
    mem.agent.close()  # the NIC refuses every registration
    pool, _ = mapped(N)
    with pytest.raises(RuntimeError):
        exporter.attach("dram:m:1", pool)
    assert exporter.exports() == []


def test_failed_metadata_undoes_the_registration(mem, exporter, monkeypatch):
    def refuse(registration):
        raise RuntimeError("no metadata")

    monkeypatch.setattr(mem, "partial_metadata", refuse)
    pool, _ = mapped(N)
    with pytest.raises(RuntimeError, match="no metadata"):
        exporter.attach("dram:m:1", pool)
    assert mem.agent.registered == []
    assert exporter.exports() == []


def test_detach_of_an_unregistered_pool_is_a_no_op(mem, exporter):
    exporter.detach("dram:m:1")
    assert exporter.exports() == []


def test_a_failed_detach_keeps_the_registration(mem, exporter, monkeypatch):
    pool, addr = mapped(N)
    exporter.attach("dram:m:1", pool)

    def refuse(handle):
        raise RuntimeError("NIC busy")

    monkeypatch.setattr(mem, "deregister", refuse)
    with pytest.raises(RuntimeError, match="NIC busy"):
        exporter.detach("dram:m:1")
    assert exporter.export("dram:m:1") is not None
    assert mem.agent.registered == [(addr, N)]
    monkeypatch.undo()
    exporter.detach("dram:m:1")
    assert mem.agent.registered == []


def test_exports_keep_the_order_registered(exporter):
    pools = [mapped(N) for _ in range(3)]
    for i, (pool, _) in enumerate(pools):
        exporter.attach(f"p{2 - i}", pool)
    assert [e.pool_id for e in exporter.exports()] == ["p2", "p1", "p0"]


class LoggedExporter:
    """Records attach/detach and whether the mapping was still open."""

    def __init__(self, log: list, refuse: bool = False, stuck: bool = False):
        self.log = log
        self.refuse = refuse
        self.stuck = stuck
        self.maps: dict[str, mmap.mmap] = {}

    def attach(self, pool_id: str, memory: mmap.mmap) -> None:
        if self.refuse:
            raise RuntimeError("NIC refused")
        self.maps[pool_id] = memory
        self.log.append(("attach", pool_id))

    def detach(self, pool_id: str) -> None:
        self.log.append(("detach", pool_id, self.maps[pool_id].closed))
        if self.stuck:
            raise RuntimeError("cannot deregister")
        del self.maps[pool_id]


def _engine(*pool_ids: str) -> tuple[TransferEngine, dict[str, mmap.mmap]]:
    engine = TransferEngine()
    maps = {pool_id: mmap.mmap(-1, N) for pool_id in pool_ids}
    for pool_id, mapping in maps.items():
        engine.add_pool(pool_id, mapping)
    engine.start()
    return engine, maps


def test_the_engine_deregisters_before_it_unmaps():
    log: list = []
    engine, maps = _engine("dram:a:1", "cxl:a:2")
    exporter = LoggedExporter(log)
    engine.export("dram:a:1", exporter)
    engine.export("cxl:a:2", exporter)
    assert engine.close() == set()
    assert log == [
        ("attach", "dram:a:1"),
        ("attach", "cxl:a:2"),
        ("detach", "dram:a:1", False),
        ("detach", "cxl:a:2", False),
    ]
    assert all(m.closed for m in maps.values())


def test_the_engine_exports_only_its_mappings():
    engine, _ = _engine("dram:a:1")
    with pytest.raises(KeyError, match="not mapped"):
        engine.export("cxl:a:2", LoggedExporter([]))
    engine.close()


def test_a_refused_export_is_not_detached():
    log: list = []
    engine, maps = _engine("dram:a:1")
    with pytest.raises(RuntimeError, match="NIC refused"):
        engine.export("dram:a:1", LoggedExporter(log, refuse=True))
    assert engine.close() == set()
    assert log == [] and maps["dram:a:1"].closed


def test_a_pool_that_cannot_be_deregistered_stays_mapped():
    log: list = []
    engine, maps = _engine("dram:a:1", "cxl:a:2")
    engine.export("dram:a:1", LoggedExporter(log, stuck=True))
    assert engine.close() == {"dram:a:1"}
    assert not maps["dram:a:1"].closed
    assert maps["cxl:a:2"].closed


def test_copies_still_run_on_an_exported_pool():
    engine, maps = _engine("dram:a:1", "cxl:a:2")
    engine.export("dram:a:1", NixlExporter(FakeTransport("mem")))
    maps["dram:a:1"][:PAGE] = bytes([5]) * PAGE
    src = Location("dram:a:1", "a1", 1, 0, PAGE, PoolKind.DRAM)
    dst = Location("cxl:a:2", "a2", 1, 0, PAGE, PoolKind.CXL)
    assert engine.run([Copy(src, dst, PAGE)]) == [True]
    assert bytes(maps["cxl:a:2"][:PAGE]) == bytes([5]) * PAGE
    assert engine.close() == set()


def test_buffer_address_of_a_mapping_does_not_pin_it():
    pool, addr = mapped(N)
    assert buffer_address(pool) == addr
    pool.close()  # no export of the buffer outlives the call

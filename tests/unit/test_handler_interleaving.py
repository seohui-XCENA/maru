# SPDX-License-Identifier: Apache-2.0
"""Public C04 placement/lifetime contracts with file-backed shared memory."""

import mmap
from collections import Counter
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from maru import MaruConfig, MaruHandler
from maru_common.protocol import BatchLookupKVResponse, LookupKVResponse, LookupResult
from maru_handler.memory import DaxMapper, OwnedRegionManager
from maru_server.server import MaruServer
from maru_shm import MaruHandle
from tests.unit.test_allocation_groups import MIB, TARGETS, FakeGroupRM


class FileRM(FakeGroupRM):
    """Independent clients mmap the same file extents, as on a shared pool."""

    def __init__(self, directory: Path) -> None:
        super().__init__()
        self.pools = [
            replace(p, total_size=2**30, free_size=2**30 - 2 * MIB) for p in self.pools
        ]
        self.files = {}
        self.fail_map_id = None
        for index, target in enumerate(TARGETS):
            path = directory / str(index)
            with path.open("wb") as stream:
                stream.truncate(2**30)
            self.files[target.dax_path] = path

    def mmap(self, handle: MaruHandle, prot: int) -> mmap.mmap:
        if handle.region_id == self.fail_map_id:
            raise RuntimeError("injected second mapping failure")
        with self.files[self.paths[handle.region_id]].open("r+b") as stream:
            return mmap.mmap(
                stream.fileno(), handle.length, offset=handle.offset, prot=prot
            )

    def munmap(self, handle: MaruHandle) -> None:
        pass


@pytest.fixture
def backend(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    try:
        import torch
    except ImportError:
        pass
    else:
        monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    rm = FileRM(tmp_path)
    monkeypatch.setattr("maru_server.allocation_manager.MaruShmClient", lambda **kw: rm)
    monkeypatch.setattr("maru_handler.memory.mapper.MaruShmClient", lambda **kw: rm)
    monkeypatch.setattr("maru_handler.memory.mapper._PREFAULT_ENABLED", False)
    server = MaruServer(
        allocation_policy="chunk_round_robin", dax_paths=[t.dax_path for t in TARGETS]
    )
    yield rm, server
    server.close()


def make_handler(monkeypatch, server, owner="writer", chunk=32 * MIB):
    rpc = MagicMock()
    rpc.handshake.return_value = {
        "success": True,
        "capabilities": ["multi_pool_alloc_v1"],
    }
    rpc.request_alloc_group.side_effect = server.request_alloc_group
    rpc.return_alloc_group.side_effect = server.return_alloc_group
    rpc.return_alloc.side_effect = server.return_alloc
    rpc.exists_kv.side_effect = server.exists_kv
    rpc.register_kv.side_effect = server.register_kv
    rpc.delete_kv.side_effect = server.delete_kv
    rpc.batch_register_kv.side_effect = server.batch_register_kv
    rpc.lookup_kv.side_effect = lambda key: LookupKVResponse(
        found=server.lookup_kv(key) is not None, **(server.lookup_kv(key) or {})
    )
    rpc.batch_lookup_kv.side_effect = lambda keys: BatchLookupKVResponse(
        [
            LookupResult(found=r is not None, **(r or {}))
            for r in server.batch_lookup_kv(keys)
        ]
    )
    monkeypatch.setattr("maru_handler.handler.RpcClient", lambda *args, **kw: rpc)
    handler = MaruHandler(
        MaruConfig(
            instance_id=owner,
            pool_size=512 * MIB,
            chunk_size_bytes=chunk,
            placement_policy="chunk_round_robin",
            auto_expand=False,
            use_async_rpc=False,
            eager_map=False,
        )
    )
    return handler, rpc


def test_two_handlers_512mib_regions_share_eight_32mib_chunks(backend, monkeypatch):
    rm, server = backend
    writer, _ = make_handler(monkeypatch, server)
    reader, _ = make_handler(monkeypatch, server, "reader")
    assert writer.connect() and reader.connect()
    assert set(writer.get_owned_region_ids()).isdisjoint(reader.get_owned_region_ids())
    assert len(rm.alloc_calls) == 4
    replay = []
    writer.set_on_region_added(
        lambda rid, count, size: replay.append((rid, count, size))
    )
    assert [entry[1:] for entry in replay] == [(8, 32 * MIB), (8, 32 * MIB)]
    writer.set_on_region_added(None)
    before = reader.get_placement_status()
    locations = []
    for index in range(8):
        handle = writer.alloc(32 * MIB)
        # Full payload, not only marker bytes; reader checks every byte.
        handle.buf[:] = bytes([index + 1]) * (32 * MIB)
        assert writer.store(str(index), handle)
        locations.append((handle.region_id, handle.page_index * 32 * MIB))
        handle.buf.release()
    assert Counter(rm.paths[rid] for rid, _ in locations) == {
        TARGETS[0].dax_path: 4,
        TARGETS[1].dax_path: 4,
    }
    results = reader.batch_retrieve([str(i) for i in range(8)] + ["missing"])
    assert results[-1] is None
    for index, info in enumerate(results[:-1]):
        assert (info.region_id, info.kv_offset) == locations[index]
        assert info.view == bytes([index + 1]) * (32 * MIB)
        info.view.release()
    single = reader.retrieve("3")
    assert (single.region_id, single.kv_offset) == locations[3]
    single.view.release()
    assert reader.get_placement_status() == before
    assert len(rm.alloc_calls) == 4
    reader.close()
    writer.close()
    # Stored keys retain writer allocations after owner disconnect.
    assert len(rm.live) == 2
    for index in range(8):
        assert server.delete_kv(str(index))
    assert not rm.live


def test_target_turns_not_region_turns_and_degraded_status(backend):
    rm, _ = backend
    mapper = DaxMapper()
    manager = OwnedRegionManager(mapper, 1024, "chunk_round_robin")
    for target, count in [("a", 1), ("a", 1), ("b", 4)]:
        handle = rm.alloc(count * 1024, TARGETS[0 if target == "a" else 1].dax_path)
        # mmap file offsets must be page aligned for this small fixture.
        handle.offset = (handle.region_id - 1) * 4096
        manager.stage_region(handle, target)
        manager.commit_regions([handle.region_id])
    allocated = [manager.allocate() for _ in range(6)]
    assert [rid for rid, _ in allocated] == [1, 3, 2, 3, 3, 3]
    status = manager.get_placement_status()
    assert status["degraded"] and status["fallback_allocations"] == 2
    assert manager.allocate() is None
    manager.free(*allocated[0])
    assert manager.allocate() == allocated[0]
    manager.close()
    mapper.close()


@pytest.mark.parametrize("failure", ["mapping", "callback", "pin"])
def test_failed_group_preparation_rolls_back_all(backend, monkeypatch, failure):
    rm, server = backend
    handler, rpc = make_handler(monkeypatch, server)
    events = []

    def added(rid, pages, slot_size):
        events.append(("added", rid))
        if rid == 2 and failure == "callback":
            raise ValueError("callback failed")

    handler.set_on_region_added(added)
    handler.set_on_region_removed(lambda rid: events.append(("removed", rid)))
    if failure == "mapping":
        rm.fail_map_id = 2
    assert not handler.connect(require_cuda_pin=failure == "pin")
    assert not handler.connected
    assert not rm.live
    assert not handler.get_staged_region_ids()
    assert not handler.get_owned_region_ids()
    assert not handler.get_placement_status()["cleanup_pending"]
    rpc.request_alloc.assert_not_called()
    if failure == "callback":
        assert events == [("added", 1), ("added", 2), ("removed", 2), ("removed", 1)]
    handler.close()


def test_lost_allocation_reply_replays_same_id(backend, monkeypatch):
    rm, server = backend
    handler, rpc = make_handler(monkeypatch, server)

    def lose_reply(*args):
        server.request_alloc_group(*args)
        raise TimeoutError("reply lost")

    rpc.request_alloc_group.side_effect = lose_reply
    assert not handler.connect()
    request_id = handler.get_placement_status()["request_id"]
    rpc.request_alloc_group.side_effect = server.request_alloc_group
    assert handler.connect()
    assert handler.get_placement_status()["request_id"] == request_id
    assert len(rm.alloc_calls) == 2
    handler.close()
    assert not rm.live


def test_close_waits_for_views_and_retries_group_return(backend, monkeypatch):
    rm, server = backend
    handler, rpc = make_handler(monkeypatch, server)
    assert handler.connect()
    view = handler.get_buffer_view(handler.get_owned_region_ids()[0], 0, 16)
    with pytest.raises(BufferError):
        handler.close()
    rpc.return_alloc_group.assert_not_called()
    assert handler.get_placement_status()["cleanup_pending"]
    assert not handler.connect()
    view.release()
    rm.fail_free = True
    with pytest.raises(RuntimeError, match="cleanup pending"):
        handler.close()
    request_id = handler.get_placement_status()["request_id"]
    rm.fail_free = False
    handler.close()
    assert all(
        call.args[1] == request_id for call in rpc.return_alloc_group.call_args_list
    )
    assert not rm.live
    handler.close()


def test_shared_offset_is_independent_of_reader_slot_size(backend, monkeypatch):
    _, server = backend
    writer, _ = make_handler(monkeypatch, server, chunk=2 * MIB)
    reader, _ = make_handler(monkeypatch, server, "reader", chunk=4 * MIB)
    assert writer.connect() and reader.connect()
    handles = [writer.alloc(7) for _ in range(3)]
    handles[-1].buf[:] = b"payload"
    assert writer.store("key", handles[-1])
    for handle in handles:
        handle.buf.release()
    info = reader.retrieve("key")
    assert info.kv_offset == 2 * MIB
    assert bytes(info.view) == b"payload"
    info.view.release()
    reader.close()
    writer.close()


@pytest.mark.parametrize("applied", [False, True])
def test_close_resolves_lost_request_with_same_id(backend, monkeypatch, applied):
    rm, server = backend
    handler, rpc = make_handler(monkeypatch, server)

    def lose_request(*args):
        if applied:
            server.request_alloc_group(*args)
        raise TimeoutError("request/reply unavailable")

    rpc.request_alloc_group.side_effect = lose_request
    assert not handler.connect()
    request_id = handler.get_placement_status()["request_id"]
    rpc.request_alloc_group.side_effect = server.request_alloc_group
    handler.close()
    assert len(rm.alloc_calls) == 2
    assert not rm.live
    assert {call.args[1] for call in rpc.request_alloc_group.call_args_list} == {
        request_id
    }


def test_group_rejection_without_allocation_leaves_no_pending_state(
    backend, monkeypatch
):
    from maru_common.protocol import RequestAllocGroupResponse

    rm, server = backend
    handler, rpc = make_handler(monkeypatch, server)
    rpc.request_alloc_group.side_effect = None
    rpc.request_alloc_group.return_value = RequestAllocGroupResponse(
        False, error="history full"
    )
    assert not handler.connect()
    assert not handler.get_placement_status()["cleanup_pending"]
    assert not rm.alloc_calls
    rpc.return_alloc_group.assert_not_called()


@pytest.mark.integration
@pytest.mark.parametrize("async_server", [False, True])
@pytest.mark.parametrize("async_client", [False, True])
def test_handler_group_with_real_transport(
    backend, server_port, async_server, async_client
):
    import threading

    from maru_server.rpc_async_server import RpcAsyncServer
    from maru_server.rpc_server import RpcServer

    rm, server = backend
    transport = (RpcAsyncServer if async_server else RpcServer)(
        server, host="127.0.0.1", port=server_port
    )
    thread = threading.Thread(target=transport.start, daemon=True)
    thread.start()
    handler = MaruHandler(
        MaruConfig(
            server_url=f"tcp://127.0.0.1:{server_port}",
            pool_size=64 * MIB,
            chunk_size_bytes=2 * MIB,
            placement_policy="chunk_round_robin",
            auto_expand=False,
            use_async_rpc=async_client,
            eager_map=False,
        )
    )
    try:
        assert handler.connect()
        handles = [handler.alloc(4) for _ in range(4)]
        assert [h.region_id for h in handles] == handler.get_owned_region_ids() * 2
        for index, handle in enumerate(handles):
            handle.buf[:] = b"data"
            assert handler.store(str(index), handle)
            handle.buf.release()
        infos = handler.batch_retrieve([str(i) for i in range(4)])
        for info in infos:
            assert bytes(info.view) == b"data"
            info.view.release()
    finally:
        handler.close()
        transport.stop()
        thread.join(timeout=5)
    assert not thread.is_alive()
    assert len(rm.alloc_calls) == 2


def test_strict_reader_rejects_unpinned_shared_region(backend, monkeypatch):
    _, server = backend
    writer, _ = make_handler(monkeypatch, server)
    assert writer.connect()
    handle = writer.alloc(4)
    handle.buf[:] = b"data"
    assert writer.store("key", handle)
    handle.buf.release()
    reader, _ = make_handler(monkeypatch, server, "reader")
    original_status = DaxMapper.get_mapping_status

    def status(mapper, rid):
        snapshot = original_status(mapper, rid)
        return replace(snapshot, cuda_pinned=rid not in writer.get_owned_region_ids())

    monkeypatch.setattr(DaxMapper, "get_mapping_status", status)
    assert reader.connect(require_cuda_pin=True)
    assert reader.retrieve("key") is None
    assert reader.batch_retrieve(["key"]) == [None]
    reader.close()
    writer.close()

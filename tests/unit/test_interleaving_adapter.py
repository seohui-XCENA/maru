# SPDX-License-Identifier: Apache-2.0
"""C04 adapter contracts with real CPU tensors and public Handler APIs."""

import pytest

torch = pytest.importorskip("torch")
mm = pytest.importorskip("lmcache.v1.memory_management")
from maru_lmcache.adapter import CxlMemoryAdapter  # noqa: E402
from tests.unit.test_handler_interleaving import (  # noqa: E402
    MIB,
    make_handler,
)
from tests.unit.test_handler_interleaving import (  # noqa: E402
    backend as file_backend,
)

backend = file_backend


def adapter_for(handler):
    return CxlMemoryAdapter(
        handler,
        [torch.Size([2, 1, 256, 2048])],
        [torch.float16],
        mm.MemoryFormat.KV_2LTD,
        2 * MIB,
    )


def test_initial_replay_and_batch_rollback(backend, monkeypatch):
    _, server = backend
    handler, rpc = make_handler(monkeypatch, server, chunk=2 * MIB)
    assert handler.connect()
    adapter = adapter_for(handler)
    assert all(adapter.has_region_pool(rid) for rid in handler.get_owned_region_ids())
    before = handler.get_placement_status()
    objects = adapter.batched_allocate(
        torch.Size([2, 1, 256, 2048]), torch.float16, 8, mm.MemoryFormat.KV_2LTD
    )
    ids = [adapter.decode_address(obj.metadata.address)[0] for obj in objects]
    assert ids == handler.get_owned_region_ids() * 4
    adapter.batched_free(objects)
    del objects
    assert (
        adapter.batched_allocate(
            torch.Size([2, 1, 256, 2048]), torch.float16, 257, mm.MemoryFormat.KV_2LTD
        )
        is None
    )
    assert handler.get_placement_status()["targets"] == before["targets"]
    rpc.request_alloc.assert_not_called()
    adapter.close()
    handler.close()


def test_offset_read_never_builds_peer_page_pool(backend, monkeypatch):
    _, server = backend
    writer, _ = make_handler(monkeypatch, server, chunk=2 * MIB)
    reader, _ = make_handler(monkeypatch, server, "reader", chunk=2 * MIB)
    assert writer.connect() and reader.connect()
    adapter = adapter_for(reader)
    handle = writer.alloc(2 * MIB)
    handle.buf[:] = b"\x37" * (2 * MIB)
    assert writer.store("key", handle)
    handle.buf.release()
    info = reader.retrieve("key")
    assert info.page_index == -1
    before = reader.get_placement_status()
    monkeypatch.setattr(
        reader,
        "get_region_page_count",
        lambda rid: pytest.fail("peer slot count must not be inferred"),
    )
    obj = adapter.get_by_offset(info.region_id, info.kv_offset, len(info.view), 8192)
    assert obj is not None
    assert bytes(obj.byte_array) == b"\x37" * (2 * MIB)
    assert not adapter.has_region_pool(info.region_id)
    assert reader.get_placement_status() == before
    with pytest.raises(ValueError, match="Borrowed"):
        adapter.free(obj)
    with pytest.raises(ValueError, match="Borrowed"):
        adapter.create_store_handle(obj)
    partial = adapter.get_by_offset(info.region_id, 8192, 8192, 8192)
    assert partial is not None
    assert partial.metadata.shape == torch.Size([2, 1, 1, 2048])
    assert bytes(partial.byte_array) == b"\x37" * 8192
    assert adapter.get_by_offset(info.region_id, -1, 8192, 8192) is None
    assert adapter.get_by_offset(info.region_id, 0, 0, 8192) is None
    assert adapter.get_by_offset(info.region_id, 0, 8191, 8192) is None
    del obj, partial
    info.view.release()
    adapter.close()
    reader.close()
    writer.close()

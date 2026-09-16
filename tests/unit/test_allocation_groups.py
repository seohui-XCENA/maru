# SPDX-License-Identifier: Apache-2.0
"""Allocation group public contracts with a deterministic fake RM and real ZMQ."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest

from maru_common.allocation_target import AllocationTarget
from maru_common.protocol import (
    AllocatedTargetRegion,
    HandshakeResponse,
    MessageType,
    RequestAllocGroupRequest,
    RequestAllocGroupResponse,
    ReturnAllocGroupRequest,
    ReturnAllocGroupResponse,
)
from maru_common.serializer import Serializer
from maru_handler.rpc_async_client import RpcAsyncClient
from maru_handler.rpc_client import RpcClient
from maru_server.allocation_manager import AllocationManager
from maru_server.rpc_async_server import RpcAsyncServer
from maru_server.rpc_server import RpcServer
from maru_server.server import MaruServer
from maru_shm.ipc import GetAccessResp
from maru_shm.types import MaruHandle, MaruPoolInfo

MIB = 1 << 20
TARGETS = (AllocationTarget("a", "/dev/dax0.0"), AllocationTarget("b", "/dev/dax1.0"))


class FakeGroupRM:
    """Fake RM with observable allocation/free calls and injected failures."""

    def __init__(self) -> None:
        self.pools = [
            MaruPoolInfo(
                dax_path=t.dax_path,
                total_size=512 * MIB,
                free_size=510 * MIB,
                align_bytes=2 * MIB,
            )
            for t in TARGETS
        ]
        self.alloc_calls: list[tuple[int, str]] = []
        self.free_calls: list[int] = []
        self.live: dict[int, MaruHandle] = {}
        self.paths: dict[int, str] = {}
        self.fail_path: str | None = None
        self.alloc_error: Exception = RuntimeError("no contiguous extent")
        self.fail_free = False
        self.duplicate_uuid = False
        self.access_error = False
        self.invalid_extent = False
        self.next_id = 1

    def is_running(self) -> bool:
        return True

    def stats(self) -> list[MaruPoolInfo]:
        return self.pools

    def alloc(self, size: int, dax_path: str = "") -> MaruHandle:
        self.alloc_calls.append((size, dax_path))
        if dax_path == self.fail_path:
            raise self.alloc_error
        pool = (
            next(p for p in self.pools if p.dax_path == dax_path)
            if dax_path
            else self.pools[0]
        )
        offset = pool.align_bytes + sum(
            h.length for rid, h in self.live.items() if self.paths[rid] == pool.dax_path
        )
        handle = MaruHandle(self.next_id, offset, size, 99)
        self.next_id += 1
        self.live[handle.region_id] = handle
        self.paths[handle.region_id] = pool.dax_path
        return handle

    def free(self, handle: MaruHandle) -> None:
        self.free_calls.append(handle.region_id)
        if self.fail_free:
            raise TimeoutError("free reply lost")
        del self.live[handle.region_id]

    def get_access_info(self, handle: MaruHandle) -> GetAccessResp:
        if self.access_error:
            raise TimeoutError("access reply lost")
        path = self.paths[handle.region_id]
        return GetAccessResp(
            dax_path=path,
            device_uuid="same" if self.duplicate_uuid else f"uuid:{path}",
            offset=handle.offset + int(self.invalid_extent),
            length=handle.length,
        )

    def get_dax_path(self, region_id: int) -> str:
        return self.paths[region_id]

    def close(self) -> None:
        pass


@pytest.fixture
def rm(monkeypatch: pytest.MonkeyPatch) -> FakeGroupRM:
    backend = FakeGroupRM()
    monkeypatch.setattr(
        "maru_server.allocation_manager.MaruShmClient", lambda **kwargs: backend
    )
    return backend


def allocate(
    manager: AllocationManager,
    request_id: str = "r1",
    owner: str = "owner",
    size: int = 64 * MIB,
    chunk: int = 8 * MIB,
) -> RequestAllocGroupResponse:
    return manager.allocate_group(owner, request_id, size, chunk, TARGETS)


def test_group_alignment_and_verified_targets(rm: FakeGroupRM) -> None:
    rm.pools = [
        replace(rm.pools[0], align_bytes=4 * MIB),
        replace(rm.pools[1], align_bytes=8 * MIB),
    ]
    result = allocate(AllocationManager(), size=60 * MIB, chunk=6 * MIB)
    assert result.success and result.state == "active"
    assert result.reserved_bytes == result.usable_bytes == 72 * MIB
    assert [r.handle.length for r in result.regions] == [48 * MIB, 24 * MIB]
    assert [r.page_count for r in result.regions] == [8, 4]
    assert [r.target_id for r in result.regions] == ["a", "b"]
    assert len({r.device_uuid for r in result.regions}) == 2


def test_same_instance_has_multiple_groups_and_chunk_sizes(rm: FakeGroupRM) -> None:
    manager = AllocationManager()
    first = allocate(manager)
    second = allocate(manager, "r2", chunk=16 * MIB)
    assert first.success and second.success
    assert len(rm.live) == 4
    assert [r.page_count for r in first.regions] == [4, 4]
    assert [r.page_count for r in second.regions] == [2, 2]
    assert manager.release_group("owner", "r1").success
    assert allocate(manager, "r1").state == "released"
    assert allocate(manager, "r2", chunk=16 * MIB) == second
    assert manager.allocated_by_instance() == {"owner": (2, 64 * MIB)}


def test_concurrent_duplicate_and_payload_mismatch(rm: FakeGroupRM) -> None:
    manager = AllocationManager()
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: allocate(manager), range(24)))
    assert all(r == results[0] and r.success for r in results)
    assert len(rm.alloc_calls) == 2
    assert not allocate(manager, size=128 * MIB).success
    assert not allocate(manager, chunk=16 * MIB).success
    assert len(rm.alloc_calls) == 2
    # A detached result cannot corrupt the retry history.
    results[0].regions[0].handle.length = 1
    assert allocate(manager).reserved_bytes == 64 * MIB
    assert allocate(manager).regions[0].handle.length == 32 * MIB


def test_partial_failure_preserves_existing_groups(rm: FakeGroupRM) -> None:
    manager = AllocationManager()
    previous = allocate(manager, "previous")
    rm.fail_path = TARGETS[1].dax_path
    failed = allocate(manager)
    assert not failed.success and failed.state == "failed"
    assert not failed.pending_region_ids and not failed.outcome_unknown
    assert len(rm.live) == 2
    assert allocate(manager, "previous") == previous
    calls = len(rm.alloc_calls)
    rm.fail_path = None
    assert allocate(manager) == failed
    assert len(rm.alloc_calls) == calls


def test_cleanup_failure_is_tracked_and_retryable(rm: FakeGroupRM) -> None:
    manager = AllocationManager()
    rm.fail_path = TARGETS[1].dax_path
    rm.fail_free = True
    failed = allocate(manager)
    assert failed.state == "cleanup_pending"
    assert failed.pending_region_ids == list(rm.live)
    assert manager.list_allocations() == []
    assert not manager.release_group("owner", "r1").success
    assert not manager.release_group("other", "r1").success
    rm.fail_free = False
    assert manager.release_group("owner", "r1").success
    assert manager.release_group("owner", "r1").success
    assert allocate(manager).state == "released"
    assert not rm.live


def test_unknown_allocation_is_never_reissued_or_claimed_clean(rm: FakeGroupRM) -> None:
    manager = AllocationManager()
    rm.fail_path = TARGETS[1].dax_path
    rm.alloc_error = TimeoutError("allocation reply lost")
    failed = allocate(manager)
    assert failed.state == "unknown" and failed.outcome_unknown
    assert not rm.live  # Known handles were reclaimed; unknown RM outcome is separate.
    assert allocate(manager) == failed
    assert len(rm.alloc_calls) == 2
    release = manager.release_group("owner", "r1")
    assert not release.success and release.outcome_unknown


@pytest.mark.parametrize(
    "failure", ["duplicate_uuid", "access_error", "invalid_extent"]
)
def test_identity_or_extent_validation_rolls_back(
    rm: FakeGroupRM, failure: str
) -> None:
    setattr(rm, failure, True)
    result = allocate(AllocationManager())
    assert not result.success and not result.outcome_unknown
    assert not rm.live


@pytest.mark.parametrize(
    ("size", "chunk"),
    [
        (0, 8 * MIB),
        (True, 8 * MIB),
        (63 * MIB, 8 * MIB),
        (8 * MIB, 8 * MIB),
        (64 * MIB, 0),
        (1 << 64, 1),
        (1, 1 << 64),
    ],
)
def test_invalid_sizes_never_allocate(rm: FakeGroupRM, size: int, chunk: int) -> None:
    assert not allocate(AllocationManager(), size=size, chunk=chunk).success
    assert not rm.alloc_calls


def test_lcm_overflow_and_capacity_failure(rm: FakeGroupRM) -> None:
    rm.pools[0] = replace(rm.pools[0], align_bytes=(1 << 63) - 1)
    assert not allocate(AllocationManager()).success
    assert not rm.alloc_calls
    rm.pools[0] = replace(rm.pools[0], align_bytes=2 * MIB, free_size=0)
    assert not allocate(AllocationManager()).success
    assert not rm.alloc_calls


def test_release_respects_kv_references_and_tombstones(rm: FakeGroupRM) -> None:
    manager = AllocationManager()
    group = allocate(manager)
    rid = group.regions[0].handle.region_id
    assert manager.increment_kv_ref(rid)
    release = manager.release_group("owner", "r1")
    assert release.success and release.retained_region_ids == [rid]
    assert allocate(manager).state == "released"
    assert manager.get_handle(rid) is not None
    assert manager.decrement_kv_ref(rid)
    assert not rm.live
    assert manager.release_group("owner", "r1").success


def test_legacy_region_return_invalidates_group_replay(rm: FakeGroupRM) -> None:
    manager = AllocationManager()
    group = allocate(manager)
    rid = group.regions[0].handle.region_id
    assert not manager.release("other", rid)
    assert manager.release("owner", rid)
    assert manager.release("owner", rid)
    assert not manager.release("other", rid)
    assert allocate(manager).state == "released"
    assert manager.release_group("owner", "r1").success
    assert not rm.live


def test_disconnect_releases_all_owner_groups_only(rm: FakeGroupRM) -> None:
    manager = AllocationManager()
    allocate(manager, "one")
    allocate(manager, "two")
    other = allocate(manager, "one", owner="other")
    manager.disconnect_client("owner")
    assert len(rm.live) == 2
    assert allocate(manager, "one").state == "released"
    assert allocate(manager, "two").state == "released"
    assert allocate(manager, "one", owner="other") == other


def test_disconnect_cleanup_errors_remain_retryable(rm: FakeGroupRM) -> None:
    manager = AllocationManager()
    allocate(manager)
    rm.fail_free = True
    with pytest.raises(ExceptionGroup):
        manager.disconnect_client("owner")
    assert len(allocate(manager).pending_region_ids) == 2
    rm.fail_free = False
    manager.disconnect_client("owner")
    assert not rm.live


def test_history_limit_does_not_evict_terminal_ids(rm: FakeGroupRM) -> None:
    manager = AllocationManager()
    # A valid uint64 request that fails divisibility is remembered too.
    for i in range(4096):
        assert not allocate(manager, str(i), size=1, chunk=2).success
    assert "history is full" in allocate(manager, "new").error
    assert "multiple" in allocate(manager, "0", size=1, chunk=2).error
    assert not rm.alloc_calls


def test_server_policy_and_target_validation(rm: FakeGroupRM) -> None:
    off = MaruServer(dax_paths=[TARGETS[0].dax_path])
    assert off.get_capabilities() == []
    assert not off.request_alloc_group("owner", "x", 64 * MIB, 8 * MIB).success
    assert not rm.alloc_calls
    assert off.request_alloc("owner", 8 * MIB) is not None
    on = MaruServer(allocation_policy="chunk_round_robin", allocation_targets=TARGETS)
    assert on.get_capabilities() == ["multi_pool_alloc_v1"]
    with pytest.raises(ValueError, match="request_alloc_group"):
        on.request_alloc("owner", 64 * MIB)
    assert on.request_alloc_group("owner", "x", 64 * MIB, 8 * MIB).success
    with pytest.raises(ValueError, match="DEV_DAX"):
        MaruServer(allocation_policy="chunk_round_robin", dax_paths=["/dev/missing"])


def test_alias_targets_cannot_allocate_same_device_twice(
    rm: FakeGroupRM, tmp_path
) -> None:
    alias = tmp_path / "alias"
    alias.symlink_to(TARGETS[0].dax_path)
    with pytest.raises(ValueError, match="distinct"):
        MaruServer(
            allocation_policy="chunk_round_robin",
            dax_paths=[TARGETS[0].dax_path, str(alias)],
        )
    assert not rm.alloc_calls


@pytest.mark.parametrize(
    "message",
    [
        RequestAllocGroupRequest("o", "r", 64 * MIB, 8 * MIB),
        ReturnAllocGroupRequest("o", "r"),
    ],
)
def test_new_request_serialization(
    message: RequestAllocGroupRequest | ReturnAllocGroupRequest,
) -> None:
    codec = Serializer()
    msg_type = (
        MessageType.REQUEST_ALLOC_GROUP
        if isinstance(message, RequestAllocGroupRequest)
        else MessageType.RETURN_ALLOC_GROUP
    )
    _, decoded = codec.decode_request(codec.encode(msg_type, message))
    assert decoded == message


def test_response_serialization_and_legacy_handshake(rm: FakeGroupRM) -> None:
    codec = Serializer()
    group = allocate(AllocationManager())
    header, _ = codec.decode(
        codec.encode(
            MessageType.REQUEST_ALLOC_GROUP,
            RequestAllocGroupRequest("o", "r", 64 * MIB, 8 * MIB),
        )
    )
    _, decoded = codec.decode_response(codec.encode_response(header, group))
    assert decoded == group
    assert isinstance(decoded.regions[0], AllocatedTargetRegion)
    assert isinstance(decoded.regions[0].handle, MaruHandle)
    _, handshake = codec.decode_as(
        codec.encode(
            MessageType.HANDSHAKE, {"success": True, "rm_address": "host:9850"}
        ),
        HandshakeResponse,
    )
    assert handshake.capabilities == []
    header.msg_type = MessageType.RETURN_ALLOC_GROUP
    release = ReturnAllocGroupResponse(False, "r", [], [1], True, "reconcile")
    assert codec.decode_response(codec.encode_response(header, release))[1] == release


@pytest.mark.integration
@pytest.mark.parametrize("server_type", [RpcServer, RpcAsyncServer])
@pytest.mark.parametrize("client_type", [RpcClient, RpcAsyncClient])
def test_group_rpc_transports_and_reconnect(
    rm: FakeGroupRM, server_port: int, server_type, client_type
) -> None:
    server = MaruServer(
        allocation_policy="chunk_round_robin", allocation_targets=TARGETS
    )
    transport = server_type(server, host="127.0.0.1", port=server_port)
    thread = threading.Thread(target=transport.start, daemon=True)
    thread.start()
    client = client_type(f"tcp://127.0.0.1:{server_port}", timeout_ms=3000)
    try:
        time.sleep(0.05)
        client.connect()
        assert client.handshake()["capabilities"] == ["multi_pool_alloc_v1"]
        result = client.request_alloc_group("owner", "r1", 64 * MIB, 8 * MIB)
        assert result.success
        # Simulate loss of the first reply to application code: reconnect and
        # repeat the application request ID, not the transport sequence number.
        client.close()
        client = client_type(f"tcp://127.0.0.1:{server_port}", timeout_ms=3000)
        client.connect()
        assert client.request_alloc_group("owner", "r1", 64 * MIB, 8 * MIB) == result
        assert len(rm.alloc_calls) == 2
        assert not client.return_alloc_group("other", "r1").success
        if isinstance(client, RpcAsyncClient):
            futures = [
                client.request_alloc_group_async("owner", "r2", 64 * MIB, 8 * MIB)
                for _ in range(4)
            ]
            assert all(f.result(timeout=3).success for f in futures)
            assert len(rm.alloc_calls) == 4
            assert (
                client.return_alloc_group_async("owner", "r2").result(timeout=3).success
            )
        assert client.return_alloc_group("owner", "r1").success
        assert client.return_alloc_group("owner", "r1").success
        assert not client.request_alloc_group("owner", "r1", 64 * MIB, 8 * MIB).success
    finally:
        client.close()
        transport.stop()
        thread.join(timeout=5)
        server.close()
    assert not thread.is_alive()
    assert not rm.live


def test_individual_free_failure_remains_visible(rm: FakeGroupRM) -> None:
    manager = AllocationManager()
    result = allocate(manager)
    rid = result.regions[0].handle.region_id
    rm.fail_free = True
    with pytest.raises(TimeoutError):
        manager.release("owner", rid)
    assert allocate(manager).pending_region_ids == [rid]
    rm.fail_free = False
    assert manager.release("owner", rid)
    assert allocate(manager).state == "released"
    assert manager.release_group("owner", "r1").success


def test_deferred_free_failure_can_be_cleaned_by_group(rm: FakeGroupRM) -> None:
    manager = AllocationManager()
    result = allocate(manager)
    rid = result.regions[0].handle.region_id
    manager.increment_kv_ref(rid)
    manager.release_group("owner", "r1")
    rm.fail_free = True
    with pytest.raises(TimeoutError):
        manager.decrement_kv_ref(rid)
    assert allocate(manager).pending_region_ids == [rid]
    rm.fail_free = False
    assert manager.release_group("owner", "r1").success
    assert not rm.live


def test_on_alias_uses_verified_pool_for_capacity(
    rm: FakeGroupRM, tmp_path: Path
) -> None:
    alias = tmp_path / "alias"
    alias.symlink_to(TARGETS[0].dax_path)
    server = MaruServer(allocation_policy="chunk_round_robin", dax_paths=[str(alias)])
    assert server.get_stats()["cxl_pool"]["total_size"] == rm.pools[0].total_size
    group = server.request_alloc_group("owner", "request", 64 * MIB, 8 * MIB)
    assert group.success and group.regions[0].dax_path == TARGETS[0].dax_path
    assert server.return_alloc_group("owner", "request").success


def test_free_applied_but_reply_lost_is_not_reported_as_success(
    rm: FakeGroupRM, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = AllocationManager()
    allocate(manager)
    real_free = rm.free

    def lose_reply(handle: MaruHandle) -> None:
        real_free(handle)
        raise TimeoutError("free applied but acknowledgement lost")

    monkeypatch.setattr(rm, "free", lose_reply)
    result = manager.release_group("owner", "r1")
    assert not result.success and len(result.pending_region_ids) == 2
    assert not rm.live
    monkeypatch.setattr(rm, "free", real_free)
    # A fresh RM request rejecting a stale handle is not proof of successful
    # cleanup. Preserve pending state until reconciliation can establish it.
    assert not manager.release_group("owner", "r1").success
    assert not allocate(manager).success
    assert len(rm.alloc_calls) == 2


@pytest.mark.integration
@pytest.mark.parametrize("client_type", [RpcClient, RpcAsyncClient])
def test_off_handshake_and_requests_keep_legacy_contract(
    rm: FakeGroupRM,
    server_port: int,
    client_type: type[RpcClient] | type[RpcAsyncClient],
) -> None:
    server = MaruServer()
    transport = RpcServer(server, host="127.0.0.1", port=server_port)
    thread = threading.Thread(target=transport.start, daemon=True)
    thread.start()
    client = client_type(f"tcp://127.0.0.1:{server_port}", timeout_ms=3000)
    try:
        time.sleep(0.05)
        client.connect()
        assert client.handshake() == {"success": True, "rm_address": server.rm_address}
        assert not client.request_alloc_group(
            "owner", "request", 64 * MIB, 8 * MIB
        ).success
        assert not rm.alloc_calls
        allocation = client.request_alloc("owner", 8 * MIB)
        assert allocation.success
        assert client.return_alloc("owner", allocation.handle.region_id)
        assert not client.return_alloc("owner", allocation.handle.region_id)
    finally:
        client.close()
        transport.stop()
        thread.join(timeout=5)
        server.close()
    assert not thread.is_alive()

# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Allocation Manager - Server-side memory allocation lifecycle management."""

import logging
import math
import os
from copy import deepcopy
from dataclasses import dataclass, field
from threading import RLock

from maru_common.allocation_target import AllocationTarget
from maru_common.protocol import (
    AllocatedTargetRegion,
    RequestAllocGroupResponse,
    ReturnAllocGroupResponse,
)
from maru_shm import MaruHandle, MaruShmClient
from maru_shm.types import DaxType, MaruPoolInfo

logger = logging.getLogger(__name__)


@dataclass
class AllocationInfo:
    """Allocation metadata managed by server."""

    handle: MaruHandle
    owner_instance_id: str
    kv_ref_count: int = 0
    owner_connected: bool = True


@dataclass
class _AllocationGroup:
    total_size: int
    chunk_size: int
    targets: tuple[AllocationTarget, ...]
    state: str = "failed"
    handles: list[MaruHandle] = field(default_factory=list)
    regions: list[AllocatedTargetRegion] = field(default_factory=list)
    pending: set[int] = field(default_factory=set)
    outcome_unknown: bool = False
    error: str | None = None


def _positive_u64(value: int, name: str) -> None:
    if type(value) is not int or not 0 < value < (1 << 64):
        raise ValueError(f"{name} must be a positive uint64")


def _validate_group_ids(instance_id: str, request_id: str) -> None:
    for name, value in (("instance_id", instance_id), ("request_id", request_id)):
        if not isinstance(value, str) or not value or len(value) > 256:
            raise ValueError(f"{name} must contain 1 to 256 characters")


def _resolve_targets(
    targets: tuple[AllocationTarget, ...], pools: list[MaruPoolInfo]
) -> list[MaruPoolInfo]:
    if not targets or len(targets) > 1024:
        raise ValueError("A group requires 1 to 1024 targets")
    resolved = []
    paths: set[str] = set()
    ids: set[str] = set()
    for target in targets:
        if target.offset_bytes is not None:
            raise ValueError("Bounded targets are not yet supported")
        matches = [
            p
            for p in pools
            if os.path.realpath(p.dax_path) == os.path.realpath(target.dax_path)
        ]
        if len(matches) != 1 or matches[0].dax_type != DaxType.DEV_DAX:
            raise ValueError(
                f"Target {target.target_id} must resolve to one DEV_DAX pool"
            )
        pool = matches[0]
        path = os.path.realpath(pool.dax_path)
        if path in paths or target.target_id in ids:
            raise ValueError("Targets must have distinct IDs and whole-device paths")
        _positive_u64(pool.align_bytes, "alignment")
        _positive_u64(pool.total_size, "pool capacity")
        ids.add(target.target_id)
        paths.add(path)
        resolved.append(pool)
    return resolved


def _group_sizes(
    total_size: int, chunk_size: int, pools: list[MaruPoolInfo]
) -> list[int]:
    _positive_u64(total_size, "total_size")
    _positive_u64(chunk_size, "chunk_size_bytes")
    if total_size % chunk_size:
        raise ValueError("total_size must be a multiple of chunk_size_bytes")
    quantum = math.lcm(chunk_size, *(p.align_bytes for p in pools))
    units = (total_size + quantum - 1) // quantum
    _positive_u64(units * quantum, "rounded group size")
    if units < len(pools):
        raise ValueError("total_size cannot provide one aligned unit per target")
    if units * quantum - total_size > total_size:
        raise ValueError("Alignment padding exceeds the requested group size")
    base, extra = divmod(units, len(pools))
    sizes = [(base + (i < extra)) * quantum for i in range(len(pools))]
    if any(size > p.free_size for size, p in zip(sizes, pools, strict=True)):
        raise ValueError("Insufficient free capacity on a target")
    return sizes


class AllocationManager:
    """Manages memory allocation lifecycle."""

    def __init__(self, rm_address: str | None = None):
        self._client = MaruShmClient(address=rm_address)
        if not self._client.is_running():
            addr = rm_address or "127.0.0.1:9850"
            raise ConnectionError(
                f"Resource manager is not running (address: {addr}).\n"
                f"Start it first: sudo maru-resource-manager"
            )
        self._allocations: dict[int, AllocationInfo] = {}  # region_id -> info
        self._lock = RLock()
        # Bounded, process-lifetime tombstones: never evict an ID and risk replay.
        self._groups: dict[tuple[str, str], _AllocationGroup] = {}
        self._region_groups: dict[int, tuple[str, str]] = {}
        self._max_groups = 4096

    def allocate(
        self, instance_id: str, size: int, dax_path: str = ""
    ) -> MaruHandle | None:
        """Allocate memory via ShmClient and track ownership."""
        try:
            handle = self._client.alloc(size, dax_path=dax_path)
        except RuntimeError as e:
            logger.warning(
                "alloc failed for instance=%s size=%d dax_path=%s: %s",
                instance_id,
                size,
                dax_path,
                e,
            )
            return None
        if handle is None:
            return None

        with self._lock:
            self._allocations[handle.region_id] = AllocationInfo(
                handle=handle,
                owner_instance_id=instance_id,
                kv_ref_count=0,
                owner_connected=True,
            )
        return handle

    def resolve_group_targets(
        self, targets: tuple[AllocationTarget, ...]
    ) -> tuple[AllocationTarget, ...]:
        """Validate whole-device targets and resolve server-local RM paths.

        Args:
            targets: Server-configured allowlist, in allocation order.

        Returns:
            Ordered targets with paths exactly as reported by the RM.

        Raises:
            ValueError: For missing, duplicate, bounded or non-DEV_DAX targets.
            RuntimeError: If RM metadata cannot be queried.
        """
        pools = _resolve_targets(targets, self._client.stats())
        return tuple(
            AllocationTarget(t.target_id, p.dax_path)
            for t, p in zip(targets, pools, strict=True)
        )

    def allocate_group(
        self,
        instance_id: str,
        request_id: str,
        total_size: int,
        chunk_size_bytes: int,
        targets: tuple[AllocationTarget, ...],
    ) -> RequestAllocGroupResponse:
        """Allocate a strict group once per instance/request pair.

        Args:
            instance_id: Owner ID; one owner can request multiple groups.
            request_id: Stable retry ID, never reused with another payload.
            total_size: Initial target-total bytes, a multiple of chunk size.
            chunk_size_bytes: Slot size for this group only.
            targets: Server-owned whole-device allowlist; clients cannot select it.

        Returns:
            Detached snapshot. Failure includes pending cleanup IDs and flags
            unknown RM outcomes. Replays never issue another allocation.

        Notes:
            Serializes groups and lifecycle operations with the allocation lock.
            At most 4096 IDs are retained for this process lifetime, including
            terminal IDs; further new IDs fail before allocating. No restart
            recovery is claimed. Do not log returned handle authentication tokens.
        """
        try:
            _validate_group_ids(instance_id, request_id)
            _positive_u64(total_size, "total_size")
            _positive_u64(chunk_size_bytes, "chunk_size_bytes")
        except ValueError as exc:
            return RequestAllocGroupResponse(False, error=str(exc))
        with self._lock:
            key = (instance_id, request_id)
            group = self._groups.get(key)
            if group is not None:
                if (group.total_size, group.chunk_size, group.targets) != (
                    total_size,
                    chunk_size_bytes,
                    targets,
                ):
                    return RequestAllocGroupResponse(
                        False,
                        request_id=request_id,
                        error="Request ID payload mismatch",
                    )
                return self._group_snapshot(request_id, group)
            if len(self._groups) >= self._max_groups:
                return RequestAllocGroupResponse(
                    False,
                    request_id=request_id,
                    error="Group request history is full; no allocation attempted",
                )
            group = _AllocationGroup(total_size, chunk_size_bytes, targets)
            self._groups[key] = group
            allocating = False
            try:
                pools = _resolve_targets(targets, self._client.stats())
                sizes = _group_sizes(total_size, chunk_size_bytes, pools)
                uuids: set[str] = set()
                for target, pool, size in zip(targets, pools, sizes, strict=True):
                    allocating = True
                    handle = self._client.alloc(size, dax_path=pool.dax_path)
                    if handle is None:
                        allocating = False
                        raise RuntimeError("RM refused a target allocation")
                    # Record ownership before any subsequent validation/RPC can fail.
                    group.handles.append(handle)
                    self._allocations[handle.region_id] = AllocationInfo(
                        handle, instance_id
                    )
                    self._region_groups[handle.region_id] = key
                    allocating = False
                    access = self._client.get_access_info(handle)
                    if (
                        not access.device_uuid
                        or access.device_uuid in uuids
                        or os.path.realpath(access.dax_path)
                        != os.path.realpath(pool.dax_path)
                        or access.offset != handle.offset
                        or access.length != handle.length
                        or handle.length != size
                        or handle.offset < pool.align_bytes
                        or handle.offset % pool.align_bytes
                        or handle.offset + handle.length > pool.total_size
                    ):
                        raise ValueError(
                            "RM returned an invalid or duplicate target extent/identity"
                        )
                    uuids.add(access.device_uuid)
                    group.regions.append(
                        AllocatedTargetRegion(
                            target.target_id,
                            pool.dax_path,
                            access.device_uuid,
                            pool.align_bytes,
                            handle,
                            handle.length // chunk_size_bytes,
                        )
                    )
                group.state = "active"
            except Exception as exc:
                # RM refusals are definitive; transport/decoding failures during
                # alloc may have committed without returning an identifiable handle.
                group.outcome_unknown = allocating and not isinstance(exc, RuntimeError)
                group.error = f"Group allocation failed ({type(exc).__name__})"
                if isinstance(exc, ValueError) and not group.handles:
                    group.error = str(exc)
                self._release_group_regions(instance_id, group)
                group.state = "failed"
            return self._group_snapshot(request_id, group)

    def release_group(
        self, instance_id: str, request_id: str
    ) -> ReturnAllocGroupResponse:
        """Release all regions of one group; repeat to retry failed cleanup.

        Args:
            instance_id: Original owner; cannot return another owner's group.
            request_id: ID used to allocate the group.

        Returns:
            Success once known regions are returned or retained only for KV
            references. Unknown RM allocation outcomes always remain failures.
            Repeated successful release is a no-op, including after KV cleanup.
        """
        try:
            _validate_group_ids(instance_id, request_id)
        except ValueError as exc:
            return ReturnAllocGroupResponse(False, error=str(exc))
        with self._lock:
            group = self._groups.get((instance_id, request_id))
            if group is None:
                return ReturnAllocGroupResponse(
                    False,
                    request_id=request_id,
                    error="Unknown allocation group for this owner",
                )
            group.state = "released"
            self._release_group_regions(instance_id, group)
            retained = [
                h.region_id
                for h in group.handles
                if h.region_id in self._allocations and h.region_id not in group.pending
            ]
            success = not group.pending and not group.outcome_unknown
            return ReturnAllocGroupResponse(
                success,
                request_id,
                retained,
                sorted(group.pending),
                group.outcome_unknown,
                None if success else "Group requires cleanup or RM reconciliation",
            )

    def get_handle(self, region_id: int) -> MaruHandle | None:
        """Get the original handle for an allocation."""
        with self._lock:
            info = self._allocations.get(region_id)
            return info.handle if info else None

    def increment_kv_ref(self, region_id: int) -> bool:
        """Increment KV reference count."""
        with self._lock:
            if region_id not in self._allocations:
                return False
            self._allocations[region_id].kv_ref_count += 1
            return True

    def decrement_kv_ref(self, region_id: int) -> bool:
        """Decrement KV reference count and free if needed."""
        with self._lock:
            if region_id not in self._allocations:
                return False

            info = self._allocations[region_id]
            if info.kv_ref_count <= 0:
                logger.warning(
                    "decrement_kv_ref called on region_id=%d with kv_ref_count=%d",
                    region_id,
                    info.kv_ref_count,
                )
                return False
            info.kv_ref_count -= 1

            if info.kv_ref_count == 0 and not info.owner_connected:
                logger.debug(
                    "[FREE] region_id=%d, owner=%s, "
                    "trigger=decrement_kv_ref (kv_ref_count reached 0, owner disconnected)",
                    region_id,
                    info.owner_instance_id,
                )
                key = self._region_groups.get(region_id)
                if key is None:
                    self._client.free(info.handle)
                else:
                    try:
                        self._client.free(info.handle)
                    except Exception:
                        self._groups[key].pending.add(region_id)
                        raise
                    self._groups[key].pending.discard(region_id)
                del self._allocations[region_id]

            return True

    def release(self, instance_id: str, region_id: int) -> bool:
        """Mark allocation as released by owner."""
        with self._lock:
            if region_id not in self._allocations:
                key = self._region_groups.get(region_id)
                return key is not None and key[0] == instance_id

            info = self._allocations[region_id]
            if info.owner_instance_id != instance_id:
                return False

            key = self._region_groups.get(region_id)
            if key is not None:
                self._groups[key].state = "released"
            info.owner_connected = False

            if info.kv_ref_count <= 0:
                logger.info(
                    "[FREE] region_id=%d, owner=%s, "
                    "trigger=release (owner disconnected, kv_ref_count=%d)",
                    region_id,
                    instance_id,
                    info.kv_ref_count,
                )
                key = self._region_groups.get(region_id)
                if key is None:
                    self._client.free(info.handle)
                else:
                    try:
                        self._client.free(info.handle)
                    except Exception:
                        self._groups[key].pending.add(region_id)
                        raise
                    self._groups[key].pending.discard(region_id)
                del self._allocations[region_id]
            else:
                logger.info(
                    "[DEFERRED] region_id=%d, owner=%s, "
                    "kv_ref_count=%d (memory kept alive for KV readers)",
                    region_id,
                    instance_id,
                    info.kv_ref_count,
                )

            return True

    def disconnect_client(self, instance_id: str) -> None:
        """Handle client disconnection - release all owned allocations."""
        with self._lock:
            failures = []
            for owner, request_id in self._groups:
                if owner == instance_id:
                    result = self.release_group(instance_id, request_id)
                    if not result.success:
                        failures.append(
                            RuntimeError(
                                f"Group {request_id} requires cleanup or reconciliation"
                            )
                        )
            to_free = []
            for region_id, info in self._allocations.items():
                if (
                    info.owner_instance_id == instance_id
                    and region_id not in self._region_groups
                ):
                    info.owner_connected = False
                    if info.kv_ref_count <= 0:
                        to_free.append(region_id)

            for region_id in to_free:
                info = self._allocations[region_id]
                logger.info(
                    "[FREE] region_id=%d, owner=%s, "
                    "trigger=disconnect (client disconnected, kv_ref_count=%d)",
                    region_id,
                    instance_id,
                    info.kv_ref_count,
                )
                self._client.free(info.handle)
                del self._allocations[region_id]

            if failures:
                raise ExceptionGroup(
                    "Allocation group disconnect cleanup failed", failures
                )

    def list_allocations(
        self, exclude_instance_id: str | None = None
    ) -> list[MaruHandle]:
        """Return handles for all active allocations.

        Args:
            exclude_instance_id: If set, exclude allocations owned by
                                 this instance (caller's own regions).

        Returns:
            List of MaruHandle for all (or filtered) active allocations.
        """
        with self._lock:
            handles = []
            for info in self._allocations.values():
                if (
                    exclude_instance_id
                    and info.owner_instance_id == exclude_instance_id
                ):
                    continue
                if not info.owner_connected:
                    continue  # owner disconnected — region may be freed soon
                handles.append(info.handle)
            return handles

    def region_owners(self) -> dict[int, str]:
        """Map region_id -> owner_instance_id for all tracked allocations.

        Includes deferred (owner_connected=False) regions, since they still
        occupy CXL memory until their kv_ref_count drops to zero.
        """
        with self._lock:
            return {
                rid: info.owner_instance_id for rid, info in self._allocations.items()
            }

    def allocated_by_instance(self) -> dict[str, tuple[int, int]]:
        """Per-owner allocation totals.

        Returns:
            Mapping of owner_instance_id -> (region_count, allocated_bytes).
            Aggregated over all tracked allocations (connected or deferred).
        """
        with self._lock:
            out: dict[str, list[int]] = {}
            for info in self._allocations.values():
                acc = out.setdefault(info.owner_instance_id, [0, 0])
                acc[0] += 1
                acc[1] += info.handle.length
            return {iid: (acc[0], acc[1]) for iid, acc in out.items()}

    def devices_by_instance(self) -> dict[str, dict[str, int]]:
        """Per-owner allocation bytes broken down by DAX device.

        Returns:
            owner_instance_id -> {dax_path -> allocated_bytes}. The device for
            each region is resolved via the shared-memory client's
            ``get_dax_path`` (served from the cache populated at ``alloc()`` —
            every region here was allocated by this client, so it is present).
            Resolution is done outside the lock and is best-effort: any failure
            or missing path falls back to ``"(unknown)"`` so GET_USAGE never
            fails on device breakdown alone.
        """
        with self._lock:
            snapshot = [
                (rid, info.owner_instance_id, info.handle.length)
                for rid, info in self._allocations.items()
            ]
        resolve = getattr(self._client, "get_dax_path", None)
        out: dict[str, dict[str, int]] = {}
        for region_id, instance_id, length in snapshot:
            dax_path = None
            if resolve is not None:
                try:
                    dax_path = resolve(region_id)
                except Exception:  # noqa: BLE001 - never fail usage on this
                    dax_path = None
            per_dev = out.setdefault(instance_id, {})
            key = dax_path or "(unknown)"
            per_dev[key] = per_dev.get(key, 0) + length
        return out

    def get_stats(self) -> dict:
        """Get allocation statistics."""
        with self._lock:
            total_allocated = sum(
                info.handle.length for info in self._allocations.values()
            )
            return {
                "num_allocations": len(self._allocations),
                "total_allocated": total_allocated,
                "active_clients": len(
                    {
                        info.owner_instance_id
                        for info in self._allocations.values()
                        if info.owner_connected
                    }
                ),
            }

    def pool_stats(self) -> list:
        """Get pool stats from resource manager."""
        return self._client.stats()

    def close(self) -> None:
        """Close the resource manager client."""
        try:
            self._client.close()
        except Exception:
            logger.warning("Failed to close resource manager client", exc_info=True)

    def _release_group_regions(self, instance_id: str, group: _AllocationGroup) -> None:
        group.pending.clear()
        for handle in reversed(group.handles):
            if handle.region_id not in self._allocations:
                continue
            try:
                self.release(instance_id, handle.region_id)
            except Exception:
                group.pending.add(handle.region_id)

    def _group_snapshot(
        self, request_id: str, group: _AllocationGroup
    ) -> RequestAllocGroupResponse:
        pending = sorted(group.pending.intersection(self._allocations))
        state = (
            "unknown"
            if group.outcome_unknown
            else "cleanup_pending"
            if pending
            else group.state
        )
        active = state == "active"
        return RequestAllocGroupResponse(
            success=active,
            request_id=request_id,
            state=state,
            regions=deepcopy(group.regions) if active else [],
            reserved_bytes=sum(h.length for h in group.handles) if active else 0,
            usable_bytes=sum(r.page_count * group.chunk_size for r in group.regions)
            if active
            else 0,
            pending_region_ids=pending,
            outcome_unknown=group.outcome_unknown,
            error=None if active else group.error or "Group has been released",
        )

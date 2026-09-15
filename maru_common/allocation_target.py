# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Allocation target configuration without device access or memory allocation."""

# Standard
import os
import re
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

_UINT64_MAX = (1 << 64) - 1
_MAX_TARGETS_PER_EXPANSION = 1024
_SIZE_UNITS = {
    "": 1,
    "B": 1,
    "KiB": 1 << 10,
    "MiB": 1 << 20,
    "GiB": 1 << 30,
    "TiB": 1 << 40,
}


def _validate_uint64(value: int, name: str, *, positive: bool = False) -> None:
    """Reject values that cannot be represented by the RM's uint64 fields."""
    minimum = 1 if positive else 0
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= _UINT64_MAX
    ):
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"{name} must be a {qualifier} uint64 integer")


def parse_byte_size(value: str) -> int:
    """Parse a byte count or an integer binary-unit size without device access.

    Args:
        value (str): Non-negative integer with an optional B, KiB, MiB, GiB,
            or TiB suffix. For example, "256GiB". Surrounding whitespace is
            allowed; fractional values and decimal GB units are not.

    Returns:
        int: Byte count, including zero for a range's base offset.

    Raises:
        ValueError: If the syntax is invalid or the result exceeds uint64.
    """
    match = re.fullmatch(r"([0-9]+)(B|KiB|MiB|GiB|TiB)?", value.strip())
    if match is None:
        raise ValueError(
            f"Invalid byte size {value!r}; use bytes or integer KiB/MiB/GiB/TiB"
        )
    result = int(match[1]) * _SIZE_UNITS[match[2] or ""]
    _validate_uint64(result, "byte size")
    return result


def parse_target_sizes(
    dax_path: str,
    *,
    target_sizes: str | None = None,
    target_size: str | None = None,
    target_count: int | None = None,
    base_offset: int = 0,
) -> tuple["AllocationTarget", ...]:
    """Expand consecutive range sizes into explicit allocation targets.

    Args:
        dax_path (str): Absolute DAX path shared by all generated targets.
        target_sizes (str | None): Comma-separated sizes such as "256GiB,256GiB".
        target_size (str | None): Repeated size, used with target_count instead.
        target_count (int | None): Number of repeated targets, from 1 to 1024.
        base_offset (int): DAX file offset in bytes; defaults to zero.

    Returns:
        tuple[AllocationTarget, ...]: Ordered targets named target-0, target-1,
            and so on, with offsets computed by cumulative addition.

    Raises:
        ValueError: If forms are mixed or incomplete, a size/count is not
            positive, more than 1024 targets are requested, or any offset/end
            exceeds uint64.

    Notes:
        Offsets are relative to the DAX file, not a region or a physical device.
        Device capacity, alignment, UUID and backing must be checked separately.
        No header adjustment or automatic remainder allocation is performed.
    """
    _validate_uint64(base_offset, "base_offset")
    if target_sizes is not None:
        if target_size is not None or target_count is not None:
            raise ValueError(
                "target_sizes cannot be combined with target_size or target_count"
            )
        items = target_sizes.split(",", _MAX_TARGETS_PER_EXPANSION)
        if len(items) > _MAX_TARGETS_PER_EXPANSION:
            raise ValueError("A target size expansion supports at most 1024 targets")
        sizes = tuple(parse_byte_size(item) for item in items)
    else:
        if target_size is None or target_count is None:
            raise ValueError(
                "Specify target_sizes or both target_size and target_count"
            )
        _validate_uint64(target_count, "target_count", positive=True)
        if target_count > _MAX_TARGETS_PER_EXPANSION:
            raise ValueError("A target size expansion supports at most 1024 targets")
        size = parse_byte_size(target_size)
        _validate_uint64(size, "target size", positive=True)
        # Validate arithmetic before constructing the repeated sequence.
        _validate_uint64(base_offset + size * target_count, "range end")
        sizes = (size,) * target_count

    result = []
    offset = base_offset
    for index, size in enumerate(sizes):
        target = AllocationTarget(
            target_id=f"target-{index}",
            dax_path=dax_path,
            offset_bytes=offset,
            length_bytes=size,
        )
        result.append(target)
        offset += size
    return tuple(result)


def normalize_allocation_targets(
    *,
    allocation_policy: "AllocationPolicy | str" = "fill_first",
    dax_paths: Sequence[str] | None = None,
    allocation_targets: Sequence["AllocationTarget"] | None = None,
) -> tuple["AllocationTarget", ...]:
    """Validate policy and normalize whole-device paths or explicit targets.

    Args:
        allocation_policy (AllocationPolicy | str): fill_first (default) or
            chunk_round_robin. This validates configuration, not runtime support.
        dax_paths (Sequence[str] | None): Legacy whole-device allowlist.
            Repeated paths are collapsed in their original order.
        allocation_targets (Sequence[AllocationTarget] | None): Explicit targets,
            requiring chunk_round_robin and excluding dax_paths.

    Returns:
        tuple[AllocationTarget, ...]: Targets in allocation order. An empty tuple
            with fill_first means the legacy "any available pool" behavior.

    Raises:
        ValueError: If the policy or input form is invalid, ON has no targets,
            IDs repeat, or targets overlap within the same lexical DAX path.

    Notes:
        No device is opened. Alias paths and UUID equivalence must be resolved
        against RM metadata before allocation. Whole-device targets include
        existing reserved ranges; the allocator remains responsible for them.
    """
    policy = AllocationPolicy(allocation_policy)
    if allocation_targets is not None:
        if dax_paths is not None:
            raise ValueError("dax_paths and allocation_targets are mutually exclusive")
        if policy == AllocationPolicy.FILL_FIRST:
            raise ValueError("allocation_targets require chunk_round_robin")
        targets = tuple(allocation_targets)
    else:
        if isinstance(dax_paths, (str, bytes)):
            raise ValueError("dax_paths must be a sequence of paths, not one string")
        paths = dict.fromkeys(os.path.normpath(path) for path in (dax_paths or ()))
        targets = tuple(
            AllocationTarget(target_id=f"target-{index}", dax_path=path)
            for index, path in enumerate(paths)
        )
    if policy == AllocationPolicy.CHUNK_ROUND_ROBIN and not targets:
        raise ValueError("chunk_round_robin requires at least one allocation target")

    ids: set[str] = set()
    by_path: dict[str, list[AllocationTarget]] = {}
    for target in targets:
        if not isinstance(target, AllocationTarget):
            raise ValueError("allocation_targets must contain AllocationTarget values")
        if target.target_id in ids:
            raise ValueError(f"Duplicate target_id: {target.target_id}")
        ids.add(target.target_id)
        by_path.setdefault(os.path.normpath(target.dax_path), []).append(target)
    for path, group in by_path.items():
        if len(group) > 1 and any(target.offset_bytes is None for target in group):
            raise ValueError(f"Overlapping allocation targets for {path}")
        ordered = sorted(group, key=lambda target: target.offset_bytes or 0)
        for previous, current in zip(ordered, ordered[1:], strict=False):
            assert (
                previous.offset_bytes is not None and previous.length_bytes is not None
            )
            assert current.offset_bytes is not None
            if previous.offset_bytes + previous.length_bytes > current.offset_bytes:
                raise ValueError(f"Overlapping allocation targets for {path}")
    return targets


class AllocationPolicy(StrEnum):
    """Placement policy names shared by server and client configuration."""

    FILL_FIRST = "fill_first"
    CHUNK_ROUND_ROBIN = "chunk_round_robin"


@dataclass(frozen=True)
class AllocationTarget:
    """One DAX device or an explicitly bounded range within it.

    Attributes:
        target_id (str): Non-empty logical identity, distinct from RM poolId.
        dax_path (str): Absolute DAX path; not opened during validation.
        offset_bytes (int | None): DAX file offset, or None for the whole device.
        length_bytes (int | None): Positive range length; supplied with offset.

    Raises:
        ValueError: If identity/path is empty, the path is not absolute, range
            fields are incomplete, or integer bounds/end overflow are invalid.

    Notes:
        This is configuration, not proof of physical device independence.
        Device UUID, capacity, alignment and reserved extents are resolved later.
    """

    target_id: str
    dax_path: str
    offset_bytes: int | None = None
    length_bytes: int | None = None

    def __post_init__(self) -> None:
        """Validate immutable identity and range fields; raise ValueError if invalid."""
        if not isinstance(self.target_id, str) or not self.target_id.strip():
            raise ValueError("target_id must be a non-empty string")
        if (
            not isinstance(self.dax_path, str)
            or not self.dax_path.strip()
            or not os.path.isabs(self.dax_path)
        ):
            raise ValueError("dax_path must be a non-empty absolute path")
        if (self.offset_bytes is None) != (self.length_bytes is None):
            raise ValueError("offset_bytes and length_bytes must be supplied together")
        if self.offset_bytes is not None:
            _validate_uint64(self.offset_bytes, "offset_bytes")
            assert self.length_bytes is not None
            _validate_uint64(self.length_bytes, "length_bytes", positive=True)
            _validate_uint64(self.offset_bytes + self.length_bytes, "range end")

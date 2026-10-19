# SPDX-License-Identifier: Apache-2.0
"""vLLM KV cache groups, as the Maru connector sees them.

A hybrid model (Qwen3.5: Gated DeltaNet + full attention; gpt-oss: sliding
window + full attention) reaches the connector as several KV cache groups.
Each group has its own block table and spec. This module turns
``kv_cache_config.kv_cache_groups`` into plain :class:`KVGroup` records and
checks which group combinations the connector supports. It holds no connector
state and imports no vLLM class, so it is testable without a GPU.

Storage unit: one Maru object per (chunk, group). vLLM gives every layer the
same bytes per block slot in every group (Mamba state is padded up to it), so
a group's object size is "layers in the group x slot bytes x blocks per
object". Attention-type groups keep one slot per token block of the chunk;
a Mamba group keeps only the state at the chunk's last block.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

GROUP_FULL = "full"
GROUP_SLIDING_WINDOW = "sliding_window"
GROUP_MAMBA = "mamba"
GROUP_UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class KVGroup:
    """One vLLM KV cache group.

    Attributes:
        group_id: Index in ``kv_cache_config.kv_cache_groups``; also the
            index of this group's list in vLLM's per-group block ids.
        kind: ``GROUP_FULL``, ``GROUP_SLIDING_WINDOW``, ``GROUP_MAMBA`` or
            ``GROUP_UNSUPPORTED``. A model with one group does not depend on
            it.
        spec_name: Class name of the group's ``kv_cache_spec``.
        layer_names: Layers in the group, in vLLM's order. The count can
            differ between groups.
        block_size: Tokens per block for this group.
        slot_bytes: Bytes one layer uses for one block (``page_size_bytes``).
        sliding_window: Window in tokens for a sliding-window group.
        mamba_cache_mode: Mamba state caching mode (``"align"`` etc.).
        state_blocks: For a Mamba group, blocks one request holds at most:
            the state being written, the previous one until vLLM frees it,
            and any speculative or prefill-checkpoint blocks. 0 otherwise.
    """

    group_id: int
    kind: str
    spec_name: str
    layer_names: tuple[str, ...]
    block_size: int
    slot_bytes: int
    sliding_window: int | None = None
    mamba_cache_mode: str | None = None
    state_blocks: int = 0

    def object_bytes(self, chunk_tokens: int) -> int:
        """Bytes of this group's object for one chunk.

        Args:
            chunk_tokens: Tokens per Maru chunk; a multiple of ``block_size``.
        """
        blocks = 1 if self.kind == GROUP_MAMBA else chunk_tokens // self.block_size
        return len(self.layer_names) * self.slot_bytes * blocks


# The spec classes the connector is verified against, by exact class name.
# A subclass can change what a block holds: vLLM 0.30's KpoolTailSpec is a
# one-block scratch ring under SlidingWindowSpec, and RSWASpec evicts blocks
# under FullAttentionSpec. Each one is refused until it is checked. Names
# rather than imports keep this working across vLLM versions.
_SUPPORTED_SPECS = {
    "FullAttentionSpec": GROUP_FULL,
    "SlidingWindowSpec": GROUP_SLIDING_WINDOW,
    "MambaSpec": GROUP_MAMBA,
}


def _spec_kind(spec_cls: type) -> str:
    """Group kind of a vLLM KV cache spec class."""
    return _SUPPORTED_SPECS.get(spec_cls.__name__, GROUP_UNSUPPORTED)


def describe_kv_groups(
    kv_cache_config: Any, default_mamba_cache_mode: str | None = None
) -> list[KVGroup]:
    """Describe the KV cache groups of ``kv_cache_config``.

    Args:
        kv_cache_config: vLLM ``KVCacheConfig``.
        default_mamba_cache_mode: ``cache_config.mamba_cache_mode``, used for
            Mamba specs from vLLM versions that do not carry the mode.

    Returns:
        One :class:`KVGroup` per group, in vLLM's group order.
    """
    groups: list[KVGroup] = []
    for group_id, group in enumerate(kv_cache_config.kv_cache_groups):
        spec = group.kv_cache_spec
        kind = _spec_kind(type(spec))
        groups.append(
            KVGroup(
                group_id=group_id,
                kind=kind,
                spec_name=type(spec).__name__,
                layer_names=tuple(group.layer_names),
                block_size=int(spec.block_size),
                slot_bytes=int(spec.page_size_bytes),
                sliding_window=getattr(spec, "sliding_window", None),
                mamba_cache_mode=(
                    getattr(spec, "mamba_cache_mode", None) or default_mamba_cache_mode
                    if kind == GROUP_MAMBA
                    else None
                ),
                # vLLM's own bound (MambaSpec.max_memory_usage_bytes, align).
                state_blocks=(
                    2
                    + getattr(spec, "num_speculative_blocks", 0)
                    + getattr(spec, "num_prefill_checkpoint_blocks", 0)
                    if kind == GROUP_MAMBA
                    else 0
                ),
            )
        )
    return groups


def blocks_for_tokens(groups: list[KVGroup], tokens: int) -> int:
    """GPU blocks a request of ``tokens`` tokens holds across all groups.

    vLLM allocates every group from one block pool, so the counts add up. An
    attention group holds one block per ``block_size`` tokens; for a
    sliding-window group, which frees blocks behind the window, that is an
    upper bound. A Mamba group holds ``state_blocks``.
    """
    return sum(
        g.state_blocks if g.kind == GROUP_MAMBA else -(-tokens // g.block_size)
        for g in groups
    )


def chunk_tokens_for_groups(
    groups: list[KVGroup], chunk_tokens: int, block_size: int
) -> int:
    """Chunk size the groups allow.

    A Mamba group keeps a state snapshot only at a block end, and in align
    mode only at the block where a scheduler step ends. A chunk can carry a
    state only if it ends on a block end, and any block end can be a step
    end, so chunk = block. Attention-only groups accept any multiple of the
    block size.

    Args:
        groups: The model's KV cache groups.
        chunk_tokens: Configured chunk size, already a multiple of block_size.
        block_size: vLLM block size in tokens.
    """
    if any(g.kind == GROUP_MAMBA for g in groups):
        return block_size
    return chunk_tokens


def check_multi_group_support(
    groups: list[KVGroup],
    *,
    block_size: int,
    use_layerwise: bool,
    speculative: bool,
) -> None:
    """Reject group combinations the connector cannot store correctly.

    A model with one group is always accepted. Several groups are supported
    in chunkwise (packed) format with full attention, sliding window and
    Mamba (``align``) groups, without speculative decoding. The CPU and mixed
    backends reject several groups on their own, in ``MaruKVConnector``.

    Args:
        groups: The model's KV cache groups.
        block_size: ``cache_config.block_size``, which the connector uses.
        use_layerwise: ``maru_use_layerwise``.
        speculative: Whether speculative decoding is configured.

    Raises:
        ValueError: With the reason and the setting to change.
    """
    if len(groups) <= 1:
        return
    unsupported = sorted({g.spec_name for g in groups if g.kind == GROUP_UNSUPPORTED})
    if unsupported:
        raise ValueError(
            "MaruKVConnector does not support KV cache groups of type "
            f"{', '.join(unsupported)} alongside other groups; supported are "
            f"{', '.join(_SUPPORTED_SPECS)}"
        )
    if use_layerwise:
        raise ValueError(
            "MaruKVConnector supports models with several KV cache groups only "
            "in chunkwise format; set maru_use_layerwise=false"
        )
    if speculative:
        raise ValueError(
            "MaruKVConnector does not support speculative decoding with models "
            "that have several KV cache groups"
        )
    for g in groups:
        if g.kind == GROUP_MAMBA and g.mamba_cache_mode != "align":
            raise ValueError(
                "MaruKVConnector needs Mamba state snapshots at block ends; "
                "start vLLM with --mamba-cache-mode align "
                f"(got {g.mamba_cache_mode!r})"
            )
    group_block_sizes = sorted({g.block_size for g in groups})
    if group_block_sizes != [block_size]:
        raise ValueError(
            "MaruKVConnector needs every KV cache group to use the block size "
            f"{block_size} (got {group_block_sizes})"
        )

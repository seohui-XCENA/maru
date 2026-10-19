# SPDX-License-Identifier: Apache-2.0
"""KV cache groups: description, support checks, hits, per-group block ids,
worker registration and stores.

The real-spec cases use the geometry vLLM 0.30 reports for Qwen3.5-0.8B
(Gated DeltaNet + full attention) and gpt-oss-20b (sliding window + full
attention).
"""

import dataclasses
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("torch")
pytest.importorskip("vllm")

import torch
from vllm.distributed.kv_transfer.kv_connector.v1.base import KVConnectorRole
from vllm.v1 import kv_cache_interface
from vllm.v1.kv_cache_interface import (
    FullAttentionSpec,
    KVCacheGroupSpec,
    MambaSpec,
    SlidingWindowSpec,
)

from maru_vllm.connector import (
    MaruConnectorMetadata,
    MaruKVConnector,
    MaruReqMeta,
    _chunk_keys,
)
from maru_vllm.kv_groups import (
    GROUP_FULL,
    GROUP_MAMBA,
    GROUP_SLIDING_WINDOW,
    GROUP_UNSUPPORTED,
    _spec_kind,
    blocks_for_tokens,
    check_multi_group_support,
    chunk_tokens_for_groups,
    describe_kv_groups,
    group_object_key,
    hit_chunks,
    state_view,
    window_chunks,
)
from tests.unit.vllm_connector_helpers import (
    attach_capturing_handler,
    fake_cached_reqs,
    fake_scheduler_output,
    make_flash_attn_metadata,
    make_scheduler,
    make_worker,
)

QWEN35_BLOCK = 544
QWEN35_SLOT = 1_114_112
GPT_OSS_ATTN = {
    "block_size": 16,
    "num_kv_heads": 8,
    "head_size": 64,
    "dtype": torch.bfloat16,
}


def qwen35_config(mamba_cache_mode="align"):
    """Qwen3.5-0.8B: 18 GDN layers in three groups, 6 attention layers."""
    gdn = [i for i in range(24) if i % 4 != 3]
    mamba = MambaSpec(
        block_size=QWEN35_BLOCK,
        shapes=((3, 6144), (16, 128, 128)),
        dtypes=(torch.bfloat16, torch.float32),
        page_size_padded=QWEN35_SLOT,
        mamba_cache_mode=mamba_cache_mode,
    )
    attn = FullAttentionSpec(
        block_size=QWEN35_BLOCK, num_kv_heads=2, head_size=256, dtype=torch.bfloat16
    )
    groups = [
        KVCacheGroupSpec([f"model.layers.{i}.linear_attn" for i in gdn[g::3]], mamba)
        for g in range(3)
    ]
    groups.append(
        KVCacheGroupSpec(
            [f"model.layers.{i}.self_attn.attn" for i in range(3, 24, 4)], attn
        )
    )
    return SimpleNamespace(kv_cache_groups=groups)


def gpt_oss_config():
    """gpt-oss-20b: 12 sliding-window and 12 full-attention layers."""
    return SimpleNamespace(
        kv_cache_groups=[
            KVCacheGroupSpec(
                [f"model.layers.{i}.attn" for i in range(0, 24, 2)],
                SlidingWindowSpec(sliding_window=128, **GPT_OSS_ATTN),
            ),
            KVCacheGroupSpec(
                [f"model.layers.{i}.attn" for i in range(1, 24, 2)],
                FullAttentionSpec(**GPT_OSS_ATTN),
            ),
        ]
    )


# =============================================================================
# Description
# =============================================================================


@pytest.mark.parametrize(
    ("name", "kind"),
    [
        ("FullAttentionSpec", GROUP_FULL),
        ("SlidingWindowSpec", GROUP_SLIDING_WINDOW),
        ("MambaSpec", GROUP_MAMBA),
        # Subclasses of the three, refused until checked.
        ("MLAAttentionSpec", GROUP_UNSUPPORTED),
        ("SinkFullAttentionSpec", GROUP_UNSUPPORTED),
        ("RSWASpec", GROUP_UNSUPPORTED),
        ("KpoolTailSpec", GROUP_UNSUPPORTED),
        ("SlidingWindowMLASpec", GROUP_UNSUPPORTED),
        # Unrelated specs.
        ("ChunkedLocalAttentionSpec", GROUP_UNSUPPORTED),
        ("CrossAttentionSpec", GROUP_UNSUPPORTED),
    ],
)
def test_spec_kind_accepts_only_verified_vllm_classes(name, kind):
    spec_cls = getattr(kv_cache_interface, name, None)
    if spec_cls is None:
        pytest.skip(f"this vLLM has no {name}")
    assert _spec_kind(spec_cls) == kind


def test_describe_qwen35_groups():
    groups = describe_kv_groups(qwen35_config())
    assert [g.kind for g in groups] == [GROUP_MAMBA] * 3 + [GROUP_FULL]
    assert [len(g.layer_names) for g in groups] == [6, 6, 6, 6]
    assert {g.block_size for g in groups} == {QWEN35_BLOCK}
    # The padded Mamba state takes the attention slot size.
    assert {g.slot_bytes for g in groups} == {QWEN35_SLOT}
    assert [g.mamba_cache_mode for g in groups] == ["align"] * 3 + [None]
    assert [g.state_blocks for g in groups] == [2, 2, 2, 0]
    # One object per (chunk, group), all the same size: 6 layers x 1 slot.
    assert {g.object_bytes(QWEN35_BLOCK) for g in groups} == {6 * QWEN35_SLOT}
    assert groups[0].object_bytes(QWEN35_BLOCK) == 6_684_672


def test_describe_gpt_oss_groups():
    groups = describe_kv_groups(gpt_oss_config())
    assert [g.kind for g in groups] == [GROUP_SLIDING_WINDOW, GROUP_FULL]
    assert [g.sliding_window for g in groups] == [128, None]
    # 256-token chunk = 16 blocks of 32 KiB for each of 12 layers.
    assert {g.object_bytes(256) for g in groups} == {12 * 32_768 * 16}


def test_mamba_mode_falls_back_to_cache_config():
    """A spec without the mode (older vLLM) takes cache_config's."""
    groups = describe_kv_groups(qwen35_config(None), default_mamba_cache_mode="align")
    assert [g.mamba_cache_mode for g in groups] == ["align"] * 3 + [None]


def test_blocks_for_tokens_adds_groups():
    # Qwen3.5: 3 Mamba groups x 2 state blocks + ceil(1500 / 544) attention.
    assert blocks_for_tokens(describe_kv_groups(qwen35_config()), 1500) == 9
    # gpt-oss: ceil(1000 / 16) in each of two groups.
    assert blocks_for_tokens(describe_kv_groups(gpt_oss_config()), 1000) == 126


def test_chunk_is_one_block_with_a_mamba_group():
    qwen = describe_kv_groups(qwen35_config())
    assert chunk_tokens_for_groups(qwen, 1088, QWEN35_BLOCK) == QWEN35_BLOCK
    gpt = describe_kv_groups(gpt_oss_config())
    assert chunk_tokens_for_groups(gpt, 256, 16) == 256


# =============================================================================
# Support checks
# =============================================================================


def _check(groups, block_size, use_layerwise=False, speculative=False):
    check_multi_group_support(
        groups,
        block_size=block_size,
        use_layerwise=use_layerwise,
        speculative=speculative,
    )


def test_supported_multi_group_models_pass():
    _check(describe_kv_groups(qwen35_config()), QWEN35_BLOCK)
    _check(describe_kv_groups(gpt_oss_config()), 16)


def test_single_group_is_always_accepted():
    single = describe_kv_groups(qwen35_config())[3:]
    _check(single, 16, use_layerwise=True, speculative=True)


def test_subclass_of_a_supported_spec_is_rejected():
    """A subclass can hold something else per block, like KpoolTailSpec's ring."""

    class ScratchRingSpec(SlidingWindowSpec):
        pass

    config = gpt_oss_config()
    config.kv_cache_groups[0] = KVCacheGroupSpec(
        config.kv_cache_groups[0].layer_names,
        ScratchRingSpec(sliding_window=128, **GPT_OSS_ATTN),
    )
    with pytest.raises(ValueError, match="ScratchRingSpec alongside other groups"):
        _check(describe_kv_groups(config), 16)


def test_layerwise_is_rejected():
    with pytest.raises(ValueError, match="maru_use_layerwise=false"):
        _check(describe_kv_groups(gpt_oss_config()), 16, use_layerwise=True)


def test_speculative_decoding_is_rejected():
    with pytest.raises(ValueError, match="speculative decoding"):
        _check(describe_kv_groups(gpt_oss_config()), 16, speculative=True)


@pytest.mark.parametrize("mode", ["none", "all"])
def test_mamba_needs_align_mode(mode):
    with pytest.raises(ValueError, match="--mamba-cache-mode align"):
        _check(describe_kv_groups(qwen35_config(mode)), QWEN35_BLOCK)


def test_block_size_must_match_cache_config():
    with pytest.raises(ValueError, match="block size 32"):
        _check(describe_kv_groups(gpt_oss_config()), 32)


# =============================================================================
# Connector
# =============================================================================


def _connector_config(block_size, **extra):
    return SimpleNamespace(
        kv_transfer_config=SimpleNamespace(kv_connector_extra_config=extra),
        cache_config=SimpleNamespace(block_size=block_size, mamba_cache_mode="align"),
        speculative_config=None,
    )


def test_connector_refuses_several_groups_until_supported():
    with pytest.raises(ValueError, match="several KV cache groups yet"):
        MaruKVConnector(
            _connector_config(QWEN35_BLOCK),
            KVConnectorRole.SCHEDULER,
            qwen35_config(),
        )


def test_connector_reports_the_setting_to_change():
    with pytest.raises(ValueError, match="maru_use_layerwise=false"):
        MaruKVConnector(
            _connector_config(16, maru_use_layerwise=True),
            KVConnectorRole.SCHEDULER,
            gpt_oss_config(),
        )


def test_request_finished_all_groups_answers_per_request():
    connector = MaruKVConnector.__new__(MaruKVConnector)
    connector._scheduler = _two_group_scheduler({"maru_async_store": True})
    request = SimpleNamespace(request_id="r1")
    assert connector.request_finished_all_groups(request, ([1, 2], [3, 4])) == (
        True,
        None,
    )
    # A parked request: vLLM already keeps its blocks for the receive.
    connector._scheduler._awaiting_deferred_recv.add("r2")
    parked = SimpleNamespace(request_id="r2")
    assert connector.request_finished_all_groups(parked, ([5], [6])) == (False, None)


# =============================================================================
# Scheduler: block ids of every group
# =============================================================================


def _two_group_scheduler(extra_config=None, **kwargs):
    """gpt-oss groups: block 16, chunk 32."""
    return make_scheduler(
        16, 32, extra_config, kv_groups=describe_kv_groups(gpt_oss_config()), **kwargs
    )


def _new_req(block_ids, num_tokens=64):
    return SimpleNamespace(
        req_id="r1", prompt_token_ids=list(range(num_tokens)), block_ids=block_ids
    )


def _step(sched, scheduled, **kwargs):
    """The one request's metadata for a scheduler step of ``scheduled`` tokens."""
    meta = sched.build_connector_meta(
        fake_scheduler_output(num_scheduled_tokens={"r1": scheduled}, **kwargs)
    )
    (req,) = meta.requests
    return req


def test_store_carries_every_group_across_chunked_prefill():
    sched = _two_group_scheduler()
    # 96 tokens = 3 chunks of 32. Step 1 covers 40 tokens in 3 blocks.
    req = _step(sched, 40, new_reqs=[_new_req(([1, 2, 3], [11, 12, 13]), 96)])
    assert req.block_ids == [1, 2, 3]
    assert req.group_block_ids == ([1, 2, 3], [11, 12, 13])

    # Step 2 fits in block 3: vLLM reports no new blocks.
    req = _step(sched, 8, cached=fake_cached_reqs(["r1"], [None], [40]))
    assert req.group_block_ids == ([1, 2, 3], [11, 12, 13])
    assert "r1" in sched._requests_need_store

    req = _step(
        sched, 48, cached=fake_cached_reqs(["r1"], [([4, 5, 6], [14, 15, 16])], [48])
    )
    assert req.block_ids == [1, 2, 3, 4, 5, 6]
    assert req.group_block_ids == ([1, 2, 3, 4, 5, 6], [11, 12, 13, 14, 15, 16])
    assert "r1" not in sched._requests_need_store


def test_single_group_leaves_group_block_ids_empty():
    sched = make_scheduler(16, 32)
    req = _step(sched, 64, new_reqs=[_new_req(([1, 2, 3, 4],))])
    assert req.block_ids == [1, 2, 3, 4]
    assert req.group_block_ids == ()


def test_deferred_load_carries_every_group():
    sched = _two_group_scheduler({"maru_async_load": True})
    request = SimpleNamespace(request_id="r1", prompt_token_ids=list(range(64)))
    blocks = MagicMock()
    blocks.get_block_ids.return_value = ([1, 2], [11, 12])
    sched.update_state_after_alloc(request, blocks, 32)

    req = sched.build_connector_meta(fake_scheduler_output()).requests[0]
    assert req.deferred_load
    assert req.block_ids == [1, 2]
    assert req.group_block_ids == ([1, 2], [11, 12])


def test_resumed_load_carries_every_group():
    """A request preempted and resumed with an external hit, loaded in step."""
    sched = _two_group_scheduler()
    tokens = list(range(64))
    request = SimpleNamespace(
        request_id="r1", prompt_token_ids=tokens, all_token_ids=tokens
    )
    sched.update_state_after_alloc(request, MagicMock(), 32)

    new_block_ids = ([1, 2, 3, 4], [11, 12, 13, 14])
    cached = fake_cached_reqs(["r1"], [new_block_ids], [32], resumed=["r1"])
    req = _step(sched, 32, cached=cached)
    assert not req.is_store
    assert req.block_ids == [1, 2, 3, 4]
    assert req.group_block_ids == new_block_ids


def test_resume_budget_counts_every_group():
    sched = make_scheduler(
        QWEN35_BLOCK,
        QWEN35_BLOCK,
        num_gpu_blocks=100,
        kv_groups=describe_kv_groups(qwen35_config()),
    )
    assert sched._resume_blocks(SimpleNamespace(num_tokens=1500)) == 9


# =============================================================================
# Hits across groups
# =============================================================================


def _has(objects):
    """``has_object`` over a set of (chunk index, group id)."""
    return lambda i, g: (i, g.group_id) in objects


def test_hit_ends_where_every_mamba_group_has_a_state():
    groups = describe_kv_groups(qwen35_config())
    attention = {(i, 3) for i in range(4)}
    # Prefill steps ended at chunks 2 and 3.
    states = {(i, g) for i in (2, 3) for g in range(3)}
    assert hit_chunks(groups, 4, QWEN35_BLOCK, _has(attention | states)) == (4, 4)
    assert hit_chunks(groups, 2, QWEN35_BLOCK, _has(attention | states)) == (0, 2)
    # Two of three Mamba groups are not a state.
    partial = attention | {(3, 0), (3, 1)}
    assert hit_chunks(groups, 4, QWEN35_BLOCK, _has(partial)) == (0, 4)


def test_hit_stops_at_a_missing_attention_chunk():
    groups = describe_kv_groups(qwen35_config())
    attention = {(i, 3) for i in range(4)} - {(2, 3)}
    states = {(i, g) for i in (1, 3) for g in range(3)}
    assert hit_chunks(groups, 4, QWEN35_BLOCK, _has(attention | states)) == (2, 2)


def test_sliding_window_needs_the_chunks_its_window_covers():
    groups = describe_kv_groups(gpt_oss_config())  # group 0 window 128, chunk 256
    full = {(i, 1) for i in range(4)}
    assert hit_chunks(groups, 4, 256, _has(full | {(3, 0)})) == (4, 4)
    # A window shorter than a chunk lies in the hit's last chunk alone.
    assert hit_chunks(groups, 4, 256, _has(full | {(1, 0)})) == (2, 4)
    assert hit_chunks(groups, 4, 256, _has(full)) == (0, 4)


def test_window_chunks():
    swa = describe_kv_groups(gpt_oss_config())[0]
    assert window_chunks(swa, 1024, 256) == range(3, 4)  # tokens 897..1023
    wide = dataclasses.replace(swa, sliding_window=300)
    assert window_chunks(wide, 1024, 256) == range(2, 4)  # tokens 725..1023
    assert window_chunks(wide, 256, 256) == range(0, 1)
    # The window includes the token being computed: W = C + 1 still needs
    # one chunk, W = C + 2 reaches into the one before.
    assert window_chunks(dataclasses.replace(swa, sliding_window=257), 1024, 256) == (
        range(3, 4)
    )
    assert window_chunks(dataclasses.replace(swa, sliding_window=258), 1024, 256) == (
        range(2, 4)
    )


class _Store:
    """A MaruHandler stand-in holding a set of keys."""

    def __init__(self, keys=()):
        self.keys = set(keys)
        self.queries = []

    def batch_exists(self, keys):
        self.queries.append(list(keys))
        return [k in self.keys for k in keys]


def _stored_by(tokens, chunk, attention_groups, state_groups, state_chunks):
    """Keys a request stores: attention objects for every chunk, states for some."""
    keys = _chunk_keys(tokens, chunk)
    stored = {group_object_key(k, g) for k in keys for g in attention_groups}
    stored |= {group_object_key(keys[i], g) for i in state_chunks for g in state_groups}
    return stored


def _qwen35_scheduler(stored):
    sched = make_scheduler(
        QWEN35_BLOCK, QWEN35_BLOCK, kv_groups=describe_kv_groups(qwen35_config())
    )
    sched._handler = _Store(stored)
    return sched


def _hit(sched, tokens, num_computed_tokens=0):
    request = SimpleNamespace(request_id="b", prompt_token_ids=tokens)
    matched, _ = sched.get_num_new_matched_tokens(request, num_computed_tokens)
    return matched


def _other(n):
    """Tokens no stored prompt contains."""
    return list(range(100_000, 100_000 + n))


def test_qwen35_prompt_of_whole_chunks_hits_nothing():
    """1,088 tokens in one prefill step: the only state is at the last chunk,
    which the held-back token excludes."""
    a = list(range(1088))
    sched = _qwen35_scheduler(_stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [1]))
    assert _hit(sched, a) == 0


def test_qwen35_same_prompt_hits_up_to_the_state():
    a = list(range(1200))  # step 1 ends at token 1,088
    sched = _qwen35_scheduler(_stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [1]))
    assert _hit(sched, a) == 1088


def test_qwen35_partial_overlap_hits_only_at_a_step_end():
    """A (2,300 tokens) ended prefill steps at tokens 1,632 and 2,176."""
    a = list(range(2300))
    stored = _stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [2, 3])
    sched = _qwen35_scheduler(stored)
    assert _hit(sched, a[:1500] + _other(300)) == 0
    assert _hit(sched, a[:1700] + _other(300)) == 1632


def test_gpt_oss_hits_whole_chunks():
    sched = make_scheduler(16, 256, kv_groups=describe_kv_groups(gpt_oss_config()))
    a = list(range(1200))
    sched._handler = _Store(_stored_by(a, 256, [0, 1], [], []))
    assert _hit(sched, a) == 1024
    # A whole-chunk prompt holds back a chunk, not a block as one group would:
    # a hit inside a chunk could need window tokens freed before the store.
    assert _hit(sched, a[:1024]) == 768


def test_lookup_asks_every_group_in_one_rpc():
    a = list(range(2300))
    sched = _qwen35_scheduler(_stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [2, 3]))
    b = a[:1700] + _other(300)
    assert _hit(sched, b) == 1632
    (query,) = sched._handler.queries
    keys = _chunk_keys(b, QWEN35_BLOCK)[:3]
    assert query == [group_object_key(k, g) for k in keys for g in range(4)]


def test_known_keys_that_give_every_chunk_skip_the_rpc():
    a = list(range(2300))
    sched = _qwen35_scheduler(_stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [2, 3]))
    b = a[:1700] + _other(300)  # 3 chunks may hit, and all 3 do
    assert _hit(sched, b) == 1632
    assert _hit(sched, b) == 1632
    assert len(sched._handler.queries) == 1


def test_a_short_hit_asks_again_for_the_missing_keys_only():
    a = list(range(2300))
    stored = _stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [2, 3])
    sched = _qwen35_scheduler(stored)
    b = a[:1500] + _other(300)
    assert _hit(sched, b) == 0
    assert _hit(sched, b) == 0
    first, second = sched._handler.queries
    assert second == [k for k in first if k not in stored]


def test_a_short_reply_leaves_the_rest_missing():
    a = list(range(1200))
    stored = _stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [1])
    sched = _qwen35_scheduler(stored)
    answer = sched._handler.batch_exists
    # Only chunk 0's four keys are answered.
    sched._handler.batch_exists = lambda keys: answer(keys)[:4]
    assert _hit(sched, a) == 0
    sched._handler.batch_exists = answer
    assert _hit(sched, a) == 1088


def test_hit_counts_past_the_gpu_prefix_cache():
    a = list(range(2300))
    sched = _qwen35_scheduler(_stored_by(a, QWEN35_BLOCK, [3], [0, 1, 2], [2, 3]))
    b = a[:1700] + _other(300)
    assert _hit(sched, b, num_computed_tokens=544) == 1088
    assert _hit(sched, b, num_computed_tokens=1632) == 0


def test_lookup_failure_is_a_miss():
    sched = _qwen35_scheduler(())
    sched._handler.batch_exists = MagicMock(side_effect=RuntimeError("down"))
    assert _hit(sched, list(range(1200))) == 0


# =============================================================================
# Worker registration
# =============================================================================

# A small Qwen3.5: layers [G, G, G, A] x 2, block 16. One block slot is
# 1,024 B for every layer: attention 2 (K/V) x 16 tokens x 2 heads x 8 x bf16,
# and the 224 B GDN state (conv 3x16 bf16 + ssm 2x4x4 fp32) padded up to it.
TINY_BLOCK = 16
TINY_SLOT = 1024
TINY_STATE = 3 * 16 * 2 + 2 * 4 * 4 * 4
TINY_BLOCKS = 12
TINY_GDN = [[0, 4], [1, 5], [2, 6]]
TINY_ATTN = [3, 7]
TINY_LAYER_BYTES = TINY_BLOCKS * TINY_SLOT


def _mamba_spec():
    return MambaSpec(
        block_size=TINY_BLOCK,
        shapes=((3, 16), (2, 4, 4)),
        dtypes=(torch.bfloat16, torch.float32),
        page_size_padded=TINY_SLOT,
        mamba_cache_mode="align",
    )


def _attn_kw():
    return {
        "block_size": TINY_BLOCK,
        "num_kv_heads": 2,
        "head_size": 8,
        "dtype": torch.bfloat16,
    }


def tiny_config(gdn=TINY_GDN, attn=TINY_ATTN):
    groups = [
        KVCacheGroupSpec([f"layers.{i}.linear_attn" for i in layers], _mamba_spec())
        for layers in gdn
    ]
    groups.append(
        KVCacheGroupSpec(
            [f"layers.{i}.attn" for i in attn], FullAttentionSpec(**_attn_kw())
        )
    )
    return SimpleNamespace(kv_cache_groups=groups)


def tiny_swa_config():
    """A small gpt-oss: sliding-window layers 0, 2 and full layers 1, 3."""
    return SimpleNamespace(
        kv_cache_groups=[
            KVCacheGroupSpec(
                ["layers.0.attn", "layers.2.attn"],
                SlidingWindowSpec(sliding_window=32, **_attn_kw()),
            ),
            KVCacheGroupSpec(
                ["layers.1.attn", "layers.3.attn"], FullAttentionSpec(**_attn_kw())
            ),
        ]
    )


def _shared(positions=3):
    """Every layer's block slots in one buffer, as vLLM allocates them: the
    layer at position p of each group starts at p x blocks x slot, so the
    groups overlap. Each byte holds a value derived from its offset."""
    return (
        torch.arange(positions * TINY_LAYER_BYTES, dtype=torch.int64)
        .remainder(251)
        .to(torch.uint8)
    )


def _region(shared, position):
    return shared[position * TINY_LAYER_BYTES : (position + 1) * TINY_LAYER_BYTES]


def _gdn_tensor(region, version):
    """A GDN layer as vLLM registers it: 0.23 a state list, 0.30 one int8 view."""
    if version == "0.30":
        return torch.as_strided(
            region.view(torch.int8),
            (TINY_BLOCKS, 1, 1, TINY_STATE),
            (TINY_SLOT, TINY_STATE, TINY_STATE, 1),
        )
    conv = torch.as_strided(
        region.view(torch.bfloat16), (TINY_BLOCKS, 3, 16), (TINY_SLOT // 2, 16, 1)
    )
    as_f32 = region.view(torch.float32)
    ssm = torch.as_strided(
        as_f32,
        (TINY_BLOCKS, 2, 4, 4),
        (TINY_SLOT // 4, 16, 4, 1),
        as_f32.storage_offset() + 96 // 4,
    )
    return [conv, ssm]


def _attn_tensor(region, version, kernel_block=TINY_BLOCK):
    """An attention layer: 0.23 flash (NB, 2, BS, NH, HS); 0.30 fused K/V
    (NB, NH, BS, 2 HS), split into kernel blocks when ``kernel_block`` is
    smaller than a block."""
    if version == "0.30":
        split = TINY_BLOCK // kernel_block
        return torch.as_strided(
            region.view(torch.bfloat16),
            (TINY_BLOCKS * split, 2, kernel_block, 16),
            (TINY_SLOT // 2 // split, 16, 32, 1),
        )
    return region.view(torch.bfloat16).view(TINY_BLOCKS, 2, 16, 2, 8)


def tiny_kv_caches(version, shared, gdn=TINY_GDN, attn=TINY_ATTN):
    caches = {
        f"layers.{i}.linear_attn": _gdn_tensor(_region(shared, p), version)
        for layers in gdn
        for p, i in enumerate(layers)
    }
    caches |= {
        f"layers.{i}.attn": _attn_tensor(_region(shared, p), version)
        for p, i in enumerate(attn)
    }
    return caches


def _worker(config=None, chunk=TINY_BLOCK, **kwargs):
    kwargs.setdefault("num_kv_heads", 2)
    kwargs.setdefault("head_size", 8)
    worker = make_worker(
        TINY_BLOCK,
        chunk,
        kv_groups=describe_kv_groups(config or tiny_config()),
        **kwargs,
    )
    worker._ensure_handler = MagicMock()
    return worker


def _registered_worker(kv_caches, config=None, chunk=TINY_BLOCK):
    worker = _worker(config, chunk)
    worker.register_kv_caches(kv_caches)
    return worker


@pytest.mark.parametrize("version", ["0.23", "0.30"])
def test_worker_maps_each_vllm_format_onto_the_groups(version):
    shared = _shared()
    kv_caches = tiny_kv_caches(version, shared)
    worker = _registered_worker(kv_caches)
    caches = worker._group_caches
    assert [c.group.kind for c in caches] == [GROUP_MAMBA] * 3 + [GROUP_FULL]
    # Layers keep the group's order: index = position in the object.
    assert caches[3].group.layer_names == ("layers.3.attn", "layers.7.attn")
    assert caches[3].tensors[1] is kv_caches["layers.7.attn"]
    # Each block's state reads as the first 224 B of its slot, at the layer's
    # offset in the shared buffer.
    for p, view in enumerate(caches[1].tensors):
        region = _region(shared, p)
        assert view.shape == (TINY_BLOCKS, TINY_STATE)
        for b in range(TINY_BLOCKS):
            slot = region[b * TINY_SLOT : b * TINY_SLOT + TINY_STATE]
            assert torch.equal(view[b], slot)
    # 0.23's flash layout has a kernel format; 0.30's fused K/V layout does not.
    layout = caches[3].layout
    assert (layout.format_name is not None) == (version == "0.23")
    assert (layout.num_blocks, layout.block_size) == (TINY_BLOCKS, TINY_BLOCK)
    # Two layers x one 1,024 B slot, for every group.
    assert worker._page_size_bytes == 2 * TINY_SLOT
    worker._ensure_handler.assert_called_once()


@pytest.mark.parametrize("version", ["0.23", "0.30"])
def test_state_view_writes_reach_the_gpu_state(version):
    """Load will write through the view, so it must alias vLLM's tensor."""
    shared = _shared()
    kv = _gdn_tensor(_region(shared, 1), version)
    view = state_view("x", kv, TINY_SLOT, TINY_STATE)
    view[4].fill_(0)
    parts = kv if isinstance(kv, list) else [kv]
    assert all(not t[4].any() for t in parts)
    assert all(t[3].any() for t in parts)
    # Only the state was written: the slot's padding and layer 0 are intact.
    assert _region(shared, 1)[4 * TINY_SLOT + TINY_STATE :][:8].any()
    assert torch.equal(_region(shared, 0), _region(_shared(), 0))


def test_page_is_the_largest_group_object():
    """GDN groups of 2, 3 and 2 layers: the page fits the middle one."""
    gdn = [[0, 4], [1, 5, 8], [2, 6]]
    shared = _shared()
    worker = _registered_worker(tiny_kv_caches("0.30", shared, gdn), tiny_config(gdn))
    assert [len(c.tensors) for c in worker._group_caches] == [2, 3, 2, 2]
    assert worker._page_size_bytes == 3 * TINY_SLOT


def test_attention_object_spans_the_chunk():
    shared = _shared()
    kv_caches = {
        f"layers.{i}.attn": _attn_tensor(_region(shared, i // 2), "0.30")
        for i in range(4)
    }
    worker = _registered_worker(kv_caches, tiny_swa_config(), chunk=4 * TINY_BLOCK)
    assert [c.group.kind for c in worker._group_caches] == [
        GROUP_SLIDING_WINDOW,
        GROUP_FULL,
    ]
    # Two layers x four blocks of 1,024 B.
    assert worker._page_size_bytes == 2 * 4 * TINY_SLOT


def test_split_kernel_blocks_are_refused():
    shared = _shared()
    kv_caches = tiny_kv_caches("0.30", shared)
    for p, i in enumerate(TINY_ATTN):
        kv_caches[f"layers.{i}.attn"] = _attn_tensor(
            _region(shared, p), "0.30", kernel_block=8
        )
    with pytest.raises(ValueError, match="16-token blocks, laid out as 8-token"):
        _registered_worker(kv_caches)


def test_attention_layout_needs_the_head_count():
    worker = _worker(num_kv_heads=None)
    with pytest.raises(ValueError, match="KV head count"):
        worker.register_kv_caches(tiny_kv_caches("0.30", _shared()))


def test_state_must_sit_contiguously_at_the_slot_start():
    region = _region(_shared(), 0)
    conv, _ = _gdn_tensor(region, "0.23")
    as_f32 = region.view(torch.float32)
    gap = torch.as_strided(as_f32, (TINY_BLOCKS, 2, 4, 4), (256, 16, 4, 1), 32)
    with pytest.raises(ValueError, match="contiguous state"):
        state_view("x", [conv, gap], TINY_SLOT, TINY_STATE)
    with pytest.raises(ValueError, match="contiguous state"):
        state_view("x", _gdn_tensor(region, "0.30"), 2 * TINY_SLOT, TINY_STATE)


def test_state_must_be_the_whole_spec_state():
    conv, _ = _gdn_tensor(_region(_shared(), 0), "0.23")
    with pytest.raises(ValueError, match="registers 96 B of Mamba state"):
        state_view("x", [conv], TINY_SLOT, TINY_STATE)


def test_registration_refuses_a_missing_layer():
    kv_caches = tiny_kv_caches("0.30", _shared())
    del kv_caches["layers.6.linear_attn"]
    with pytest.raises(ValueError, match="no registered tensor"):
        _registered_worker(kv_caches)


def test_attention_layers_must_match_in_dtype():
    kv_caches = tiny_kv_caches("0.23", _shared())
    kv_caches["layers.7.attn"] = kv_caches["layers.7.attn"].view(torch.float16)
    with pytest.raises(ValueError, match="differ in shape, dtype or strides"):
        _registered_worker(kv_caches)


def test_attention_block_may_not_exceed_the_spec():
    kv_caches = tiny_kv_caches("0.23", _shared())
    for i in TINY_ATTN:
        kv_caches[f"layers.{i}.attn"] = torch.zeros(
            TINY_BLOCKS, 2, 16, 2, 16, dtype=torch.bfloat16
        )
    with pytest.raises(ValueError, match="2048 B on the GPU, more than"):
        _registered_worker(kv_caches)


def test_a_padded_attention_page_is_accepted():
    """vLLM may pad an attention page; the KV then fills only part of it."""
    kv_caches = tiny_kv_caches("0.23", _shared())
    for i in TINY_ATTN:
        kv_caches[f"layers.{i}.attn"] = torch.zeros(
            TINY_BLOCKS, 2, 16, 2, 4, dtype=torch.bfloat16
        )
    worker = _registered_worker(kv_caches)
    assert worker._page_size_bytes == 2 * TINY_SLOT


def test_explicit_page_size_must_hold_the_largest_object():
    worker = make_worker(
        TINY_BLOCK,
        TINY_BLOCK,
        {"maru_chunk_size": TINY_SLOT},
        num_kv_heads=2,
        head_size=8,
        kv_groups=describe_kv_groups(tiny_config()),
    )
    worker._ensure_handler = MagicMock()
    with pytest.raises(ValueError, match="cannot hold one"):
        worker.register_kv_caches(tiny_kv_caches("0.30", _shared()))


# =============================================================================
# Stores
# =============================================================================


def _store_meta(tokens, group_block_ids, computed, scheduled, req_ids=("r1",)):
    return MaruConnectorMetadata(
        requests=[
            MaruReqMeta(
                req_id=rid,
                token_ids=tokens,
                block_ids=group_block_ids[0],
                is_store=True,
                num_scheduled_tokens=scheduled,
                num_computed_tokens=computed,
                group_block_ids=group_block_ids,
            )
            for rid in req_ids
        ]
    )


def _token_rows(kv, block, version):
    """One block's KV as ``[2, block_size, row]``, the order Maru stores.

    0.23's flash layout has a K/V axis; 0.30's fused layout keeps K and V in
    one row per token, which Maru halves.
    """
    if version == "0.30":
        rows = kv[block].transpose(0, 1).reshape(TINY_BLOCK, -1)
        return rows.reshape(TINY_BLOCK, 2, -1).transpose(0, 1)
    return kv[block].reshape(2, TINY_BLOCK, -1)


def _expected_slab(tensors, blocks, version):
    """KV_2LTD slab ``[2, layers, tokens, row]`` of ``blocks`` for each layer."""
    return torch.stack(
        [
            torch.stack(
                [
                    torch.cat([_token_rows(kv, b, version)[plane] for b in blocks])
                    for kv in tensors
                ]
            )
            for plane in range(2)
        ]
    )


def _slab(raw, like):
    return torch.frombuffer(bytearray(raw), dtype=torch.bfloat16).view(like.shape)


def _storing_worker(kv_caches, config=None, chunk=TINY_BLOCK, extra=None):
    worker = make_worker(
        TINY_BLOCK,
        chunk,
        extra,
        num_kv_heads=2,
        head_size=8,
        kv_groups=describe_kv_groups(config or tiny_config()),
    )
    worker._ensure_handler = MagicMock()
    worker.register_kv_caches(kv_caches)
    store = attach_capturing_handler(worker)
    return worker, store


# GDN groups 0-2 hold a state only at block position 1; attention uses 4, 5.
QWEN_BLOCKS = ([0, 1], [0, 2], [0, 3], [4, 5])


@pytest.mark.parametrize("version", ["0.23", "0.30"])
def test_a_step_ending_on_a_chunk_stores_every_group(version):
    shared = _shared()
    worker, store = _storing_worker(tiny_kv_caches(version, shared))
    tokens = list(range(40))
    worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))

    k0, k1 = _chunk_keys(tokens, TINY_BLOCK)
    # Attention: both chunks the step completed. Mamba: only the chunk the
    # step ended on; chunk 0's state was overwritten within the step.
    assert set(store) == {
        group_object_key(k0, 3),
        group_object_key(k1, 3),
        *(group_object_key(k1, g) for g in range(3)),
    }
    attn = worker._group_caches[3].tensors
    for key, block in ((k0, 4), (k1, 5)):
        expected = _expected_slab(attn, [block], version)
        assert torch.equal(_slab(store[group_object_key(key, 3)], expected), expected)
    # A state object holds each layer's state at a stride of one block slot.
    for g in range(3):
        obj = store[group_object_key(k1, g)]
        assert len(obj) == 2 * TINY_SLOT
        block = QWEN_BLOCKS[g][1]
        for p in range(2):
            state = _region(shared, p)[block * TINY_SLOT :][:TINY_STATE]
            assert obj[p * TINY_SLOT :][:TINY_STATE] == bytes(state)
    # Everything stored in the step is registered in one RPC.
    assert worker._handler.batch_store.call_count == 1


def test_a_step_ending_inside_a_chunk_stores_no_state():
    worker, store = _storing_worker(tiny_kv_caches("0.30", _shared()))
    tokens = list(range(48))
    worker.wait_for_save(
        _store_meta(tokens, ([0, 0, 1], [0, 0, 2], [0, 0, 3], [4, 5, 6]), 0, 40)
    )
    keys = _chunk_keys(tokens, TINY_BLOCK)
    assert set(store) == {group_object_key(k, 3) for k in keys[:2]}


def test_a_null_state_block_stores_no_state():
    worker, store = _storing_worker(tiny_kv_caches("0.30", _shared()))
    tokens = list(range(40))
    worker.wait_for_save(_store_meta(tokens, ([0, 0], [0, 2], [0, 3], [4, 5]), 0, 32))
    k1 = _chunk_keys(tokens, TINY_BLOCK)[1]
    assert group_object_key(k1, 0) not in store
    assert group_object_key(k1, 1) in store


def test_shared_prefix_is_stored_once_per_key():
    worker, store = _storing_worker(tiny_kv_caches("0.30", _shared()))
    tokens = list(range(40))
    worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32, ("r1", "r2")))
    (keys, _), _ = worker._handler.batch_store.call_args
    assert len(keys) == len(set(keys)) == 5
    assert len(store) == 5


def test_sliding_window_reads_the_null_block_behind_the_window():
    """Window 32, chunk 64: when the step starting at token 48 began, vLLM
    freed block 0 (tokens 0-15), whose id may now be another request's."""
    shared = _shared()
    kv_caches = {
        f"layers.{i}.attn": _attn_tensor(_region(shared, i // 2), "0.30")
        for i in range(4)
    }
    worker, store = _storing_worker(kv_caches, tiny_swa_config(), chunk=64)
    tokens = list(range(80))
    blocks = ([7, 8, 9, 10], [3, 4, 5, 6])
    worker.wait_for_save(_store_meta(tokens, blocks, 48, 16))

    (k0,) = _chunk_keys(tokens, 64)
    swa, full = (c.tensors for c in worker._group_caches)
    expected = _expected_slab(swa, [0, 8, 9, 10], "0.30")
    assert torch.equal(_slab(store[group_object_key(k0, 0)], expected), expected)
    expected = _expected_slab(full, [3, 4, 5, 6], "0.30")
    assert torch.equal(_slab(store[group_object_key(k0, 1)], expected), expected)


def test_without_the_kernel_a_write_behind_store_is_synchronous():
    worker, store = _storing_worker(
        tiny_kv_caches("0.23", _shared()), extra={"maru_async_store": True}
    )
    tokens = list(range(40))
    worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))
    assert len(store) == 5


def test_save_kv_layer_leaves_several_groups_to_wait_for_save():
    worker, store = _storing_worker(tiny_kv_caches("0.30", _shared()))
    kv = worker._group_caches[3].tensors[0]
    worker.save_kv_layer(
        "layers.3.attn", kv, None, _store_meta(list(range(40)), QWEN_BLOCKS, 0, 32)
    )
    worker._handler.alloc.assert_not_called()
    assert store == {}


def test_connector_wait_for_save_hands_several_groups_to_the_worker():
    connector = MaruKVConnector.__new__(MaruKVConnector)
    connector._kv_groups = describe_kv_groups(tiny_config())
    connector._worker = MagicMock()
    meta = MaruConnectorMetadata()
    connector._connector_metadata = meta
    connector.wait_for_save()
    connector._worker.wait_for_save.assert_called_once_with(meta)

    connector._kv_groups = connector._kv_groups[3:]
    connector._worker.reset_mock()
    connector.wait_for_save()
    connector._worker.wait_for_save.assert_not_called()


def test_a_later_step_stores_the_state_at_its_own_end():
    """Tokens 32-47 of a 56-token prompt: the state is at block position 2."""
    worker, store = _storing_worker(tiny_kv_caches("0.30", _shared()))
    tokens = list(range(56))
    blocks = ([0, 0, 1], [0, 0, 2], [0, 0, 3], [4, 5, 6])
    worker.wait_for_save(_store_meta(tokens, blocks, 32, 16))
    k2 = _chunk_keys(tokens, TINY_BLOCK)[2]
    assert set(store) == {group_object_key(k2, g) for g in range(4)}
    shared = _shared()
    obj = store[group_object_key(k2, 1)]
    assert obj[:TINY_STATE] == bytes(_region(shared, 0)[2 * TINY_SLOT :][:TINY_STATE])


def test_a_step_past_the_last_whole_chunk_stores_nothing():
    """The step ends at 48, past the prompt's two whole chunks."""
    worker, store = _storing_worker(tiny_kv_caches("0.30", _shared()))
    blocks = ([0, 0, 1], [0, 0, 2], [0, 0, 3], [4, 5, 6])
    worker.wait_for_save(_store_meta(list(range(40)), blocks, 32, 16))
    assert store == {}
    worker._handler.alloc.assert_not_called()


def test_a_failing_group_keeps_the_others_and_leaks_nothing():
    """r2 lacks the attention group's block ids: its attention store fails
    after r1's was gathered. r1's slab and both requests' states register."""
    worker, store = _storing_worker(tiny_kv_caches("0.30", _shared()))
    a, b = list(range(40)), list(range(1000, 1040))
    meta = MaruConnectorMetadata(
        requests=[
            *_store_meta(a, QWEN_BLOCKS, 0, 32).requests,
            *_store_meta(b, ([0, 6], [0, 7], [0, 8]), 0, 32, ("r2",)).requests,
        ]
    )
    worker.wait_for_save(meta)
    ka, kb = _chunk_keys(a, TINY_BLOCK), _chunk_keys(b, TINY_BLOCK)
    assert set(store) == {
        *(group_object_key(k, 3) for k in ka),
        *(group_object_key(k, g) for k in (ka[1], kb[1]) for g in range(3)),
    }
    assert worker._handler.batch_store.call_count == 1
    assert worker._handler.alloc.call_count == len(store)


def test_padded_pages_store_without_the_kernel():
    """vLLM pads a page with an as_strided view; the kernel, which knows only
    the base pointer, would read the padding as KV."""
    kv_caches = tiny_kv_caches("0.23", _shared())
    padded = _shared()
    for p, i in enumerate(TINY_ATTN):
        kv_caches[f"layers.{i}.attn"] = torch.as_strided(
            _region(padded, p).view(torch.bfloat16),
            (TINY_BLOCKS, 2, 16, 2, 4),
            (TINY_SLOT // 2, 128, 8, 4, 1),
        )
    worker = _worker()
    worker.register_kv_caches(kv_caches)
    worker._packed_load_kernel_ctx = MagicMock(return_value=("ops",) * 6)
    assert worker._group_store_kernel(worker._group_caches[3]) is None
    worker._packed_load_kernel_ctx.assert_not_called()

    dense = _worker()
    dense.register_kv_caches(tiny_kv_caches("0.23", _shared()))
    dense._packed_load_kernel_ctx = MagicMock(return_value=("ops",) * 6)
    assert dense._group_store_kernel(dense._group_caches[3]) == ("ops",) * 6


def _kernel_worker(extra=None):
    """A worker on CUDA whose attention group stores through maru_kv_ops."""
    from maru_vllm.connector import _resolve_kv_ops

    if not torch.cuda.is_available() or _resolve_kv_ops() is None:
        pytest.skip("needs CUDA and the maru_kv_ops kernels")
    shared = _shared().cuda()
    worker, store = _storing_worker(tiny_kv_caches("0.23", shared), extra=extra)
    worker._last_attn_metadata = make_flash_attn_metadata()
    return worker, store, shared


def _check_qwen_objects(worker, store, shared, tokens):
    k0, k1 = _chunk_keys(tokens, TINY_BLOCK)
    attn = [t.cpu() for t in worker._group_caches[3].tensors]
    for key, block in ((k0, 4), (k1, 5)):
        expected = _expected_slab(attn, [block], "0.23")
        assert torch.equal(_slab(store[group_object_key(key, 3)], expected), expected)
    for g in range(3):
        obj = store[group_object_key(k1, g)]
        block = QWEN_BLOCKS[g][1]
        for p in range(2):
            state = _region(shared, p)[block * TINY_SLOT :][:TINY_STATE].cpu()
            assert obj[p * TINY_SLOT :][:TINY_STATE] == bytes(state)


def test_kernel_store_uses_the_group_pointer_table():
    worker, store, shared = _kernel_worker()
    tokens = list(range(40))
    worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))
    assert worker._group_kernels[3] is not None
    assert len(store) == 5
    _check_qwen_objects(worker, store, shared, tokens)


def test_kernel_write_behind_stores_attention_later_and_states_now():
    worker, store, shared = _kernel_worker({"maru_async_store": True})
    tokens = list(range(40))
    worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))
    k1 = _chunk_keys(tokens, TINY_BLOCK)[1]
    assert all(group_object_key(k1, g) in store for g in range(3))
    worker._store_executor.shutdown(wait=True)
    assert len(store) == 5
    _check_qwen_objects(worker, store, shared, tokens)


def test_kernel_store_reads_the_null_block_behind_the_window():
    from maru_vllm.connector import _resolve_kv_ops

    if not torch.cuda.is_available() or _resolve_kv_ops() is None:
        pytest.skip("needs CUDA and the maru_kv_ops kernels")
    shared = _shared().cuda()
    kv_caches = {
        f"layers.{i}.attn": _attn_tensor(_region(shared, i // 2), "0.23")
        for i in range(4)
    }
    worker, store = _storing_worker(kv_caches, tiny_swa_config(), chunk=64)
    worker._last_attn_metadata = make_flash_attn_metadata()
    tokens = list(range(80))
    worker.wait_for_save(_store_meta(tokens, ([7, 8, 9, 10], [3, 4, 5, 6]), 48, 16))
    assert worker._group_kernels[0] is not None

    (k0,) = _chunk_keys(tokens, 64)
    swa = [t.cpu() for t in worker._group_caches[0].tensors]
    expected = _expected_slab(swa, [0, 8, 9, 10], "0.23")
    assert torch.equal(_slab(store[group_object_key(k0, 0)], expected), expected)


class _HeldExecutor:
    """Holds submitted write-behind completions until the test runs them."""

    def __init__(self):
        self.jobs = []

    def submit(self, fn, *args):
        self.jobs.append((fn, args))

    def run(self):
        for fn, args in self.jobs:
            fn(*args)


def test_a_request_is_held_until_its_group_objects_are_registered():
    worker, store, _ = _kernel_worker({"maru_async_store": True})
    worker._store_executor = _HeldExecutor()
    tokens = list(range(40))
    worker.wait_for_save(_store_meta(tokens, QWEN_BLOCKS, 0, 32))
    # The states are in; the attention objects are still being written.
    assert len(store) == 3
    assert worker.get_finished_saving({"r1"}) is None
    worker._store_executor.run()
    assert len(store) == 5
    assert worker.get_finished_saving(set()) == {"r1"}

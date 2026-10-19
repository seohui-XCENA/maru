# SPDX-License-Identifier: Apache-2.0
"""KV cache groups: description, support checks, hits and per-group block ids.

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

from maru_vllm.connector import MaruKVConnector, _chunk_keys
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
    window_chunks,
)
from tests.unit.vllm_connector_helpers import (
    fake_cached_reqs,
    fake_scheduler_output,
    make_scheduler,
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

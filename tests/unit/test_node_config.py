# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageService node config: example configs, defaults and every rule."""

import copy
import json

import pytest

from maru_common.storage_types import PoolKind, Visibility
from maru_storage_service.cli import EXIT_CONFIG, EXIT_NOT_SERVING, main
from maru_storage_service.config import (
    DEFAULT_RM,
    NodeConfigError,
    load_node_config,
    parse_node_config,
    parse_size,
)

M = 1 << 20
G = 1 << 30
T = 1 << 40

# Example configs: a GPU node and a memory node
GPU_NODE = {
    "node_id": "gpu-a",
    "socket": "/run/maru/node.sock",
    "server": "tcp://maru-server:5555",
    "page_size": "32M",
    "tiers": [
        {"store": "dram", "size": "64G", "demotion": "back"},
        {"store": "cxl", "size": "256G", "rm": "127.0.0.1:9850", "demotion": "back"},
        {"store": "ssd", "size": "2T", "path": "/mnt/nvme0/maru"},
    ],
}
MEMORY_NODE = {
    "node_id": "mem-1",
    "role": "memory",
    "server": "tcp://maru-server:5555",
    "page_size": "32M",
    "remote_server": {"listen": "tcp://0.0.0.0:5600", "key_file": "~/.maru/agent.key"},
    "tiers": [
        {"store": "dram", "size": "256G"},
        {"store": "cxl", "size": "1T", "rm": "127.0.0.1:9850"},
        {"store": "ssd", "size": "8T", "path": "/mnt/nvme0/maru"},
    ],
}


def edit(base, **changes):
    raw = copy.deepcopy(base)
    raw.update(changes)
    return raw


def with_tier(base, index, **changes):
    raw = copy.deepcopy(base)
    raw["tiers"][index].update(changes)
    return raw


def rejects(raw, match):
    with pytest.raises(NodeConfigError, match=match):
        parse_node_config(raw)


class TestSize:
    @pytest.mark.parametrize(
        "text,expected",
        [
            (4096, 4096),
            ("4096", 4096),
            ("32M", 32 * M),
            ("32MiB", 32 * M),
            ("1g", G),
            ("2T", 2 * T),
            ("8 K", 8 << 10),
        ],
    )
    def test_parses(self, text, expected):
        assert parse_size(text, "x") == expected

    @pytest.mark.parametrize("bad", ["", "M", "1.5G", "-1", -1, True, None, "10X"])
    def test_rejects(self, bad):
        with pytest.raises(NodeConfigError):
            parse_size(bad, "x")


class TestExamples:
    def test_gpu_node(self):
        c = parse_node_config(GPU_NODE)
        assert (c.node_id, c.role, c.page_size, c.local_pin) == (
            "gpu-a",
            "gpu",
            32 * M,
            False,
        )
        assert [t.store for t in c.tiers] == ["dram", "cxl", "ssd"]
        dram, cxl, ssd = c.tiers
        assert (dram.size, dram.demotion, dram.hugepage) == (64 * G, "back", True)
        assert (cxl.size, cxl.region_size, cxl.rm, cxl.visibility) == (
            256 * G,
            G,
            DEFAULT_RM,
            Visibility.COHERENT,
        )
        assert (ssd.size, ssd.demotion, ssd.path) == (2 * T, None, "/mnt/nvme0/maru")
        assert (dram.high_watermark, dram.low_watermark) == (0.9, 0.8)
        assert c.remote_server is None

    def test_memory_node(self):
        c = parse_node_config(MEMORY_NODE)
        assert c.role == "memory"
        assert c.socket == "/run/maru/node.sock"
        assert c.remote_server.listen == "tcp://0.0.0.0:5600"

    def test_budget_defaults(self):
        b = parse_node_config(GPU_NODE).budgets
        # credit comes from the DRAM and, short of it, the CXL tier
        assert b.credit_total_max_bytes == int((64 + 256) * G * 0.25)
        assert b.credit_max_bytes == b.credit_total_max_bytes
        assert (b.credit_refill_low, b.credit_refill_high) == (4 * 32 * M, 16 * 32 * M)
        # what waits for its commit came from credit
        assert b.commit_pending_max_bytes == b.credit_total_max_bytes
        assert b.transfer_reserve_bytes == {
            "dram": int(64 * G * 0.05),
            "cxl": int(256 * G * 0.05),
        }
        assert (
            b.fetch_request_max_bytes,
            b.fetch_max_inflight_bytes,
            b.fetch_max_batch_bytes,
        ) == (G, 4 * G, 256 * M)

    @pytest.mark.parametrize(
        "page, pages",
        [("1M", 512), ("3M", 170), ("32M", 16), ("64M", 16)],
    )
    def test_credit_at_hand_is_16_pages_or_512m(self, page, pages):
        raw = edit(GPU_NODE, page_size=page)
        raw["tiers"][1]["region_size"] = "192M"  # a multiple of every page
        b = parse_node_config(raw).budgets
        size = parse_size(page, "page")
        assert b.credit_refill_low == int(pages * size * 0.25)
        assert b.credit_refill_high == pages * size
        assert b.credit_max_bytes == b.credit_total_max_bytes // size * size

    def test_without_fallback_credit_comes_from_the_top_tier(self):
        raw = with_tier(GPU_NODE, 1, credit_fallback=False)
        c = parse_node_config(raw)
        assert c.tiers[1].credit_fallback is False
        assert c.budgets.credit_total_max_bytes == int(64 * G * 0.25)
        traits = c.traits()
        assert not traits[PoolKind.CXL].credit_fallback

    def test_explicit_cap_bounds_the_credit_at_hand(self):
        raw = edit(
            GPU_NODE,
            page_size="1M",
            tiers=[{"store": "dram", "size": "32G"}],
            budgets={"credit_max_bytes": "64M"},
        )
        b = parse_node_config(raw).budgets
        assert (b.credit_max_bytes, b.credit_refill_high) == (64 * M, 64 * M)
        assert b.credit_refill_low == 16 * M
        raw["budgets"] = {"credit_refill_low": "8M", "credit_refill_high": "100M"}
        b = parse_node_config(raw).budgets
        assert (b.credit_refill_low, b.credit_refill_high) == (8 * M, 100 * M)
        assert b.credit_max_bytes == b.credit_total_max_bytes // M * M

    def test_explicit_credit_max_wins_over_the_default(self):
        raw = edit(GPU_NODE, page_size="1M", budgets={"credit_max_bytes": "8M"})
        raw["tiers"][1]["region_size"] = "192M"
        assert parse_node_config(raw).budgets.credit_max_bytes == 8 * M

    def test_remote_tier(self):
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"][2]["demotion"] = "through"
        raw["tiers"].append(
            {"store": "remote", "url": "tcp://mem-1:5600, tcp://mem-2:5600"}
        )
        remote = parse_node_config(raw).tiers[-1]
        assert remote.url == ("tcp://mem-1:5600", "tcp://mem-2:5600")
        assert (remote.timeout_s, remote.rdma_device) == (30.0, None)


class TestRules:
    def test_required_fields(self):
        for key in ("node_id", "server", "page_size", "tiers"):
            raw = copy.deepcopy(GPU_NODE)
            del raw[key]
            rejects(raw, key)

    def test_unknown_keys(self):
        rejects(edit(GPU_NODE, nodeid="x"), "unknown keys")
        rejects(with_tier(GPU_NODE, 0, region_size="1G"), "unknown keys")
        rejects(edit(GPU_NODE, budgets={"credit_max": 1}), "unknown keys")

    def test_store_twice(self):
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"].insert(1, {"store": "dram", "size": "1G"})
        rejects(raw, "appears twice")

    def test_top_tier_is_memory(self):
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"] = [{"store": "ssd", "size": "1T", "path": "/x"}]
        rejects(raw, "first tier")

    def test_remote_is_last_and_gpu_only(self):
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"].insert(1, {"store": "remote", "url": "tcp://m:1"})
        rejects(raw, "remote must be the last")
        raw = copy.deepcopy(MEMORY_NODE)
        raw["tiers"][-1]["demotion"] = "back"
        raw["tiers"].append({"store": "remote", "url": "tcp://m:1"})
        rejects(raw, "memory node cannot have a remote")

    def test_last_tier_has_no_demotion(self):
        rejects(with_tier(GPU_NODE, 2, demotion="back"), "last tier")

    def test_demotion_values(self):
        rejects(with_tier(GPU_NODE, 0, demotion="later"), "demotion")
        assert (
            parse_node_config(with_tier(GPU_NODE, 0, demotion="through"))
            .tiers[0]
            .demotion
            == "through"
        )

    @pytest.mark.parametrize(
        "low,high", [(0.9, 0.9), (0.95, 0.9), (0, 0.5), (0.5, 1.1)]
    )
    def test_watermarks(self, low, high):
        rejects(
            with_tier(GPU_NODE, 0, low_watermark=low, high_watermark=high), "watermark"
        )

    def test_sizes(self):
        rejects(with_tier(GPU_NODE, 0, size=None), "size: required")
        rejects(with_tier(GPU_NODE, 0, size="1M"), "smaller than page_size")
        rejects(with_tier(GPU_NODE, 1, region_size="48M"), "multiple of page_size")
        rejects(with_tier(GPU_NODE, 1, size="512M"), "smaller than region_size")
        rejects(with_tier(GPU_NODE, 2, path=""), "path")

    def test_cxl_without_size_needs_explicit_budgets(self):
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"] = [{"store": "cxl"}]
        rejects(raw, "credit_total_max_bytes: required")
        raw["budgets"] = {
            "credit_total_max_bytes": "4G",
            "commit_pending_max_bytes": "2G",
        }
        rejects(raw, "transfer_reserve_bytes: required")
        raw["budgets"]["transfer_reserve_bytes"] = "1G"
        c = parse_node_config(raw)
        assert c.tiers[0].size is None and c.budgets.transfer_reserve_bytes == {
            "cxl": G
        }

    def test_visibility(self):
        assert (
            parse_node_config(with_tier(GPU_NODE, 1, visibility="flush"))
            .tiers[1]
            .visibility
            is Visibility.FLUSH
        )
        rejects(with_tier(GPU_NODE, 1, visibility="fenced"), "visibility")

    def test_local_pin_is_not_supported_yet(self):
        rejects(edit(GPU_NODE, local_pin=True), "local pin")

    def test_role(self):
        rejects(edit(GPU_NODE, role="cpu"), "role")
        raw = copy.deepcopy(MEMORY_NODE)
        del raw["remote_server"]
        rejects(raw, "remote_server: required")

    def test_budget_order(self):
        rejects(
            edit(GPU_NODE, budgets={"credit_max_bytes": "16M"}),
            "smaller than page_size",
        )
        rejects(
            edit(GPU_NODE, budgets={"credit_refill_low": "1G"}), "credit_refill_low <="
        )
        rejects(
            edit(
                GPU_NODE,
                budgets={"credit_max_bytes": "512M", "credit_total_max_bytes": "64M"},
            ),
            "credit_max_bytes <= credit_total",
        )
        # with only the total set, the per-connection default shrinks to fit
        c = parse_node_config(edit(GPU_NODE, budgets={"credit_total_max_bytes": "64M"}))
        assert c.budgets.credit_max_bytes == 64 * M
        rejects(
            edit(GPU_NODE, budgets={"fetch_max_batch_bytes": "2G"}),
            "fetch_max_batch_bytes <=",
        )

    def test_budgets_fit_the_tiers_credit_comes_from(self):
        # dram + cxl hold 320 GiB, less the pending budget and both reserves
        rejects(
            edit(GPU_NODE, budgets={"credit_total_max_bytes": "310G"}),
            r"exceed the tiers credit comes from \(dram \+ cxl",
        )
        no_fallback = with_tier(GPU_NODE, 1, credit_fallback=False)
        rejects(
            edit(no_fallback, budgets={"credit_total_max_bytes": "60G"}),
            r"exceed the tiers credit comes from \(dram:",
        )

    def test_fallback_is_for_a_tier_below_the_top(self):
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"] = [{"store": "cxl", "size": "32G", "credit_fallback": True}]
        rejects(raw, "only a tier below the top")

    def test_transfer_reserve_per_tier(self):
        c = parse_node_config(
            edit(GPU_NODE, budgets={"transfer_reserve_bytes": {"dram": "1G"}})
        )
        assert c.budgets.transfer_reserve_bytes == {
            "dram": G,
            "cxl": int(256 * G * 0.05),
        }
        rejects(
            edit(GPU_NODE, budgets={"transfer_reserve_bytes": {"ssd": "1G"}}),
            "no memory tier",
        )


class TestFileAndCli:
    def test_load(self, tmp_path):
        path = tmp_path / "node.json"
        path.write_text(json.dumps(GPU_NODE))
        assert load_node_config(path).source == str(path)

    def test_load_names_the_file(self, tmp_path):
        path = tmp_path / "node.json"
        path.write_text("{not json")
        with pytest.raises(NodeConfigError, match="node.json"):
            load_node_config(path)
        with pytest.raises(NodeConfigError, match="cannot read"):
            load_node_config(tmp_path / "missing.json")

    def test_check_prints_resolved_config(self, tmp_path, capsys):
        path = tmp_path / "node.json"
        path.write_text(json.dumps(GPU_NODE))
        assert main(["--config", str(path), "--check"]) == 0
        out = json.loads(capsys.readouterr().out)
        assert out["page_size"] == 32 * M
        assert out["tiers"][1]["visibility"] == "coherent"
        assert out["budgets"]["transfer_reserve_bytes"]["dram"] == int(64 * G * 0.05)
        assert out["budgets"]["credit_refill_high"] == 512 * M
        assert out["tiers"][1]["credit_fallback"] is True

    def test_check_prints_the_credit_default_of_small_pages(self, tmp_path, capsys):
        raw = edit(GPU_NODE, page_size="1M")
        raw["tiers"][1]["region_size"] = "192M"
        path = tmp_path / "node.json"
        path.write_text(json.dumps(raw))
        assert main(["--config", str(path), "--check"]) == 0
        budgets = json.loads(capsys.readouterr().out)["budgets"]
        assert budgets["credit_max_bytes"] == budgets["credit_total_max_bytes"]
        assert budgets["credit_refill_low"] == 128 * M
        assert budgets["credit_refill_high"] == 512 * M

    def test_bad_config_exit_code(self, tmp_path, capsys):
        path = tmp_path / "node.json"
        path.write_text(json.dumps(edit(GPU_NODE, role="x")))
        assert main(["--config", str(path), "--check"]) == EXIT_CONFIG
        assert "role" in capsys.readouterr().err

    def test_does_not_pretend_to_serve(self, tmp_path, capsys):
        path = tmp_path / "node.json"
        path.write_text(json.dumps(GPU_NODE))
        assert main(["--config", str(path)]) == EXIT_NOT_SERVING
        assert "not implemented" in capsys.readouterr().err


class TestReviewEdges:
    def test_small_top_tier_starts_with_defaults(self):
        raw = edit(GPU_NODE, tiers=[{"store": "dram", "size": "4G"}])
        b = parse_node_config(raw).budgets
        # 25% of 4 GiB is 1 GiB: 32 pages of 32 MiB, 16 of them at hand
        assert b.credit_total_max_bytes == G
        assert b.credit_max_bytes == 32 * 32 * M
        assert b.credit_refill_high == 16 * 32 * M

    def test_top_tier_too_small_for_one_page_names_the_defaults(self):
        raw = edit(GPU_NODE, tiers=[{"store": "dram", "size": "64M"}])
        rejects(raw, "defaults")

    def test_cpu_is_read_as_dram(self):
        raw = with_tier(GPU_NODE, 0, store="cpu")
        assert parse_node_config(raw).tiers[0].store == "dram"

    @pytest.mark.parametrize("value", [float("nan"), float("inf")])
    def test_numbers_must_be_finite(self, value):
        rejects(with_tier(GPU_NODE, 0, high_watermark=value), "finite")
        raw = copy.deepcopy(GPU_NODE)
        raw["tiers"][2]["demotion"] = "back"
        raw["tiers"].append({"store": "remote", "url": "tcp://m:1", "timeout_s": value})
        rejects(raw, "finite")

    def test_nan_in_the_file(self, tmp_path):
        path = tmp_path / "node.json"
        path.write_text(json.dumps(GPU_NODE).replace('"64G"', "NaN"))
        with pytest.raises(NodeConfigError, match="NaN"):
            load_node_config(path)

    def test_duplicate_keys(self, tmp_path):
        path = tmp_path / "node.json"
        path.write_text(json.dumps(GPU_NODE)[:-1] + ', "node_id": "other"}')
        with pytest.raises(NodeConfigError, match="appears twice"):
            load_node_config(path)

    def test_not_utf8(self, tmp_path):
        path = tmp_path / "node.json"
        path.write_bytes(b'{"node_id": "\xff"}')
        with pytest.raises(NodeConfigError, match="UTF-8"):
            load_node_config(path)

    def test_ssd_path_is_absolute_and_normalized(self):
        assert (
            parse_node_config(with_tier(GPU_NODE, 2, path="/mnt/x/")).tiers[2].path
            == "/mnt/x"
        )
        rejects(with_tier(GPU_NODE, 2, path="mnt/x"), "absolute")

    @pytest.mark.parametrize("node_id", ["gpu:a", "gpu a", "-gpu", "gpu/a"])
    def test_node_id_characters(self, node_id):
        rejects(edit(GPU_NODE, node_id=node_id), "node_id")

    def test_key_file_home_is_expanded(self):
        c = parse_node_config(MEMORY_NODE)
        assert not c.remote_server.key_file.startswith("~")

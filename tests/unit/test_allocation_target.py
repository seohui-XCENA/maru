# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Public configuration contracts for allocation targets and rollout guards."""

# Standard
from dataclasses import FrozenInstanceError
from unittest.mock import patch

# Third Party
import pytest

# First Party
from maru import MaruConfig, MaruHandler
from maru_common.allocation_target import (
    AllocationPolicy,
    AllocationTarget,
    normalize_allocation_targets,
    parse_byte_size,
    parse_target_sizes,
)
from maru_server.server import MaruServer, main

GIB = 1 << 30
UINT64_MAX = (1 << 64) - 1


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("0", 0),
        ("64", 64),
        ("64B", 64),
        ("2KiB", 2048),
        ("32MiB", 32 << 20),
        (" 256GiB ", 256 * GIB),
        ("1TiB", 1 << 40),
        (str(UINT64_MAX), UINT64_MAX),
    ],
)
def test_parse_byte_size(value: str, expected: int) -> None:
    """Byte and binary-unit strings preserve their exact byte count."""
    assert parse_byte_size(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "",
        "-1",
        "+1",
        "1.5GiB",
        "256GB",
        "1e9",
        "2 GiB",
        "1GiBjunk",
        str(1 << 64),
        "16777216TiB",
    ],
)
def test_invalid_byte_size(value: str) -> None:
    """Malformed, ambiguous or overflowing sizes are rejected."""
    with pytest.raises(ValueError):
        parse_byte_size(value)


def test_whole_device_target_is_immutable() -> None:
    """Whole-device configuration has no invented range or device access."""
    target = AllocationTarget("a", "/dev/not-present")
    assert target.offset_bytes is None
    assert target.length_bytes is None
    with pytest.raises(FrozenInstanceError):
        target.target_id = "b"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("offset", "length"),
    [(0, 1), (0, 256 * GIB), (256 * GIB, 256 * GIB), (UINT64_MAX - 1, 1)],
)
def test_range_target(offset: int, length: int) -> None:
    """Explicit ranges retain DAX-relative offsets without header adjustment."""
    target = AllocationTarget("a", "/dev/dax0.0", offset, length)
    assert target.offset_bytes == offset
    assert target.length_bytes == length


@pytest.mark.parametrize(
    ("offset", "length"),
    [
        (None, 1),
        (0, None),
        (-1, 1),
        (0, 0),
        (0, -1),
        (True, 1),
        (0, False),
        (1.5, 2),
        (UINT64_MAX, 1),
        (0, 1 << 64),
    ],
)
def test_invalid_range(offset: object, length: object) -> None:
    """Incomplete, non-integer and overflowing range fields are rejected."""
    with pytest.raises(ValueError):
        AllocationTarget("a", "/dev/dax0.0", offset, length)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("target_id", "path"),
    [("", "/dev/dax0.0"), ("  ", "/dev/dax0.0"), ("a", ""), ("a", "dax0.0")],
)
def test_invalid_identity(target_id: str, path: str) -> None:
    """Targets require a meaningful ID and an absolute device path."""
    with pytest.raises(ValueError):
        AllocationTarget(target_id, path)


def test_size_list_matches_repeated_size() -> None:
    """Both user-facing forms generate the documented 256 GiB boundaries."""
    listed = parse_target_sizes("/dev/dax0.0", target_sizes="256GiB,256GiB")
    repeated = parse_target_sizes(
        "/dev/dax0.0",
        target_size="256GiB",
        target_count=2,
    )
    assert (
        listed
        == repeated
        == (
            AllocationTarget("target-0", "/dev/dax0.0", 0, 256 * GIB),
            AllocationTarget("target-1", "/dev/dax0.0", 256 * GIB, 256 * GIB),
        )
    )


def test_unequal_sizes_and_base_offset() -> None:
    """The next target starts at the previous end even for unequal lengths."""
    targets = parse_target_sizes(
        "/dev/dax0.0",
        target_sizes="64MiB,128MiB",
        base_offset=2 << 20,
    )
    assert [(t.offset_bytes, t.length_bytes) for t in targets] == [
        (2 << 20, 64 << 20),
        (66 << 20, 128 << 20),
    ]


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"target_sizes": ""},
        {"target_sizes": "1GiB,"},
        {"target_sizes": "0,1GiB"},
        {"target_sizes": "1GiB", "target_size": "1GiB", "target_count": 1},
        {"target_sizes": "1GiB", "target_count": 2},
        {"target_size": "1GiB"},
        {"target_count": 2},
        {"target_size": "1GiB", "target_count": 0},
        {"target_size": "1GiB", "target_count": -1},
        {"target_size": "1GiB", "target_count": True},
        {"target_size": "0", "target_count": 2},
        {"target_sizes": "1", "base_offset": -1},
        {"target_sizes": f"{UINT64_MAX},1"},
        {"target_sizes": "1", "base_offset": UINT64_MAX},
        {"target_size": str(UINT64_MAX), "target_count": 2},
    ],
)
def test_invalid_size_options(kwargs: dict[str, object]) -> None:
    """Mixed forms, incomplete forms and cumulative overflow fail clearly."""
    with pytest.raises(ValueError):
        parse_target_sizes("/dev/dax0.0", **kwargs)  # type: ignore[arg-type]


def test_default_policy_preserves_any_pool_and_allowlist() -> None:
    """OFF retains any-pool behavior and the first occurrence of each path."""
    assert normalize_allocation_targets() == ()
    assert normalize_allocation_targets(dax_paths=[]) == ()
    targets = normalize_allocation_targets(
        allocation_policy="fill_first",
        dax_paths=["/dev/dax1.0", "/dev/dax0.0", "/dev/dax1.0"],
    )
    assert [t.dax_path for t in targets] == ["/dev/dax1.0", "/dev/dax0.0"]
    assert all(t.offset_bytes is None for t in targets)


def test_adjacent_targets_are_valid_and_keep_input_order() -> None:
    """Adjacent ranges and matching offsets in distinct files do not overlap."""
    targets = [
        AllocationTarget("b", "/dev/dax0.0", 256 * GIB, 256 * GIB),
        AllocationTarget("a", "/dev/dax0.0", 0, 256 * GIB),
        AllocationTarget("c", "/dev/dax1.0", 0, 256 * GIB),
    ]
    assert normalize_allocation_targets(
        allocation_policy="chunk_round_robin",
        allocation_targets=targets,
    ) == tuple(targets)


@pytest.mark.parametrize(
    "targets",
    [
        [AllocationTarget("a", "/dev/dax0.0"), AllocationTarget("a", "/dev/dax1.0")],
        [
            AllocationTarget("a", "/dev/dax0.0"),
            AllocationTarget("b", "/dev/dax0.0", 0, 1),
        ],
        [
            AllocationTarget("a", "/dev/dax0.0", 0, 64),
            AllocationTarget("b", "/dev/dax0.0", 32, 64),
        ],
        [
            AllocationTarget("a", "/dev/dax0.0", 0, 64),
            AllocationTarget("b", "/dev/../dev/dax0.0", 32, 1),
        ],
        [AllocationTarget("a", "/dev/dax0.0"), AllocationTarget("b", "/dev/dax0.0")],
    ],
)
def test_duplicate_or_overlapping_targets(targets: list[AllocationTarget]) -> None:
    """Identity collisions and intersecting ranges within a DAX are rejected."""
    with pytest.raises(ValueError):
        normalize_allocation_targets(
            allocation_policy="chunk_round_robin",
            allocation_targets=targets,
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"allocation_policy": "unknown"},
        {"allocation_policy": "chunk_round_robin"},
        {"allocation_policy": "chunk_round_robin", "allocation_targets": []},
        {"allocation_targets": [AllocationTarget("a", "/dev/dax0.0", 0, 1)]},
        {
            "allocation_policy": "chunk_round_robin",
            "dax_paths": [],
            "allocation_targets": [AllocationTarget("a", "/dev/dax0.0")],
        },
        {"dax_paths": "/dev/dax0.0"},
        {"allocation_policy": "chunk_round_robin", "allocation_targets": ["invalid"]},
    ],
)
def test_invalid_normalization_inputs(kwargs: dict[str, object]) -> None:
    """Explicit targets require ON, one input form and valid model instances."""
    with pytest.raises(ValueError):
        normalize_allocation_targets(**kwargs)  # type: ignore[arg-type]


def test_client_policy_defaults_and_validation() -> None:
    """Client defaults remain OFF while a valid future ON config can be represented."""
    assert MaruConfig().placement_policy == AllocationPolicy.FILL_FIRST
    assert MaruConfig(placement_policy="fill_first").auto_expand is True
    config = MaruConfig(placement_policy="chunk_round_robin", auto_expand=False)
    assert config.placement_policy == AllocationPolicy.CHUNK_ROUND_ROBIN
    with pytest.raises(ValueError, match="auto_expand=False"):
        MaruConfig(placement_policy="chunk_round_robin")
    with pytest.raises(ValueError):
        MaruConfig(placement_policy="unknown")


def test_handler_rejects_on_before_constructing_rpc() -> None:
    """Unsupported ON execution cannot quietly allocate through the legacy path."""
    config = MaruConfig(placement_policy="chunk_round_robin", auto_expand=False)
    with patch("maru_handler.rpc_async_client.RpcAsyncClient") as rpc:
        with pytest.raises(NotImplementedError, match="not yet supported"):
            MaruHandler(config)
        rpc.assert_not_called()


def test_server_rejects_on_before_constructing_rm() -> None:
    """Future target configuration cannot trigger an RM connection in C01."""
    with patch("maru_server.server.AllocationManager") as manager:
        with pytest.raises(NotImplementedError, match="not yet supported"):
            MaruServer(
                allocation_policy="chunk_round_robin",
                allocation_targets=parse_target_sizes(
                    "/dev/dax0.0",
                    target_sizes="256GiB,256GiB",
                ),
            )
        manager.assert_not_called()


def test_server_off_rejects_explicit_targets() -> None:
    """OFF cannot silently ignore a requested range restriction."""
    with pytest.raises(ValueError, match="require chunk_round_robin"):
        MaruServer(allocation_targets=[AllocationTarget("a", "/dev/dax0.0", 0, 1)])


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (
            ["--dax-path", "/dev/dax0.0", "--target-sizes", "256GiB,256GiB"],
            "require chunk_round_robin",
        ),
        (
            [
                "--allocation-policy",
                "chunk_round_robin",
                "--dax-path",
                "/dev/dax0.0",
                "--target-size",
                "256GiB",
                "--target-count",
                "2",
            ],
            "not yet supported",
        ),
        (
            ["--allocation-policy", "chunk_round_robin", "--dax-path", "/dev/dax0.0"],
            "not yet supported",
        ),
        (["--target-sizes", "1GiB,1GiB"], "exactly one --dax-path"),
        (
            [
                "--dax-path",
                "/dev/dax0.0",
                "--dax-path",
                "/dev/dax1.0",
                "--target-sizes",
                "1GiB,1GiB",
            ],
            "exactly one --dax-path",
        ),
        (
            ["--dax-path", "/dev/dax0.0", "--target-size", "1GiB"],
            "both target_size and target_count",
        ),
        (
            ["--dax-path", "/dev/dax0.0", "--target-count", "2"],
            "both target_size and target_count",
        ),
        (
            ["--dax-path", "/dev/dax0.0", "--target-base-offset", "0"],
            "both target_size and target_count",
        ),
        (["--target-base-offset", "-1"], "invalid"),
        (["--allocation-policy", "bad"], "invalid choice"),
    ],
)
def test_cli_rejects_invalid_or_unsupported_execution(
    arguments: list[str],
    message: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI validation fails before constructing a server or reserving memory."""
    with (
        patch("sys.argv", ["maru-server", *arguments]),
        patch("maru_server.server.MaruServer") as server,
    ):
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 2
        server.assert_not_called()
    assert message in capsys.readouterr().err


def test_cli_explicit_off_preserves_legacy_paths() -> None:
    """Explicit OFF passes the original allowlist to the existing server path."""
    arguments = [
        "maru-server",
        "--allocation-policy",
        "fill_first",
        "--dax-path",
        "/dev/dax1.0",
        "--dax-path",
        "/dev/dax0.0",
    ]
    with (
        patch("sys.argv", arguments),
        patch("maru_server.server.MaruServer") as server,
        patch("maru_server.rpc_server.RpcServer") as rpc,
        patch("signal.signal"),
    ):
        main()
        server.assert_called_once_with(
            rm_address="127.0.0.1:9850",
            dax_paths=["/dev/dax1.0", "/dev/dax0.0"],
            allocation_policy="fill_first",
            allocation_targets=None,
        )
        rpc.return_value.start.assert_called_once()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"target_size": "1", "target_count": 1025},
        {"target_sizes": ",".join(["1"] * 1025)},
    ],
)
def test_size_expansion_limit(kwargs: dict[str, object]) -> None:
    """The documented expansion limit rejects huge configuration allocations."""
    with pytest.raises(ValueError, match="at most 1024"):
        parse_target_sizes("/dev/dax0.0", **kwargs)  # type: ignore[arg-type]


def test_size_expansion_limit_boundary() -> None:
    """The maximum supported number of consecutive target sizes remains valid."""
    targets = parse_target_sizes("/dev/dax0.0", target_size="1MiB", target_count=1024)
    assert len(targets) == 1024
    assert targets[-1].offset_bytes == 1023 << 20

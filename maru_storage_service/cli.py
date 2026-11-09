# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""``maru-storage``: the StorageService command.

``maru-storage --config node.json --check`` checks the node config and prints
it with every default filled in. Serving starts in stage S2; until then the
command refuses to run without ``--check`` instead of pretending to serve.
"""

import argparse
import dataclasses
import json
import sys
from enum import Enum
from typing import Any

from .config import NodeConfig, NodeConfigError, load_node_config

EXIT_CONFIG = 2
EXIT_NOT_SERVING = 3


def config_to_dict(config: NodeConfig) -> dict[str, Any]:
    """The resolved config as plain JSON values."""

    def plain(value: Any) -> Any:
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, dict):
            return {k: plain(v) for k, v in value.items()}
        if isinstance(value, list | tuple):
            return [plain(v) for v in value]
        return value

    out = plain(dataclasses.asdict(config))
    out.pop("source", None)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="maru-storage",
        description="StorageService: the pool manager of one maru node.",
    )
    parser.add_argument("--config", required=True, help="node config file (JSON)")
    parser.add_argument(
        "--check",
        action="store_true",
        help="check the config, print it with defaults filled in, and exit",
    )
    args = parser.parse_args(argv)
    try:
        config = load_node_config(args.config)
    except NodeConfigError as e:
        print(f"maru-storage: {e}", file=sys.stderr)
        return EXIT_CONFIG
    if args.check:
        print(json.dumps(config_to_dict(config), indent=2))
        return 0
    print(
        "maru-storage: serving is not implemented yet (stage S2); use --check",
        file=sys.stderr,
    )
    return EXIT_NOT_SERVING

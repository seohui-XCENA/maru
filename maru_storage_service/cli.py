# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""``maru-storage``: serve a node until SIGTERM/SIGINT, then NODE_CLOSE(restart).
A run that ends on its own is followed by a new run in this process.
``--check`` prints the resolved config."""

import argparse
import dataclasses
import json
import logging
import signal
import sys
from enum import Enum
from typing import Any

from .config import NodeConfig, NodeConfigError, load_node_config

EXIT_CONFIG = 2
EXIT_NOT_SERVING = 3
EXIT_START = 4
# Another run claimed the node in the RM; never claimed back from here
EXIT_LOST = 5
# A run could not be torn down: no run may follow it in this process
EXIT_FAILED = 6

logger = logging.getLogger(__name__)


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

    out: dict[str, Any] = plain(dataclasses.asdict(config))
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
    parser.add_argument(
        "--log-level", default="INFO", help="logging level (default INFO)"
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
    return serve(config, args.log_level)


def serve(config: NodeConfig, log_level: str = "INFO") -> int:
    """Run the node until SIGTERM or SIGINT."""
    from .drivers.cxl import CxlTierError
    from .drivers.dram import DramPoolError
    from .local_server import LocalServerError
    from .server_client import NodeOpenError
    from .service import Node, NotServableError

    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        node = Node(config)
    except NotServableError as e:
        print(f"maru-storage: {e}", file=sys.stderr)
        return EXIT_NOT_SERVING

    def on_signal(signum, _frame) -> None:
        logger.info("signal %d: stopping", signum)
        # Also stops a start still waiting for the RM or MaruServer
        node.stop()

    previous = {
        sig: signal.signal(sig, on_signal) for sig in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        try:
            if node.run():
                return 0
        except (
            CxlTierError,
            DramPoolError,
            LocalServerError,
            NodeOpenError,
            OSError,
        ) as e:
            print(f"maru-storage: cannot start: {e}", file=sys.stderr)
            return EXIT_START
        if node.failed is not None:
            print(
                f"maru-storage: a run's teardown failed: {node.failed}", file=sys.stderr
            )
            return EXIT_FAILED
        print(f"maru-storage: stopped: {node.ended}", file=sys.stderr)
        return EXIT_LOST
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)

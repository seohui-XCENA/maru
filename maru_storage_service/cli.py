# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""``maru-storage``: serve a node until SIGTERM/SIGINT, then NODE_CLOSE(restart).
``--check`` prints the resolved config."""

import argparse
import dataclasses
import json
import logging
import signal
import sys
import threading
from enum import Enum
from typing import Any

from maru_common.storage_types import CloseMode

from .config import NodeConfig, NodeConfigError, load_node_config

EXIT_CONFIG = 2
EXIT_NOT_SERVING = 3
EXIT_START = 4
# The run let go of its CXL regions (lease); a supervisor restarts it
EXIT_LOST = 5

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
    """Run the StorageService until SIGTERM or SIGINT."""
    from .drivers.cxl import CxlTierError
    from .drivers.dram import DramPoolError
    from .local_server import LocalServerError
    from .ports import ServerStopped
    from .server_client import NodeOpenError
    from .service import NotServableError, StorageService

    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        service = StorageService(config)
    except NotServableError as e:
        print(f"maru-storage: {e}", file=sys.stderr)
        return EXIT_NOT_SERVING
    stop = threading.Event()

    def on_signal(signum, _frame) -> None:
        logger.info("signal %d: stopping", signum)
        stop.set()
        # Also stops a start still waiting for the RM or MaruServer
        service.interrupt()

    previous = {
        sig: signal.signal(sig, on_signal) for sig in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        try:
            service.start()
        except ServerStopped:
            if service.lost is not None:  # the lease ran out during the start
                service.wait_finished()
                print(f"maru-storage: stopped: {service.lost}", file=sys.stderr)
                return EXIT_LOST
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
        while not stop.wait(1.0) and not service.wait(0):
            pass
        if service.lost is not None:
            service.wait_finished()
            print(f"maru-storage: stopped: {service.lost}", file=sys.stderr)
            return EXIT_LOST
        service.stop(CloseMode.RESTART)
        return 0
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)

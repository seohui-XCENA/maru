# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Peer driver: pools of another node reached over RDMA. Not implemented yet.

Its TierDriver opens a memory node's pool from its PoolInfo as an IO_ONLY
DriverPool (no descriptor, no fd); it creates none, since the memory node makes
its own pools. close() releases the connection and its registered memory.
"""

from ..config_fields import NodeConfigError, TierContext, TierSpec, _number
from ..tiers import Level

DEFAULT_RDMA_TIMEOUT_S = 30.0  # the same as maru_rdma_timeout_s today


def _parse(raw: dict, ctx: TierContext) -> dict:
    what = ctx.what
    urls = raw.get("url")
    if isinstance(urls, str):
        urls = [u.strip() for u in urls.split(",")]
    if (
        not isinstance(urls, list)
        or not urls
        or not all(isinstance(u, str) and u for u in urls)
    ):
        raise NodeConfigError(f"{what}.url: required, one address or several")
    timeout = _number(raw, "timeout_s", what, DEFAULT_RDMA_TIMEOUT_S)
    if timeout <= 0:
        raise NodeConfigError(
            f"{what}.timeout_s: expected a positive number, got {timeout!r}"
        )
    device = raw.get("rdma_device")
    if device is not None and not isinstance(device, str):
        raise NodeConfigError(f"{what}.rdma_device: expected a string, got {device!r}")
    return {
        "url": tuple(urls),
        "rdma_device": device or None,
        "timeout_s": float(timeout),
    }


# The memory nodes' pools: not this node's, and of their own kinds
SPEC = TierSpec(
    store="remote",
    level=Level.L2,
    local=False,
    kind=None,
    keys=frozenset({"store", "url", "rdma_device", "timeout_s"}),
    served=False,
    common=False,
    last_only=True,
    gpu_only=True,
    parse=_parse,
)

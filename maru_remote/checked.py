# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""UCX limits under which a deregistered range refuses late RDMA.

A released range must refuse late RDMA, which holds only where the NIC
checks the remote key of every request and a deregistration really reaches
the NIC. UCX is therefore limited to its InfiniBand/RoCE transports (over TCP
or shared memory a write to a deregistered range still lands), and its
registration cache is turned off (with it, a NIXL deregistration leaves the
NIC registration in place and late writes still land).

Every process that registers memory for RDMA calls
:func:`require_checked_transport` before it creates its NIXL agent.
"""

from collections.abc import MutableMapping

# UCX transports where the NIC checks the remote key of each RDMA, so a
# deregistered range refuses late writes ("self" never leaves the process).
CHECKED_UCX_TRANSPORTS = frozenset(
    {
        "rc", "rc_v", "rc_x", "rc_verbs", "rc_mlx5",
        "dc", "dc_x", "dc_mlx5",
        "self",
    }
)  # fmt: skip
# UD has no one-sided RDMA, so UCX would carry RDMA over it in software. It
# passes only as an auxiliary transport (":aux"), which UCX uses to set up rc
# connections; "ib" is left out because it enables UD for RDMA too.
AUX_ONLY_UCX_TRANSPORTS = frozenset({"ud", "ud_v", "ud_x", "ud_verbs", "ud_mlx5"})
# NIXL 1.5 requires CUDA memory support on hosts with NVIDIA GPUs, even when
# Maru registers only DRAM. cuda_copy is a local memory-type helper, not a
# replacement for a remote key-checking RC/DC data transport.
DEFAULT_UCX_TRANSPORTS = "rc,dc,cuda_copy"
# UCX's registration cache keeps a registration alive after NIXL releases it.
# Only spellings known to turn it off pass; anything else must be set to n.
RCACHE_OFF = ("n", "no", "0")


class UncheckedTransportError(RuntimeError):
    """UCX may use a transport that does not check remote keys."""


def require_checked_transport(
    environ: MutableMapping[str, str], allow_unchecked: bool = False
) -> str:
    """Limit UCX to transports whose NIC checks remote keys, without a
    registration cache.

    Sets ``UCX_TLS`` to :data:`DEFAULT_UCX_TRANSPORTS` and
    ``UCX_RCACHE_ENABLE`` to ``n`` when they are unset, so it must run before
    the NIXL agent is created.

    Args:
        environ: Process environment (``os.environ``).
        allow_unchecked: Accept any ``UCX_TLS`` (tests on one host).

    Returns:
        The ``UCX_TLS`` value in effect.

    Raises:
        UncheckedTransportError: if ``UCX_TLS`` names another transport or
            ``UCX_RCACHE_ENABLE`` turns the registration cache on.
    """
    rcache = environ.get("UCX_RCACHE_ENABLE", "").strip().lower()
    if not rcache:
        environ["UCX_RCACHE_ENABLE"] = "n"
    elif rcache not in RCACHE_OFF and not allow_unchecked:
        raise UncheckedTransportError(
            f"UCX_RCACHE_ENABLE={rcache!r} keeps a released region registered "
            "with the NIC, so a late write still lands; unset it or set it to n"
        )
    value = environ.get("UCX_TLS", "").strip()
    if not value:
        environ["UCX_TLS"] = DEFAULT_UCX_TRANSPORTS
        return DEFAULT_UCX_TRANSPORTS
    items = [item.strip() for item in value.split(",") if item.strip()]
    unchecked = [item for item in items if not _checked_transport(item)]
    if unchecked and not allow_unchecked:
        raise UncheckedTransportError(
            f"UCX_TLS={value!r} allows {', '.join(unchecked)}, where a released "
            "region can still be written; use InfiniBand/RoCE transports "
            f"(e.g. {DEFAULT_UCX_TRANSPORTS}) or --allow-unchecked-transport"
        )
    if "cuda_copy" in items and not allow_unchecked:
        rdma = CHECKED_UCX_TRANSPORTS - {"self"}
        if not any(item in rdma for item in items):
            raise UncheckedTransportError(
                f"UCX_TLS={value!r} enables local cuda_copy without an RC/DC "
                "remote data transport; include rc or dc"
            )
    return value


def _checked_transport(item: str) -> bool:
    """Whether one ``UCX_TLS`` entry keeps RDMA on key-checking transports."""
    if item == "cuda_copy":
        return True
    name, _, flag = item.partition(":")
    if name in CHECKED_UCX_TRANSPORTS:
        return True
    return name in AUX_ONLY_UCX_TRANSPORTS and flag == "aux"

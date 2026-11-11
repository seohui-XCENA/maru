# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageService node config: JSON, checked before anything starts.

Unknown keys are errors so a typo never falls back to a default.
"""

import grp
import json
import math
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from maru_common.node_protocol import DEFAULT_NODE_SOCKET, Budgets, TransferDefaults
from maru_common.storage_types import PoolKind, Visibility


class NodeConfigError(ValueError):
    """The node config breaks a rule; the message names the field."""


STORES = ("dram", "cxl", "ssd", "remote")
# Old names read as the current store (the session backends called DRAM "cpu")
STORE_ALIASES = {"cpu": "dram"}
# node_id goes into pool ids such as dram:{node_id}:{node_epoch}
_NODE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
MEMORY_STORES = ("dram", "cxl")
DEMOTIONS = ("back", "through")
ROLES = ("gpu", "memory")

DEFAULT_RM = "127.0.0.1:9850"
DEFAULT_REGION_SIZE = 1 << 30
DEFAULT_HIGH_WATERMARK = 0.9
DEFAULT_LOW_WATERMARK = 0.8
DEFAULT_RDMA_TIMEOUT_S = 30.0  # the same as maru_rdma_timeout_s today

# Budget defaults (all provisional until measured)
CREDIT_MAX_PAGES = 16
# A worker's credit cap is at least this much: one vLLM step of an 8B model
# stores about 512 MiB, which small pages must not split into credit waits
CREDIT_MAX_MIN_BYTES = 512 << 20
# Node credit: the credit tiers' share that may be out being written at once.
# Above one step (2 GiB for an 8B model at 16384 tokens) on a 16 GiB DRAM;
# a step asks only for what it stores, so the rest stays cache
CREDIT_TOTAL_FRACTION = 0.25
CREDIT_REFILL_LOW_FRACTION = 0.25
TRANSFER_RESERVE_FRACTION = 0.05
DEFAULT_FETCH_REQUEST_MAX = 1 << 30
DEFAULT_FETCH_MAX_INFLIGHT = 4 << 30
DEFAULT_FETCH_MAX_BATCH = 256 << 20

_TOP_KEYS = {
    "node_id",
    "role",
    "socket",
    "socket_group",
    "server",
    "page_size",
    "local_pin",
    "tiers",
    "budgets",
    "remote_server",
}
# Keys a tier may carry, by store
_TIER_KEYS = {
    "dram": {
        "store",
        "size",
        "demotion",
        "high_watermark",
        "low_watermark",
        "hugepage",
    },
    "cxl": {
        "store",
        "size",
        "demotion",
        "high_watermark",
        "low_watermark",
        "region_size",
        "rm",
        "visibility",
        "credit_fallback",
    },
    "ssd": {"store", "size", "demotion", "high_watermark", "low_watermark", "path"},
    "remote": {"store", "url", "rdma_device", "timeout_s"},
}
_BUDGET_KEYS = {
    "credit_max_bytes",
    "credit_total_max_bytes",
    "credit_refill_low",
    "credit_refill_high",
    "commit_pending_max_bytes",
    "transfer_reserve_bytes",
    "fetch_request_max_bytes",
    "fetch_max_inflight_bytes",
    "fetch_max_batch_bytes",
}
_REMOTE_SERVER_KEYS = {"listen", "key_file"}

_SIZE_RE = re.compile(r"^\s*(\d+)\s*([KMGT]?)(?:i?B)?\s*$", re.IGNORECASE)
_UNITS = {"": 1, "K": 1 << 10, "M": 1 << 20, "G": 1 << 30, "T": 1 << 40}


def parse_size(value: Any, what: str) -> int:
    """Bytes from an int or a string like "32M" or "1GiB" (binary units)."""
    if isinstance(value, bool):
        raise NodeConfigError(f"{what}: expected a size, got {value!r}")
    if isinstance(value, int):
        if value < 0:
            raise NodeConfigError(f"{what}: must not be negative, got {value}")
        return value
    if isinstance(value, str):
        m = _SIZE_RE.match(value)
        if m:
            return int(m.group(1)) * _UNITS[m.group(2).upper()]
    raise NodeConfigError(f'{what}: not a size: {value!r} (e.g. 4096, "32M", "1G")')


@dataclass(frozen=True)
class TierConfig:
    store: str
    size: int | None = None
    demotion: str | None = None
    high_watermark: float | None = None
    low_watermark: float | None = None
    hugepage: bool | None = None
    region_size: int | None = None
    rm: str | None = None
    visibility: Visibility | None = None
    path: str | None = None
    url: tuple[str, ...] = ()
    rdma_device: str | None = None
    timeout_s: float | None = None
    # A tier below the top that workers map gives the credit the top lacks
    credit_fallback: bool = False

    @property
    def is_memory(self) -> bool:
        return self.store in MEMORY_STORES


@dataclass(frozen=True)
class TierTraits:
    """What the StorageService does with a tier's pools, instead of asking
    which medium they are."""

    kind: PoolKind
    credit: bool  # workers get credit from it (the top tier)
    evicts: bool  # pressure makes room: by demotion if it demotes, else by retiring
    demotes_to: PoolKind | None = None  # write-back target (demotion "back")
    # Workers get the credit the tiers above lack from it
    credit_fallback: bool = False


@dataclass(frozen=True)
class RemoteServerConfig:
    listen: str
    key_file: str


@dataclass(frozen=True)
class NodeConfig:
    node_id: str
    role: str
    socket: str
    server: str
    page_size: int
    local_pin: bool
    tiers: tuple[TierConfig, ...]
    budgets: Budgets
    remote_server: RemoteServerConfig | None = None
    # Group the socket is given to (mode 0660); None keeps the service's group
    socket_group: str | None = None
    source: str = field(default="", compare=False)

    @property
    def top(self) -> TierConfig:
        return self.tiers[0]

    def tier(self, store: str) -> TierConfig | None:
        return next((t for t in self.tiers if t.store == store), None)

    def traits(self) -> dict[PoolKind, TierTraits]:
        """Traits of each memory tier, by the kind of its pools. The top tier
        gives credit, and a tier with credit_fallback the credit the tiers
        above lack. A tier with demotion "back" over a memory tier evicts by
        moving its pages there; a tier that gets new pages (credit or
        demotion) and demotes nowhere evicts by dropping."""
        out: dict[PoolKind, TierTraits] = {}
        receives = True  # the top tier gets the writes
        for i, t in enumerate(self.tiers):
            if not t.is_memory:
                receives = False
                continue
            below = self.tiers[i + 1] if i + 1 < len(self.tiers) else None
            demotes = t.demotion == "back" and below is not None and below.is_memory
            fallback = i > 0 and t.credit_fallback
            out[PoolKind(t.store)] = TierTraits(
                kind=PoolKind(t.store),
                credit=i == 0,
                evicts=receives or fallback,
                demotes_to=PoolKind(below.store) if demotes and below else None,
                credit_fallback=fallback,
            )
            receives = demotes
        return out

    def transfer_defaults(self) -> TransferDefaults:
        """Workers park loads when a tier can only be fetched from; a store
        ends in a tier workers map, so it needs no write-behind."""
        fetched = any(t.store not in MEMORY_STORES for t in self.tiers)
        return TransferDefaults(async_load=fetched, async_store=False)


def _require_keys(obj: Any, allowed: set[str], what: str) -> dict:
    if not isinstance(obj, dict):
        raise NodeConfigError(f"{what}: expected an object, got {type(obj).__name__}")
    unknown = sorted(set(obj) - allowed)
    if unknown:
        raise NodeConfigError(
            f"{what}: unknown keys {unknown} (allowed: {sorted(allowed)})"
        )
    return obj


def _str(obj: dict, key: str, what: str, default: str | None = None) -> str:
    value = obj.get(key, default)
    if value is None:
        raise NodeConfigError(f"{what}.{key}: required")
    if not isinstance(value, str) or not value.strip():
        raise NodeConfigError(
            f"{what}.{key}: expected a non-empty string, got {value!r}"
        )
    return value


def _bool(obj: dict, key: str, what: str, default: bool) -> bool:
    value = obj.get(key, default)
    if not isinstance(value, bool):
        raise NodeConfigError(f"{what}.{key}: expected true or false, got {value!r}")
    return value


def _number(obj: dict, key: str, what: str, default: float) -> float:
    value = obj.get(key, default)
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(value)
    ):
        raise NodeConfigError(f"{what}.{key}: expected a finite number, got {value!r}")
    return float(value)


def _parse_tier(raw: Any, index: int, page_size: int, last: bool) -> TierConfig:
    what = f"tiers[{index}]"
    if not isinstance(raw, dict):
        raise NodeConfigError(f"{what}: expected an object, got {type(raw).__name__}")
    store = raw.get("store")
    if isinstance(store, str):
        store = STORE_ALIASES.get(store, store)
    if store not in STORES:
        raise NodeConfigError(f"{what}.store: one of {list(STORES)}, got {store!r}")
    what = f"{what} ({store})"
    _require_keys(raw, _TIER_KEYS[store], what)

    if store == "remote":
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
            raise NodeConfigError(
                f"{what}.rdma_device: expected a string, got {device!r}"
            )
        return TierConfig(
            store=store,
            url=tuple(urls),
            rdma_device=device or None,
            timeout_s=float(timeout),
        )

    size = raw.get("size")
    if size is None:
        if store != "cxl":
            raise NodeConfigError(f"{what}.size: required")
    else:
        size = parse_size(size, f"{what}.size")
        if size < page_size:
            raise NodeConfigError(
                f"{what}.size: {size} is smaller than page_size {page_size}"
            )

    if last:
        if "demotion" in raw:
            raise NodeConfigError(
                f"{what}.demotion: the last tier has nothing below it"
            )
        demotion = None
    else:
        demotion = raw.get("demotion", "back")
        if demotion not in DEMOTIONS:
            raise NodeConfigError(
                f"{what}.demotion: one of {list(DEMOTIONS)}, got {demotion!r}"
            )

    high = _number(raw, "high_watermark", what, DEFAULT_HIGH_WATERMARK)
    low = _number(raw, "low_watermark", what, DEFAULT_LOW_WATERMARK)
    if not 0 < low < high <= 1:
        raise NodeConfigError(
            f"{what}: need 0 < low_watermark < high_watermark <= 1, got {low} and {high}"
        )

    tier: dict[str, Any] = {
        "store": store,
        "size": size,
        "demotion": demotion,
        "high_watermark": high,
        "low_watermark": low,
    }
    if store == "dram":
        tier["hugepage"] = _bool(raw, "hugepage", what, True)
    elif store == "cxl":
        region = parse_size(
            raw.get("region_size", DEFAULT_REGION_SIZE), f"{what}.region_size"
        )
        if region < page_size or region % page_size:
            raise NodeConfigError(
                f"{what}.region_size: {region} must be a multiple of page_size {page_size}"
            )
        if size is not None and size < region:
            raise NodeConfigError(
                f"{what}.size: {size} is smaller than region_size {region}"
            )
        tier["region_size"] = region
        tier["rm"] = _str(raw, "rm", what, DEFAULT_RM)
        if index == 0 and "credit_fallback" in raw:
            raise NodeConfigError(
                f"{what}.credit_fallback: only a tier below the top gives fallback credit"
            )
        tier["credit_fallback"] = index > 0 and _bool(
            raw, "credit_fallback", what, True
        )
        visibility = raw.get("visibility", Visibility.COHERENT.value)
        try:
            tier["visibility"] = Visibility(visibility)
        except ValueError:
            raise NodeConfigError(
                f"{what}.visibility: one of {[v.value for v in Visibility]}, got {visibility!r}"
            ) from None
    elif store == "ssd":
        path = _str(raw, "path", what)
        if not os.path.isabs(path):
            raise NodeConfigError(
                f"{what}.path: must be an absolute path, got {path!r}"
            )
        # The path is part of the pool id, so /mnt/x and /mnt/x/ must be one pool
        tier["path"] = os.path.normpath(path)
    return TierConfig(**tier)


def _parse_budgets(raw: Any, page_size: int, tiers: tuple[TierConfig, ...]) -> Budgets:
    raw = _require_keys(raw if raw is not None else {}, _BUDGET_KEYS, "budgets")
    top = tiers[0]

    def size_or(key: str, default: int | None, why: str = "") -> int:
        if key in raw:
            return parse_size(raw[key], f"budgets.{key}")
        if default is None:
            raise NodeConfigError(f"budgets.{key}: required {why}".rstrip())
        return default

    def share(tier: TierConfig, fraction: float) -> int | None:
        return None if tier.size is None else int(tier.size * fraction)

    no_size = f"because tiers[0] ({top.store}) has no size"
    credit_tiers = [t for i, t in enumerate(tiers) if i == 0 or t.credit_fallback]
    sized = [t for t in credit_tiers if t.size is not None]
    credit_total = size_or(
        "credit_total_max_bytes",
        None
        if top.size is None
        else int(sum(t.size or 0 for t in sized) * CREDIT_TOTAL_FRACTION),
        no_size,
    )
    # A worker may hold up to the node total: it asks for each step's stores
    # when the step starts. What it keeps at hand between steps is 16 pages
    # or 512 MiB, whichever is more, in whole pages and within its cap
    credit_max = size_or(
        "credit_max_bytes", max(page_size, credit_total // page_size * page_size)
    )
    refill_high = size_or(
        "credit_refill_high",
        max(
            page_size,
            min(max(CREDIT_MAX_PAGES * page_size, CREDIT_MAX_MIN_BYTES), credit_max)
            // page_size
            * page_size,
        ),
    )
    refill_low = size_or(
        "credit_refill_low", int(refill_high * CREDIT_REFILL_LOW_FRACTION)
    )
    # Writes waiting for their commit come from credit: as much may wait
    commit_pending = size_or("commit_pending_max_bytes", credit_total)

    memory_tiers = [t for t in tiers if t.is_memory]
    reserve_raw = raw.get("transfer_reserve_bytes")
    reserve: dict[str, int] = {}
    for t in memory_tiers:
        if isinstance(reserve_raw, dict):
            unknown = sorted(set(reserve_raw) - {m.store for m in memory_tiers})
            if unknown:
                raise NodeConfigError(
                    f"budgets.transfer_reserve_bytes: no memory tier named {unknown}"
                )
            value = reserve_raw.get(t.store)
            if value is not None:
                reserve[t.store] = parse_size(
                    value, f"budgets.transfer_reserve_bytes.{t.store}"
                )
                continue
        elif reserve_raw is not None:
            reserve[t.store] = parse_size(reserve_raw, "budgets.transfer_reserve_bytes")
            continue
        default = share(t, TRANSFER_RESERVE_FRACTION)
        if default is None:
            raise NodeConfigError(
                f"budgets.transfer_reserve_bytes: required for the {t.store} tier because it has no size"
            )
        reserve[t.store] = default

    fetch_request = size_or("fetch_request_max_bytes", DEFAULT_FETCH_REQUEST_MAX)
    fetch_inflight = size_or("fetch_max_inflight_bytes", DEFAULT_FETCH_MAX_INFLIGHT)
    fetch_batch = size_or("fetch_max_batch_bytes", DEFAULT_FETCH_MAX_BATCH)

    if credit_max < page_size:
        raise NodeConfigError(
            f"budgets.credit_max_bytes: {credit_max} is smaller than page_size {page_size}"
        )
    if not refill_low <= refill_high <= credit_max <= credit_total:
        raise NodeConfigError(
            "budgets: need credit_refill_low <= credit_refill_high <= credit_max_bytes "
            f"<= credit_total_max_bytes, got {refill_low}, {refill_high}, {credit_max}, "
            f"{credit_total} (values not set in budgets are defaults; a top tier this "
            "small cannot give one page of credit)"
        )
    if not 0 < fetch_batch <= fetch_request <= fetch_inflight:
        raise NodeConfigError(
            "budgets: need 0 < fetch_max_batch_bytes <= fetch_request_max_bytes "
            f"<= fetch_max_inflight_bytes, got {fetch_batch}, {fetch_request}, {fetch_inflight}"
        )
    if top.size is not None:
        # Credit comes from the top tier and the fallback tiers, each less
        # its transfer reserve
        capacity = sum(t.size or 0 for t in sized)
        used = (
            credit_total + commit_pending + sum(reserve.get(t.store, 0) for t in sized)
        )
        if used > capacity:
            names = " + ".join(t.store for t in sized)
            raise NodeConfigError(
                f"budgets: credit_total_max_bytes + commit_pending_max_bytes + "
                f"transfer_reserve_bytes ({used}) exceed the tiers credit comes "
                f"from ({names}: {capacity})"
            )
    return Budgets(
        credit_max_bytes=credit_max,
        credit_total_max_bytes=credit_total,
        credit_refill_low=refill_low,
        credit_refill_high=refill_high,
        commit_pending_max_bytes=commit_pending,
        transfer_reserve_bytes=reserve,
        fetch_request_max_bytes=fetch_request,
        fetch_max_inflight_bytes=fetch_inflight,
        fetch_max_batch_bytes=fetch_batch,
    )


def parse_node_config(raw: Any, source: str = "") -> NodeConfig:
    """Check a decoded node config and fill its defaults."""
    raw = _require_keys(raw, _TOP_KEYS, "config")
    node_id = _str(raw, "node_id", "config")
    if not _NODE_ID_RE.match(node_id):
        raise NodeConfigError(
            f"config.node_id: letters, digits, '.', '_' and '-' only, got {node_id!r}"
        )
    server = _str(raw, "server", "config")
    socket_path = _str(raw, "socket", "config", DEFAULT_NODE_SOCKET)
    socket_group = None
    if raw.get("socket_group") is not None:
        socket_group = _str(raw, "socket_group", "config")
        try:
            grp.getgrnam(socket_group)
        except KeyError:
            raise NodeConfigError(
                f"config.socket_group: no group named {socket_group!r}"
            ) from None
    role = raw.get("role", "gpu")
    if role not in ROLES:
        raise NodeConfigError(f"config.role: one of {list(ROLES)}, got {role!r}")
    if "page_size" not in raw:
        raise NodeConfigError("config.page_size: required")
    page_size = parse_size(raw["page_size"], "config.page_size")
    if page_size <= 0:
        raise NodeConfigError("config.page_size: must be positive")
    local_pin = _bool(raw, "local_pin", "config", False)
    if local_pin:
        raise NodeConfigError(
            "config.local_pin: the local pin path is not supported yet"
        )

    raw_tiers = raw.get("tiers")
    if not isinstance(raw_tiers, list) or not raw_tiers:
        raise NodeConfigError("config.tiers: required, a non-empty list")
    tiers = tuple(
        _parse_tier(t, i, page_size, last=(i == len(raw_tiers) - 1))
        for i, t in enumerate(raw_tiers)
    )
    stores = [t.store for t in tiers]
    duplicated = sorted({s for s in stores if stores.count(s) > 1})
    if duplicated:
        raise NodeConfigError(f"config.tiers: a store appears twice: {duplicated}")
    if not tiers[0].is_memory:
        raise NodeConfigError(
            f"config.tiers[0]: the first tier must be dram or cxl (workers copy to it from the GPU), got {tiers[0].store}"
        )
    if "remote" in stores:
        if role == "memory":
            raise NodeConfigError(
                "config.tiers: a memory node cannot have a remote tier"
            )
        if stores[-1] != "remote":
            raise NodeConfigError("config.tiers: remote must be the last tier")

    remote_server = None
    if raw.get("remote_server") is not None:
        rs = _require_keys(
            raw["remote_server"], _REMOTE_SERVER_KEYS, "config.remote_server"
        )
        remote_server = RemoteServerConfig(
            listen=_str(rs, "listen", "config.remote_server"),
            key_file=os.path.expanduser(_str(rs, "key_file", "config.remote_server")),
        )
    if role == "memory" and remote_server is None:
        raise NodeConfigError(
            "config.remote_server: required on a memory node (other nodes reach it there)"
        )

    budgets = _parse_budgets(raw.get("budgets"), page_size, tiers)
    return NodeConfig(
        node_id=node_id,
        role=role,
        socket=socket_path,
        socket_group=socket_group,
        server=server,
        page_size=page_size,
        local_pin=local_pin,
        tiers=tiers,
        budgets=budgets,
        remote_server=remote_server,
        source=source,
    )


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict:
    keys = [k for k, _ in pairs]
    duplicated = sorted({k for k in keys if keys.count(k) > 1})
    if duplicated:
        raise NodeConfigError(f"a key appears twice in one object: {duplicated}")
    return dict(pairs)


def _reject_constant(name: str) -> Any:
    raise NodeConfigError(f"{name} is not a valid number here")


def load_node_config(path: str | Path) -> NodeConfig:
    """Read and check the node config file."""
    path = Path(path)
    try:
        text = path.read_bytes().decode("utf-8")
        raw = json.loads(
            text,
            object_pairs_hook=_no_duplicate_keys,
            parse_constant=_reject_constant,
        )
    except OSError as e:
        raise NodeConfigError(f"{path}: cannot read: {e}") from e
    except UnicodeDecodeError as e:
        raise NodeConfigError(f"{path}: not UTF-8 text: {e}") from e
    except json.JSONDecodeError as e:
        raise NodeConfigError(f"{path}: not valid JSON: {e}") from e
    except NodeConfigError as e:
        raise NodeConfigError(f"{path}: {e}") from None
    try:
        return parse_node_config(raw, source=str(path))
    except NodeConfigError as e:
        raise NodeConfigError(f"{path}: {e}") from None

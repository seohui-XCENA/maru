# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Node and worker sessions, node restarts (spec 3.1, 3.2, 7.2, 7.3), and
what NODE_HEARTBEAT replies carry (access info, block requests).

Not thread safe: callers hold the service lock.
"""

import heapq
import itertools
import logging
import re
import time
import uuid
from collections import OrderedDict, deque
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field

from maru_common.protocol import (
    NodeHeartbeatRequest,
    NodeHeartbeatResponse,
    NodeOpenRequest,
    NodeOpenResponse,
    NodeRecoverListResponse,
    RecoverItem,
    WorkerOpenRequest,
    WorkerOpenResponse,
)
from maru_common.storage_types import (
    EPOCH_SEQ_MAX,
    MAX_BATCH,
    MS_PROTOCOL,
    AccessInfo,
    BlockRequest,
    CloseMode,
    ItemOutcome,
    ItemResult,
    KeptCause,
    Key,
    KeyLocation,
    Location,
    MissReason,
    PoolInfo,
    PoolKind,
    PoolUsage,
    SessionRole,
    Status,
    WorkerRef,
)

from .catalog import MAPPABLE_KINDS, Catalog, Entry, PoolRecord, PoolState, Requester
from .op_ledger import OpLedger, OriginKey
from .pin_manager import Pin, PinManager

logger = logging.getLogger(__name__)

NODE_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
# ended node_epochs kept per node, so a late old process cannot come back
RETIRED_EPOCHS_KEPT = 4096
# access info locations kept per node, and sent per heartbeat
ACCESS_MAX_PER_NODE = 1 << 18
ACCESS_PER_HEARTBEAT = 1 << 14
# block requests: items per block, blocks per heartbeat reply
BLOCK_ITEMS = 256
BLOCKS_PER_HEARTBEAT = 4
# pins released inline by a request; the rest by the maintenance thread
PINS_INLINE = MAX_BATCH


def _on_cxl(pin: Pin) -> bool:
    return pin.location.kind == PoolKind.CXL


def node_origin(node_epoch: str) -> OriginKey:
    return ("node", node_epoch)


def worker_origin(incarnation: str) -> OriginKey:
    return ("worker", incarnation)


class OpenError(Exception):
    """NODE_OPEN or WORKER_OPEN refused with ``status``."""

    def __init__(self, status: Status, message: str):
        super().__init__(message)
        self.status = status


@dataclass(eq=False)
class NodeSession:
    token: str
    node_id: str
    node_epoch: str
    epoch_seq: int
    host_boot_id: str
    rm_domains: frozenset[str]
    deadline: float
    capabilities: tuple[str, ...] = ()
    closed_at: float | None = None  # NODE_CLOSE restart
    ended: bool = False  # renewed, replaced, left or given up

    def valid(self, now: float) -> bool:
        return not self.ended and self.closed_at is None and now < self.deadline

    @property
    def creator(self) -> tuple[str, str]:
        return (self.node_id, self.node_epoch)


@dataclass(eq=False)
class _Listing:
    pool_id: str
    entries: list[Entry]
    # the last page, kept after the snapshot is released so a resend matches
    last: tuple[str, NodeRecoverListResponse] | None = None


@dataclass(eq=False)
class _Block:
    block_id: str
    items: tuple[KeyLocation, ...]
    reason: str


@dataclass(eq=False)
class NodeRecord:
    node_id: str
    epoch_seq: int = 0  # the last one given
    node_epoch: str | None = None  # of the latest run
    session: NodeSession | None = None  # of the latest run, while it lasts
    retired_epochs: deque[str] = field(
        default_factory=lambda: deque(maxlen=RETIRED_EPOCHS_KEPT)
    )
    gone: bool = False  # left or given up: no locations, no session
    adopted: list[str] = field(default_factory=list)
    listings: dict[str, _Listing] = field(default_factory=dict)  # by listing id
    listing_ids: Iterator[int] = field(default_factory=lambda: itertools.count(1))
    usage: dict[str, PoolUsage] = field(default_factory=dict)
    blocks: OrderedDict[str, _Block] = field(default_factory=OrderedDict)
    blocked: set[tuple[Key, Location]] = field(default_factory=set)


@dataclass(eq=False)
class WorkerSession:
    token: str
    incarnation: str
    role: SessionRole
    node_id: str
    node_epoch: str
    node_epoch_seq: int  # which pools the worker maps (worker role)
    engine_id: str
    dp_index: int
    tp_rank: int
    pid: int
    pid_start: int
    namespace: str
    deadline: float
    closed: bool = False
    # its node left or was given up; kept only for its CXL pins
    orphan: bool = False

    def valid(self, now: float) -> bool:
        return not self.closed and now < self.deadline

    def ref(self) -> WorkerRef:
        return WorkerRef(
            engine_id=self.engine_id,
            dp_index=self.dp_index,
            tp_rank=self.tp_rank,
            pid=self.pid,
            pid_start=self.pid_start,
            incarnation=self.incarnation,
            worker_session=self.token,
        )


class NodeRegistry:
    def __init__(
        self,
        catalog: Catalog,
        pins: PinManager,
        ledger: OpLedger,
        server_epoch: str,
        session_ttl_s: float,
        heartbeat_s: float,
        node_give_up_s: float,
        wall_clock=time.time,
    ):
        self.catalog = catalog
        self.pins = pins
        self.ledger = ledger
        self.server_epoch = server_epoch
        self.session_ttl_s = session_ttl_s
        self.heartbeat_s = heartbeat_s
        self.node_give_up_s = node_give_up_s
        self._wall = wall_clock
        self.nodes: dict[str, NodeRecord] = {}
        self._node_sessions: dict[str, NodeSession] = {}
        self._workers: dict[str, WorkerSession] = {}
        self._incarnations: dict[tuple[str, SessionRole], str] = {}
        self._workers_by_node: dict[str, set[str]] = {}
        # (deadline, token) of probe and orphan sessions, checked on expiry
        self._expiry: list[tuple[float, str]] = []
        self._access: dict[str, OrderedDict[tuple[str, str, int], list]] = {}
        # holders whose pins are still to release (but those ``keep`` accepts)
        self._pin_releases: deque[tuple[str, Callable[[Pin], bool] | None]] = deque()
        self.policy_version = 0

    # ================================================================ lookup

    def node_session(self, token: str, now: float) -> NodeSession | None:
        session = self._node_sessions.get(token)
        return session if session is not None and session.valid(now) else None

    def worker_session(self, token: str, now: float) -> WorkerSession | None:
        session = self._workers.get(token)
        return session if session is not None and session.valid(now) else None

    def known_worker(self, token: str) -> WorkerSession | None:
        """Not closed; may be expired."""
        session = self._workers.get(token)
        return session if session is not None and not session.closed else None

    def node_online(self, node_id: str, now: float) -> bool:
        node = self.nodes.get(node_id)
        return bool(node and node.session and node.session.valid(now))

    def stale_run(self, worker: WorkerSession) -> bool:
        """A worker attached to a node run that has been replaced."""
        if worker.role != SessionRole.WORKER:
            return False
        node = self.nodes.get(worker.node_id)
        return node is None or node.gone or node.node_epoch != worker.node_epoch

    def requester(self, worker: WorkerSession, now: float) -> Requester:
        return Requester(
            node_id=worker.node_id,
            can_map=lambda pool: self.can_map(worker, pool),
            online=lambda node_id: self.node_online(node_id, now),
        )

    def can_map(self, worker: WorkerSession, pool: PoolRecord) -> bool:
        """Spec 3.3: the worker maps its node's DRAM of the run it attached
        to and that run's CXL regions; a probe answers for its node."""
        if pool.node_id != worker.node_id:
            return self._maps_shared_cxl(worker, pool)
        if pool.kind not in MAPPABLE_KINDS:
            return False
        if worker.role == SessionRole.PROBE:
            return True
        if pool.kind == PoolKind.DRAM:
            return pool.since_epoch_seq == worker.node_epoch_seq
        return pool.since_epoch_seq <= worker.node_epoch_seq

    @staticmethod
    def _maps_shared_cxl(worker: WorkerSession, pool: PoolRecord) -> bool:
        """S8 hook: another host's CXL region mapped directly."""
        return False

    # ============================================================ node open

    def node_open(self, req: NodeOpenRequest, now: float) -> NodeOpenResponse:
        """Raises OpenError when refused."""
        self._check_node_open(req)
        node = self.nodes.get(req.node_id)
        if node is not None and req.node_epoch in node.retired_epochs:
            raise OpenError(
                Status.REJECTED,
                f"node_epoch {req.node_epoch} of {req.node_id} has ended; "
                "a new StorageService process opens with a new node_epoch",
            )
        if (
            node is not None
            and node.node_epoch == req.node_epoch
            and node.session is not None
        ):
            return self._reopen_same_epoch(node, req, now)
        return self._open_new_epoch(node, req, now)

    def _check_node_open(self, req: NodeOpenRequest) -> None:
        if req.protocol != MS_PROTOCOL:
            raise OpenError(Status.REJECTED, f"unknown protocol {req.protocol!r}")
        if req.resume is not None:
            raise OpenError(
                Status.DISABLED,
                "NODE_OPEN with resume (reopening after a MaruServer restart) "
                "is stage S4 and not implemented",
            )
        if not NODE_ID_RE.match(req.node_id):
            raise OpenError(Status.REJECTED, f"bad node_id {req.node_id!r}")
        if not req.node_epoch or not req.host_boot_id:
            raise OpenError(Status.REJECTED, "node_epoch and host_boot_id are needed")
        if len(req.pools) > MAX_BATCH or len(req.rm_domains) > MAX_BATCH:
            raise OpenError(Status.REJECTED, f"more than {MAX_BATCH} pools or domains")
        seen = set()
        for info in req.pools:
            error = self._pool_error(req, info)
            if error is None and info.pool_id in seen:
                error = "listed twice"
            if error is not None:
                raise OpenError(Status.REJECTED, f"pool {info.pool_id}: {error}")
            seen.add(info.pool_id)

    @staticmethod
    def _pool_error(req: NodeOpenRequest, info: PoolInfo) -> str | None:
        """Spec 3.1: only this run's DRAM pool and its SSD pools; CXL regions
        come from MaruServer."""
        if info.node_id != req.node_id:
            return f"belongs to node {info.node_id}"
        if info.pool_generation < 1:
            return "pool_generation starts at 1"
        if info.page_size <= 0 or info.capacity < info.page_size:
            return "needs page_size > 0 and capacity >= page_size"
        if info.kind == PoolKind.DRAM:
            expected = f"dram:{req.node_id}:{req.node_epoch}"
        elif info.kind == PoolKind.SSD:
            if not info.path:
                return "an SSD pool needs its path"
            expected = f"ssd:{req.node_id}:{info.path}"
        else:
            return "CXL regions are granted by MaruServer (REGION_REQUEST, S5)"
        if info.pool_id != expected:
            return f"pool_id must be {expected!r}"
        return None

    def _reopen_same_epoch(
        self, node: NodeRecord, req: NodeOpenRequest, now: float
    ) -> NodeOpenResponse:
        """Same process again (expired session, lost reply): the run keeps
        its epoch_seq, pools, pins, op records and blocks."""
        old = node.session
        assert old is not None
        if old.closed_at is not None or old.ended:
            raise OpenError(
                Status.REJECTED,
                f"node_epoch {req.node_epoch} closed its session; a new process "
                "opens with a new node_epoch",
            )
        if old.host_boot_id != req.host_boot_id:
            raise OpenError(Status.REJECTED, "host_boot_id changed within a run")
        to_register = []
        for info in req.pools:
            pool = self.catalog.pool(info.pool_id)
            if pool is None:
                to_register.append(info)
            elif pool.state != PoolState.ACTIVE or pool.info != info:
                raise OpenError(
                    Status.REJECTED,
                    f"pool {info.pool_id} differs from the one this run registered",
                )
        for info in to_register:
            self.catalog.register_pool(info, node.epoch_seq)
        session = self._new_session(node, req, now)
        self.pins.move_holder(old.token, session.token)
        old.ended = True
        self._node_sessions.pop(old.token, None)
        logger.info(
            "node %s reopened its session (node_epoch %s, epoch_seq %d)",
            node.node_id,
            req.node_epoch,
            node.epoch_seq,
        )
        return self._open_reply(node, session, previous=None)

    def _open_new_epoch(
        self, node: NodeRecord | None, req: NodeOpenRequest, now: float
    ) -> NodeOpenResponse:
        if node is None:
            node = NodeRecord(node_id=req.node_id)
        if node.epoch_seq >= EPOCH_SEQ_MAX:
            raise OpenError(Status.REJECTED, f"epoch_seq of {req.node_id} exhausted")
        listed = {info.pool_id: info for info in req.pools}
        old = node.session
        previous = node.node_epoch
        adopt, remove = [], []
        for pool in self.catalog.pools_of(req.node_id):
            if pool.state == PoolState.REMOVING:
                continue  # already going; a listed one is registered anew
            if self._adoptable(pool, old, req, listed):
                adopt.append(pool)
            else:
                remove.append(pool)
        self.nodes[node.node_id] = node
        if old is not None and not old.ended:
            self._end_run(node, old)
        for pool in remove:
            self.catalog.remove_pool(pool)
        node.epoch_seq += 1
        node.node_epoch = req.node_epoch
        node.gone = False
        node.adopted = sorted(pool.pool_id for pool in adopt)
        adopted_ids = set(node.adopted)
        self._keep_blocks(node, adopted_ids)
        for info in req.pools:
            if info.pool_id not in adopted_ids:
                self.catalog.register_pool(info, node.epoch_seq)
        session = self._new_session(node, req, now)
        logger.info(
            "node %s opened node_epoch %s (epoch_seq %d): %d pools adopted, %d removed",
            node.node_id,
            req.node_epoch,
            node.epoch_seq,
            len(adopt),
            len(remove),
        )
        return self._open_reply(node, session, previous=previous)

    @staticmethod
    def _adoptable(
        pool: PoolRecord,
        old: NodeSession | None,
        req: NodeOpenRequest,
        listed: dict[str, PoolInfo],
    ) -> bool:
        """Spec 7.2: CXL if no reboot and the same rm_domain is listed; SSD
        if listed unchanged (a changed one starts empty); never DRAM."""
        if pool.kind == PoolKind.DRAM:
            return False
        if pool.kind == PoolKind.CXL:
            return (
                old is not None
                and old.host_boot_id == req.host_boot_id
                and pool.info.rm_domain in {d.rm_domain for d in req.rm_domains}
            )
        return listed.get(pool.pool_id) == pool.info

    def _new_session(
        self, node: NodeRecord, req: NodeOpenRequest, now: float
    ) -> NodeSession:
        session = NodeSession(
            token=uuid.uuid4().hex,
            node_id=req.node_id,
            node_epoch=req.node_epoch,
            epoch_seq=node.epoch_seq,
            host_boot_id=req.host_boot_id,
            rm_domains=frozenset(d.rm_domain for d in req.rm_domains),
            capabilities=tuple(req.capabilities),
            deadline=now + self.session_ttl_s,
        )
        node.session = session
        self._node_sessions[session.token] = session
        return session

    def _open_reply(
        self, node: NodeRecord, session: NodeSession, previous: str | None
    ) -> NodeOpenResponse:
        adopted = []
        for pool_id in node.adopted:
            pool = self.catalog.pool(pool_id)
            if pool is not None and pool.state == PoolState.ACTIVE:
                adopted.append(pool.info)
        old_workers = [
            worker.ref()
            for worker in self._node_workers(node.node_id)
            if worker.role == SessionRole.WORKER
            and worker.node_epoch != session.node_epoch
        ]
        return NodeOpenResponse(
            status=Status.OK,
            server_epoch=self.server_epoch,
            node_session=session.token,
            epoch_seq=session.epoch_seq,
            heartbeat_s=self.heartbeat_s,
            session_ttl_s=self.session_ttl_s,
            recovering=False,
            adopted=adopted,
            old_workers=old_workers,
            previous_node_epoch=previous if previous != session.node_epoch else None,
        )

    def _end_run(
        self, node: NodeRecord, session: NodeSession, inline: int = PINS_INLINE
    ):
        """The run behind a session ends: its pins, op records, blocks and
        recovery listings go, and its epoch may not open again."""
        session.ended = True
        self._node_sessions.pop(session.token, None)
        if node.session is session:
            node.session = None
        self._release_pins(session.token, inline)
        self.ledger.drop_origin(node_origin(session.node_epoch))
        node.retired_epochs.append(session.node_epoch)
        node.listings.clear()
        node.usage.clear()

    @staticmethod
    def _keep_blocks(node: NodeRecord, pool_ids: set[str]) -> None:
        """Deletes queued for pools the new run adopted carry over."""
        for block_id in list(node.blocks):
            block = node.blocks[block_id]
            items = tuple(i for i in block.items if i.location.pool_id in pool_ids)
            if items:
                block.items = items
            else:
                del node.blocks[block_id]
        node.blocked = {
            (i.key, i.location) for b in node.blocks.values() for i in b.items
        }

    def _release_pins(
        self, holder: str, inline: int, keep: Callable[[Pin], bool] | None = None
    ) -> None:
        if self.pins.release_holder(holder, inline, keep) >= inline:
            self._pin_releases.append((holder, keep))  # maybe more left

    def _node_workers(self, node_id: str) -> list[WorkerSession]:
        return [
            self._workers[t] for t in sorted(self._workers_by_node.get(node_id, ()))
        ]

    # ===================================================== node session ops

    def node_heartbeat(
        self, session: NodeSession, req: NodeHeartbeatRequest, now: float
    ) -> NodeHeartbeatResponse:
        session.deadline = now + self.session_ttl_s
        node = self.nodes[session.node_id]
        for usage in req.usage:
            pool = self.catalog.pool(usage.pool_id)
            if pool is not None and pool.node_id == node.node_id:
                node.usage[usage.pool_id] = usage
        return NodeHeartbeatResponse(
            status=Status.OK,
            access=self._take_access(node.node_id),
            blocks=self._pending_blocks(node),
            policy_version=self.policy_version,
        )

    def node_close(self, session: NodeSession, mode: CloseMode, now: float) -> None:
        """restart hides the DRAM locations; leave removes all of them."""
        if mode == CloseMode.RESTART:
            session.closed_at = now
            self._node_sessions.pop(session.token, None)
            for pool in self.catalog.pools_of(session.node_id):
                if pool.kind == PoolKind.DRAM:
                    self.catalog.hide_pool(pool)
            logger.info("node %s closed for a restart", session.node_id)
            return
        self._drop_node(self.nodes[session.node_id], "left", inline=PINS_INLINE)

    def _drop_node(self, node: NodeRecord, why: str, inline: int) -> None:
        """Left or given up: the run ends, its locations go, and so do its
        worker sessions (with their pins and op records)."""
        if node.session is not None and not node.session.ended:
            self._end_run(node, node.session, inline)
        elif node.node_epoch is not None and node.node_epoch not in node.retired_epochs:
            node.retired_epochs.append(node.node_epoch)
        node.session = None
        for pool in self.catalog.pools_of(node.node_id):
            self.catalog.remove_pool(pool, force=True)
        for worker in self._node_workers(node.node_id):
            self.ledger.drop_origin(worker_origin(worker.incarnation), node.node_id)
            if any(_on_cxl(p) for p in self.pins.held_by(worker.token)):
                # a live worker may still read mapped CXL: keep those pins
                # (never released by time) so the region is not returned
                worker.orphan = True
                self._release_pins(worker.token, 0, keep=_on_cxl)
                heapq.heappush(self._expiry, (worker.deadline, worker.token))
            else:
                self._release_pins(worker.token, 0)
                self._forget_worker(worker)
        node.gone = True
        node.adopted = []
        node.blocks.clear()
        node.blocked.clear()
        self._access.pop(node.node_id, None)
        logger.info("node %s %s: its locations are removed", node.node_id, why)

    def recover_list(
        self, session: NodeSession, pool_id: str, cursor: str | None
    ) -> NodeRecoverListResponse:
        """Cursors index one snapshot per pool, so a resent page is the same;
        the snapshot goes after its last page."""
        node = self.nodes[session.node_id]
        if pool_id not in node.adopted:
            return NodeRecoverListResponse(
                status=Status.REJECTED,
                error=f"{pool_id} is not a pool this run adopted",
            )
        listing_id = next(
            (k for k, v in node.listings.items() if v.pool_id == pool_id), ""
        )
        listing = node.listings.get(listing_id)
        if listing is not None and listing.last is not None:
            if cursor == listing.last[0]:
                return listing.last[1]
            if cursor is not None:
                return self._bad_cursor(cursor)
            del node.listings[listing_id]  # a fresh listing
            listing = None
        if cursor is None:
            if listing is None:
                listing_id = str(next(node.listing_ids))
                entries = list(self.catalog.entries_in(pool_id).values())
                listing = node.listings[listing_id] = _Listing(pool_id, entries)
            position = 0
        else:
            given_id, _, pos = cursor.partition(":")
            if (
                listing is None
                or given_id != listing_id
                or not pos.isdigit()
                or int(pos) > len(listing.entries)
            ):
                return self._bad_cursor(cursor)
            position = int(pos)
        current = self.catalog.entries_in(pool_id)
        end = min(position + MAX_BATCH, len(listing.entries))
        items = [
            RecoverItem(
                key=entry.key,
                location=entry.location,
                logical_bytes=entry.logical_bytes,
                pinned=bool(self.pins.count(entry.key, entry.location)),
            )
            for entry in listing.entries[position:end]
            if current.get(entry.location.allocation_id) is entry
        ]
        done = end >= len(listing.entries)
        response = NodeRecoverListResponse(
            status=Status.OK,
            items=items,
            next_cursor=None if done else f"{listing_id}:{end}",
        )
        if done:
            listing.entries = []
            listing.last = (cursor or "", response)
        return response

    @staticmethod
    def _bad_cursor(cursor: str) -> NodeRecoverListResponse:
        return NodeRecoverListResponse(
            status=Status.REJECTED, error=f"unknown cursor {cursor!r}"
        )

    # =========================================================== worker ops

    def worker_open(self, req: WorkerOpenRequest, now: float) -> WorkerOpenResponse:
        if req.protocol != MS_PROTOCOL:
            raise OpenError(Status.REJECTED, f"unknown protocol {req.protocol!r}")
        if not req.incarnation:
            raise OpenError(Status.REJECTED, "empty incarnation")
        node_epoch_seq = 0
        if req.role == SessionRole.WORKER:
            node = self.nodes.get(req.node_id)
            if node is None or node.gone or node.node_epoch != req.node_epoch:
                raise OpenError(
                    Status.EPOCH,
                    f"node_epoch {req.node_epoch} is not the current run of "
                    f"node {req.node_id}; attach to its StorageService again",
                )
            node_epoch_seq = node.epoch_seq
        previous = self._workers.get(
            self._incarnations.get((req.incarnation, req.role), "")
        )
        identity = (
            req.node_id,
            req.engine_id,
            req.dp_index,
            req.tp_rank,
            req.pid,
            req.pid_start,
            req.namespace,
        )
        if previous is not None and identity != (
            previous.node_id,
            previous.engine_id,
            previous.dp_index,
            previous.tp_rank,
            previous.pid,
            previous.pid_start,
            previous.namespace,
        ):
            raise OpenError(
                Status.REJECTED,
                f"incarnation {req.incarnation} is open with another identity",
            )
        session = WorkerSession(
            token=uuid.uuid4().hex,
            incarnation=req.incarnation,
            role=req.role,
            node_id=req.node_id,
            node_epoch=req.node_epoch,
            node_epoch_seq=node_epoch_seq,
            engine_id=req.engine_id,
            dp_index=req.dp_index,
            tp_rank=req.tp_rank,
            pid=req.pid,
            pid_start=req.pid_start,
            namespace=req.namespace,
            deadline=now + self.session_ttl_s,
        )
        if previous is not None:
            self.pins.move_holder(previous.token, session.token)
            self._forget_worker(previous)
        self._workers[session.token] = session
        self._incarnations[(session.incarnation, session.role)] = session.token
        self._workers_by_node.setdefault(session.node_id, set()).add(session.token)
        if session.role == SessionRole.PROBE:
            heapq.heappush(self._expiry, (session.deadline, session.token))
        return WorkerOpenResponse(
            status=Status.OK,
            server_epoch=self.server_epoch,
            worker_session=session.token,
            heartbeat_s=self.heartbeat_s,
            session_ttl_s=self.session_ttl_s,
            recovering=False,
        )

    def worker_heartbeat(self, session: WorkerSession, now: float) -> None:
        session.deadline = now + self.session_ttl_s

    def worker_close(self, session: WorkerSession) -> None:
        self._release_pins(session.token, PINS_INLINE)
        self._forget_worker(session)
        if session.role == SessionRole.WORKER:  # a probe has no op records
            self.ledger.drop_origin(worker_origin(session.incarnation), session.node_id)

    def _forget_worker(self, session: WorkerSession) -> None:
        session.closed = True
        self._workers.pop(session.token, None)
        key = (session.incarnation, session.role)
        if self._incarnations.get(key) == session.token:
            del self._incarnations[key]
        tokens = self._workers_by_node.get(session.node_id)
        if tokens is not None:
            tokens.discard(session.token)
            if not tokens:
                del self._workers_by_node[session.node_id]

    def worker_exit(
        self, session: NodeSession, workers: Iterable[WorkerRef]
    ) -> list[ItemOutcome]:
        """A worker of another node or another pid is REJECTED; an unknown
        one is OK. Op records go only for workers of this node."""
        outcomes = []
        for ref in workers:
            token = self._incarnations.get((ref.incarnation, SessionRole.WORKER))
            worker = self._workers.get(token) if token else None
            if worker is not None and (
                worker.node_id != session.node_id
                or (worker.pid, worker.pid_start) != (ref.pid, ref.pid_start)
            ):
                outcomes.append(ItemOutcome(ItemResult.REJECTED))
                continue
            if worker is not None:
                self.worker_close(worker)
            self.ledger.drop_origin(worker_origin(ref.incarnation), session.node_id)
            outcomes.append(ItemOutcome(ItemResult.OK))
        return outcomes

    # ========================================================== access info

    def record_hit(self, pool: PoolRecord, location: Location) -> None:
        """A reference-path HIT, for the owning node's LRU (spec 3.1)."""
        access = self._access.get(pool.node_id)
        if access is None:
            access = self._access[pool.node_id] = OrderedDict()
        target = (location.pool_id, location.allocation_id, location.generation)
        hit = access.get(target)
        if hit is not None:
            hit[0] += 1
            hit[1] = self._wall()
        elif len(access) < ACCESS_MAX_PER_NODE:
            access[target] = [1, self._wall()]

    def _take_access(self, node_id: str) -> list[AccessInfo]:
        access = self._access.get(node_id)
        taken: list[AccessInfo] = []
        while access and len(taken) < ACCESS_PER_HEARTBEAT:
            (pool_id, allocation_id, generation), (hits, last) = access.popitem(
                last=False
            )
            taken.append(AccessInfo(pool_id, allocation_id, generation, hits, last))
        return taken

    # ======================================================= block requests

    def delete(self, keys: Iterable[Key], reason: str = "delete") -> list[ItemOutcome]:
        """Spec 3.1: block requests to the owning nodes, which retire the
        locations themselves. OK when queued (or already queued); MISS when
        the key has no location (offline: none a running node can block)."""
        outcomes = []
        fresh: dict[str, list[KeyLocation]] = {}
        for key in keys:
            if self.catalog.is_preserved(key):
                outcomes.append(ItemOutcome(ItemResult.KEPT, cause=KeptCause.PINNED))
                continue
            queued = present = False
            for entry in self.catalog.locations(key):
                if entry.pool.state == PoolState.REMOVING:
                    continue
                present = True
                node = self.nodes.get(entry.pool.node_id)
                if (
                    entry.pool.state != PoolState.ACTIVE
                    or node is None
                    or not self._may_block(node)
                ):
                    continue
                queued = True
                target = (key, entry.location)
                if target not in node.blocked:
                    node.blocked.add(target)
                    fresh.setdefault(node.node_id, []).append(KeyLocation(*target))
            if queued:
                outcomes.append(ItemOutcome(ItemResult.OK))
            else:
                reason_ = MissReason.OFFLINE if present else MissReason.ABSENT
                outcomes.append(ItemOutcome(ItemResult.MISS, reason=reason_))
        for node_id, items in fresh.items():
            node = self.nodes[node_id]
            for start in range(0, len(items), BLOCK_ITEMS):
                block = _Block(
                    uuid.uuid4().hex, tuple(items[start : start + BLOCK_ITEMS]), reason
                )
                node.blocks[block.block_id] = block
        return outcomes

    @staticmethod
    def _may_block(node: NodeRecord) -> bool:
        """Its run goes on (maybe expired): a block would reach it."""
        session = node.session
        return session is not None and not session.ended and session.closed_at is None

    def _pending_blocks(self, node: NodeRecord) -> list[BlockRequest]:
        """Sent in turn (each goes to the back) until their items are gone."""
        out: list[BlockRequest] = []
        for block_id in list(node.blocks):
            if len(out) >= BLOCKS_PER_HEARTBEAT:
                break
            block = node.blocks[block_id]
            live = []
            for item in block.items:
                if self._live(item):
                    live.append(item)
                else:
                    node.blocked.discard((item.key, item.location))
            if not live:
                del node.blocks[block_id]
                continue
            block.items = tuple(live)
            node.blocks.move_to_end(block_id)
            out.append(BlockRequest(block_id, block.items, block.reason))
        return out

    def _live(self, item: KeyLocation) -> bool:
        entry = self.catalog.entry_exact(item.key, item.location)
        return entry is not None and entry.pool.state == PoolState.ACTIVE

    def blocks_waiting(self) -> int:
        return sum(len(n.blocks) for n in self.nodes.values())

    # ========================================================== maintenance

    def give_up_due(self, now: float) -> list[NodeRecord]:
        due = []
        for node in self.nodes.values():
            session = node.session
            if node.gone or session is None or session.ended or session.valid(now):
                continue
            offline_since = (
                session.closed_at if session.closed_at is not None else session.deadline
            )
            if now >= offline_since + self.node_give_up_s:
                due.append(node)
        return due

    def give_up(self, now: float) -> None:
        for node in self.give_up_due(now):
            self._drop_node(node, f"stayed away over {self.node_give_up_s}s", 0)

    def maintain(self, now: float, budget: int) -> int:
        """One bounded step: expired probe sessions and queued pin releases;
        returns the work done."""
        work = 0
        while self._expiry and work < budget:
            deadline, token = self._expiry[0]
            if deadline > now:
                break
            heapq.heappop(self._expiry)
            work += 1
            session = self._workers.get(token)
            if session is None or session.closed:
                continue
            if session.valid(now):
                heapq.heappush(self._expiry, (session.deadline, token))
            elif session.orphan and any(_on_cxl(p) for p in self.pins.held_by(token)):
                # pins never go by time: WORKER_EXIT or a reopen frees them
                continue
            else:
                # a probe holds nothing; an orphan without CXL pins can go
                self._release_pins(token, 0)
                self._forget_worker(session)
        while self._pin_releases and work < budget:
            holder, keep = self._pin_releases[0]
            limit = budget - work
            released = self.pins.release_holder(holder, limit, keep)
            work += max(released, 1)
            if released < limit:
                self._pin_releases.popleft()
        return work

    def worker_sessions(self) -> list[WorkerSession]:
        return list(self._workers.values())

    def orphan_sessions(self) -> int:
        return sum(1 for w in self._workers.values() if w.orphan)

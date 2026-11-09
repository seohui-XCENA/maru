# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruClient: the storage client behind MaruHandler on maru_node.

ATTACH to the node, map its pools, open the worker session with the node-view
pid, keep credit at hand. On a UDS break it attaches again: to the same run it
keeps credit and resends unresolved ops; to a new run it drops them, answers
the restart drain and unmaps the old pool once unused.
"""

import collections
import functools
import logging
import os
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass

from maru_common.config import MaruConfig
from maru_common.ms_rpc import PLACEHOLDER_OP
from maru_common.node_protocol import (
    AbortRequest,
    AttachRequest,
    AttachResponse,
    CreditRequest,
    CreditReturnRequest,
    DetachRequest,
    DrainAckRequest,
    GroupLayout,
    NodeMessageType,
    Notify,
    UnmapAckRequest,
    WriteDoneItem,
    WriteDoneRequest,
    process_start_time,
)
from maru_common.storage_types import (
    MAX_BATCH,
    MS_PROTOCOL,
    DrainKind,
    ItemOutcome,
    ItemResult,
    Key,
    Location,
    NotifyKind,
    Status,
    StorageLocation,
    StorageUnavailableError,
    WaitLevel,
)

from ..storage.cpu import CpuReadLease
from .credit_cache import CreditCache
from .pin_table import PinTable
from .pool_maps import LocationError, PoolMaps
from .server_conn import ServerConn, WorkerIdentity
from .storage_conn import NodeNotListeningError, OpTracker, StorageConn, close_fds

logger = logging.getLogger(__name__)

RECONNECT_S = 0.5
REFILL_IDLE_S = 0.2
REFILL_EMPTY_S = 0.05
# Wait for DETACH and the last requests of close()
CLOSE_TIMEOUT_S = 5.0
BUSY_RESEND_S = 0.2
ATTACH_WARN_S = 30.0


def _release_view(view: memoryview) -> None:
    """Release our view; if the caller still exports it, GC releases it later
    (the pool stays mapped until then: the view count guards the unmap)."""
    try:
        view.release()
    except BufferError:
        logger.warning("a released buffer is still exported by the caller")


class CreditUnavailableError(ValueError):
    """No credit at hand: this store is skipped."""


@dataclass(eq=False)
class CreditHandle:
    """A credit page lent to the caller; ``buf`` is ``size`` bytes."""

    buf: memoryview
    location: Location
    node_epoch: str
    size: int
    # writing -> submitted -> done; or writing -> freed / dropped
    state: str = "writing"


@dataclass(eq=False)
class PinnedRead(CpuReadLease):
    """A pinned read; release() after the last read or copy."""

    key: str = ""
    pin_id: str = ""
    server_epoch: str = ""


class MaruClient:
    def __init__(
        self,
        config: MaruConfig,
        *,
        dp_index: int = 0,
        tp_rank: int = 0,
        rpc_timeout_s: float | None = None,
        reconnect_s: float = RECONNECT_S,
    ):
        if not config.maru_node:
            raise ValueError("MaruClient needs maru_node (the StorageService socket)")
        if not config.engine_id or not config.cache_namespace:
            raise ValueError("maru_node needs engine_id and cache_namespace")
        self.config = config
        self._socket_path: str = config.maru_node
        # Per client: one process may hold two (TP=1 scheduler and worker)
        self.identity = WorkerIdentity(
            engine_id=config.engine_id,
            dp_index=dp_index,
            tp_rank=tp_rank,
            incarnation=uuid.uuid4().hex,
            namespace=config.cache_namespace,
        )
        self.rpc_timeout_s = (
            rpc_timeout_s if rpc_timeout_s is not None else config.timeout_ms / 1000
        )
        self.reconnect_s = reconnect_s
        self.pools = PoolMaps(pin_pools=config.pin_pools)
        self.credits = CreditCache()
        self.pins = PinTable()

        self._lock = threading.RLock()
        self._cv = threading.Condition(self._lock)
        self._storage: StorageConn | None = None
        self._connecting: StorageConn | None = None  # an ATTACH under way
        self._server: ServerConn | None = None
        self._ops: OpTracker | None = None
        self.attach_info: AttachResponse | None = None
        self.node_epoch: str | None = None
        self._old_epochs: list[str] = []
        self._handles: set[CreditHandle] = set()
        self._reads: set[PinnedRead] = set()
        self._returns: list[Location] = []
        self._connected = False
        self._closing = threading.Event()
        self._refill_wake = threading.Event()
        self._threads: list[threading.Thread] = []
        self._lost = threading.Event()
        self._credit_op: tuple[OpTracker, int] | None = None
        self._sends: collections.deque = collections.deque()
        self._sends_cv = threading.Condition()
        # Unavailable and absent are counted apart
        self.stats: collections.Counter[str] = collections.Counter()

    # ------------------------------------------------------------- lifecycle

    def connect(self) -> bool:
        with self._lock:
            if self._closing.is_set():
                raise RuntimeError("Create a fresh handler after close")
            if self._connected:
                return True
        if self.config.metadata_only:
            raise RuntimeError(
                "a metadata-only handler on maru_node is not supported yet"
            )
        server = None
        try:
            conn, response, fds = self._first_attach()
            self._take_attach(conn, response, fds, first=True)
            server = ServerConn(
                response.server,
                self.identity,
                node_id=response.node_id,
                node_epoch=response.node_epoch,
                pid=response.pid,
                pid_start=response.pid_start,
                timeout_s=self.rpc_timeout_s,
            )
            server.on_server_epoch.append(self._server_epoch_changed)
            server.on_stale_node.append(self._stale_node)
            server.open()
            with self._lock:
                if self._closing.is_set():
                    raise StorageUnavailableError("closed while connecting")
                self._server = server
                self._connected = True
            conn.start_notifications()
            server.start()
            self._refill_once()
        except BaseException:
            if server is not None and self._server is not server:
                server.close()  # no allocation yet: WORKER_CLOSE is safe
            try:
                self._teardown()
            except Exception:
                logger.exception("cleaning up a failed connect")
            raise
        for target, name in (
            (self._refill_loop, "maru-credit-refill"),
            (self._send_loop, "maru-write-done"),
            (self._keep_attached, "maru-storage-keeper"),
        ):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self._threads.append(thread)
        return True

    def _attach_request(self) -> AttachRequest:
        pid = os.getpid()
        return AttachRequest(
            engine_id=self.identity.engine_id,
            dp_index=self.identity.dp_index,
            tp_rank=self.identity.tp_rank,
            pid=pid,
            pid_start=process_start_time(pid),
            incarnation=self.identity.incarnation,
            namespace=self.identity.namespace,
            groups=[
                GroupLayout(
                    object_bytes=self.config.chunk_size_bytes,
                    layout=self.config.storage_schema,
                )
            ],
            protocol=MS_PROTOCOL,
            capabilities=[],
        )

    def _first_attach(self) -> tuple[StorageConn, AttachResponse, list[int]]:
        """ATTACH, waiting up to node_connect_timeout_s for a node that is not
        listening yet (starting or restarting)."""
        deadline = time.monotonic() + self.config.node_connect_timeout_s
        pause = 0.05
        while True:
            try:
                return self._attach(
                    max(deadline - time.monotonic(), self.rpc_timeout_s)
                )
            except NodeNotListeningError as e:
                if time.monotonic() + pause > deadline:
                    raise
                if self._closing.wait(pause):
                    raise StorageUnavailableError(
                        "closed while waiting for the node"
                    ) from e
                pause = min(pause * 2, 0.5)

    def _attach(
        self, timeout_s: float
    ) -> tuple[StorageConn, AttachResponse, list[int]]:
        """One ATTACH within ``timeout_s``; close() cancels it."""
        conn = StorageConn(
            self._socket_path, on_notify=self._on_notify, on_lost=self._on_lost
        )
        with self._lock:
            if self._closing.is_set():
                raise StorageUnavailableError("the client is closed")
            self._connecting = conn
        try:
            response, fds = conn.connect(self._attach_request(), timeout_s)
        finally:
            with self._lock:
                self._connecting = None
        return conn, response, fds

    def _take_attach(
        self, conn: StorageConn, response: AttachResponse, fds: list[int], first: bool
    ) -> None:
        """Take in an ATTACH reply (pools, credit and ops of its run); after
        close() nothing is installed (a re-attach close() gave up waiting on)."""
        added: list[str] = []
        try:
            if self._closing.is_set():
                raise StorageUnavailableError("the client is closed")
            if response.page_size < self.config.chunk_size_bytes:
                raise StorageUnavailableError(
                    f"the node page ({response.page_size} bytes) is smaller than "
                    f"chunk_size_bytes ({self.config.chunk_size_bytes})"
                )
            if response.budgets is None:
                raise StorageUnavailableError("the ATTACH reply has no budgets")
            same_run = response.node_epoch == self.node_epoch
            if same_run:
                close_fds(fds)  # the pools of this run are mapped already
            else:
                for descriptor in response.pools:
                    if descriptor.memfd is None:
                        continue  # only DRAM pools are mapped so far
                    # pools.add closes the fd, also on failure
                    fd, fds[descriptor.memfd] = fds[descriptor.memfd], -1
                    self.pools.add(descriptor, fd, response.node_epoch)
                    added.append(descriptor.pool.pool_id)
        except BaseException:
            close_fds(fds)
            conn.close()
            raise
        with self._lock:
            # close() sets _closing under this lock: it sees what is installed
            closed = self._closing.is_set()
            if not closed:
                if not same_run:
                    if self.node_epoch is not None:
                        self._old_epochs.append(self.node_epoch)
                        logger.warning(
                            "the StorageService restarted (node epoch %s -> %s); "
                            "old credit and unresolved writes are dropped",
                            self.node_epoch,
                            response.node_epoch,
                        )
                    self.node_epoch = response.node_epoch
                    self._ops = OpTracker(
                        self.identity.incarnation, response.node_epoch
                    )
                    self._returns.clear()
                    self.credits.reset(response.node_epoch)
                self.attach_info = response
                self._storage = conn
                self.stats["attach"] += 1
                self._cv.notify_all()
        if closed:
            conn.close()
            for pool_id in added:
                self.pools.remove(pool_id)
            raise StorageUnavailableError("the client is closed")
        if not first:
            if same_run:
                self._resend_unresolved()
            elif self._server is not None:
                self._server.move_to(
                    response.node_epoch, response.pid, response.pid_start
                )
            conn.start_notifications()
            self._refill_wake.set()
            self._unmap_unused_old_pools()

    def close(self) -> None:
        """Return unused credit, DETACH, WORKER_CLOSE and unmap the pools.
        Pools of allocations a copy may still write stay mapped and
        registered until the process ends, so each such close() keeps them."""
        with self._lock:
            if self._reads:
                raise RuntimeError("Release read pins before closing the handler")
            if self._closing.is_set():
                return
            self._closing.set()
            handles = [h for h in self._handles if h.state == "writing"]
            connecting = self._connecting
        if connecting is not None:
            connecting.close()
        if handles:
            # Copies into them may still run: no DETACH, no WORKER_CLOSE, and
            # their pools stay mapped and registered; the node cleans up when
            # this process ends
            logger.warning(
                "closing with %d allocations not stored or freed; their credit "
                "stays with the StorageService until this process ends",
                len(handles),
            )
        self._refill_wake.set()
        self._lost.set()
        with self._sends_cv:
            self._sends_cv.notify_all()
        for thread in self._threads:
            thread.join(timeout=CLOSE_TIMEOUT_S)
        storage = self._storage
        if storage is not None and not storage.closed:
            unused = self.credits.take_all()
            try:
                for i in range(0, len(unused), MAX_BATCH):
                    storage.request(
                        NodeMessageType.CREDIT_RETURN,
                        CreditReturnRequest(credits=unused[i : i + MAX_BATCH]),
                        CLOSE_TIMEOUT_S,
                    )
                if not handles:
                    storage.request(
                        NodeMessageType.DETACH, DetachRequest(), CLOSE_TIMEOUT_S
                    )
            except Exception as e:
                logger.warning("returning credit before closing failed: %s", e)
        self._teardown(goodbye=not handles, writing=handles)

    def _teardown(
        self, goodbye: bool = True, writing: list[CreditHandle] | None = None
    ) -> None:
        """``writing``: handles a copy may still write; their views and pools
        are left as they are."""
        kept = set(writing or ())
        with self._lock:
            storage, self._storage = self._storage, None
            server, self._server = self._server, None
            self._connected = False
            # A later connect() starts as a new run: its pools get mapped
            self.node_epoch = None
            self._ops = None
            self.attach_info = None
            for handle in self._handles:
                if handle not in kept:
                    _release_view(handle.buf)
            self._handles.clear()
            self._cv.notify_all()
        if storage is not None:
            storage.close()
        if server is not None:
            server.close(goodbye=goodbye)
        self.pools.close(keep={h.location.pool_id for h in kept})

    @property
    def connected(self) -> bool:
        with self._lock:
            return self._connected and not self._closing.is_set()

    @property
    def storage_connected(self) -> bool:
        with self._lock:
            return self._storage is not None and not self._storage.closed

    def _ensure(self, data: bool = True) -> None:
        if not self.connected:
            raise StorageUnavailableError("the maru_node client is not connected")

    def healthcheck(self) -> bool:
        """Both connections are up and MaruServer answers a heartbeat."""
        server = self._server
        return bool(
            self.connected
            and self.storage_connected
            and server is not None
            and server.heartbeat_once()
        )

    def local_stats(self) -> dict:
        """This client's counters and holdings (no MaruServer call)."""
        with self._lock:
            return {
                "node_epoch": self.node_epoch,
                "storage_connected": self.storage_connected,
                "unused_credit_bytes": self.credits.unused_bytes,
                "allocations": len(self._handles),
                "pinned_reads": len(self._reads),
                "pools": self.pools.pool_ids(),
                "counters": dict(self.stats),
            }

    @property
    def server_conn(self) -> ServerConn | None:
        return self._server

    @property
    def page_size(self) -> int:
        return self.attach_info.page_size if self.attach_info else 0

    # ----------------------------------------------------------------- writes

    def alloc(self, size: int, route_key: str | None = None) -> CreditHandle:
        """A credit page for ``size`` bytes, without an RPC."""
        self._ensure()
        if type(size) is not int or not 0 < size <= self.config.chunk_size_bytes:
            raise ValueError(
                f"an allocation holds 1..{self.config.chunk_size_bytes} bytes"
            )
        if not self.storage_connected:
            self.stats["store_skipped_unavailable"] += 1
            raise CreditUnavailableError(
                "the StorageService is unreachable; skipping this store"
            )
        taken = self.credits.take()
        if taken is None:
            self.stats["store_skipped_no_credit"] += 1
            self._refill_wake.set()
            raise CreditUnavailableError("no credit at hand; skipping this store")
        credit, node_epoch = taken
        try:
            view = self.pools.view(credit, size)
        except LocationError:
            logger.exception("a credit outside the mapped pools")
            raise
        handle = CreditHandle(
            buf=view, location=credit, node_epoch=node_epoch, size=size
        )
        with self._lock:
            self._handles.add(handle)
            info = self.attach_info
        if (
            info is not None
            and info.budgets is not None
            and self.credits.unused_bytes < info.budgets.credit_refill_low
        ):
            self._refill_wake.set()
        return handle

    def _own(self, handle) -> CreditHandle:
        with self._lock:
            if not isinstance(handle, CreditHandle) or handle not in self._handles:
                raise ValueError("Allocation does not belong to this maru_node client")
        return handle

    def free(self, handle: CreditHandle) -> None:
        """Give back an unstored allocation; a stored one is left alone."""
        with self._lock:
            if not isinstance(handle, CreditHandle) or handle not in self._handles:
                return
            if handle.state != "writing":
                return
            self._finish_handle(handle, "freed")
            reuse = handle.node_epoch == self.node_epoch
        if reuse:
            self.credits.put([handle.location], handle.node_epoch)
        else:
            self._unmap_unused_old_pools()

    def _finish_handle(self, handle: CreditHandle, state: str) -> None:
        """Caller holds the lock."""
        handle.state = state
        _release_view(handle.buf)
        self._handles.discard(handle)
        self._cv.notify_all()

    def has_local(self, key: str) -> bool:
        """KV has no owner here."""
        return False

    def batch_store(self, keys: list[str], handles: list[CreditHandle]) -> list[bool]:
        """Publish written allocations; one bool per key. Waits at most
        ``store_timeout_s``: unfinished items are False, their op stays
        tracked and their pages stay with the node."""
        chunks = self._submit_store(keys, handles)
        out = [False] * len(keys)
        deadline = time.monotonic() + self.config.store_timeout_s
        for start, future in chunks:
            try:
                results = future.result(max(0.0, deadline - time.monotonic()))
            except FutureTimeout:
                self.stats["store_timeout"] += 1
                continue
            out[start : start + len(results)] = results
        return out

    def store_async(self, keys: list[str], handles: list[CreditHandle]) -> Future:
        """batch_store as a future (no timeout); WRITE_DONE goes from the
        sender thread."""
        chunks = self._submit_store(keys, handles)
        future: Future = Future()
        if not chunks:
            future.set_result([])
            return future

        def gather(_=None) -> None:
            if all(c.done() for _, c in chunks) and not future.done():
                out = []
                for _, c in chunks:
                    out.extend(c.result())
                future.set_result(out)

        for _, c in chunks:
            c.add_done_callback(gather)
        return future

    def _submit_store(self, keys, handles) -> list[tuple[int, Future]]:
        """Check and mark every handle at once (free() cannot slip between),
        then queue WRITE_DONE in batches."""
        if len(keys) != len(handles):
            raise ValueError("keys and handles must have the same length")
        self._ensure()
        if len({id(h) for h in handles}) != len(handles):
            raise ValueError("An allocation can only be stored once per batch")
        for key in keys:
            if not isinstance(key, str) or not key or len(key) > 4096:
                raise ValueError("Invalid key")
        with self._lock:
            for handle in handles:
                self._own(handle)
                if handle.state != "writing":
                    raise ValueError("The allocation was already submitted")
            for handle in handles:
                handle.state = "submitted"
        chunks = []
        for i in range(0, len(keys), MAX_BATCH):
            future: Future = Future()
            chunks.append((i, future))
            self._queue_store(
                keys[i : i + MAX_BATCH], handles[i : i + MAX_BATCH], future
            )
        return chunks

    def _queue_store(self, keys, handles, future: Future) -> None:
        def send() -> None:
            try:
                self._send_write_done(keys, handles, future)
            except Exception:
                logger.exception("sending WRITE_DONE failed")
                if not future.done():
                    future.set_result([False] * len(keys))

        self._post(send)

    def _post(self, task: Callable[[], object]) -> None:
        """Run ``task`` on the sender thread (never on the receive thread)."""
        with self._sends_cv:
            self._sends.append(task)
            self._sends_cv.notify()

    def _send_loop(self) -> None:
        while True:
            with self._sends_cv:
                while not self._sends and not self._closing.is_set():
                    self._sends_cv.wait()
                if not self._sends:
                    return
                task = self._sends.popleft()
            try:
                task()
            except Exception:
                logger.exception("a send task failed")

    def _send_write_done(self, keys, handles, future: Future) -> None:
        results = [False] * len(keys)
        live = []
        with self._lock:
            storage, ops, epoch = self._storage, self._ops, self.node_epoch
            for i, handle in enumerate(handles):
                if handle.node_epoch != epoch:
                    self._finish_handle(handle, "dropped")
                elif storage is None or storage.closed:
                    self._finish_handle(handle, "freed")
                    self.stats["store_skipped_unavailable"] += 1
                    self.credits.put([handle.location], handle.node_epoch)
                else:
                    live.append(i)
        if len(live) < len(handles):
            self._unmap_unused_old_pools()
        if not live:
            future.set_result(results)
            return
        assert ops is not None  # set and cleared with the storage connection
        request = ops.origin.stamp(
            WriteDoneRequest(
                op=PLACEHOLDER_OP,
                items=[
                    WriteDoneItem(
                        key=Key(self.identity.namespace, keys[i]),
                        credit=handles[i].location,
                        logical_bytes=handles[i].size,
                    )
                    for i in live
                ],
                wait=WaitLevel.PUBLISHED,
            )
        )
        credits = [handles[i].location for i in live]
        with self._lock:
            for i in live:
                self._finish_handle(handles[i], "done")

        def on_result(response) -> None:
            if response.status != Status.OK:
                logger.error(
                    "WRITE_DONE refused: %s %s", response.status, response.error
                )
                self._abort(credits)
                return
            rejected = [
                credit
                for credit, outcome in zip(credits, response.items, strict=True)
                if outcome.result not in (ItemResult.ADDED, ItemResult.DUPLICATE)
            ]
            if rejected:
                self._abort(rejected)

        reply = self._send_op(NodeMessageType.WRITE_DONE, request, on_result, ops)

        def done(f: Future) -> None:
            try:
                response = f.result()
            except Exception:
                self.stats["write_done_unknown"] += 1
                future.set_result(results)
                return
            if response.status == Status.OK and len(response.items) == len(live):
                for i, outcome in zip(live, response.items, strict=True):
                    results[i] = outcome.result in (
                        ItemResult.ADDED,
                        ItemResult.DUPLICATE,
                    )
            future.set_result(results)

        reply.add_done_callback(done)

    def _abort(self, credits: list[Location]) -> None:
        """ABORT refused credits (from the sender thread); never written again."""
        self._post(lambda: self._abort_now(credits))

    def _abort_now(self, credits: list[Location]) -> None:
        with self._lock:
            ops = self._ops
        if ops is None or not credits:
            return
        for i in range(0, len(credits), MAX_BATCH):
            request = ops.origin.stamp(
                AbortRequest(op=PLACEHOLDER_OP, credits=credits[i : i + MAX_BATCH])
            )
            self._send_op(NodeMessageType.ABORT, request, lambda _r: None, ops)

    def _send_op(
        self,
        code: NodeMessageType,
        request,
        on_result: Callable,
        ops: OpTracker,
        resend: bool = False,
    ) -> Future:
        """Send an op, tracked until its result is known; on_result runs once.
        Unknown and BUSY stay tracked: resent after a reconnect or a pause."""
        reply: Future = Future()
        with self._lock:
            if ops is not self._ops:
                reply.set_exception(StorageUnavailableError("the run ended"))
                return reply
            if resend:
                if not ops.update(request):
                    reply.set_exception(StorageUnavailableError("already resolved"))
                    return reply
            else:
                ops.track(code, request, on_result)
            storage = self._storage
        if storage is None or storage.closed:
            reply.set_exception(StorageUnavailableError("not connected"))
            return reply
        reply = storage.submit(code, request)
        seq = request.op.seq

        def done(f: Future) -> None:
            try:
                response = f.result()
            except Exception:
                return  # unresolved; resent after reconnecting
            if response.status == Status.BUSY:
                timer = threading.Timer(
                    BUSY_RESEND_S,
                    lambda: self._post(
                        lambda: self._send_op(
                            code, ops.origin.refresh(request), on_result, ops, True
                        )
                    ),
                )
                timer.daemon = True
                timer.start()
                return
            if not ops.resolved(seq):
                return  # another reply of this op was handled already
            try:
                on_result(response)
            except Exception:
                logger.exception("handling the result of 0x%02X failed", code)

        reply.add_done_callback(done)
        return reply

    def _resend_unresolved(self) -> None:
        with self._lock:
            ops = self._ops
        if ops is None:
            return
        for code, request, on_result in ops.unresolved():
            self._send_op(code, ops.origin.refresh(request), on_result, ops, True)
        with self._lock:
            returns, self._returns = self._returns, []
        if returns:
            self._return_credits(returns)

    # ----------------------------------------------------------------- credit

    def _refill_once(self) -> bool:
        """One CREDIT if below the low mark; whether credit came."""
        with self._lock:
            storage, ops, server, info = (
                self._storage,
                self._ops,
                self._server,
                self.attach_info,
            )
        if (
            storage is None
            or storage.closed
            or ops is None
            or server is None
            or not server.session
            or info is None
            or info.budgets is None
        ):
            return False
        with self._lock:
            pending = self._credit_op
        if (
            pending is not None
            and pending[0] is ops
            and pending[1] in ops.origin.outstanding
        ):
            return False  # one CREDIT at a time; that one is resent until known
        budgets = info.budgets
        unused = self.credits.unused_bytes
        if unused >= budgets.credit_refill_low and unused > 0:
            return False
        want = budgets.credit_refill_high - unused
        if want <= 0:
            return False
        request = ops.origin.stamp(
            CreditRequest(
                op=PLACEHOLDER_OP,
                worker_session=server.session,
                want_bytes=want,
                kind=None,
            )
        )
        epoch = ops.node_epoch
        got = []
        with self._lock:
            self._credit_op = (ops, request.op.seq)

        def on_result(response) -> None:
            if response.status != Status.OK:
                logger.warning("CREDIT: %s %s", response.status, response.error)
                return
            if response.credits and not self.credits.put(response.credits, epoch):
                return
            got.extend(response.credits)

        reply = self._send_op(NodeMessageType.CREDIT, request, on_result, ops)
        try:
            reply.result(self.rpc_timeout_s * 5)
        except Exception:
            return False
        return bool(got)

    def _refill_loop(self) -> None:
        while not self._closing.is_set():
            self._refill_wake.wait(REFILL_IDLE_S)
            self._refill_wake.clear()
            if self._closing.is_set():
                return
            try:
                if not self._refill_once() and self.credits.unused_bytes == 0:
                    self._closing.wait(REFILL_EMPTY_S)
                    self._refill_wake.set()
            except Exception:
                logger.exception("credit refill failed")

    def _return_credits(self, credits: list[Location]) -> None:
        with self._lock:
            storage = self._storage
        if storage is None or storage.closed:
            with self._lock:
                self._returns.extend(credits)
            return
        for i in range(0, len(credits), MAX_BATCH):
            batch = credits[i : i + MAX_BATCH]
            reply = storage.submit(
                NodeMessageType.CREDIT_RETURN, CreditReturnRequest(credits=batch)
            )

            def done(f: Future, batch=batch) -> None:
                try:
                    f.result()
                except Exception:
                    with self._lock:
                        self._returns.extend(batch)  # sent again after reconnect

            reply.add_done_callback(done)

    # ------------------------------------------------------------------ reads

    def batch_retrieve(
        self, keys: list[str], route_key: str | None = None
    ) -> list[PinnedRead | None]:
        """Pinned reads; None for keys that cannot be read now."""
        self._ensure()
        out: list[PinnedRead | None] = []
        try:
            for i in range(0, len(keys), MAX_BATCH):
                out.extend(self._retrieve_chunk(keys[i : i + MAX_BATCH]))
        except BaseException:
            self.release_retrieved(out)
            raise
        return out

    def _retrieve_chunk(self, keys: list[str]) -> list[PinnedRead | None]:
        if not keys:
            return []
        server = self._server
        if server is None:
            self.stats["retrieve_unavailable"] += len(keys)
            return [None] * len(keys)
        outcomes = server.lookup_and_pin(
            [Key(self.identity.namespace, k) for k in keys]
        )
        if outcomes is None:
            self.stats["retrieve_unavailable"] += len(keys)
            return [None] * len(keys)
        epoch = server.server_epoch
        out: list[PinnedRead | None] = []
        try:
            for key, outcome in zip(keys, outcomes, strict=True):
                out.append(self._take_outcome(server, key, outcome, epoch))
        except BaseException:
            self.release_retrieved(out)
            for outcome in outcomes[len(out) :]:
                if outcome.pin_id is not None:
                    server.unpin([outcome.pin_id])
            raise
        return out

    def _take_outcome(
        self,
        server: ServerConn,
        key: str,
        outcome: ItemOutcome,
        server_epoch: str | None,
    ) -> PinnedRead | None:
        if outcome.result != ItemResult.HIT:
            self.stats[f"retrieve_{outcome.result.value.lower()}"] += 1
            if outcome.pin_id is not None:
                server.unpin([outcome.pin_id])
            return None
        if outcome.location is None or outcome.pin_id is None:
            logger.error("a HIT without location or pin for %s", key)
            if outcome.pin_id is not None:
                server.unpin([outcome.pin_id])
            return None
        if self.pins.holds(outcome.pin_id):
            # Never unpin it: it belongs to a read already handed out
            logger.error("MaruServer gave pin %s twice", outcome.pin_id)
            return None
        try:
            view = self.pools.view(outcome.location)
        except LocationError as e:
            # #59: never touch bytes outside a mapped pool
            logger.error("MaruServer gave %s for %s: %s", outcome.location, key, e)
            self.stats["retrieve_bad_location"] += 1
            server.unpin([outcome.pin_id])
            return None
        if server_epoch is None:  # not open any more
            server.unpin([outcome.pin_id])
            return None
        self.pins.add(
            outcome.pin_id,
            Key(self.identity.namespace, key),
            outcome.location,
            server_epoch,
        )
        loc = outcome.location
        read = PinnedRead(
            view=view,
            location=StorageLocation(
                loc.pool_id,
                loc.allocation_id,
                loc.generation,
                loc.offset,
                loc.length,
                medium=loc.kind.value,
            ),
            _release=lambda: None,
            key=key,
            pin_id=outcome.pin_id,
            server_epoch=server_epoch,
        )
        read._release = functools.partial(self._release_reads, [read])
        with self._lock:
            self._reads.add(read)
        self.stats["retrieve_hit"] += 1
        return read

    def release_retrieved(self, infos: list) -> None:
        """Release reads with one UNPIN."""
        reads = []
        for info in infos:
            if isinstance(info, PinnedRead) and not info._released:
                info._released = True
                _release_view(info.view)
                reads.append(info)
            elif info is not None and not isinstance(info, PinnedRead):
                info.release()
        self._release_reads(reads)

    def _release_reads(self, reads: list[PinnedRead]) -> None:
        if not reads:
            return
        with self._lock:
            for read in reads:
                self._reads.discard(read)
            server = self._server
            self._cv.notify_all()
        epoch = server.server_epoch if server is not None else None
        unpins = self.pins.release([r.pin_id for r in reads], epoch)
        if unpins and server is not None:
            server.unpin(unpins)
        self._unmap_unused_old_pools()

    def batch_exists(self, keys: list[str], route_key: str | None = None) -> list[bool]:
        """PROBE each key (a hint, no pin)."""
        self._ensure(data=False)
        out: list[bool] = []
        for i in range(0, len(keys), MAX_BATCH):
            batch = keys[i : i + MAX_BATCH]
            server = self._server
            outcomes = (
                None
                if server is None
                else server.probe([Key(self.identity.namespace, k) for k in batch])
            )
            if outcomes is None:
                out.extend([False] * len(batch))
                continue
            # Only a HIT is readable: fetching from another node is not
            # supported yet
            out.extend(o.result == ItemResult.HIT for o in outcomes)
        return out

    def _server_epoch_changed(self, old: str, new: str) -> None:
        dead = self.pins.drop_epoch(old)
        if dead:
            logger.warning(
                "%d pins of server epoch %s are gone with it; their reads finish "
                "but are not unpinned",
                len(dead),
                old,
            )

    # ------------------------------------------------------- connection loss

    def _on_lost(self, conn: StorageConn) -> None:
        with self._lock:
            if conn is self._storage:
                self._storage = None
        self._lost.set()

    def _keep_attached(self) -> None:
        """Attach again whenever the StorageService connection is gone."""
        failing_since = None
        last_warning = 0.0
        while not self._closing.is_set():
            self._lost.wait(self.reconnect_s)
            self._lost.clear()
            with self._lock:
                storage = self._storage
            if self._closing.is_set() or (storage is not None and not storage.closed):
                continue
            try:
                conn, response, fds = self._attach(
                    max(self.config.node_connect_timeout_s, self.rpc_timeout_s)
                )
                self._take_attach(conn, response, fds, first=False)
            except Exception as e:
                now = time.monotonic()
                failing_since = failing_since or now
                if now - last_warning >= ATTACH_WARN_S:
                    last_warning = now
                    logger.warning(
                        "cannot attach to the StorageService for %.0f s: %s",
                        now - failing_since,
                        e,
                    )
                continue
            failing_since = None
            logger.info("attached again to node epoch %s", response.node_epoch)

    def _stale_node(self, node_epoch: str) -> None:
        """WORKER_OPEN was refused for ``node_epoch``. Reopen under the run the
        UDS is attached to if that is newer; never close a healthy UDS (a new
        run breaks it anyway)."""
        server = self._server
        with self._lock:
            info = self.attach_info if self._storage is not None else None
        if server is None or info is None or info.node_epoch == node_epoch:
            return  # MaruServer does not know this run yet: nothing to gain
        logger.warning(
            "worker session on node epoch %s; reopening on %s",
            node_epoch,
            info.node_epoch,
        )
        server.move_to(info.node_epoch, info.pid, info.pid_start)

    def _unmap_unused_old_pools(self) -> None:
        """Unmap pools of ended runs once unused."""
        with self._lock:
            current = self.node_epoch
            old = [
                pool_id
                for pool_id in self.pools.pool_ids()
                if (m := self.pools.get(pool_id)) is not None
                and m.node_epoch != current
            ]
            busy = {h.location.pool_id for h in self._handles} | {
                r.location.pool_id for r in self._reads
            }
        for pool_id in old:
            if pool_id not in busy and self.pools.remove(pool_id):
                logger.info("unmapped %s of an ended StorageService run", pool_id)

    # ----------------------------------------------------------------- notify

    def _on_notify(self, notify: Notify) -> None:
        if notify.kind == NotifyKind.DRAIN:
            threading.Thread(
                target=self._drain, args=(notify,), name="maru-drain", daemon=True
            ).start()
        elif notify.kind == NotifyKind.CREDIT_RETURN:
            credits = self.credits.take_bytes(notify.bytes or 0)
            if credits:
                self._return_credits(credits)
        elif notify.kind == NotifyKind.POOL_REMOVE:
            threading.Thread(
                target=self._pool_remove,
                args=(notify,),
                name="maru-pool-remove",
                daemon=True,
            ).start()
        elif notify.kind == NotifyKind.POOL_ADD:
            logger.warning(
                "pool_add for %s: mapping added pools is not supported yet",
                notify.pool.pool.pool_id if notify.pool else None,
            )
        elif notify.kind == NotifyKind.EPOCH:
            logger.info(
                "StorageService reports node epoch %s, server epoch %s",
                notify.node_epoch,
                notify.server_epoch,
            )

    def _drain(self, notify: Notify) -> None:
        """DRAIN_ACK once the ended epoch's accesses finish; never on a timeout."""
        kind = notify.drain_kind
        if kind is None:
            logger.warning("NOTIFY drain without drain_kind; ignored")
            return
        if kind == DrainKind.NODE:
            with self._lock:
                ack_epoch = notify.epoch or (
                    self._old_epochs[-1] if self._old_epochs else ""
                )
                # Every older run, not only the one named: a superset is safe
                self._cv.wait_for(
                    lambda: (
                        self._closing.is_set()
                        or all(h.node_epoch == self.node_epoch for h in self._handles)
                    )
                )
        else:
            with self._lock:
                ack_epoch = notify.epoch or ""
                before = set(self._handles)
                self._cv.wait_for(
                    lambda: (
                        self._closing.is_set()
                        or not (before & self._handles)
                        and not any(r.server_epoch == ack_epoch for r in self._reads)
                    )
                )
        if self._closing.is_set():
            return
        with self._lock:
            storage = self._storage
        if storage is None or storage.closed:
            return  # the StorageService asks again after the next ATTACH
        storage.submit(
            NodeMessageType.DRAIN_ACK,
            DrainAckRequest(kind=kind, epoch=ack_epoch),
        )
        logger.info("drained %s epoch %r", kind.value, ack_epoch)

    def _pool_remove(self, notify: Notify) -> None:
        pool_id, generation = notify.pool_id, notify.pool_generation
        if pool_id is None or generation is None:
            logger.warning("NOTIFY pool_remove without pool_id or generation; ignored")
            return
        with self._lock:
            self._cv.wait_for(
                lambda: (
                    self._closing.is_set()
                    or not any(h.location.pool_id == pool_id for h in self._handles)
                    and not any(r.location.pool_id == pool_id for r in self._reads)
                )
            )
        if self._closing.is_set():
            return
        credits = self.credits.remove_pool(pool_id)
        if credits:
            self._return_credits(credits)
        if not self.pools.remove(pool_id):
            logger.warning(
                "pool %s is still in use by the caller; not unmapped", pool_id
            )
            return
        with self._lock:
            storage = self._storage
        if storage is not None and not storage.closed:
            storage.submit(
                NodeMessageType.UNMAP_ACK,
                UnmapAckRequest(pool_id=pool_id, pool_generation=generation),
            )

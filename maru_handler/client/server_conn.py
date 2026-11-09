# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""ServerConn: the worker session with MaruServer.

Reopening with the same incarnation keeps pins unless the server epoch
changed. A lookup with an unknown result is resent with the same op in the
background and its pins released.
"""

import dataclasses
import logging
import threading

from maru_common.ms_rpc import PLACEHOLDER_OP, MsRpcClient, OpOrigin, RpcUnknownError
from maru_common.protocol import (
    LookupAndPinRequest,
    LookupAndPinResponse,
    MessageType,
    ProbeRequest,
    ProbeResponse,
    UnpinRequest,
    WorkerCloseRequest,
    WorkerHeartbeatRequest,
    WorkerHeartbeatResponse,
    WorkerOpenRequest,
)
from maru_common.storage_types import (
    MAX_BATCH,
    MS_PROTOCOL,
    ItemOutcome,
    ItemResult,
    Key,
    SessionRole,
    Status,
    StorageUnavailableError,
)

logger = logging.getLogger(__name__)

MAX_UNRESOLVED_LOOKUPS = 32


@dataclasses.dataclass(frozen=True)
class WorkerIdentity:
    engine_id: str
    dp_index: int
    tp_rank: int
    incarnation: str
    namespace: str


class ServerConn:
    def __init__(
        self,
        url: str,
        identity: WorkerIdentity,
        *,
        node_id: str,
        node_epoch: str,
        pid: int,
        pid_start: int,
        timeout_s: float,
    ):
        self.url = url
        self.identity = identity
        self.node_id = node_id
        self.node_epoch = node_epoch
        self.pid = pid
        self.pid_start = pid_start
        self._rpc = MsRpcClient(url, timeout_s=timeout_s, name="maru-worker-rpc")
        self._lock = threading.RLock()
        self.session: str | None = None
        self.server_epoch: str | None = None
        self.heartbeat_s = 0.0
        self._origin: OpOrigin | None = None
        # Lookups whose result is unknown: op seq -> request
        self._unresolved: dict[int, LookupAndPinRequest] = {}
        # Pins to unpin that MaruServer has not confirmed yet
        self._unpins: list[str] = []
        self._stop = threading.Event()
        self._reopen = False
        self._opens = 0  # successful WORKER_OPENs
        self._thread: threading.Thread | None = None
        self._resolver: threading.Thread | None = None
        self._wake = threading.Event()
        self.on_server_epoch: list = []
        # Called with the node epoch MaruServer no longer accepts
        self.on_stale_node: list = []
        self.late_hits = 0

    # ---------------------------------------------------------------- session

    def open(self) -> None:
        """WORKER_OPEN; StorageUnavailableError if unreachable or refused."""
        request = WorkerOpenRequest(
            node_id=self.node_id,
            node_epoch=self.node_epoch,
            engine_id=self.identity.engine_id,
            dp_index=self.identity.dp_index,
            tp_rank=self.identity.tp_rank,
            pid=self.pid,
            pid_start=self.pid_start,
            incarnation=self.identity.incarnation,
            namespace=self.identity.namespace,
            role=SessionRole.WORKER,
            protocol=MS_PROTOCOL,
            capabilities=[],
        )
        try:
            response = self._rpc.call(MessageType.WORKER_OPEN, request)
        except RpcUnknownError as e:
            raise StorageUnavailableError(f"WORKER_OPEN: {e}") from e
        if response.status == Status.EPOCH:
            # Refused for this node epoch: the stale-node callback may reopen
            # on the run the UDS is attached to
            with self._lock:
                self._reopen = True
                generation = self._opens
            for callback in self.on_stale_node:
                callback(request.node_epoch)
            with self._lock:
                if self._opens != generation:
                    return  # a nested open succeeded
        if response.status != Status.OK:
            raise StorageUnavailableError(
                f"WORKER_OPEN refused: {response.status} {response.error}"
            )
        with self._lock:
            self._reopen = False
            self._opens += 1
            old_epoch = self.server_epoch
            self.session = response.worker_session
            self.server_epoch = response.server_epoch
            self.heartbeat_s = response.heartbeat_s
            if old_epoch != response.server_epoch:
                # A new MaruServer has no record of earlier ops
                self._origin = OpOrigin(self.identity.incarnation)
                self._unresolved.clear()
                self._unpins.clear()
        if old_epoch is not None and old_epoch != response.server_epoch:
            logger.warning(
                "MaruServer restarted (epoch %s -> %s); pins of the old epoch "
                "are gone with it",
                old_epoch,
                response.server_epoch,
            )
            for callback in self.on_server_epoch:
                callback(old_epoch, response.server_epoch)

    def move_to(self, node_epoch: str, pid: int, pid_start: int) -> None:
        """Reopen under a new node epoch (pins move); retried until it works."""
        with self._lock:
            self.node_epoch, self.pid, self.pid_start = node_epoch, pid, pid_start
            self._reopen = True
        try:
            self.open()
        except StorageUnavailableError as e:
            logger.warning("reopening the worker session: %s", e)

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._loop, name="maru-worker-heartbeat", daemon=True
        )
        self._resolver = threading.Thread(
            target=self._resolve_loop, name="maru-worker-resolve", daemon=True
        )
        self._thread.start()
        self._resolver.start()

    def close(self, goodbye: bool = True) -> None:
        """Flush UNPINs and stop; with ``goodbye`` also WORKER_CLOSE (only after
        every GPU copy ended)."""
        self._stop.set()
        self._wake.set()
        for thread in (self._thread, self._resolver):
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout=5.0)
        if self.session is not None:
            self._flush_unpins()
        session, epoch = self.session, self.server_epoch
        if goodbye and session is not None and epoch is not None:
            try:
                self._rpc.call(
                    MessageType.WORKER_CLOSE,
                    WorkerCloseRequest(session=session, server_epoch=epoch),
                )
            except RpcUnknownError as e:
                logger.warning("WORKER_CLOSE unanswered: %s", e)
        self._rpc.close()

    def _loop(self) -> None:
        """Heartbeat only; lookups and UNPINs are resolved on their own thread."""
        while not self._stop.wait(self.heartbeat_s or 1.0):
            try:
                if self._reopen:
                    self.open()
                session, epoch = self._ids()
                response = self._rpc.call(
                    MessageType.WORKER_HEARTBEAT,
                    WorkerHeartbeatRequest(session=session, server_epoch=epoch),
                )
                if response.status == Status.EPOCH:
                    self.open()  # expired, or MaruServer restarted
            except (RpcUnknownError, StorageUnavailableError) as e:
                logger.warning("worker heartbeat: %s", e)

    def heartbeat_once(self) -> bool:
        """One WORKER_HEARTBEAT; whether it was answered OK."""
        try:
            session, epoch = self._ids()
            response: WorkerHeartbeatResponse = self._rpc.call(
                MessageType.WORKER_HEARTBEAT,
                WorkerHeartbeatRequest(session=session, server_epoch=epoch),
            )
        except (RpcUnknownError, StorageUnavailableError):
            return False
        return response.status == Status.OK

    def _ids(self) -> tuple[str, str]:
        """(session, server_epoch) of the open session."""
        with self._lock:
            session, epoch = self.session, self.server_epoch
        if session is None or epoch is None:
            raise StorageUnavailableError("the worker session is not open")
        return session, epoch

    def _resolve_loop(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(self.heartbeat_s or 1.0)
            self._wake.clear()
            if self._stop.is_set():
                return
            try:
                self._resolve_lookups()
                self._flush_unpins()
            except Exception:
                logger.exception("resolving lookups failed")

    def _reopen_after_epoch(self) -> bool:
        """Reopen after EPOCH; whether the server epoch is unchanged."""
        before = self.server_epoch
        self.open()
        return self.server_epoch == before

    # ----------------------------------------------------------------- lookup

    def lookup_and_pin(self, keys: list[Key]) -> list[ItemOutcome] | None:
        """One LOOKUP_AND_PIN; None when unknown or unavailable."""
        if not 0 < len(keys) <= MAX_BATCH:
            raise ValueError(f"{len(keys)} keys; a lookup carries 1..{MAX_BATCH}")
        with self._lock:
            if len(self._unresolved) >= MAX_UNRESOLVED_LOOKUPS:
                # MaruServer is not answering: no new ops until these resolve
                return None
            origin = self._origin
            if origin is None or self.session is None or self.server_epoch is None:
                return None  # not open
            request = origin.stamp(
                LookupAndPinRequest(
                    session=self.session,
                    server_epoch=self.server_epoch,
                    op=PLACEHOLDER_OP,
                    items=list(keys),
                )
            )
        return self._send_lookup(origin, request)

    def _send_lookup(
        self, origin: OpOrigin, request: LookupAndPinRequest
    ) -> list[ItemOutcome] | None:
        seq = request.op.seq
        for _ in range(2):
            try:
                response: LookupAndPinResponse = self._rpc.call(
                    MessageType.LOOKUP_AND_PIN, request
                )
            except RpcUnknownError as e:
                logger.warning("LOOKUP_AND_PIN op %d: result unknown (%s)", seq, e)
                with self._lock:
                    if origin is self._origin:
                        self._unresolved[seq] = request
                return None
            if response.status == Status.EPOCH:
                try:
                    same = self._reopen_after_epoch()
                except StorageUnavailableError as e:
                    logger.warning("reopening the worker session: %s", e)
                    with self._lock:
                        if origin is self._origin:
                            self._unresolved[seq] = request
                    return None
                if not same:
                    return None  # the op and its pins are gone with the server
                # The digest leaves out the session
                session, epoch = self._ids()
                request = dataclasses.replace(
                    request, session=session, server_epoch=epoch
                )
                continue
            if response.status == Status.BUSY:
                with self._lock:
                    if origin is self._origin:
                        self._unresolved[seq] = request
                return None
            with self._lock:
                if origin is self._origin:  # not a newer ledger's seq
                    self._unresolved.pop(seq, None)
            origin.resolved(seq)
            if response.status != Status.OK or len(response.items) != len(
                request.items
            ):
                logger.error(
                    "LOOKUP_AND_PIN op %d: %s %s (%d results for %d keys)",
                    seq,
                    response.status,
                    response.error,
                    len(response.items),
                    len(request.items),
                )
                if response.status == Status.OK:
                    self.unpin(
                        [o.pin_id for o in response.items if o.pin_id is not None]
                    )
                return None
            return response.items
        # EPOCH again after reopening: not run; resent in the background
        with self._lock:
            if origin is self._origin:
                self._unresolved[seq] = request
        return None

    def _resolve_lookups(self) -> None:
        """Resend unresolved lookups with the same op, one at a time while
        answers come back, and unpin their HITs; nobody reads them (not
        counted as hits)."""
        while not self._stop.is_set():
            with self._lock:
                origin = self._origin
                if not self._unresolved or origin is None:
                    return
                session, epoch = self._ids()
                request = origin.refresh(
                    dataclasses.replace(
                        next(iter(self._unresolved.values())),
                        session=session,
                        server_epoch=epoch,
                    )
                )
            items = self._send_lookup(origin, request)
            if items is None:
                return  # still no answer; try again next beat
            pins = [
                o.pin_id
                for o in items
                if o.result == ItemResult.HIT and o.pin_id is not None
            ]
            self.late_hits += len(pins)
            if pins:
                self.unpin(pins)

    def unresolved_lookups(self) -> int:
        with self._lock:
            return len(self._unresolved)

    def probe(self, keys: list[Key]) -> list[ItemOutcome] | None:
        """PROBE (no pins); None when unavailable."""
        if not 0 < len(keys) <= MAX_BATCH:
            raise ValueError(f"{len(keys)} keys; a probe carries 1..{MAX_BATCH}")
        for _ in range(2):
            try:
                session, epoch = self._ids()
                response: ProbeResponse = self._rpc.call(
                    MessageType.PROBE,
                    ProbeRequest(session=session, server_epoch=epoch, items=list(keys)),
                )
            except (RpcUnknownError, StorageUnavailableError) as e:
                logger.warning("PROBE: %s", e)
                return None
            if response.status == Status.EPOCH:
                try:
                    self._reopen_after_epoch()
                except StorageUnavailableError:
                    return None
                continue
            if response.status != Status.OK or len(response.items) != len(keys):
                logger.error("PROBE: %s %s", response.status, response.error)
                return None
            return response.items
        return None

    # ------------------------------------------------------------------ unpin

    def unpin(self, pin_ids: list[str]) -> None:
        """Queue pins for UNPIN; the resolver thread sends them at once, so
        the caller never waits on MaruServer."""
        if pin_ids:
            with self._lock:
                self._unpins.extend(pin_ids)
            self._wake.set()

    def _flush_unpins(self) -> bool:
        with self._lock:
            pins = list(self._unpins)
            session, epoch = self.session, self.server_epoch
        if not pins:
            return True
        if session is None or epoch is None:
            return False  # not open: sent once it is
        for i in range(0, len(pins), MAX_BATCH):
            batch = pins[i : i + MAX_BATCH]
            try:
                response = self._rpc.call(
                    MessageType.UNPIN,
                    UnpinRequest(session=session, server_epoch=epoch, pin_ids=batch),
                )
            except RpcUnknownError as e:
                logger.warning("UNPIN: %s; sent again later", e)
                return False
            if response.status == Status.EPOCH:
                try:
                    same = self._reopen_after_epoch()
                except StorageUnavailableError:
                    return False
                if not same:
                    return True  # open() dropped them: gone with the old server
                return self._flush_unpins()
            if response.status != Status.OK:
                logger.error("UNPIN refused: %s %s", response.status, response.error)
            else:
                refused = [
                    pin
                    for pin, outcome in zip(batch, response.items, strict=False)
                    if outcome.result != ItemResult.OK
                ]
                if refused:
                    # Not this session's pins: nothing to retry
                    logger.error(
                        "UNPIN refused for %d pins: %s", len(refused), refused[:4]
                    )
            with self._lock:
                done = set(batch)
                self._unpins = [p for p in self._unpins if p not in done]
        return True

    def pending_unpins(self) -> int:
        with self._lock:
            return len(self._unpins)

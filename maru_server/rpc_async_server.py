# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""RpcAsyncServer - ZeroMQ RPC server with a poll-based proxy and worker pool.

The proxy routes multi-storage requests (0x40-0x5F) to their own worker lane,
so legacy requests stuck on a slow resource manager never take their threads
(D10); a full lane is answered at once instead of stalling the other.
"""

import logging
import threading
import time
from collections import deque
from typing import TYPE_CHECKING

import zmq

from maru_common import MessageHeader, MessageType, Serializer
from maru_common.protocol import HEADER_SIZE, MS_MESSAGE_CLASSES
from maru_common.storage_types import Status

from .rpc_handler_mixin import RpcHandlerMixin, is_ms_frame

if TYPE_CHECKING:
    from .server import MaruServer

logger = logging.getLogger(__name__)

_WORKER_ENDPOINT = "inproc://workers"
_MS_WORKER_ENDPOINT = "inproc://ms-workers"
# frames the proxy holds per lane while its workers are busy
LANE_QUEUE_MAX = 10000
# how long start() waits for every worker to connect
CONNECT_TIMEOUT_S = 10.0


class RpcAsyncServer(RpcHandlerMixin):
    """ROUTER frontend, one DEALER backend per lane, REP workers with their
    own Serializer. ``server`` must be thread safe; once started, the proxy
    thread owns the three sockets."""

    def __init__(
        self,
        server: "MaruServer",
        host: str = "127.0.0.1",
        port: int = 5555,
        num_workers: int = 4,
        num_ms_workers: int | None = None,
        lane_queue_max: int = LANE_QUEUE_MAX,
        connect_timeout_s: float = CONNECT_TIMEOUT_S,
    ):
        self._server = server
        self._host = host
        self._port = port
        self._num_workers = num_workers
        self._num_ms_workers = num_workers if num_ms_workers is None else num_ms_workers
        if self._num_ms_workers < 1:
            raise ValueError("the multi-storage lane needs at least one thread")
        self._context: zmq.Context | None = None
        self._frontend: zmq.Socket | None = None
        self._backend: zmq.Socket | None = None
        self._ms_backend: zmq.Socket | None = None
        self._owns_sockets = False  # set by start(): the proxy closes them
        self._running = False
        self._stop_event = threading.Event()
        self._proxy_thread: threading.Thread | None = None
        self._worker_threads: list[threading.Thread] = []
        self._proxy_serializer = Serializer()
        self._lane_queue_max = lane_queue_max
        self._connect_timeout_s = connect_timeout_s
        self._connected = threading.Semaphore(0)  # one release per worker

    @property
    def address(self) -> str:
        """Return the server address."""
        return f"tcp://{self._host}:{self._port}"

    def start(self) -> None:
        """Start the RPC async server (blocks until stop() is called)."""
        self._context = zmq.Context()

        # Frontend: ROUTER socket facing clients
        self._frontend = self._context.socket(zmq.ROUTER)
        self._frontend.setsockopt(zmq.LINGER, 0)
        self._frontend.bind(self.address)

        # Backend: DEALER socket facing workers; sends never block (the
        # proxy queues what a busy lane cannot take)
        self._backend = self._context.socket(zmq.DEALER)
        self._backend.setsockopt(zmq.LINGER, 0)
        self._backend.setsockopt(zmq.SNDTIMEO, 0)
        self._backend.bind(_WORKER_ENDPOINT)

        self._owns_sockets = True
        self._running = True
        self._stop_event.clear()
        self._connected = threading.Semaphore(0)

        # Start worker threads; the proxy thread binds the multi-storage
        # backend (inproc allows connecting first)
        lanes = [
            (_WORKER_ENDPOINT, self._num_workers, "rpc-worker"),
            (_MS_WORKER_ENDPOINT, self._num_ms_workers, "rpc-ms-worker"),
        ]
        for endpoint, count, name in lanes:
            for i in range(count):
                t = threading.Thread(
                    target=self._worker_routine,
                    args=(i, endpoint),
                    name=f"{name}-{i}",
                    daemon=True,
                )
                self._worker_threads.append(t)
                t.start()

        # Forward only once every worker is connected; requests wait at the
        # frontend meanwhile
        deadline = time.monotonic() + self._connect_timeout_s
        for _ in range(self._num_workers + self._num_ms_workers):
            while self._running and not self._connected.acquire(timeout=0.05):
                if time.monotonic() >= deadline:
                    self.stop()
                    raise RuntimeError(
                        f"RPC workers did not connect within "
                        f"{self._connect_timeout_s:g} s"
                    )
        if not self._running:  # stopped while starting
            self._stop_event.wait()
            return

        # Start proxy in a separate thread
        self._proxy_thread = threading.Thread(
            target=self._proxy_routine,
            name="rpc-proxy",
            daemon=True,
        )
        self._proxy_thread.start()

        logger.info(
            "RPC Async Server started on %s with %d workers "
            "(+%d multi-storage workers)",
            self.address,
            self._num_workers,
            self._num_ms_workers,
        )

        # Block until stop() is called
        self._stop_event.wait()

    def stop(self) -> None:
        """Gracefully stop the RPC async server."""
        self._running = False

        # 1. Wait for worker threads to notice _running=False and exit
        for t in self._worker_threads:
            if t.is_alive():
                t.join(timeout=2.0)
        self._worker_threads.clear()

        # 2. Wait for the proxy thread, which closes its sockets on exit
        if self._proxy_thread and self._proxy_thread.is_alive():
            self._proxy_thread.join(timeout=2.0)
        proxy_alive = bool(self._proxy_thread and self._proxy_thread.is_alive())
        if proxy_alive and self._owns_sockets:
            # its sockets are its own to close: leave them and the context
            logger.warning("RPC proxy thread did not stop; leaving its sockets")
            self._stop_event.set()
            return

        # 3. Close what no proxy thread closed
        self._close_sockets()

        # 4. Unblock start() BEFORE context.term() to prevent hang
        self._stop_event.set()

        # 5. Terminate ZMQ context
        if self._context:
            self._context.term()
            self._context = None

        logger.info("RPC Async Server stopped")

    # =========================================================================
    # Internal Routines
    # =========================================================================

    def _close_sockets(self) -> None:
        for name in ("_frontend", "_backend", "_ms_backend"):
            sock = getattr(self, name)
            if sock is not None:
                sock.close()
                setattr(self, name, None)

    def _proxy_routine(self) -> None:
        """Poll-based proxy routing each request to its lane's backend."""
        if self._context is not None and self._ms_backend is None:
            self._ms_backend = self._context.socket(zmq.DEALER)
            self._ms_backend.setsockopt(zmq.LINGER, 0)
            self._ms_backend.setsockopt(zmq.SNDTIMEO, 0)
            self._ms_backend.bind(_MS_WORKER_ENDPOINT)
        try:
            self._proxy_loop()
        finally:
            if self._owns_sockets:
                self._close_sockets()

    def _proxy_loop(self) -> None:
        frontend, backend, ms_backend = self._frontend, self._backend, self._ms_backend
        assert frontend is not None and backend is not None
        # frames a lane could not take yet, sent in order once it can
        queues: dict[zmq.Socket, deque[list[bytes]]] = {backend: deque()}
        if ms_backend is not None:
            queues[ms_backend] = deque()
        poller = zmq.Poller()
        poller.register(frontend, zmq.POLLIN)

        def forward(lane: zmq.Socket, msg: list[bytes]) -> None:
            queue = queues[lane]
            if not queue:
                try:
                    lane.send_multipart(msg)
                    return
                except zmq.Again:
                    pass
            if len(queue) >= self._lane_queue_max:
                self._reply_busy(frontend, msg, lane is ms_backend)
            else:
                queue.append(msg)

        def flush(lane: zmq.Socket) -> None:
            queue = queues[lane]
            while queue:
                try:
                    lane.send_multipart(queue[0])
                except zmq.Again:
                    return
                queue.popleft()

        while self._running:
            for lane, queue in queues.items():
                poller.register(lane, zmq.POLLIN | (zmq.POLLOUT if queue else 0))
            try:
                socks = dict(poller.poll(100))
            except zmq.ZMQError:
                break
            try:
                for lane in queues:
                    if socks.get(lane, 0) & zmq.POLLOUT:
                        flush(lane)
                if frontend in socks:
                    msg = frontend.recv_multipart(zmq.NOBLOCK)
                    lane = backend
                    if ms_backend is not None and msg and is_ms_frame(msg[-1]):
                        lane = ms_backend
                    forward(lane, msg)
                for lane in queues:
                    if socks.get(lane, 0) & zmq.POLLIN:
                        frontend.send_multipart(lane.recv_multipart(zmq.NOBLOCK))
            except zmq.ZMQError:
                break

    def _reply_busy(self, frontend: zmq.Socket, msg: list[bytes], ms: bool) -> None:
        """A lane's queue is full: BUSY for multi-storage, the generic error
        reply for legacy requests."""
        frame = msg[-1]
        if len(frame) < HEADER_SIZE:
            return
        header = MessageHeader.unpack(frame[:HEADER_SIZE])
        if ms:
            _, response_cls = MS_MESSAGE_CLASSES[MessageType(header.msg_type)]
            reply = response_cls(status=Status.BUSY, error="server busy; resend later")
        else:
            reply = {"error": "server busy"}
        body = self._proxy_serializer.encode_response(header, reply)
        frontend.send_multipart(msg[:-1] + [body])

    def _worker_routine(self, worker_id: int, endpoint: str = _WORKER_ENDPOINT) -> None:
        """Worker thread: connect REP socket to backend and process requests."""
        socket = self._context.socket(zmq.REP)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.RCVTIMEO, 100)  # 100ms timeout to check _running
        socket.connect(endpoint)
        self._connected.release()

        serializer = Serializer()

        logger.debug("Worker %d (%s) started", worker_id, endpoint)

        while self._running:
            header = None
            try:
                raw_data = socket.recv()
                if is_ms_frame(raw_data):
                    # Multi-storage: strict decode with the raw payload
                    socket.send(self._process_ms_frame(serializer, raw_data))
                    continue
                header, request = serializer.decode_request(raw_data)

                # Dispatch to handler
                response = self._handle_message(header.msg_type, request)

                # Send response
                response_data = serializer.encode_response(header, response)
                socket.send(response_data)

            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    # Receive timeout — loop back to check _running
                    continue
                if self._running:
                    logger.error("Worker %d ZMQ error: %s", worker_id, e)
                break
            except Exception:
                logger.error(
                    "Worker %d error handling message", worker_id, exc_info=True
                )
                # Send error response preserving sequence from request
                error_response = {"error": "internal server error"}
                try:
                    error_header = (
                        header if header is not None else MessageHeader(msg_type=0)
                    )
                    error_data = serializer.encode_response(
                        error_header, error_response
                    )
                    socket.send(error_data)
                except Exception:
                    logger.debug(
                        "Worker %d: failed to send error response",
                        worker_id,
                        exc_info=True,
                    )

        socket.close()
        logger.debug("Worker %d stopped", worker_id)

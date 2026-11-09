# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Request lanes over RPC: legacy requests stuck on a 2 s RM release leave
lookup_and_pin p99 unchanged. Needs no resource manager."""

import socket
import threading
import time

import pytest
import zmq

from maru_common.protocol import MessageType, StorageRequest
from maru_common.serializer import Serializer
from maru_common.storage_types import Status
from maru_server.replica_directory import StoragePool
from maru_server.rpc_async_server import RpcAsyncServer
from maru_server.server import MaruServer
from maru_shm import MaruHandle
from tests.unit.ms_fakes import FakeNode, FakeWorker, RpcWire, key

pytestmark = pytest.mark.integration

RM_DELAY_S = 2.0


def slow_rm_release(server: MaruServer) -> list[float]:
    """Every ReplicaDirectory request retries a release that takes
    RM_DELAY_S under its lock; returns the call start times."""
    directory = server.replica_directory
    started: list[float] = []

    def release(owner: str, region_id: int) -> bool:
        started.append(time.monotonic())
        time.sleep(RM_DELAY_S)
        return False  # unconfirmed: retried on the next request

    directory._release_cxl = release
    directory._releasing["stuck"] = StoragePool(
        pool_id="stuck",
        medium="remote_cxl",
        capacity=1 << 20,
        page_size=1 << 20,
        handle=MaruHandle(region_id=1, offset=0, length=1 << 20, auth_token=1),
        owner_id="gone",
    )
    return started


def p99(samples: list[float]) -> float:
    ordered = sorted(samples)
    return ordered[min(len(ordered) - 1, int(len(ordered) * 0.99))]


def measure(lookup, duration_s: float, minimum: int = 200) -> list[float]:
    """Latencies of back-to-back lookups for ``duration_s``."""
    samples = []
    end = time.monotonic() + duration_s
    while time.monotonic() < end or len(samples) < minimum:
        start = time.perf_counter()
        lookup()
        samples.append(time.perf_counter() - start)
    return samples


def unchanged(base: list[float], loaded: list[float]) -> bool:
    """p99 within noise of the baseline and far below the RM delay."""
    limit = max(3 * p99(base), p99(base) + 0.025)
    return p99(loaded) <= limit and p99(loaded) < RM_DELAY_S / 4


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_lookup_p99_over_rpc_with_legacy_requests_stuck_on_the_rm():
    server = MaruServer(enable_cxl=False)
    port = free_port()
    address = f"tcp://127.0.0.1:{port}"
    rpc = RpcAsyncServer(server, host="127.0.0.1", port=port, num_workers=2)
    serving = threading.Thread(target=rpc.start, daemon=True)
    serving.start()
    time.sleep(0.2)
    wire = RpcWire(address, server.ms_service)
    stop = threading.Event()
    legacy_latency = []
    threads = []
    try:
        node = FakeNode(wire, "n1")
        assert node.open().status == Status.OK
        keys = [key(f"k{i}") for i in range(32)]
        node.commit(*[(k, node.loc(node.dram(), i)) for i, k in enumerate(keys)])
        worker = FakeWorker(wire, node)
        worker.open()

        def lookup():
            response = worker.lookup(*keys)
            assert response.status == Status.OK
            worker.unpin(*[item.pin_id for item in response.items])

        base = measure(lookup, 1.0)
        started = slow_rm_release(server)

        def legacy():
            # more legacy clients than legacy threads: all of them get stuck
            context = zmq.Context()
            sock = context.socket(zmq.REQ)
            sock.setsockopt(zmq.LINGER, 0)
            sock.setsockopt(zmq.RCVTIMEO, 30000)
            sock.connect(address)
            serializer = Serializer()
            frame = serializer.encode(MessageType.STORAGE, StorageRequest("usage"))
            try:
                while not stop.is_set():
                    t0 = time.monotonic()
                    sock.send(frame)
                    sock.recv()
                    legacy_latency.append(time.monotonic() - t0)
            finally:
                sock.close()
                context.term()

        threads = [threading.Thread(target=legacy, daemon=True) for _ in range(4)]
        for t in threads:
            t.start()
        while not started:
            time.sleep(0.01)
        loaded = measure(lookup, RM_DELAY_S + 0.5)
    finally:
        server.replica_directory._releasing.clear()  # no more slow releases
        stop.set()
        for t in threads:
            t.join(15)
        wire.close()
        rpc.stop()
        serving.join(5)
        server.close()

    assert max(legacy_latency) >= RM_DELAY_S * 0.9
    assert unchanged(base, loaded), (p99(base), p99(loaded))

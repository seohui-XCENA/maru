#!/usr/bin/env python3
"""Maru RPC Microbenchmark - Measures sync & async RPC performance.

Benchmarks throughput and latency of ZeroMQ-based RPC for single and
batch KV operations.  Supports sync (REQ-REP) and async (DEALER-ROUTER)
modes, and can run both for side-by-side comparison.

Usage:
    python benchmarks/rpc_benchmark.py                   # sync only (default)
    python benchmarks/rpc_benchmark.py --mode async      # async only
    python benchmarks/rpc_benchmark.py --mode both       # comparison
    python benchmarks/rpc_benchmark.py --clients 10 --ops 2000
"""

# =============================================================================
# Mock pyxcmp BEFORE any maru imports (hardware dependency)
# =============================================================================

import ctypes
import sys
from enum import IntEnum
from unittest.mock import MagicMock


class MockDaxType(IntEnum):
    DEV_DAX = 0
    FS_DAX = 1


class MockHandle:
    def __init__(self, region_id=0, offset=0, length=0, auth_token=0):
        self.region_id = region_id
        self.offset = offset
        self.length = length
        self.auth_token = auth_token

    def to_dict(self):
        return {
            "region_id": self.region_id,
            "offset": self.offset,
            "length": self.length,
            "auth_token": self.auth_token,
        }

    @staticmethod
    def from_dict(d):
        return MockHandle(
            region_id=d["region_id"],
            offset=d["offset"],
            length=d["length"],
            auth_token=d["auth_token"],
        )


class MockPoolInfo:
    def __init__(self):
        self.pool_id = 0
        self.dax_type = MockDaxType.DEV_DAX
        self.total_size = 0
        self.free_size = 0
        self.align_bytes = 4096

    def to_dict(self):
        return {
            "pool_id": self.pool_id,
            "dax_type": int(self.dax_type),
            "total_size": self.total_size,
            "free_size": self.free_size,
            "align_bytes": self.align_bytes,
        }

    @staticmethod
    def from_dict(d):
        info = MockPoolInfo()
        info.pool_id = d["pool_id"]
        info.dax_type = MockDaxType(d.get("dax_type", 0))
        info.total_size = d["total_size"]
        info.free_size = d["free_size"]
        info.align_bytes = d.get("align_bytes", 4096)
        return info


_alloc_counter = 0


def _mock_alloc(size):
    global _alloc_counter
    _alloc_counter += 1
    return MockHandle(region_id=_alloc_counter, offset=0, length=size, auth_token=12345)


def _mock_alloc_from_pool(size, pool_id):
    global _alloc_counter
    _alloc_counter += 1
    return MockHandle(region_id=_alloc_counter, offset=0, length=size, auth_token=12345)


_TEST_BUFFER_SIZE = 64 * 1024 * 1024
_test_buffer = (ctypes.c_char * _TEST_BUFFER_SIZE)()
_test_buffer_addr = ctypes.addressof(_test_buffer)
_mmap_offset = 0


def _mock_mmap(handle, prot, flags=0):
    global _mmap_offset
    addr = _test_buffer_addr + _mmap_offset
    _mmap_offset += handle.length
    if _mmap_offset > _TEST_BUFFER_SIZE:
        raise MemoryError("Test buffer exhausted")
    return addr


mock_pyxcmp = MagicMock()
mock_pyxcmp.DaxType = MockDaxType
mock_pyxcmp.Handle = MockHandle
mock_pyxcmp.PoolInfo = MockPoolInfo
mock_pyxcmp.PROT_NONE = 0x0
mock_pyxcmp.PROT_READ = 0x1
mock_pyxcmp.PROT_WRITE = 0x2
mock_pyxcmp.PROT_EXEC = 0x4
mock_pyxcmp.MAP_SHARED = 0x01
mock_pyxcmp.MAP_PRIVATE = 0x02
mock_pyxcmp.stats.return_value = []
mock_pyxcmp.alloc.side_effect = _mock_alloc
mock_pyxcmp.alloc_from_pool.side_effect = _mock_alloc_from_pool
mock_pyxcmp.free.return_value = None
mock_pyxcmp.mmap.side_effect = _mock_mmap
mock_pyxcmp.munmap.return_value = None

sys.modules["pyxcmp"] = mock_pyxcmp

# =============================================================================
# Now safe to import maru modules
# =============================================================================

import argparse  # noqa: E402
import logging  # noqa: E402
import socket  # noqa: E402
import threading  # noqa: E402
import time  # noqa: E402
from concurrent.futures import ThreadPoolExecutor, as_completed  # noqa: E402
from dataclasses import dataclass, field  # noqa: E402
from typing import Any  # noqa: E402

from maru_handler.rpc_async_client import RpcAsyncClient  # noqa: E402
from maru_handler.rpc_client import RpcClient  # noqa: E402
from maru_server.rpc_async_server import RpcAsyncServer  # noqa: E402
from maru_server.rpc_server import RpcServer  # noqa: E402
from maru_server.server import MaruServer  # noqa: E402

logger = logging.getLogger(__name__)


# =============================================================================
# Delayed Server (simulates real I/O)
# =============================================================================


class DelayedMaruServer(MaruServer):
    """MaruServer with configurable delay to simulate real I/O (CXL mmap, memcpy)."""

    def __init__(self, delay_us: int = 0):
        super().__init__()
        self._delay_s = delay_us / 1_000_000

    def _simulate_io(self):
        if self._delay_s > 0:
            time.sleep(self._delay_s)

    def register_kv(self, **kwargs):
        self._simulate_io()
        return super().register_kv(**kwargs)

    def lookup_kv(self, **kwargs):
        self._simulate_io()
        return super().lookup_kv(**kwargs)

    def exists_kv(self, **kwargs):
        self._simulate_io()
        return super().exists_kv(**kwargs)

    def delete_kv(self, **kwargs):
        self._simulate_io()
        return super().delete_kv(**kwargs)

    def batch_register_kv(self, entries):
        self._simulate_io()
        return super().batch_register_kv(entries)

    def batch_lookup_kv(self, keys):
        self._simulate_io()
        return super().batch_lookup_kv(keys)

    def batch_exists_kv(self, keys):
        self._simulate_io()
        return super().batch_exists_kv(keys)

    def request_alloc(self, **kwargs):
        self._simulate_io()
        return super().request_alloc(**kwargs)

    def return_alloc(self, **kwargs):
        self._simulate_io()
        return super().return_alloc(**kwargs)


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class BenchmarkConfig:
    """Benchmark configuration parameters."""

    num_clients: int = 5
    ops_per_client: int = 1000
    warmup_ops: int = 100
    batch_size: int = 10
    timeout_ms: int = 10000
    mode: str = "sync"  # "sync", "async", or "both"
    server_delay_us: int = 0


@dataclass
class OperationResult:
    """Result of benchmarking a single operation type."""

    name: str
    latencies_ns: list[int] = field(default_factory=list)
    total_ops: int = 0
    elapsed_s: float = 0.0

    @property
    def throughput(self) -> float:
        if self.elapsed_s <= 0:
            return 0.0
        return self.total_ops / self.elapsed_s

    @property
    def p50_ms(self) -> float:
        if not self.latencies_ns:
            return 0.0
        sorted_lats = sorted(self.latencies_ns)
        idx = len(sorted_lats) // 2
        return sorted_lats[idx] / 1_000_000

    @property
    def p99_ms(self) -> float:
        if not self.latencies_ns:
            return 0.0
        sorted_lats = sorted(self.latencies_ns)
        idx = int(len(sorted_lats) * 0.99)
        idx = min(idx, len(sorted_lats) - 1)
        return sorted_lats[idx] / 1_000_000


# =============================================================================
# Utility Functions
# =============================================================================


def find_available_port() -> int:
    """Find an available TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_server(port: int, mode: str = "sync", server_delay_us: int = 0) -> tuple:
    """Start RPC server + MaruServer in a background daemon thread."""
    if server_delay_us > 0:
        maru_server = DelayedMaruServer(delay_us=server_delay_us)
    else:
        maru_server = MaruServer()
    if mode == "async":
        rpc_server = RpcAsyncServer(
            maru_server, host="127.0.0.1", port=port, num_workers=4
        )
    else:
        rpc_server = RpcServer(maru_server, host="127.0.0.1", port=port)

    server_thread = threading.Thread(target=rpc_server.start, daemon=True)
    server_thread.start()

    # Wait briefly for server to bind
    time.sleep(0.3)
    return rpc_server, maru_server, server_thread


def create_client(port: int, config: BenchmarkConfig):
    """Create and connect an RPC client (sync or async based on config.mode)."""
    if config.mode == "async":
        client = RpcAsyncClient(
            server_url=f"tcp://127.0.0.1:{port}",
            timeout_ms=config.timeout_ms,
        )
    else:
        client = RpcClient(
            server_url=f"tcp://127.0.0.1:{port}",
            timeout_ms=config.timeout_ms,
        )
    client.connect()
    return client


# =============================================================================
# Pre-population: Allocate regions and register keys for lookup/exists
# =============================================================================


def prepopulate_keys(port: int, config: BenchmarkConfig) -> dict[int, int]:
    """
    Pre-populate the server with KV entries so lookup/exists have data.

    Returns:
        Mapping of client_id -> region_id for use during benchmark.
    """
    client = create_client(port, config)
    client_regions: dict[int, int] = {}

    try:
        for client_id in range(config.num_clients):
            # Request an allocation for this client
            resp = client.request_alloc(
                instance_id=f"bench-prepop-{client_id}",
                size=4096,
            )
            if not resp.success or resp.handle is None:
                raise RuntimeError(f"Failed to allocate region for client {client_id}")
            region_id = resp.handle.region_id
            client_regions[client_id] = region_id

            # Register keys that will be looked up / checked during benchmark
            total_keys = config.ops_per_client + config.warmup_ops
            for op_idx in range(total_keys):
                key = client_id * total_keys + op_idx
                client.register_kv(
                    key=key,
                    region_id=region_id,
                    kv_offset=op_idx * 64,
                    kv_length=64,
                )

            # Also pre-register batch keys (same total KV count as single ops)
            batch_total = total_keys
            batch_base = 10_000_000 + client_id * batch_total
            batch_iters = total_keys // config.batch_size
            for op_idx in range(batch_iters):
                entries = []
                for b in range(config.batch_size):
                    batch_key = batch_base + op_idx * config.batch_size + b
                    entries.append((batch_key, region_id, b * 64, 64))
                client.batch_register_kv(entries)

    finally:
        client.close()

    return client_regions


# =============================================================================
# Client Worker Functions
# =============================================================================


def run_single_ops_worker(
    client_id: int,
    port: int,
    config: BenchmarkConfig,
    region_id: int,
    barrier: threading.Barrier,
) -> dict[str, list[int]]:
    """Worker function for a single client running single KV operations."""
    client = create_client(port, config)
    latencies: dict[str, list[int]] = {
        "register_kv": [],
        "lookup_kv": [],
        "exists_kv": [],
    }

    total_keys = config.ops_per_client + config.warmup_ops
    key_base = client_id * total_keys

    try:
        # --- Warmup (not measured) ---
        for i in range(config.warmup_ops):
            key = key_base + i
            warmup_key = 20_000_000 + client_id * config.warmup_ops + i
            client.register_kv(
                key=warmup_key, region_id=region_id, kv_offset=i * 64, kv_length=64
            )
            client.lookup_kv(key=key)
            client.exists_kv(key=key)

        # --- Synchronize all clients before measurement ---
        barrier.wait()

        # --- Measured: register_kv ---
        for i in range(config.ops_per_client):
            reg_key = 30_000_000 + client_id * config.ops_per_client + i
            t0 = time.perf_counter_ns()
            client.register_kv(
                key=reg_key, region_id=region_id, kv_offset=i * 64, kv_length=64
            )
            t1 = time.perf_counter_ns()
            latencies["register_kv"].append(t1 - t0)

        # --- Measured: lookup_kv (keys pre-populated) ---
        for i in range(config.ops_per_client):
            key = key_base + i
            t0 = time.perf_counter_ns()
            client.lookup_kv(key=key)
            t1 = time.perf_counter_ns()
            latencies["lookup_kv"].append(t1 - t0)

        # --- Measured: exists_kv (keys pre-populated) ---
        for i in range(config.ops_per_client):
            key = key_base + i
            t0 = time.perf_counter_ns()
            client.exists_kv(key=key)
            t1 = time.perf_counter_ns()
            latencies["exists_kv"].append(t1 - t0)

    finally:
        client.close()

    return latencies


def run_batch_ops_worker(
    client_id: int,
    port: int,
    config: BenchmarkConfig,
    region_id: int,
    barrier: threading.Barrier,
) -> dict[str, list[int]]:
    """Worker function for a single client running batch KV operations."""
    client = create_client(port, config)
    latencies: dict[str, list[int]] = {
        "batch_register": [],
        "batch_lookup": [],
        "batch_exists": [],
    }

    total_keys = config.ops_per_client + config.warmup_ops
    batch_total = total_keys
    batch_base = 10_000_000 + client_id * batch_total

    # Same total KV ops as single: ops_per_client KV ops = batch_iters * batch_size
    batch_iters = config.ops_per_client // config.batch_size
    warmup_iters = config.warmup_ops // config.batch_size

    try:
        # --- Warmup (not measured) ---
        for i in range(warmup_iters):
            warmup_entries = [
                (
                    40_000_000
                    + client_id * config.warmup_ops
                    + i * config.batch_size
                    + b,
                    region_id,
                    b * 64,
                    64,
                )
                for b in range(config.batch_size)
            ]
            client.batch_register_kv(warmup_entries)

            lookup_keys = [
                batch_base + i * config.batch_size + b for b in range(config.batch_size)
            ]
            client.batch_lookup_kv(lookup_keys)
            client.batch_exists_kv(lookup_keys)

        # --- Synchronize all clients before measurement ---
        barrier.wait()

        # --- Measured: batch_register_kv ---
        for i in range(batch_iters):
            entries = [
                (
                    50_000_000
                    + client_id * config.ops_per_client
                    + i * config.batch_size
                    + b,
                    region_id,
                    b * 64,
                    64,
                )
                for b in range(config.batch_size)
            ]
            t0 = time.perf_counter_ns()
            client.batch_register_kv(entries)
            t1 = time.perf_counter_ns()
            latencies["batch_register"].append(t1 - t0)

        # --- Measured: batch_lookup_kv (keys pre-populated) ---
        for i in range(batch_iters):
            keys = [
                batch_base + i * config.batch_size + b for b in range(config.batch_size)
            ]
            t0 = time.perf_counter_ns()
            client.batch_lookup_kv(keys)
            t1 = time.perf_counter_ns()
            latencies["batch_lookup"].append(t1 - t0)

        # --- Measured: batch_exists_kv (keys pre-populated) ---
        for i in range(batch_iters):
            keys = [
                batch_base + i * config.batch_size + b for b in range(config.batch_size)
            ]
            t0 = time.perf_counter_ns()
            client.batch_exists_kv(keys)
            t1 = time.perf_counter_ns()
            latencies["batch_exists"].append(t1 - t0)

    finally:
        client.close()

    return latencies


# =============================================================================
# Benchmark Runner
# =============================================================================


def run_benchmark(config: BenchmarkConfig) -> list[OperationResult]:
    """Run the full benchmark suite and return results."""
    port = find_available_port()
    rpc_server, maru_server, server_thread = start_server(
        port, config.mode, config.server_delay_us
    )

    results: list[OperationResult] = []

    try:
        # Pre-populate server with data for lookup/exists operations
        print("Pre-populating server with KV entries...")
        client_regions = prepopulate_keys(port, config)
        print(f"  Registered keys for {config.num_clients} clients")

        # -----------------------------------------------------------------
        # Phase 1: Single operations
        # -----------------------------------------------------------------
        print(
            f"\nRunning single operations "
            f"({config.num_clients} clients x {config.ops_per_client} KV ops/client)..."
        )
        barrier = threading.Barrier(config.num_clients)

        single_futures = {}
        with ThreadPoolExecutor(max_workers=config.num_clients) as executor:
            t_single_start = time.perf_counter()
            for cid in range(config.num_clients):
                future = executor.submit(
                    run_single_ops_worker,
                    client_id=cid,
                    port=port,
                    config=config,
                    region_id=client_regions[cid],
                    barrier=barrier,
                )
                single_futures[future] = cid

            # Collect single-op results
            all_single_latencies: dict[str, list[int]] = {
                "register_kv": [],
                "lookup_kv": [],
                "exists_kv": [],
            }
            for future in as_completed(single_futures):
                worker_lats = future.result()
                for op_name, lats in worker_lats.items():
                    all_single_latencies[op_name].extend(lats)
            t_single_end = time.perf_counter()

        single_elapsed = t_single_end - t_single_start

        for op_name in ["register_kv", "lookup_kv", "exists_kv"]:
            lats = all_single_latencies[op_name]
            result = OperationResult(
                name=op_name,
                latencies_ns=lats,
                total_ops=len(lats),
                elapsed_s=single_elapsed,
            )
            results.append(result)

        # -----------------------------------------------------------------
        # Phase 2: Batch operations
        # -----------------------------------------------------------------
        batch_iters = config.ops_per_client // config.batch_size
        print(
            f"Running batch operations "
            f"({config.num_clients} clients x {batch_iters} batches x {config.batch_size} "
            f"= {config.ops_per_client} KV ops/client)..."
        )
        barrier = threading.Barrier(config.num_clients)

        batch_futures = {}
        with ThreadPoolExecutor(max_workers=config.num_clients) as executor:
            t_batch_start = time.perf_counter()
            for cid in range(config.num_clients):
                future = executor.submit(
                    run_batch_ops_worker,
                    client_id=cid,
                    port=port,
                    config=config,
                    region_id=client_regions[cid],
                    barrier=barrier,
                )
                batch_futures[future] = cid

            all_batch_latencies: dict[str, list[int]] = {
                "batch_register": [],
                "batch_lookup": [],
                "batch_exists": [],
            }
            for future in as_completed(batch_futures):
                worker_lats = future.result()
                for op_name, lats in worker_lats.items():
                    all_batch_latencies[op_name].extend(lats)
            t_batch_end = time.perf_counter()

        batch_elapsed = t_batch_end - t_batch_start

        for op_name in ["batch_register", "batch_lookup", "batch_exists"]:
            lats = all_batch_latencies[op_name]
            result = OperationResult(
                name=op_name,
                latencies_ns=lats,
                total_ops=len(lats) * config.batch_size,
                elapsed_s=batch_elapsed,
            )
            results.append(result)

    finally:
        rpc_server.stop()

    return results


# =============================================================================
# Report Formatting
# =============================================================================


def print_report(
    config: BenchmarkConfig,
    results: list[OperationResult],
    label: str | None = None,
) -> None:
    """Print a formatted benchmark report."""
    mode_label = label or config.mode.upper()
    print()
    print("=" * 60)
    print(f"  Maru RPC Microbenchmark ({mode_label})")
    print("=" * 60)
    print(f"Clients: {config.num_clients}, Ops/client: {config.ops_per_client}")
    if config.server_delay_us > 0:
        print(f"Server delay: {config.server_delay_us}us per operation (simulated I/O)")
    print()

    # Single operations
    single_ops = [r for r in results if not r.name.startswith("batch_")]
    print("--- Single Operations ---")
    for r in single_ops:
        print(
            f"{r.name + ':':16s} throughput={r.throughput:8.0f} ops/s  "
            f"p50={r.p50_ms:7.3f}ms  p99={r.p99_ms:7.3f}ms"
        )
    print()

    # Batch operations
    batch_ops = [r for r in results if r.name.startswith("batch_")]
    print(f"--- Batch Operations (batch_size={config.batch_size}) ---")
    for r in batch_ops:
        print(
            f"{r.name + ':':16s} throughput={r.throughput:8.0f} ops/s  "
            f"p50={r.p50_ms:7.3f}ms  p99={r.p99_ms:7.3f}ms"
        )
    print()

    # Totals
    total_ops = sum(r.total_ops for r in results)
    total_time = sum(r.elapsed_s for r in results)
    agg_throughput = total_ops / total_time if total_time > 0 else 0

    print("--- Total ---")
    print(f"Total operations: {total_ops}")
    print(f"Total time: {total_time:.2f}s")
    print(f"Aggregate throughput: {agg_throughput:.0f} ops/s")
    print("=" * 60)


# =============================================================================
# Main
# =============================================================================


# =============================================================================
# Pipeline Benchmark (v1 non-blocking API)
# =============================================================================


def run_pipeline_benchmark(config: BenchmarkConfig) -> dict[str, Any]:
    """
    Benchmark pipeline (non-blocking) vs sequential (blocking) async patterns.

    Scenarios:
    1. Sequential: N x register_kv() blocking calls
    2. Pipeline: N x register_kv_async() fire-all, then collect
    3. Pipeline + compute overlap: register_kv_async() + simulated compute interleaved
    4. Batch comparison: batch_register_kv() for reference

    Also sweeps max_inflight values to find optimal setting.

    Returns dict with all timing results.
    """
    port = find_available_port()
    rpc_server, maru_server, server_thread = start_server(
        port, "async", config.server_delay_us
    )

    results: dict[str, Any] = {}
    n = config.ops_per_client  # number of operations per scenario

    try:
        # Setup: allocate a region
        setup_client = RpcAsyncClient(
            f"tcp://127.0.0.1:{port}", timeout_ms=config.timeout_ms
        )
        setup_client.connect()
        alloc_resp = setup_client.request_alloc("pipeline-bench", 4096)
        assert alloc_resp.success
        region_id = alloc_resp.handle.region_id
        setup_client.close()

        # ---- Scenario 1: Sequential blocking ----
        client = RpcAsyncClient(f"tcp://127.0.0.1:{port}", timeout_ms=config.timeout_ms)
        client.connect()

        # Warmup
        for i in range(config.warmup_ops):
            client.register_kv(
                key=70_000_000 + i, region_id=region_id, kv_offset=i * 64, kv_length=64
            )

        key_base = 71_000_000
        t0 = time.perf_counter()
        for i in range(n):
            client.register_kv(
                key=key_base + i, region_id=region_id, kv_offset=i * 64, kv_length=64
            )
        seq_time = time.perf_counter() - t0
        client.close()

        results["sequential"] = {
            "ops": n,
            "time_s": seq_time,
            "throughput": n / seq_time if seq_time > 0 else 0,
            "per_call_ms": (seq_time / n * 1000) if n > 0 else 0,
        }

        # ---- Scenario 2: Pipeline (fire-all, collect-all) ----
        client = RpcAsyncClient(f"tcp://127.0.0.1:{port}", timeout_ms=config.timeout_ms)
        client.connect()

        # Warmup
        for i in range(config.warmup_ops):
            client.register_kv(
                key=72_000_000 + i, region_id=region_id, kv_offset=i * 64, kv_length=64
            )

        key_base = 73_000_000
        t0 = time.perf_counter()
        futures = []
        for i in range(n):
            f = client.register_kv_async(
                key=key_base + i, region_id=region_id, kv_offset=i * 64, kv_length=64
            )
            futures.append(f)
        for f in futures:
            f.result(timeout=30.0)
        pipe_time = time.perf_counter() - t0
        client.close()

        results["pipeline"] = {
            "ops": n,
            "time_s": pipe_time,
            "throughput": n / pipe_time if pipe_time > 0 else 0,
            "per_call_ms": (pipe_time / n * 1000) if n > 0 else 0,
        }

        # ---- Scenario 3: Pipeline + compute overlap ----
        compute_us = 50  # simulate 50us compute per item
        client = RpcAsyncClient(f"tcp://127.0.0.1:{port}", timeout_ms=config.timeout_ms)
        client.connect()

        key_base = 74_000_000
        t0 = time.perf_counter()
        futures = []
        for i in range(n):
            f = client.register_kv_async(
                key=key_base + i, region_id=region_id, kv_offset=i * 64, kv_length=64
            )
            futures.append(f)
            # Simulate compute work between sends
            _busy_wait_us(compute_us)
        for f in futures:
            f.result(timeout=30.0)
        overlap_time = time.perf_counter() - t0
        client.close()

        # Sequential with same compute
        client = RpcAsyncClient(f"tcp://127.0.0.1:{port}", timeout_ms=config.timeout_ms)
        client.connect()
        key_base = 75_000_000
        t0 = time.perf_counter()
        for i in range(n):
            client.register_kv(
                key=key_base + i, region_id=region_id, kv_offset=i * 64, kv_length=64
            )
            _busy_wait_us(compute_us)
        seq_compute_time = time.perf_counter() - t0
        client.close()

        results["pipeline_compute_overlap"] = {
            "ops": n,
            "compute_us": compute_us,
            "pipeline_time_s": overlap_time,
            "sequential_time_s": seq_compute_time,
            "speedup": seq_compute_time / overlap_time if overlap_time > 0 else 0,
        }

        # ---- Scenario 4: Batch reference ----
        client = RpcAsyncClient(f"tcp://127.0.0.1:{port}", timeout_ms=config.timeout_ms)
        client.connect()
        key_base = 76_000_000
        entries = [(key_base + i, region_id, i * 64, 64) for i in range(n)]
        t0 = time.perf_counter()
        client.batch_register_kv(entries)
        batch_time = time.perf_counter() - t0
        client.close()

        results["batch"] = {
            "ops": n,
            "time_s": batch_time,
            "throughput": n / batch_time if batch_time > 0 else 0,
        }

        # ---- max_inflight sweep ----
        sweep_results = {}
        for max_inf in [1, 4, 16, 64, 256]:
            client = RpcAsyncClient(
                f"tcp://127.0.0.1:{port}",
                timeout_ms=config.timeout_ms,
                max_inflight=max_inf,
            )
            client.connect()
            key_base = 77_000_000 + max_inf * n
            t0 = time.perf_counter()
            futs = []
            for i in range(n):
                f = client.register_kv_async(
                    key=key_base + i,
                    region_id=region_id,
                    kv_offset=i * 64,
                    kv_length=64,
                )
                futs.append(f)
            for f in futs:
                f.result(timeout=30.0)
            sweep_time = time.perf_counter() - t0
            client.close()
            sweep_results[max_inf] = {
                "time_s": sweep_time,
                "throughput": n / sweep_time if sweep_time > 0 else 0,
            }
        results["max_inflight_sweep"] = sweep_results

        # ---- run_coroutine_threadsafe overhead (Task 8) ----
        client = RpcAsyncClient(f"tcp://127.0.0.1:{port}", timeout_ms=config.timeout_ms)
        client.connect()
        # Warmup
        for _i in range(100):
            client.heartbeat()

        latencies_ns = []
        for _i in range(1000):
            t0 = time.perf_counter_ns()
            client.heartbeat()
            t1 = time.perf_counter_ns()
            latencies_ns.append(t1 - t0)
        client.close()

        sorted_lats = sorted(latencies_ns)
        results["rcts_overhead"] = {
            "samples": len(sorted_lats),
            "median_us": sorted_lats[len(sorted_lats) // 2] / 1000,
            "p95_us": sorted_lats[int(len(sorted_lats) * 0.95)] / 1000,
            "p99_us": sorted_lats[int(len(sorted_lats) * 0.99)] / 1000,
            "min_us": sorted_lats[0] / 1000,
            "max_us": sorted_lats[-1] / 1000,
        }

        # ---- High-load stability test (Task 9) ----
        stress_inflight = 256
        stress_ops = min(n * 10, 10000)
        client = RpcAsyncClient(
            f"tcp://127.0.0.1:{port}",
            timeout_ms=config.timeout_ms,
            max_inflight=stress_inflight,
        )
        client.connect()
        alloc2 = client.request_alloc("stress-test", 4096)
        stress_region = alloc2.handle.region_id
        key_base = 80_000_000

        errors = 0
        t0 = time.perf_counter()
        futs = []
        for i in range(stress_ops):
            f = client.register_kv_async(
                key=key_base + i,
                region_id=stress_region,
                kv_offset=i * 64,
                kv_length=64,
            )
            futs.append(f)
        for f in futs:
            try:
                f.result(timeout=30.0)
            except Exception:
                errors += 1
        stress_time = time.perf_counter() - t0
        client.close()

        results["stability"] = {
            "total_ops": stress_ops,
            "max_inflight": stress_inflight,
            "time_s": stress_time,
            "throughput": stress_ops / stress_time if stress_time > 0 else 0,
            "errors": errors,
            "error_rate": errors / stress_ops if stress_ops > 0 else 0,
        }

    finally:
        rpc_server.stop()

    return results


def _busy_wait_us(us: int) -> None:
    """Busy-wait for given microseconds (more precise than sleep for short durations)."""
    end = time.perf_counter() + us / 1_000_000
    while time.perf_counter() < end:
        pass


def print_pipeline_report(config: BenchmarkConfig, results: dict[str, Any]) -> None:
    """Print formatted pipeline benchmark results."""
    n = config.ops_per_client
    print()
    print("=" * 70)
    print("  Pipeline Benchmark Results (v1 asyncio)")
    print("=" * 70)
    print(f"Operations: {n}")
    if config.server_delay_us > 0:
        print(f"Server delay: {config.server_delay_us}us")
    print()

    # Core comparison
    seq = results["sequential"]
    pipe = results["pipeline"]
    batch = results["batch"]
    speedup = seq["time_s"] / pipe["time_s"] if pipe["time_s"] > 0 else 0

    print("--- Sequential vs Pipeline ---")
    print(
        f"  Sequential (blocking):  {seq['time_s']:.4f}s  ({seq['throughput']:.0f} ops/s, {seq['per_call_ms']:.3f} ms/call)"
    )
    print(
        f"  Pipeline (non-block):   {pipe['time_s']:.4f}s  ({pipe['throughput']:.0f} ops/s, {pipe['per_call_ms']:.3f} ms/call)"
    )
    print(f"  Pipeline speedup:       {speedup:.2f}x")
    print(
        f"  Batch (reference):      {batch['time_s']:.4f}s  ({batch['throughput']:.0f} ops/s)"
    )
    print()

    # Compute overlap
    if "pipeline_compute_overlap" in results:
        co = results["pipeline_compute_overlap"]
        print(f"--- Compute Overlap (compute={co['compute_us']}us/op) ---")
        print(f"  Seq + compute:   {co['sequential_time_s']:.4f}s")
        print(f"  Pipe + compute:  {co['pipeline_time_s']:.4f}s")
        print(f"  Overlap speedup: {co['speedup']:.2f}x")
        print()

    # max_inflight sweep
    if "max_inflight_sweep" in results:
        print("--- max_inflight Sweep ---")
        print(f"  {'max_inflight':>12s}  {'time_s':>8s}  {'throughput':>12s}")
        for mf, data in sorted(results["max_inflight_sweep"].items()):
            print(f"  {mf:>12d}  {data['time_s']:8.4f}  {data['throughput']:12.0f}")
        print()

    # run_coroutine_threadsafe overhead
    if "rcts_overhead" in results:
        oh = results["rcts_overhead"]
        print("--- run_coroutine_threadsafe Overhead (heartbeat round-trip) ---")
        print(f"  Samples: {oh['samples']}")
        print(f"  Median:  {oh['median_us']:.1f} us")
        print(f"  P95:     {oh['p95_us']:.1f} us")
        print(f"  P99:     {oh['p99_us']:.1f} us")
        print(f"  Min:     {oh['min_us']:.1f} us, Max: {oh['max_us']:.1f} us")
        print()

    # Stability
    if "stability" in results:
        st = results["stability"]
        print(
            f"--- High-load Stability (inflight={st['max_inflight']}, ops={st['total_ops']}) ---"
        )
        print(f"  Time:       {st['time_s']:.2f}s")
        print(f"  Throughput: {st['throughput']:.0f} ops/s")
        print(f"  Errors:     {st['errors']} ({st['error_rate']:.4%})")
        print()

    print("=" * 70)


def print_comparison(
    config: BenchmarkConfig,
    sync_results: list[OperationResult],
    async_results: list[OperationResult],
) -> None:
    """Print side-by-side comparison of sync vs async results."""
    print()
    print("=" * 78)
    print("  Maru RPC Microbenchmark — SYNC vs ASYNC Comparison")
    print("=" * 78)
    print(f"Clients: {config.num_clients}, Ops/client: {config.ops_per_client}")
    if config.server_delay_us > 0:
        print(f"Server delay: {config.server_delay_us}us per operation (simulated I/O)")
    print()

    sync_map = {r.name: r for r in sync_results}
    async_map = {r.name: r for r in async_results}

    header = f"{'Operation':18s} {'Sync ops/s':>12s} {'Async ops/s':>12s} {'Speedup':>8s}  {'Sync p50':>9s} {'Async p50':>9s} {'Sync p99':>9s} {'Async p99':>9s}"
    print(header)
    print("-" * len(header))

    for name in [
        "register_kv",
        "lookup_kv",
        "exists_kv",
        "batch_register",
        "batch_lookup",
        "batch_exists",
    ]:
        s = sync_map.get(name)
        a = async_map.get(name)
        if s and a:
            speedup = a.throughput / s.throughput if s.throughput > 0 else 0
            print(
                f"{name + ':':18s} {s.throughput:12.0f} {a.throughput:12.0f} "
                f"{speedup:7.2f}x  {s.p50_ms:8.3f}ms {a.p50_ms:8.3f}ms "
                f"{s.p99_ms:8.3f}ms {a.p99_ms:8.3f}ms"
            )

    # Totals
    sync_total_ops = sum(r.total_ops for r in sync_results)
    sync_total_time = sum(r.elapsed_s for r in sync_results)
    async_total_ops = sum(r.total_ops for r in async_results)
    async_total_time = sum(r.elapsed_s for r in async_results)
    sync_agg = sync_total_ops / sync_total_time if sync_total_time > 0 else 0
    async_agg = async_total_ops / async_total_time if async_total_time > 0 else 0
    total_speedup = async_agg / sync_agg if sync_agg > 0 else 0

    print()
    print(
        f"{'Aggregate:':18s} {sync_agg:12.0f} {async_agg:12.0f} {total_speedup:7.2f}x"
    )
    print("=" * 78)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Maru RPC Microbenchmark (Sync & Async)"
    )
    parser.add_argument(
        "--clients",
        type=int,
        default=5,
        help="Number of concurrent clients (default: 5)",
    )
    parser.add_argument(
        "--ops",
        type=int,
        default=1000,
        help="Operations per client (default: 1000)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=100,
        help="Warmup operations per client (default: 100)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Batch size for batch operations (default: 10)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10000,
        help="Client timeout in milliseconds (default: 10000)",
    )
    parser.add_argument(
        "--mode",
        choices=["sync", "async", "both", "pipeline"],
        default="sync",
        help="RPC mode: sync, async, both (comparison), pipeline (v1 non-blocking benchmark)",
    )
    parser.add_argument(
        "--server-delay-us",
        type=int,
        default=0,
        help="Artificial server-side delay per operation in microseconds (simulates CXL I/O). "
        "Demonstrates async advantage when server ops take real time. (default: 0)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )
        # Also set library package loggers to DEBUG
        for name in (
            "maru",
            "maru_common",
            "maru_handler",
            "maru_shm",
            "maru_server",
        ):
            logging.getLogger(name).setLevel(logging.DEBUG)
    else:
        # Suppress library logging for clean benchmark output
        # (library loggers have propagate=False, so basicConfig doesn't affect them)
        logging.basicConfig(level=logging.WARNING)
        for name in (
            "maru",
            "maru_common",
            "maru_handler",
            "maru_shm",
            "maru_server",
        ):
            logging.getLogger(name).setLevel(logging.WARNING)

    if args.mode == "pipeline":
        # Pipeline benchmark (v1 non-blocking API)
        pipeline_config = BenchmarkConfig(
            num_clients=1,
            ops_per_client=args.ops,
            warmup_ops=args.warmup,
            batch_size=args.batch_size,
            timeout_ms=args.timeout,
            mode="async",
            server_delay_us=args.server_delay_us,
        )
        print("Running pipeline benchmark (v1 asyncio non-blocking)...")
        pipeline_results = run_pipeline_benchmark(pipeline_config)
        print_pipeline_report(pipeline_config, pipeline_results)
        return

    if args.mode == "both":
        # Run sync then async, print comparison
        sync_config = BenchmarkConfig(
            num_clients=args.clients,
            ops_per_client=args.ops,
            warmup_ops=args.warmup,
            batch_size=args.batch_size,
            timeout_ms=args.timeout,
            mode="sync",
            server_delay_us=args.server_delay_us,
        )
        async_config = BenchmarkConfig(
            num_clients=args.clients,
            ops_per_client=args.ops,
            warmup_ops=args.warmup,
            batch_size=args.batch_size,
            timeout_ms=args.timeout,
            mode="async",
            server_delay_us=args.server_delay_us,
        )

        print("[1/2] Running SYNC benchmark...")
        sync_results = run_benchmark(sync_config)
        print_report(sync_config, sync_results, label="SYNC")

        print("\n[2/2] Running ASYNC benchmark...")
        async_results = run_benchmark(async_config)
        print_report(async_config, async_results, label="ASYNC")

        print_comparison(sync_config, sync_results, async_results)
    else:
        config = BenchmarkConfig(
            num_clients=args.clients,
            ops_per_client=args.ops,
            warmup_ops=args.warmup,
            batch_size=args.batch_size,
            timeout_ms=args.timeout,
            mode=args.mode,
            server_delay_us=args.server_delay_us,
        )
        results = run_benchmark(config)
        print_report(config, results)


if __name__ == "__main__":
    main()

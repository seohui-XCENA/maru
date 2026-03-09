"""Micro-benchmark for marufs syscall latencies.

Measures: open, ftruncate, mmap, name_offset, find_name, clear_name ioctls.

Usage:
    python tests/bench_marufs_syscalls.py [--mount /mnt/marufs] [--n 100]
"""

import argparse
import statistics
import time

from marufs import MarufsClient
from marufs.ioctl import PERM_ALL


def bench(name: str, fn, n: int) -> dict:
    """Run fn() n times, return latency stats in microseconds."""
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1e6)  # us
    return {
        "name": name,
        "n": n,
        "min": min(times),
        "p50": statistics.median(times),
        "p99": sorted(times)[int(n * 0.99)] if n >= 100 else max(times),
        "max": max(times),
        "avg": statistics.mean(times),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mount", default="/mnt/marufs")
    parser.add_argument("--n", type=int, default=100, help="iterations per syscall")
    parser.add_argument("--region-size", default="256M")
    args = parser.parse_args()

    # Parse region size
    s = args.region_size.upper()
    if s.endswith("G"):
        region_size = int(float(s[:-1]) * 1024**3)
    elif s.endswith("M"):
        region_size = int(float(s[:-1]) * 1024**2)
    else:
        region_size = int(s)

    client = MarufsClient(args.mount)
    region_name = "bench_syscall_test"
    chunk_size = 3 * 1024 * 1024  # 3MB (typical KV chunk)
    n = args.n

    # Cleanup from previous run
    try:
        client.delete_region(region_name)
    except OSError:
        pass

    print(
        f"marufs syscall benchmark: mount={args.mount} n={n} region_size={region_size}"
    )
    print(
        f"{'operation':<25} {'n':>5} {'min':>10} {'p50':>10} {'p99':>10} {'max':>10} {'avg':>10}  (μs)"
    )
    print("-" * 95)

    results = []

    # 1. create_region (open + ftruncate)
    def do_create():
        try:
            client.delete_region(region_name)
        except OSError:
            pass
        client.create_region(region_name, region_size)

    r = bench("create_region", do_create, min(n, 20))  # fewer iterations (has cleanup)
    results.append(r)

    # 2. perm_set_default
    fd = client.get_fd(region_name)
    r = bench("perm_set_default", lambda: client.perm_set_default(fd, PERM_ALL), n)
    results.append(r)

    # 3. mmap_region
    import mmap as mmap_mod

    def do_mmap():
        mm = client.mmap_region(
            fd, region_size, mmap_mod.PROT_READ | mmap_mod.PROT_WRITE
        )
        mm.close()

    r = bench("mmap_region", do_mmap, min(n, 20))
    results.append(r)

    # 4. name_offset ioctl (register key in global index)
    keys = [f"bench_key_{i:06d}" for i in range(n)]
    offsets = [(i * chunk_size) % region_size for i in range(n)]

    name_offset_times = []
    for i in range(n):
        t0 = time.perf_counter()
        client.name_offset(fd, keys[i], offsets[i])
        t1 = time.perf_counter()
        name_offset_times.append((t1 - t0) * 1e6)

    r = {
        "name": "name_offset (ioctl)",
        "n": n,
        "min": min(name_offset_times),
        "p50": statistics.median(name_offset_times),
        "p99": sorted(name_offset_times)[int(n * 0.99)]
        if n >= 100
        else max(name_offset_times),
        "max": max(name_offset_times),
        "avg": statistics.mean(name_offset_times),
    }
    results.append(r)

    # 5. find_name ioctl (lookup key from global index)
    dir_fd = client.get_dir_fd()

    find_name_times = []
    for i in range(n):
        t0 = time.perf_counter()
        client.find_name(dir_fd, keys[i])
        t1 = time.perf_counter()
        find_name_times.append((t1 - t0) * 1e6)

    r = {
        "name": "find_name (ioctl)",
        "n": n,
        "min": min(find_name_times),
        "p50": statistics.median(find_name_times),
        "p99": sorted(find_name_times)[int(n * 0.99)]
        if n >= 100
        else max(find_name_times),
        "max": max(find_name_times),
        "avg": statistics.mean(find_name_times),
    }
    results.append(r)

    # 6. find_name MISS (key not found)
    miss_times = []
    for i in range(n):
        t0 = time.perf_counter()
        client.find_name(dir_fd, f"nonexistent_key_{i:06d}")
        t1 = time.perf_counter()
        miss_times.append((t1 - t0) * 1e6)

    r = {
        "name": "find_name MISS (ioctl)",
        "n": n,
        "min": min(miss_times),
        "p50": statistics.median(miss_times),
        "p99": sorted(miss_times)[int(n * 0.99)] if n >= 100 else max(miss_times),
        "max": max(miss_times),
        "avg": statistics.mean(miss_times),
    }
    results.append(r)

    # 7. clear_name ioctl
    clear_times = []
    for i in range(n):
        t0 = time.perf_counter()
        client.clear_name(fd, keys[i])
        t1 = time.perf_counter()
        clear_times.append((t1 - t0) * 1e6)

    r = {
        "name": "clear_name (ioctl)",
        "n": n,
        "min": min(clear_times),
        "p50": statistics.median(clear_times),
        "p99": sorted(clear_times)[int(n * 0.99)] if n >= 100 else max(clear_times),
        "max": max(clear_times),
        "avg": statistics.mean(clear_times),
    }
    results.append(r)

    # Print results
    for r in results:
        print(
            f"{r['name']:<25} {r['n']:>5} {r['min']:>10.1f} {r['p50']:>10.1f} "
            f"{r['p99']:>10.1f} {r['max']:>10.1f} {r['avg']:>10.1f}  (μs)"
        )

    # Cleanup
    try:
        client.delete_region(region_name)
    except OSError:
        pass
    client.close()

    print("\nDone. All times in microseconds (μs).")


if __name__ == "__main__":
    main()

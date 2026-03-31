# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for CXL primitives (ntstore, clflush).

Uses anonymous mmap (DRAM) to test the primitives without requiring
a real CXL DAX device. On a single node, hardware cache coherence
ensures correctness, but ntstore/clflush are still exercised as
real x86 instructions.
"""

import ctypes
import mmap
import multiprocessing
import os
import struct
import time

import pytest

from maru_common.cxl_primitives import clflush, ntstore

# 4KB test region
TEST_REGION_SIZE = 4096


@pytest.fixture
def shared_mmap():
    """Create an anonymous shared mmap region (DRAM-backed)."""
    mm = mmap.mmap(-1, TEST_REGION_SIZE, mmap.MAP_SHARED | mmap.MAP_ANONYMOUS)
    yield mm
    mm.close()


def _get_mmap_ptr(mm: mmap.mmap) -> int:
    """Get the raw pointer address of an mmap region."""
    return ctypes.addressof(ctypes.c_char.from_buffer(mm))


class TestNtstore:
    def test_basic_write_read(self, shared_mmap):
        """ntstore writes data that can be read back."""
        ptr = _get_mmap_ptr(shared_mmap)
        data = b"hello CXL"

        ntstore(ptr, data)

        shared_mmap.seek(0)
        result = shared_mmap.read(len(data))
        assert result == data

    def test_write_exact_cacheline(self, shared_mmap):
        """ntstore can write exactly 64 bytes (1 cache line)."""
        ptr = _get_mmap_ptr(shared_mmap)
        data = bytes(range(64))

        ntstore(ptr, data)

        shared_mmap.seek(0)
        assert shared_mmap.read(64) == data

    def test_write_multiple_cachelines(self, shared_mmap):
        """ntstore can write across multiple cache lines."""
        ptr = _get_mmap_ptr(shared_mmap)
        data = bytes(range(256)) * 4  # 1024 bytes = 16 cache lines

        ntstore(ptr, data)

        shared_mmap.seek(0)
        assert shared_mmap.read(len(data)) == data

    def test_write_small(self, shared_mmap):
        """ntstore handles small writes (< 16 bytes)."""
        ptr = _get_mmap_ptr(shared_mmap)

        for size in [1, 3, 7, 15]:
            data = bytes(range(size))
            ntstore(ptr, data)

            shared_mmap.seek(0)
            assert shared_mmap.read(size) == data

    def test_write_at_offset(self, shared_mmap):
        """ntstore can write at a non-zero offset."""
        ptr = _get_mmap_ptr(shared_mmap)
        offset = 128
        data = b"offset write"

        ntstore(ptr + offset, data)

        shared_mmap.seek(offset)
        assert shared_mmap.read(len(data)) == data

    def test_write_unaligned(self, shared_mmap):
        """ntstore handles unaligned destination addresses."""
        ptr = _get_mmap_ptr(shared_mmap)

        for offset in [1, 3, 7, 13, 33]:
            data = b"A" * 100
            ntstore(ptr + offset, data)

            shared_mmap.seek(offset)
            assert shared_mmap.read(len(data)) == data

    def test_overwrite(self, shared_mmap):
        """ntstore overwrites previous data."""
        ptr = _get_mmap_ptr(shared_mmap)

        ntstore(ptr, b"first value\x00\x00\x00\x00\x00")
        ntstore(ptr, b"second value")

        shared_mmap.seek(0)
        assert shared_mmap.read(12) == b"second value"

    def test_write_struct(self, shared_mmap):
        """ntstore can write packed struct data (simulates RPC slot)."""
        ptr = _get_mmap_ptr(shared_mmap)

        status = 1  # REQ_READY
        seq_num = 42
        payload = b"lookup_kv"
        packed = struct.pack("<II", status, seq_num) + payload

        ntstore(ptr, packed)

        shared_mmap.seek(0)
        raw = shared_mmap.read(len(packed))
        s, seq = struct.unpack_from("<II", raw, 0)
        assert s == 1
        assert seq == 42
        assert raw[8:8 + len(payload)] == payload


class TestClflush:
    def test_flush_and_read(self, shared_mmap):
        """clflush + read returns the latest written value."""
        ptr = _get_mmap_ptr(shared_mmap)

        # Write via regular store (through mmap)
        shared_mmap.seek(0)
        shared_mmap.write(b"before flush")

        # Flush and read
        clflush(ptr, 64)

        shared_mmap.seek(0)
        assert shared_mmap.read(12) == b"before flush"

    def test_flush_range(self, shared_mmap):
        """clflush can flush a range spanning multiple cache lines."""
        ptr = _get_mmap_ptr(shared_mmap)

        shared_mmap.seek(0)
        data = b"X" * 256  # 4 cache lines
        shared_mmap.write(data)

        # Flush all 4 cache lines
        clflush(ptr, 256)

        shared_mmap.seek(0)
        assert shared_mmap.read(256) == data

    def test_flush_unaligned(self, shared_mmap):
        """clflush handles unaligned address (rounds down to cacheline)."""
        ptr = _get_mmap_ptr(shared_mmap)

        shared_mmap.seek(13)
        shared_mmap.write(b"unaligned")

        # Flush from unaligned address
        clflush(ptr + 13, 9)

        shared_mmap.seek(13)
        assert shared_mmap.read(9) == b"unaligned"


class TestNtstoreClflushRoundtrip:
    """Simulate CXL-RPC: writer uses ntstore, reader uses clflush + load."""

    def test_single_process_roundtrip(self, shared_mmap):
        """ntstore then clflush+read in same process."""
        ptr = _get_mmap_ptr(shared_mmap)

        # Writer: ntstore
        data = struct.pack("<II", 1, 7) + b"request payload here"
        ntstore(ptr, data)

        # Reader: clflush + load
        clflush(ptr, 64)
        shared_mmap.seek(0)
        raw = shared_mmap.read(len(data))
        assert raw == data

    def test_cross_process_roundtrip(self):
        """ntstore in one process, clflush+load in another (via shared mmap)."""
        # Use /dev/shm for cross-process shared memory
        shm_path = "/dev/shm/maru_cxl_test"
        size = TEST_REGION_SIZE

        try:
            # Create shared file
            fd = os.open(shm_path, os.O_CREAT | os.O_RDWR, 0o666)
            os.ftruncate(fd, size)
            os.close(fd)

            def writer():
                fd = os.open(shm_path, os.O_RDWR)
                mm = mmap.mmap(fd, size)
                ptr = ctypes.addressof(ctypes.c_char.from_buffer(mm))

                # Signal ready at offset 64, write data at offset 0
                data = b"cross process data!!"
                ntstore(ptr, data)
                # Signal: write 1 at offset 64
                ntstore(ptr + 64, struct.pack("<I", 1))

                mm.close()
                os.close(fd)

            def reader():
                fd = os.open(shm_path, os.O_RDWR)
                mm = mmap.mmap(fd, size)
                ptr = ctypes.addressof(ctypes.c_char.from_buffer(mm))

                # Poll for signal at offset 64
                deadline = time.monotonic() + 5.0
                while time.monotonic() < deadline:
                    clflush(ptr + 64, 4)
                    mm.seek(64)
                    sig = struct.unpack("<I", mm.read(4))[0]
                    if sig == 1:
                        break
                    time.sleep(0.001)
                else:
                    raise TimeoutError("reader: writer signal not received")

                # Read data
                clflush(ptr, 64)
                mm.seek(0)
                result = mm.read(20)

                mm.close()
                os.close(fd)
                return result

            # Run writer in child process
            p = multiprocessing.Process(target=writer)
            p.start()

            result = reader()
            p.join(timeout=5)
            assert p.exitcode == 0
            assert result == b"cross process data!!"

        finally:
            if os.path.exists(shm_path):
                os.unlink(shm_path)

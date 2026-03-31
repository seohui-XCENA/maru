# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Python wrapper for CXL memory primitives (ntstore, clflush).

These wrap x86 non-temporal store and cache line flush instructions
via a small C shared library. Used for CXL-RPC communication where
cross-node cache coherence is not provided by hardware.

Usage:
    from maru_common.cxl_primitives import ntstore, clflush

    # Write to CXL memory (cache-bypassing, visible to other nodes)
    ntstore(mmap_ptr + offset, data_bytes)

    # Invalidate local cache before reading (get fresh data from CXL)
    clflush(mmap_ptr + offset, length)
"""

import ctypes
import os
import pathlib

_LIB_NAME = "libcxl_primitives.so"
_LIB_DIR = pathlib.Path(__file__).parent


def _load_lib() -> ctypes.CDLL:
    lib_path = _LIB_DIR / _LIB_NAME
    if not lib_path.exists():
        raise FileNotFoundError(
            f"{_LIB_NAME} not found at {lib_path}. "
            "Build it with: gcc -O2 -msse2 -shared -fPIC "
            f"-o {lib_path} {_LIB_DIR / 'cxl_primitives.c'}"
        )
    lib = ctypes.CDLL(str(lib_path))

    # void ntstore(void *dst, const void *src, size_t len)
    lib.ntstore.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t]
    lib.ntstore.restype = None

    # void clflush(const void *addr, size_t len)
    lib.clflush.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    lib.clflush.restype = None

    return lib


_lib = _load_lib()


def ntstore(dst: int, data: bytes) -> None:
    """Non-temporal store: write `data` to address `dst`, bypassing CPU cache.

    The data is written directly to CXL memory and is immediately visible
    to other nodes without requiring explicit flush.

    Args:
        dst: Destination address (integer, from mmap pointer arithmetic).
        data: Bytes to write.
    """
    src = ctypes.c_char_p(data)
    _lib.ntstore(ctypes.c_void_p(dst), src, len(data))


def clflush(addr: int, length: int) -> None:
    """Flush cache lines covering [addr, addr+length).

    After this call, subsequent loads from this address range will
    fetch fresh data from CXL memory instead of stale cached copies.

    Args:
        addr: Start address (integer, from mmap pointer arithmetic).
        length: Number of bytes to flush.
    """
    _lib.clflush(ctypes.c_void_p(addr), length)

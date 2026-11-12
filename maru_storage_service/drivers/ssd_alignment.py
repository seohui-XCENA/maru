# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Linux direct I/O limits and caller-owned aligned buffers."""

import ctypes
import errno
import fcntl
import math
import mmap
import os
import struct
from contextlib import contextmanager
from dataclasses import dataclass

_STATX_DIOALIGN = 0x2000
_AT_EMPTY_PATH = 0x1000
_XFS_IOC_DIOINFO = 0x800C581E
_LIBC = ctypes.CDLL(None, use_errno=True)


class SsdAlignmentError(RuntimeError):
    """Direct I/O alignment is unsupported or cannot be established."""


@dataclass(frozen=True)
class DioAlignment:
    memory: int
    offset: int

    def __post_init__(self):
        for value in (self.memory, self.offset):
            if value <= 0 or value & (value - 1) or value > 1 << 20:
                raise SsdAlignmentError(f"invalid direct I/O alignment: {value}")

    @property
    def step(self) -> int:
        return math.lcm(self.memory, self.offset)


def _statx_alignment(fd: int) -> DioAlignment | None:
    statx = getattr(_LIBC, "statx", None)
    if statx is None:
        return None
    statx.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_uint,
        ctypes.c_void_p,
    ]
    statx.restype = ctypes.c_int
    # Linux statx ABI: 256 bytes, with the two DIO fields at 0x98.
    result = ctypes.create_string_buffer(256)
    if statx(fd, b"", _AT_EMPTY_PATH, _STATX_DIOALIGN, result) < 0:
        error = ctypes.get_errno()
        if error in (errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP):
            return None
        raise OSError(error, os.strerror(error))
    if not struct.unpack_from("=I", result)[0] & _STATX_DIOALIGN:
        return None
    return DioAlignment(*struct.unpack_from("=II", result, 0x98))


def direct_alignment(fd: int) -> DioAlignment:
    alignment = _statx_alignment(fd)
    if alignment is not None:
        return alignment
    # Older XFS kernels report the inode's limits through DIOINFO.
    try:
        result = fcntl.ioctl(fd, _XFS_IOC_DIOINFO, bytes(12))
    except OSError as e:
        raise SsdAlignmentError(
            "direct I/O alignment is unknown on this filesystem"
        ) from e
    memory, offset, maximum = struct.unpack("=III", result)
    if maximum < offset:
        raise SsdAlignmentError("invalid XFS direct I/O limits")
    return DioAlignment(memory, offset)


def round_up(size: int, alignment: int) -> int:
    return (size + alignment - 1) // alignment * alignment


def buffer_address(view: memoryview) -> int:
    return ctypes.addressof(ctypes.c_char.from_buffer(view))


@contextmanager
def aligned_buffer(size: int, alignment: int):
    """The owner must retain this context until its synchronous I/O ends."""
    if size <= 0 or alignment <= 0:
        raise ValueError("buffer size and alignment must be positive")
    with mmap.mmap(-1, size + alignment - 1) as allocation:
        with memoryview(allocation) as raw:
            start = -buffer_address(raw) % alignment
            with raw[start : start + size] as view:
                yield view

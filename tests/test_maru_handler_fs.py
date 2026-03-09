"""Tests for MaruHandlerFs with marufs Global Partitioned Index.

Uses tmpdir as mock marufs mount point. ioctl operations (name_offset,
find_name, clear_name) are patched with an in-memory global dict since
marufs kernel module is not available in test environments.
"""

import errno
import os
import tempfile
from unittest.mock import patch

import pytest

from maru_common import MaruConfig
from maru_handler.handler_fs import (
    MaruHandlerFs,
    _key_to_name,
    _region_name_for_instance,
)
from maru_handler.memory import MemoryInfo
from marufs.ioctl import MARUFS_NAME_MAX

# =============================================================================
# ioctl Mock — global index backed by in-memory dict
# =============================================================================


class IoctlMock:
    """In-memory mock for marufs ioctl global index operations.

    Simulates the marufs global partitioned index:
    - NAME_OFFSET: registers name → (region_filename, offset) in global index
    - FIND_NAME: searches global index, returns (region_name, offset)
    - CLEAR_NAME: removes name from global index

    The global index is keyed by name_bytes alone (not per-inode),
    matching real marufs behavior where FIND_NAME searches across all regions.
    """

    def __init__(self):
        # Global index: name_bytes → (region_filename, offset)
        self.store: dict[bytes, tuple[str, int]] = {}

    def _fd_to_filename(self, fd: int) -> str:
        """Resolve fd to filename via /proc/self/fd symlink."""
        try:
            path = os.readlink(f"/proc/self/fd/{fd}")
            return os.path.basename(path)
        except OSError:
            return f"unknown_fd_{fd}"

    def __call__(self, fd, request, arg=None, mutate_flag=None):
        from marufs.ioctl import (
            MARUFS_IOC_CLEAR_NAME,
            MARUFS_IOC_FIND_NAME,
            MARUFS_IOC_NAME_OFFSET,
        )

        if request == MARUFS_IOC_NAME_OFFSET:
            name_bytes = bytes(arg.name).rstrip(b"\x00")
            region_name = self._fd_to_filename(fd)
            self.store[name_bytes] = (region_name, arg.offset)
            return 0

        elif request == MARUFS_IOC_FIND_NAME:
            name_bytes = bytes(arg.name).rstrip(b"\x00")
            if name_bytes not in self.store:
                raise OSError(errno.ENOENT, "not found")
            region_name, offset = self.store[name_bytes]
            arg.region_name = region_name.encode("utf-8")
            arg.offset = offset
            return 0

        elif request == MARUFS_IOC_CLEAR_NAME:
            name_bytes = bytes(arg.name).rstrip(b"\x00")
            self.store.pop(name_bytes, None)
            return 0

        # Ignore other ioctl commands (permissions, etc.)
        return 0


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mount_dir():
    """Create a temporary directory simulating an marufs mount point."""
    with tempfile.TemporaryDirectory(prefix="marufs_test_") as d:
        yield d


@pytest.fixture
def ioctl_mock():
    """Provide an IoctlMock instance and patch fcntl.ioctl."""
    mock = IoctlMock()
    with patch("marufs.client.fcntl.ioctl", side_effect=mock):
        yield mock


@pytest.fixture
def config(mount_dir):
    """Create a MaruConfig for marufs/fs mode with small pool for testing."""
    return MaruConfig(
        mount_path=mount_dir,
        pool_size=4096,  # 4KB pool (4 pages)
        chunk_size_bytes=1024,  # 1KB pages
        auto_connect=False,
    )


@pytest.fixture
def handler(config, ioctl_mock):
    """Create and connect a MaruHandlerFs for testing."""
    h = MaruHandlerFs(config)
    assert h.connect()
    yield h
    h.close()


# =============================================================================
# Unit Tests — Pure Functions
# =============================================================================


class TestKeyToName:
    """Test _key_to_name encoding (string keys, direct use)."""

    def test_passthrough_short_key(self):
        key = "model@1@0@abc123@float16"
        assert _key_to_name(key) == key

    def test_passthrough_typical_key(self):
        key = "meta-llama/Llama-3.1-8B-Instruct@1@0@deadbeef01234567@float16"
        result = _key_to_name(key)
        assert result == key
        assert len(result.encode("utf-8")) <= MARUFS_NAME_MAX

    def test_truncation_long_key(self):
        key = "A" * 200
        result = _key_to_name(key)
        assert len(result.encode("utf-8")) <= MARUFS_NAME_MAX

    def test_empty_key(self):
        assert _key_to_name("") == ""

    def test_max_length_key(self):
        key = "x" * MARUFS_NAME_MAX
        assert _key_to_name(key) == key


class TestRegionNaming:
    """Test _region_name_for_instance."""

    def test_format(self):
        name = _region_name_for_instance("abcd-1234-5678-9abc", 0)
        assert name.startswith("maru_")
        assert name.endswith("_0000")

    def test_length_within_limit(self):
        name = _region_name_for_instance("a" * 40, 9999)
        assert len(name) <= MARUFS_NAME_MAX


# =============================================================================
# Integration Tests — Handler Lifecycle
# =============================================================================


class TestHandlerInit:
    """Test handler initialization and connection."""

    def test_connect(self, config, ioctl_mock):
        h = MaruHandlerFs(config)
        assert not h.connected
        assert h.connect()
        assert h.connected
        h.close()
        assert not h.connected

    def test_double_connect(self, handler):
        assert handler.connect()  # already connected — no-op

    def test_context_manager(self, config, ioctl_mock):
        config.auto_connect = True
        with MaruHandlerFs(config) as h:
            assert h.connected
        assert not h.connected


class TestHandlerStore:
    """Test store operations."""

    def test_store_basic(self, handler):
        data = b"hello world"
        info = MemoryInfo(view=memoryview(bytearray(data)))
        assert handler.store(key="model@1@0@abc@f16", info=info)

    def test_store_exists(self, handler):
        info = MemoryInfo(view=memoryview(bytearray(b"data")))
        handler.store(key="model@1@0@42@f16", info=info)
        assert handler.exists("model@1@0@42@f16")

    def test_store_overwrite(self, handler):
        key = "model@1@0@1@f16"
        info1 = MemoryInfo(view=memoryview(bytearray(b"first")))
        info2 = MemoryInfo(view=memoryview(bytearray(b"second")))
        assert handler.store(key=key, info=info1)
        assert handler.store(key=key, info=info2)
        result = handler.retrieve(key)
        assert result is not None
        assert bytes(result.view[:6]) == b"second"

    def test_store_too_large(self, handler):
        # chunk_size is 1024 — data larger than that should fail
        big = bytearray(2000)
        info = MemoryInfo(view=memoryview(big))
        assert not handler.store(key="model@1@0@big@f16", info=info)

    def test_store_with_prefix(self, handler):
        key = "model@1@0@pfx@f16"
        prefix = b"\x01\x02"
        data = bytearray(b"payload")
        info = MemoryInfo(view=memoryview(data))
        assert handler.store(key=key, info=info, prefix=prefix)

        result = handler.retrieve(key)
        assert result is not None
        assert bytes(result.view[:2]) == prefix
        assert bytes(result.view[2 : 2 + len(data)]) == data

    def test_store_fills_region(self, handler):
        # 4KB pool / 1KB chunk = 4 pages
        results = []
        for i in range(4):
            info = MemoryInfo(view=memoryview(bytearray(100)))
            results.append(handler.store(key=f"model@1@0@{i}@f16", info=info))
        assert all(results)

    def test_store_auto_expand(self, handler):
        # Fill first region (4 pages), then one more triggers expansion
        for i in range(4):
            info = MemoryInfo(view=memoryview(bytearray(100)))
            handler.store(key=f"model@1@0@{i}@f16", info=info)

        info = MemoryInfo(view=memoryview(bytearray(100)))
        assert handler.store(key="model@1@0@999@f16", info=info)

    def test_store_registers_in_global_index(self, handler, ioctl_mock):
        key = "model@1@0@abcd@f16"
        info = MemoryInfo(view=memoryview(bytearray(b"data")))
        handler.store(key=key, info=info)

        # Verify the key is in the ioctl mock's global store
        key_name = _key_to_name(key).encode("utf-8")
        assert key_name in ioctl_mock.store, "Key not found in global index"


class TestHandlerRetrieve:
    """Test retrieve operations."""

    def test_retrieve_basic(self, handler):
        key = "model@1@0@1@f16"
        data = bytearray(b"test data 123")
        info = MemoryInfo(view=memoryview(data))
        handler.store(key=key, info=info)

        result = handler.retrieve(key)
        assert result is not None
        assert bytes(result.view[: len(data)]) == data

    def test_retrieve_not_found(self, handler):
        assert handler.retrieve("model@1@0@missing@f16") is None

    def test_retrieve_via_local_cache(self, handler):
        key = "model@1@0@cached@f16"
        info = MemoryInfo(view=memoryview(bytearray(b"cached")))
        handler.store(key=key, info=info)
        # Second retrieve should hit local cache
        result = handler.retrieve(key)
        assert result is not None

    def test_retrieve_via_global_index(self, handler, ioctl_mock):
        """Simulate cross-instance lookup via global index."""
        key = "model@1@0@77@f16"
        info = MemoryInfo(view=memoryview(bytearray(b"data")))
        handler.store(key=key, info=info)

        # Clear local cache to force global index lookup
        handler._key_to_location.clear()

        result = handler.retrieve(key)
        assert result is not None
        assert bytes(result.view[:4]) == b"data"

    def test_retrieve_multiple(self, handler):
        for i in range(3):
            key = f"model@1@0@{i}@f16"
            data = bytearray(f"value_{i}".encode())
            handler.store(key=key, info=MemoryInfo(view=memoryview(data)))

        for i in range(3):
            key = f"model@1@0@{i}@f16"
            result = handler.retrieve(key)
            assert result is not None
            expected = f"value_{i}".encode()
            assert bytes(result.view[: len(expected)]) == expected


class TestHandlerExists:
    """Test exists operations."""

    def test_exists_true(self, handler):
        key = "model@1@0@1@f16"
        info = MemoryInfo(view=memoryview(bytearray(b"x")))
        handler.store(key=key, info=info)
        assert handler.exists(key)

    def test_exists_false(self, handler):
        assert not handler.exists("model@1@0@nope@f16")

    def test_exists_via_global_index(self, handler, ioctl_mock):
        key = "model@1@0@42@f16"
        info = MemoryInfo(view=memoryview(bytearray(b"x")))
        handler.store(key=key, info=info)
        handler._key_to_location.clear()
        assert handler.exists(key)


class TestHandlerDelete:
    """Test delete operations."""

    def test_delete_basic(self, handler):
        key = "model@1@0@1@f16"
        info = MemoryInfo(view=memoryview(bytearray(b"data")))
        handler.store(key=key, info=info)
        assert handler.delete(key)
        assert not handler.exists(key)

    def test_delete_not_found(self, handler):
        assert not handler.delete("model@1@0@nope@f16")

    def test_delete_clears_global_index(self, handler, ioctl_mock):
        key = "model@1@0@1@f16"
        info = MemoryInfo(view=memoryview(bytearray(b"data")))
        handler.store(key=key, info=info)

        key_name = _key_to_name(key).encode("utf-8")
        assert key_name in ioctl_mock.store

        handler.delete(key)
        assert key_name not in ioctl_mock.store

    def test_delete_frees_page(self, handler):
        # Fill all 4 pages
        for i in range(4):
            info = MemoryInfo(view=memoryview(bytearray(100)))
            handler.store(key=f"model@1@0@{i}@f16", info=info)

        # Delete one — should free a page
        handler.delete("model@1@0@0@f16")

        # Should be able to store again without expansion
        info = MemoryInfo(view=memoryview(bytearray(100)))
        assert handler.store(key="model@1@0@100@f16", info=info)


class TestHandlerBatch:
    """Test batch operations."""

    def test_batch_store(self, handler):
        keys = ["model@1@0@1@f16", "model@1@0@2@f16", "model@1@0@3@f16"]
        infos = [MemoryInfo(view=memoryview(bytearray(b"v"))) for _ in range(3)]
        results = handler.batch_store(keys, infos)
        assert results == [True, True, True]

    def test_batch_retrieve(self, handler):
        for i in range(3):
            handler.store(
                key=f"model@1@0@{i}@f16",
                info=MemoryInfo(view=memoryview(bytearray(b"d"))),
            )
        results = handler.batch_retrieve(
            [
                "model@1@0@0@f16",
                "model@1@0@1@f16",
                "model@1@0@2@f16",
                "model@1@0@999@f16",
            ]
        )
        assert results[0] is not None
        assert results[1] is not None
        assert results[2] is not None
        assert results[3] is None

    def test_batch_exists(self, handler):
        handler.store(
            key="model@1@0@1@f16",
            info=MemoryInfo(view=memoryview(bytearray(b"x"))),
        )
        results = handler.batch_exists(["model@1@0@1@f16", "model@1@0@2@f16"])
        assert results == [True, False]

    def test_batch_store_length_mismatch(self, handler):
        with pytest.raises(ValueError):
            handler.batch_store(
                ["k1", "k2"], [MemoryInfo(view=memoryview(bytearray(b"x")))]
            )


class TestCrossInstance:
    """Test cross-instance KV sharing via global index."""

    def test_cross_instance_retrieve(self, config, ioctl_mock, mount_dir):
        """Key stored by instance A should be retrievable by instance B."""
        h1 = MaruHandlerFs(config)
        h1.connect()

        key = "model@1@0@shared@f16"
        data = bytearray(b"shared data")
        h1.store(key=key, info=MemoryInfo(view=memoryview(data)))

        h2 = MaruHandlerFs(
            MaruConfig(
                mount_path=mount_dir,
                pool_size=4096,
                chunk_size_bytes=1024,
                auto_connect=False,
            )
        )
        h2.connect()

        # h2 should find key via the global index
        result = h2.retrieve(key)
        assert result is not None
        assert bytes(result.view[: len(data)]) == data

        h1.close()
        h2.close()


class TestHandlerStats:
    """Test get_stats."""

    def test_stats_structure(self, handler):
        stats = handler.get_stats()
        assert "store_regions" in stats
        assert "key_count" in stats

    def test_stats_key_count(self, handler):
        for i in range(3):
            handler.store(
                key=f"model@1@0@{i}@f16",
                info=MemoryInfo(view=memoryview(bytearray(b"x"))),
            )
        assert handler.get_stats()["key_count"] == 3


class TestHandlerHealthcheck:
    """Test healthcheck."""

    def test_healthy(self, handler):
        assert handler.healthcheck()

    def test_unhealthy_after_close(self, config, ioctl_mock):
        h = MaruHandlerFs(config)
        h.connect()
        h.close()
        assert not h.healthcheck()


class TestHandlerErrorCases:
    """Test error handling."""

    def test_store_before_connect(self, config, ioctl_mock):
        h = MaruHandlerFs(config)
        with pytest.raises(RuntimeError, match="Not connected"):
            h.store(key="k", info=MemoryInfo(view=memoryview(bytearray(b"x"))))

    def test_retrieve_before_connect(self, config, ioctl_mock):
        h = MaruHandlerFs(config)
        with pytest.raises(RuntimeError, match="Not connected"):
            h.retrieve("k")

    def test_operations_after_close(self, config, ioctl_mock):
        h = MaruHandlerFs(config)
        h.connect()
        h.close()
        with pytest.raises(RuntimeError):
            h.store(key="k", info=MemoryInfo(view=memoryview(bytearray(b"x"))))

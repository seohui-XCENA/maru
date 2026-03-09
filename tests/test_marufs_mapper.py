"""Unit tests for MarufsMapper.

VFS operations are exercised against a real tmpfs directory (tempfile.mkdtemp).
CUDA pinning is skipped in CI (no GPU).
"""

import os
import tempfile
from unittest.mock import patch

import pytest

from maru_handler.memory.marufs_mapper import MarufsMappedRegion, MarufsMapper
from marufs.client import MarufsClient


@pytest.fixture()
def tmpdir():
    """Provide a temporary directory that simulates an marufs mount point."""
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture()
def marufs_client(tmpdir):
    """MarufsClient backed by a tmpfs directory (perm ioctls mocked)."""
    c = MarufsClient(tmpdir)
    with patch.object(c, "perm_set_default"):
        yield c
    c.close()


@pytest.fixture()
def mapper(marufs_client):
    """MarufsMapper backed by the tmpfs MarufsClient."""
    m = MarufsMapper(marufs_client)
    yield m
    m.close()


# ---------------------------------------------------------------------------
# map_owned_region
# ---------------------------------------------------------------------------


class TestMapOwnedRegion:
    def test_map_owned_region_returns_region(self, mapper):
        """map_owned_region returns an MarufsMappedRegion."""
        region = mapper.map_owned_region("r1", 4096)
        assert isinstance(region, MarufsMappedRegion)

    def test_map_owned_region_is_mapped(self, mapper):
        """Mapped owned region reports is_mapped=True."""
        region = mapper.map_owned_region("r1", 4096)
        assert region.is_mapped is True

    def test_map_owned_region_owned_flag(self, mapper):
        """Owned region has owned=True."""
        region = mapper.map_owned_region("r1", 4096)
        assert region.owned is True

    def test_map_owned_region_size(self, mapper):
        """Mapped region size matches requested size."""
        region = mapper.map_owned_region("r1", 8192)
        assert region.size == 8192

    def test_map_owned_region_buffer_writable(self, mapper):
        """Owned region buffer is writable."""
        region = mapper.map_owned_region("r1", 4096)
        view = region.get_buffer_view(0, 5)
        assert view is not None
        assert not view.readonly

    def test_map_owned_region_idempotent(self, mapper):
        """Calling map_owned_region twice returns the same object."""
        r1 = mapper.map_owned_region("r1", 4096)
        r2 = mapper.map_owned_region("r1", 4096)
        assert r1 is r2

    def test_map_owned_region_is_tracked(self, mapper):
        """Mapped region is accessible via is_mapped() and get_region()."""
        mapper.map_owned_region("r1", 4096)
        assert mapper.is_mapped("r1") is True
        assert mapper.get_region("r1") is not None


# ---------------------------------------------------------------------------
# map_shared_region
# ---------------------------------------------------------------------------


class TestMapSharedRegion:
    def test_map_shared_region_returns_region(self, mapper, marufs_client):
        """map_shared_region returns an MarufsMappedRegion for existing file."""
        # Create the file first (simulates another process creating it)
        path = os.path.join(marufs_client._mount_path, "shared1")
        with open(path, "wb") as f:
            f.write(b"\x00" * 4096)
        region = mapper.map_shared_region("shared1")
        assert isinstance(region, MarufsMappedRegion)

    def test_map_shared_region_owned_flag(self, mapper, marufs_client):
        """Shared region has owned=False."""
        path = os.path.join(marufs_client._mount_path, "shared1")
        with open(path, "wb") as f:
            f.write(b"\x00" * 4096)
        region = mapper.map_shared_region("shared1")
        assert region.owned is False

    def test_map_shared_region_buffer_writable_for_cuda(self, mapper, marufs_client):
        """Shared region buffer is writable (RDWR) for CUDA cudaHostRegister compatibility."""
        path = os.path.join(marufs_client._mount_path, "shared1")
        with open(path, "wb") as f:
            f.write(b"\x00" * 4096)
        region = mapper.map_shared_region("shared1")
        view = region.get_buffer_view(0, 4)
        assert view is not None
        assert view.readonly is False

    def test_map_shared_region_idempotent(self, mapper, marufs_client):
        """Calling map_shared_region twice returns the same object."""
        path = os.path.join(marufs_client._mount_path, "shared1")
        with open(path, "wb") as f:
            f.write(b"\x00" * 4096)
        r1 = mapper.map_shared_region("shared1")
        r2 = mapper.map_shared_region("shared1")
        assert r1 is r2


# ---------------------------------------------------------------------------
# get_buffer_view
# ---------------------------------------------------------------------------


class TestGetBufferView:
    def test_get_buffer_view_valid_slice(self, mapper):
        """get_buffer_view returns a correctly-sized memoryview slice."""
        mapper.map_owned_region("r1", 4096)
        view = mapper.get_buffer_view("r1", 0, 16)
        assert view is not None
        assert len(view) == 16

    def test_get_buffer_view_offset(self, mapper):
        """get_buffer_view respects offset."""
        mapper.map_owned_region("r1", 4096)
        view = mapper.get_buffer_view("r1", 100, 50)
        assert view is not None
        assert len(view) == 50

    def test_get_buffer_view_unknown_region(self, mapper):
        """get_buffer_view returns None for unknown region name."""
        result = mapper.get_buffer_view("nonexistent", 0, 16)
        assert result is None

    def test_get_buffer_view_out_of_bounds(self, mapper):
        """get_buffer_view returns None when slice exceeds region size."""
        mapper.map_owned_region("r1", 4096)
        result = mapper.get_buffer_view("r1", 4090, 100)
        assert result is None


# ---------------------------------------------------------------------------
# is_mapped / get_fd
# ---------------------------------------------------------------------------


class TestIsMappedGetFd:
    def test_is_mapped_true(self, mapper):
        """is_mapped returns True after mapping."""
        mapper.map_owned_region("r1", 4096)
        assert mapper.is_mapped("r1") is True

    def test_is_mapped_false_before_map(self, mapper):
        """is_mapped returns False for unmapped region."""
        assert mapper.is_mapped("ghost") is False

    def test_get_fd_returns_valid_fd(self, mapper):
        """get_fd returns a non-negative integer after mapping."""
        mapper.map_owned_region("r1", 4096)
        fd = mapper.get_fd("r1")
        assert fd is not None
        assert fd >= 0

    def test_get_fd_unknown_returns_none(self, mapper):
        """get_fd returns None for unknown region."""
        assert mapper.get_fd("ghost") is None


# ---------------------------------------------------------------------------
# unmap_region
# ---------------------------------------------------------------------------


class TestUnmapRegion:
    def test_unmap_region_returns_true(self, mapper):
        """unmap_region returns True on success."""
        mapper.map_owned_region("r1", 4096)
        result = mapper.unmap_region("r1")
        assert result is True

    def test_unmap_region_removes_from_tracking(self, mapper):
        """After unmap, is_mapped returns False."""
        mapper.map_owned_region("r1", 4096)
        mapper.unmap_region("r1")
        assert mapper.is_mapped("r1") is False

    def test_unmap_region_nonexistent_returns_false(self, mapper):
        """unmap_region returns False for unknown name."""
        result = mapper.unmap_region("ghost")
        assert result is False

    def test_unmap_region_closes_fd(self, mapper):
        """After unmap, the fd should be closed."""
        mapper.map_owned_region("r1", 4096)
        fd = mapper.get_fd("r1")
        mapper.unmap_region("r1")
        # fd should now be invalid
        with pytest.raises(OSError):
            os.fstat(fd)


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------


class TestClose:
    def test_close_clears_all_regions(self, marufs_client):
        """close() removes all tracked regions."""
        m = MarufsMapper(marufs_client)
        m.map_owned_region("r1", 4096)
        m.map_owned_region("r2", 4096)
        m.close()
        assert m.is_mapped("r1") is False
        assert m.is_mapped("r2") is False

    def test_close_closes_fds(self, marufs_client):
        """close() closes all file descriptors."""
        m = MarufsMapper(marufs_client)
        m.map_owned_region("r1", 4096)
        fd = m.get_fd("r1")
        m.close()
        with pytest.raises(OSError):
            os.fstat(fd)

    def test_close_idempotent(self, marufs_client):
        """Calling close() twice does not raise."""
        m = MarufsMapper(marufs_client)
        m.map_owned_region("r1", 4096)
        m.close()
        m.close()  # should not raise


# ---------------------------------------------------------------------------
# write/read cycle
# ---------------------------------------------------------------------------


class TestWriteReadCycle:
    def test_write_owned_read_via_view(self, mapper):
        """Write via owned region memoryview, read back via get_buffer_view."""
        region = mapper.map_owned_region("rw1", 4096)
        view = region.get_buffer_view(0, 5)
        assert view is not None
        view[:] = b"hello"

        # Read back via mapper convenience method
        read_view = mapper.get_buffer_view("rw1", 0, 5)
        assert read_view is not None
        assert bytes(read_view) == b"hello"

    def test_write_owned_read_shared(self, tmpdir, marufs_client):
        """Write via owned mapper, read via separate shared mapper."""
        owned_mapper = MarufsMapper(marufs_client)
        region = owned_mapper.map_owned_region("shared_rw", 4096)
        write_view = region.get_buffer_view(0, 5)
        assert write_view is not None
        write_view[:] = b"world"

        # Flush the mmap to disk so the shared open can see the data
        region._mmap_obj.flush()

        # Open separately for reading (new MarufsClient to avoid fd cache)
        reader_marufs = MarufsClient(tmpdir)
        shared_mapper = MarufsMapper(reader_marufs)
        shared_region = shared_mapper.map_shared_region("shared_rw")
        read_view = shared_region.get_buffer_view(0, 5)
        assert read_view is not None
        assert bytes(read_view) == b"world"

        shared_mapper.close()
        reader_marufs.close()
        owned_mapper.close()

"""Unit tests for MarufsClient.

VFS operations are exercised against a real tmpfs directory (tempfile.mkdtemp).
ioctl operations are mocked because the marufs kernel module is not available
in the test environment.
"""

import errno
import mmap
import os
import tempfile
from unittest.mock import patch

import pytest

from marufs.client import MarufsClient
from marufs.ioctl import (
    MARUFS_IOC_CLEAR_NAME,
    MARUFS_IOC_NAME_OFFSET,
    MARUFS_IOC_PERM_GRANT,
    MARUFS_IOC_PERM_REVOKE,
    MARUFS_IOC_PERM_SET_DEFAULT,
    PERM_READ,
    PERM_WRITE,
)


@pytest.fixture()
def tmpdir():
    """Provide a temporary directory that simulates an marufs mount point."""
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture()
def client(tmpdir):
    """MarufsClient backed by a tmpfs directory."""
    c = MarufsClient(tmpdir)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# VFS operations
# ---------------------------------------------------------------------------


class TestCreateRegion:
    def test_create_region_file_exists(self, client, tmpdir):
        """create_region must create the file on disk."""
        client.create_region("r1", 4096)
        assert os.path.exists(os.path.join(tmpdir, "r1"))

    def test_create_region_fd_cached(self, client):
        """create_region must cache the returned fd."""
        fd = client.create_region("r1", 4096)
        assert client.get_fd("r1") == fd

    def test_create_region_size(self, client, tmpdir):
        """create_region must call ftruncate to set the requested size."""
        client.create_region("r1", 8192)
        assert os.path.getsize(os.path.join(tmpdir, "r1")) == 8192


class TestOpenRegion:
    def test_open_region_returns_fd(self, client, tmpdir):
        """open_region (readonly) must open an existing file and return a valid fd."""
        # Create file first via OS so we are testing open, not create
        path = os.path.join(tmpdir, "r2")
        with open(path, "wb") as f:
            f.write(b"\x00" * 512)
        fd = client.open_region("r2", readonly=True)
        assert isinstance(fd, int)
        assert fd >= 0

    def test_open_region_fd_cached(self, client, tmpdir):
        """open_region must cache the fd after the first open."""
        path = os.path.join(tmpdir, "r2")
        with open(path, "wb") as f:
            f.write(b"\x00" * 512)
        fd = client.open_region("r2")
        assert client.get_fd("r2") == fd

    def test_open_region_cached_returns_same_fd(self, client, tmpdir):
        """Calling open_region twice for the same name returns the same fd."""
        path = os.path.join(tmpdir, "r3")
        with open(path, "wb") as f:
            f.write(b"\x00" * 512)
        fd1 = client.open_region("r3")
        fd2 = client.open_region("r3")
        assert fd1 == fd2

    def test_open_region_rdwr(self, client, tmpdir):
        """open_region with readonly=False should open for writing."""
        path = os.path.join(tmpdir, "r4")
        with open(path, "wb") as f:
            f.write(b"\x00" * 512)
        fd = client.open_region("r4", readonly=False)
        # We can write to the fd if it was opened O_RDWR
        os.write(fd, b"hello")


class TestDeleteRegion:
    def test_delete_region_removes_file(self, client, tmpdir):
        """delete_region must unlink the file."""
        client.create_region("r5", 1024)
        client.delete_region("r5")
        assert not os.path.exists(os.path.join(tmpdir, "r5"))

    def test_delete_region_removes_fd_cache(self, client):
        """delete_region must evict the fd from the internal cache."""
        client.create_region("r5", 1024)
        client.delete_region("r5")
        assert client.get_fd("r5") is None

    def test_delete_nonexistent_raises(self, client):
        """delete_region on a missing file must raise OSError."""
        with pytest.raises(OSError):
            client.delete_region("nonexistent")


class TestListRegions:
    def test_list_all(self, client, tmpdir):
        """list_regions returns all region files sorted."""
        for name in ("b", "a", "c"):
            client.create_region(name, 64)
        assert client.list_regions() == ["a", "b", "c"]

    def test_list_with_prefix(self, client):
        """list_regions with prefix returns only matching entries."""
        for name in ("kv_1", "kv_2", "meta_1"):
            client.create_region(name, 64)
        result = client.list_regions(prefix="kv_")
        assert result == ["kv_1", "kv_2"]

    def test_list_empty_mount(self, tmpdir):
        """list_regions on empty directory returns []."""
        c = MarufsClient(tmpdir)
        assert c.list_regions() == []
        c.close()

    def test_list_bad_mount(self):
        """list_regions on non-existent path returns []."""
        c = MarufsClient("/nonexistent/mount")
        assert c.list_regions() == []


class TestExists:
    def test_exists_true(self, client, tmpdir):
        """exists returns True for a file that was created."""
        client.create_region("r6", 256)
        assert client.exists("r6") is True

    def test_exists_false(self, client):
        """exists returns False for a file that was not created."""
        assert client.exists("ghost") is False


# ---------------------------------------------------------------------------
# Name index (ioctl) — mocked
# ---------------------------------------------------------------------------


class TestNameOffset:
    @patch("marufs.client.fcntl.ioctl")
    def test_name_offset_calls_ioctl(self, mock_ioctl):
        """name_offset must call ioctl with MARUFS_IOC_NAME_OFFSET."""
        c = MarufsClient("/mnt/marufs")
        c.name_offset(3, "key1", 1024)
        assert mock_ioctl.called
        args = mock_ioctl.call_args[0]
        assert args[0] == 3
        assert args[1] == MARUFS_IOC_NAME_OFFSET

    @patch("marufs.client.fcntl.ioctl")
    def test_find_name_returns_value(self, mock_ioctl):
        """find_name returns (region_name, offset) from the kernel response."""

        def _fill_req(fd, cmd, req):
            req.region_name = b"maru_abc_0000"
            req.offset = 4096

        mock_ioctl.side_effect = _fill_req
        c = MarufsClient("/mnt/marufs")
        result = c.find_name(3, "key1")
        assert result is not None
        region_name, offset = result
        assert region_name == "maru_abc_0000"
        assert offset == 4096

    @patch("marufs.client.fcntl.ioctl")
    def test_find_name_not_found_returns_none(self, mock_ioctl):
        """find_name returns None when ioctl raises ENOENT."""
        mock_ioctl.side_effect = OSError(errno.ENOENT, "Not found")
        c = MarufsClient("/mnt/marufs")
        result = c.find_name(3, "missing_key")
        assert result is None

    @patch("marufs.client.fcntl.ioctl")
    def test_find_name_propagates_other_errors(self, mock_ioctl):
        """find_name re-raises non-ENOENT OSError."""
        mock_ioctl.side_effect = OSError(errno.EACCES, "Permission denied")
        c = MarufsClient("/mnt/marufs")
        with pytest.raises(OSError) as exc_info:
            c.find_name(3, "key1")
        assert exc_info.value.errno == errno.EACCES

    @patch("marufs.client.fcntl.ioctl")
    def test_clear_name_calls_ioctl(self, mock_ioctl):
        """clear_name must call ioctl with MARUFS_IOC_CLEAR_NAME."""
        c = MarufsClient("/mnt/marufs")
        c.clear_name(3, "key1")
        args = mock_ioctl.call_args[0]
        assert args[1] == MARUFS_IOC_CLEAR_NAME


# ---------------------------------------------------------------------------
# Permission management (ioctl) — mocked
# ---------------------------------------------------------------------------


class TestPermissions:
    @patch("marufs.client.fcntl.ioctl")
    def test_perm_grant_calls_ioctl(self, mock_ioctl):
        """perm_grant must call ioctl with MARUFS_IOC_PERM_GRANT."""
        c = MarufsClient("/mnt/marufs")
        c.perm_grant(5, node_id=0, pid=1234, perms=PERM_READ | PERM_WRITE)
        args = mock_ioctl.call_args[0]
        assert args[0] == 5
        assert args[1] == MARUFS_IOC_PERM_GRANT

    @patch("marufs.client.fcntl.ioctl")
    def test_perm_revoke_calls_ioctl(self, mock_ioctl):
        """perm_revoke must call ioctl with MARUFS_IOC_PERM_REVOKE."""
        c = MarufsClient("/mnt/marufs")
        c.perm_revoke(5, node_id=0, pid=1234)
        args = mock_ioctl.call_args[0]
        assert args[1] == MARUFS_IOC_PERM_REVOKE

    @patch("marufs.client.fcntl.ioctl")
    def test_perm_set_default_calls_ioctl(self, mock_ioctl):
        """perm_set_default must call ioctl with MARUFS_IOC_PERM_SET_DEFAULT."""
        c = MarufsClient("/mnt/marufs")
        c.perm_set_default(5, perms=PERM_READ)
        args = mock_ioctl.call_args[0]
        assert args[1] == MARUFS_IOC_PERM_SET_DEFAULT


# ---------------------------------------------------------------------------
# Close / lifecycle
# ---------------------------------------------------------------------------


class TestClose:
    def test_close_closes_all_fds(self, tmpdir):
        """close() must close every cached fd."""
        c = MarufsClient(tmpdir)
        fds = []
        for name in ("a", "b", "c"):
            fd = c.create_region(name, 64)
            fds.append(fd)
        c.close()
        # After close, the fd should be invalid
        for fd in fds:
            with pytest.raises(OSError):
                os.fstat(fd)

    def test_close_fd_removes_single_fd(self, tmpdir):
        """close_fd removes only the specified fd from the cache."""
        c = MarufsClient(tmpdir)
        c.create_region("x", 64)
        c.create_region("y", 64)
        fd_x = c.get_fd("x")
        c.close_fd("x")
        assert c.get_fd("x") is None
        assert c.get_fd("y") is not None  # y still cached
        # fd for x should now be closed
        with pytest.raises(OSError):
            os.fstat(fd_x)
        c.close()


# ---------------------------------------------------------------------------
# mmap
# ---------------------------------------------------------------------------


class TestMmapRegion:
    def test_mmap_read(self, client, tmpdir):
        """mmap_region (PROT_READ) returns a readable mmap object."""
        fd = client.create_region("mmap_r", 4096)
        # Write some data via os.write before mapping
        os.write(fd, b"hello")
        os.lseek(fd, 0, os.SEEK_SET)
        mm = client.mmap_region(fd, 4096, prot=mmap.PROT_READ)
        assert mm[0:5] == b"hello"
        mm.close()

    def test_mmap_write(self, client, tmpdir):
        """mmap_region (PROT_READ|PROT_WRITE) returns a writable mmap object."""
        fd = client.create_region("mmap_rw", 4096)
        mm = client.mmap_region(fd, 4096, prot=mmap.PROT_READ | mmap.PROT_WRITE)
        mm[0:5] = b"world"
        mm.flush()
        mm.close()
        # Verify data on disk
        os.lseek(fd, 0, os.SEEK_SET)
        data = os.read(fd, 5)
        assert data == b"world"

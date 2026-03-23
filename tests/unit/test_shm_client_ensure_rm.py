# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for MaruShmClient: _try_connect, _connect, is_running,
read-only mmap, __del__, and other coverage gaps.

These tests exercise the real MaruShmClient class (not the MockShmClient from
conftest), using mock UDS servers and targeted patching.
"""

import os
import socket
import tempfile
import threading
from unittest.mock import patch

import pytest

from maru_shm.client import MaruShmClient
from maru_shm.constants import MAP_SHARED, PROT_READ, PROT_WRITE
from maru_shm.ipc import (
    HEADER_SIZE,
    AllocResp,
    FreeResp,
    MsgHeader,
    MsgType,
)
from maru_shm.types import MaruHandle
from maru_shm.uds_helpers import read_full, send_with_fd, write_full

# =============================================================================
# Helpers (same pattern as test_shm_client.py)
# =============================================================================


def _recv_request(sock):
    """Read request header + payload from socket."""
    hdr = MsgHeader.unpack(read_full(sock, HEADER_SIZE))
    payload = read_full(sock, hdr.payload_len) if hdr.payload_len > 0 else b""
    return hdr, payload


def _send_response(sock, msg_type, resp):
    """Pack resp and send as a simple (no-FD) response."""
    payload = resp.pack()
    hdr = MsgHeader(msg_type=msg_type, payload_len=len(payload))
    write_full(sock, hdr.pack() + payload)


def _make_temp_fd(size=4096, fill=b"\x00"):
    """Create a temp file and return (fd, path). Caller must close/unlink."""
    tmp_fd, tmp_path = tempfile.mkstemp()
    os.write(tmp_fd, fill * size)
    os.close(tmp_fd)
    fd = os.open(tmp_path, os.O_RDWR)
    return fd, tmp_path


def _send_response_with_fd(sock, msg_type, resp, *, size=4096, fill=b"\x00"):
    """Pack resp, create a temp FD, and send header + payload-with-FD."""
    fd, tmp_path = _make_temp_fd(size, fill)
    payload = resp.pack()
    hdr = MsgHeader(msg_type=msg_type, payload_len=len(payload))
    write_full(sock, hdr.pack())
    send_with_fd(sock, payload, fd)
    os.close(fd)
    os.unlink(tmp_path)


class MockResourceManagerServer:
    """Mini mock resource manager that serves requests on a UDS socket."""

    def __init__(self, handler):
        self._handler = handler
        self._sock = None

    def start(self, tmpdir):
        path = os.path.join(tmpdir, "test.sock")
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.bind(path)
        self._sock.listen(4)
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()
        return path

    def _accept(self):
        while True:
            try:
                client, _ = self._sock.accept()
            except OSError:
                break
            try:
                self._handler(client)
            except Exception:
                pass
            finally:
                client.close()

    def stop(self):
        if self._sock:
            self._sock.close()


# =============================================================================
# _try_connect tests
# =============================================================================


class TestTryConnect:
    def test_success_when_server_listening(self):
        """_try_connect returns True when server is listening."""

        def handler(sock):
            pass  # Accept and close

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                assert client._try_connect() is True
            finally:
                server.stop()

    def test_failure_when_no_server(self):
        """_try_connect returns False when no server is listening."""
        client = MaruShmClient(socket_path="/tmp/_maru_nonexistent_test_sock.sock")
        assert client._try_connect() is False


# =============================================================================
# _connect tests
# =============================================================================


class TestIsRunning:
    def test_true_when_server_listening(self):
        """is_running() returns True when server is listening."""

        def handler(sock):
            pass

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                assert client.is_running() is True
            finally:
                server.stop()

    def test_false_when_no_server(self):
        """is_running() returns False when no server is listening."""
        client = MaruShmClient(socket_path="/tmp/_maru_nonexistent_test_sock.sock")
        assert client.is_running() is False


class TestConnect:
    def test_direct_connection(self):
        """_connect succeeds when server is running."""

        def handler(sock):
            pass

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                sock = client._connect()
                assert sock is not None
                sock.close()
            finally:
                server.stop()

    def test_raises_connection_error_when_not_running(self):
        """_connect raises ConnectionError with instructions when server is not running."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sock_path = os.path.join(tmpdir, "test.sock")
            client = MaruShmClient(socket_path=sock_path)

            with pytest.raises(ConnectionError, match="Resource manager is not running"):
                client._connect()


# =============================================================================
# mmap edge cases
# =============================================================================


class TestMmapEdgeCases:
    def test_mmap_read_only(self):
        """mmap with PROT_READ uses ACCESS_READ."""

        def handler(sock):
            hdr, _ = _recv_request(sock)
            if hdr.msg_type == MsgType.ALLOC_REQ:
                handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=999)
                resp = AllocResp(status=0, handle=handle, requested_size=4096)
                _send_response_with_fd(sock, MsgType.ALLOC_RESP, resp)

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                handle = client.alloc(4096)

                # Read-only mmap — hits ACCESS_READ branch
                mm = client.mmap(handle, PROT_READ)
                assert mm is not None
                assert len(mm) == 4096
                # Read should work
                _ = mm[0]

                client.close()
            finally:
                server.stop()

    def test_mmap_with_explicit_flags(self):
        """mmap with non-zero flags skips MAP_SHARED default."""

        def handler(sock):
            hdr, _ = _recv_request(sock)
            if hdr.msg_type == MsgType.ALLOC_REQ:
                handle = MaruHandle(region_id=1, offset=0, length=4096, auth_token=999)
                resp = AllocResp(status=0, handle=handle, requested_size=4096)
                _send_response_with_fd(sock, MsgType.ALLOC_RESP, resp)

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                handle = client.alloc(4096)

                # Explicit flags — skips `if flags == 0: flags = MAP_SHARED`
                mm = client.mmap(handle, PROT_READ | PROT_WRITE, flags=MAP_SHARED)
                assert mm is not None
                assert len(mm) == 4096

                client.close()
            finally:
                server.stop()


# =============================================================================
# free edge cases
# =============================================================================


class TestFreeEdgeCases:
    def test_free_without_cached_resources(self):
        """free() works for a handle with no cached FD/mmap."""

        def handler(sock):
            hdr, _ = _recv_request(sock)
            if hdr.msg_type == MsgType.FREE_REQ:
                _send_response(sock, MsgType.FREE_RESP, FreeResp(status=0))

        with tempfile.TemporaryDirectory() as tmpdir:
            server = MockResourceManagerServer(handler)
            sock_path = server.start(tmpdir)
            try:
                client = MaruShmClient(socket_path=sock_path)
                # Free without prior alloc — no cached FD/mmap
                handle = MaruHandle(
                    region_id=999, offset=0, length=4096, auth_token=123
                )
                client.free(handle)
                assert 999 not in client._fd_cache
                assert 999 not in client._mmap_cache
            finally:
                server.stop()


# =============================================================================
# __del__ test
# =============================================================================


class TestDel:
    def test_del_calls_close(self):
        """__del__ delegates to close()."""
        with tempfile.TemporaryDirectory() as tmpdir:
            client = MaruShmClient(socket_path=os.path.join(tmpdir, "fake.sock"))
            with patch.object(client, "close") as mock_close:
                client.__del__()
                mock_close.assert_called_once()

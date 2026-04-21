# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Unit tests for MaruHandler._check_backend_readiness — backend-aware
readiness check executed right after the server handshake.

The helper only consults the handshake dict and os.path.isdir, so these
tests do not need a live RPC or DAX device.
"""

from unittest.mock import patch

from maru import MaruConfig, MaruHandler


def _make_handler():
    """Build a Handler without starting RPC/connect — just enough for method calls."""
    config = MaruConfig(auto_connect=False)
    return MaruHandler(config)


class TestCheckBackendReadiness:
    def test_legacy_response_without_backend_passes(self):
        """Older servers may not include backend/expected_mounts — still OK."""
        handler = _make_handler()
        assert handler._check_backend_readiness({}) is True

    def test_maru_backend_skips_mount_check(self):
        """backend='maru' never inspects the filesystem."""
        handler = _make_handler()
        resp = {"backend": "maru", "expected_mounts": ["/does/not/exist"]}
        # isdir is not even consulted for the maru backend.
        with patch("os.path.isdir", side_effect=AssertionError("must not be called")):
            assert handler._check_backend_readiness(resp) is True

    def test_marufs_backend_with_all_mounts_present(self):
        """All advertised marufs mounts exist locally → pass."""
        handler = _make_handler()
        resp = {
            "backend": "marufs",
            "expected_mounts": ["/mnt/marufs", "/mnt/marufs2"],
        }
        with patch("os.path.isdir", return_value=True):
            assert handler._check_backend_readiness(resp) is True

    def test_marufs_backend_with_missing_mount_fails(self):
        """Any missing mount → False, error logged."""
        handler = _make_handler()
        resp = {
            "backend": "marufs",
            "expected_mounts": ["/mnt/marufs", "/mnt/marufs_missing"],
        }

        def isdir(p):
            return p == "/mnt/marufs"

        with patch("os.path.isdir", side_effect=isdir):
            assert handler._check_backend_readiness(resp) is False

    def test_marufs_backend_with_empty_mount_list_passes(self):
        """Server advertises marufs but reports no pools yet → pass (warning logged
        on the server side, but the handler itself has nothing to verify)."""
        handler = _make_handler()
        resp = {"backend": "marufs", "expected_mounts": []}
        with patch("os.path.isdir", side_effect=AssertionError("must not be called")):
            assert handler._check_backend_readiness(resp) is True

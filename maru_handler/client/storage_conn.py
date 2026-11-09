# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""StorageConn: the UDS connection to this node's StorageService (stage S2).

Sends ATTACH, CREDIT, WRITE_DONE, ABORT, FETCH and acks, and handles NOTIFY on
its receive thread.
"""

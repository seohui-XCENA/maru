# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""LocalServer: the UDS endpoint for workers of this node (wire spec 4.1).

Accepts ATTACH, CREDIT, CREDIT_RETURN, WRITE_DONE, ABORT, FETCH, FETCH_CANCEL,
LOCAL_PIN, LOCAL_UNPIN, DRAIN_ACK, UNMAP_ACK and DETACH; sends NOTIFY. On
ATTACH it passes the memfd of every DRAM pool with SCM_RIGHTS and checks the
peer pid from SO_PEERCRED. Implemented in stage S2.
"""

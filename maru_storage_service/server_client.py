# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""ServerClient: the node session with MaruServer (wire spec 3.1).

Batches COMMIT, RETIRE and FINALIZE_MOVE, requests regions, receives access
information, block requests and policy with each heartbeat, and resends a
request with the same op when its result is unknown. Implemented in stage S2.
"""

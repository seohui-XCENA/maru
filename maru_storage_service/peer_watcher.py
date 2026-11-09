# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PeerWatcher: notices workers that ended (wire spec 7.3).

Opens a pidfd for every attached worker. When one ends it moves the worker's
credited pages to QUARANTINED, reports WORKER_EXIT to MaruServer, and frees
the pages once GPU access is known to have ended. Process exit and the end of
GPU or RDMA access are confirmed separately. Implemented in stage S2.
"""

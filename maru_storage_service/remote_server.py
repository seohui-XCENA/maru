# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""RemoteServer: a memory node's endpoint for GPU nodes' remote tiers, on
the wire of maru_common.remote_protocol."""

from abc import ABC, abstractmethod


class RemoteServer(ABC):
    """One per memory node run. It starts where a GPU node claims its local
    socket, is ready last and stops first."""

    @abstractmethod
    def start(self) -> None:
        """Bind and listen; raises if the address is taken (another start of
        the node). Until ready, a HELLO is answered BUSY (not_ready)."""

    @abstractmethod
    def ready(self) -> None:
        """The run is open and its pools registered: HELLOs are served."""

    @abstractmethod
    def stop(self) -> None:
        """New ops are refused and every connection closes. Idempotent."""

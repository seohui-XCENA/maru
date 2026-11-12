# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""NixlExporter: a node run's whole L1 mappings, registered with its NIC.

A pool is registered once per run, whole, and stays registered until the run
ends: a peer reaches any page of it by the pool's base address and the key
in its partial metadata. Deregistration is the only fence for RDMA that may
still be in the network, so a mapping is unmapped only after it succeeded.
"""

from __future__ import annotations

import logging
import mmap
import threading
from dataclasses import dataclass
from typing import Any

from .transport import NixlTransport, buffer_address

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Export:
    """A registered pool: ``base`` is the address of its byte 0 here."""

    pool_id: str
    base: int
    nbytes: int
    metadata: bytes  # partial NIXL metadata: this range and the connection


class NixlExporter:
    """Thread safe. One per node run, on the run's transport."""

    def __init__(self, transport: NixlTransport) -> None:
        self.transport = transport
        self._lock = threading.Lock()
        self._exports: dict[str, tuple[Export, Any]] = {}

    def attach(self, pool_id: str, memory: mmap.mmap) -> None:
        """Register the whole mapping; raises, registering nothing, if the
        NIC refuses it or the pool is already registered."""
        base, nbytes = buffer_address(memory), len(memory)
        with self._lock:
            if pool_id in self._exports:
                raise ValueError(f"{pool_id} is already registered")
            registration = self.transport.register(base, nbytes, tag=pool_id)
            try:
                metadata = self.transport.partial_metadata(registration)
            except BaseException:
                self.transport.deregister(registration)
                raise
            export = Export(pool_id, base, nbytes, metadata)
            self._exports[pool_id] = (export, registration)
        logger.info("registered %s (%d bytes) with the NIC", pool_id, nbytes)

    def detach(self, pool_id: str) -> None:
        """Deregister; a pool never registered is a no-op. If this raises,
        the pool stays registered and must stay mapped."""
        with self._lock:
            entry = self._exports.get(pool_id)
            if entry is None:
                return
            self.transport.deregister(entry[1])
            del self._exports[pool_id]
        logger.info("deregistered %s from the NIC", pool_id)

    def export(self, pool_id: str) -> Export | None:
        """The pool's registration, or None if it is not registered."""
        with self._lock:
            entry = self._exports.get(pool_id)
        return None if entry is None else entry[0]

    def exports(self) -> list[Export]:
        """Every registered pool, in the order registered."""
        with self._lock:
            return [export for export, _ in self._exports.values()]

# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""KV Manager implementation for managing KV metadata."""

import logging
import time
from dataclasses import dataclass
from threading import RLock

logger = logging.getLogger(__name__)


@dataclass
class KVEntry:
    """KV entry referencing an allocation."""

    region_id: int  # Region ID (allocation identifier)
    kv_offset: int  # Offset within allocation (relative to handle.offset)
    kv_length: int  # Size of KV data
    pin_count: int = 0  # Pin count for eviction protection


class KVManager:
    """
    Manages KV metadata - mapping from chunk hash to shared memory location.

    Responsibilities:
    - Store KV location metadata (hash -> location)
    - Track reference counts for each KV entry
    - Thread-safe operations
    """

    def __init__(self, pin_timeout_sec: float = 60.0):
        self._store: dict[str, KVEntry] = {}
        self._lock = RLock()
        self._pin_timeout_sec = pin_timeout_sec
        self._pin_timestamps: dict[str, float] = {}  # key -> first pin time

    def register(
        self, key: str, region_id: int, kv_offset: int, kv_length: int
    ) -> tuple[bool, int | None]:
        """
        Register a KV entry.

        Args:
            key: The chunk key string
            region_id: Region ID of the allocation
            kv_offset: Offset within the allocation
            kv_length: Size of the KV data

        Returns:
            (is_new, region_id_to_increment) - region_id if new entry created
        """
        with self._lock:
            if key in self._store:
                # Key already exists — skip (idempotent)
                return (False, None)

            self._store[key] = KVEntry(
                region_id=region_id,
                kv_offset=kv_offset,
                kv_length=kv_length,
            )
            logger.debug("Registered KV: key=%s, region_id=%d", key, region_id)
            return (True, region_id)  # New entry, need to increment alloc ref

    def lookup(self, key: str) -> KVEntry | None:
        """
        Lookup a KV entry by its key.

        Args:
            key: The chunk key string

        Returns:
            KVEntry if found, None otherwise
        """
        with self._lock:
            return self._store.get(key)

    def exists(self, key: str) -> bool:
        """Check if a KV entry exists."""
        with self._lock:
            return key in self._store

    def unpin(self, key: str) -> bool:
        """Decrement pin_count for a KV entry.

        Returns:
            True if successfully unpinned, False if key not found or not pinned.
        """
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                logger.warning("Unpin failed: key=%s not found", key)
                return False
            if entry.pin_count <= 0:
                logger.warning("Unpin failed: key=%s pin_count already 0", key)
                return False
            entry.pin_count -= 1
            if entry.pin_count == 0:
                self._pin_timestamps.pop(key, None)
            logger.debug("Unpinned KV: key=%s, pin_count=%d", key, entry.pin_count)
            return True

    def delete(self, key: str) -> tuple[bool, int | None]:
        """
        Delete a KV entry.

        Returns:
            (key_existed, region_id_to_decrement)
            - (False, None): key didn't exist
            - (True, region_id): entry deleted, allocation ref needs decrement
        """
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return (False, None)

            if entry.pin_count > 0:
                logger.warning(
                    "Delete refused: key=%s is pinned (pin_count=%d)",
                    key,
                    entry.pin_count,
                )
                return (False, None)

            region_id = self._store.pop(key).region_id
            self._pin_timestamps.pop(key, None)
            logger.debug("Deleted KV: key=%s, region_id=%d", key, region_id)
            return (True, region_id)

    def get_stats(self) -> dict:
        """Get KV statistics."""
        with self._lock:
            return {
                "total_entries": len(self._store),
                "total_size": sum(e.kv_length for e in self._store.values()),
            }

    # =========================================================================
    # Batch Operations
    # =========================================================================

    def batch_register(self, entries: list[tuple[str, int, int, int]]) -> list[bool]:
        """
        Register multiple KV entries in a single operation.

        Args:
            entries: List of (key, region_id, kv_offset, kv_length) tuples

        Returns:
            List of booleans indicating if each entry was newly registered
        """
        results = []
        with self._lock:
            for key, region_id, kv_offset, kv_length in entries:
                if key in self._store:
                    # Duplicate key — skip
                    results.append(False)
                else:
                    self._store[key] = KVEntry(
                        region_id=region_id,
                        kv_offset=kv_offset,
                        kv_length=kv_length,
                    )
                    results.append(True)
        return results

    def batch_lookup(self, keys: list[str]) -> list[KVEntry | None]:
        """
        Lookup multiple KV entries in a single operation.

        Args:
            keys: List of chunk key strings

        Returns:
            List of KVEntry or None for each key
        """
        with self._lock:
            return [self._store.get(key) for key in keys]

    def batch_exists(self, keys: list[str]) -> list[bool]:
        """
        Check existence of multiple KV entries in a single operation.

        Args:
            keys: List of chunk key strings

        Returns:
            List of booleans indicating if each key exists
        """
        with self._lock:
            return [key in self._store for key in keys]

    def batch_exists_and_pin(self, keys: list[str]) -> list[bool]:
        """Check existence and pin prefix-contiguous KV entries atomically.

        Stops at the first miss — only the contiguous prefix of existing
        keys is pinned. This matches LMCache's prefix-contiguous lookup
        pattern and avoids pin leaks for non-prefix keys.

        Returns:
            List of booleans — True if key exists (and was pinned).
            After first False, remaining entries are all False.
        """
        now = time.monotonic()
        with self._lock:
            results = []
            for key in keys:
                entry = self._store.get(key)
                if entry is None:
                    # First miss: fill rest with False and stop
                    results.extend([False] * (len(keys) - len(results)))
                    break
                entry.pin_count += 1
                if key not in self._pin_timestamps:
                    self._pin_timestamps[key] = now
                results.append(True)
            return results

    def batch_unpin(self, keys: list[str]) -> list[bool]:
        """Unpin multiple KV entries.

        Returns:
            List of booleans — True if successfully unpinned.
        """
        with self._lock:
            results = []
            for key in keys:
                entry = self._store.get(key)
                if entry is None or entry.pin_count <= 0:
                    results.append(False)
                else:
                    entry.pin_count -= 1
                    if entry.pin_count == 0:
                        self._pin_timestamps.pop(key, None)
                    results.append(True)
            return results

    # =========================================================================
    # Pin Timeout Monitor
    # =========================================================================

    def check_pin_timeouts(self) -> tuple[int, int]:
        """Force-unpin entries that exceeded pin_timeout_sec.

        Returns:
            (pinned_count, force_unpinned_count)
        """
        now = time.monotonic()
        with self._lock:
            pinned_count = len(self._pin_timestamps)
            expired_keys = [
                key
                for key, ts in self._pin_timestamps.items()
                if now - ts > self._pin_timeout_sec
            ]

            for key in expired_keys:
                entry = self._store.get(key)
                if entry is not None and entry.pin_count > 0:
                    logger.warning(
                        "Pin timeout: key=%s, pin_count=%d, elapsed=%.1fs — force unpin",
                        key,
                        entry.pin_count,
                        now - self._pin_timestamps[key],
                    )
                    entry.pin_count = 0
                self._pin_timestamps.pop(key, None)

        if expired_keys:
            logger.warning(
                "PinMonitor: force unpinned %d/%d entries",
                len(expired_keys),
                pinned_count,
            )
        else:
            logger.debug(
                "PinMonitor: pinned_entries=%d, force_unpinned=0", pinned_count
            )

        return pinned_count, len(expired_keys)

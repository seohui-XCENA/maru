# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""MaruHandler - Main interface for Maru shared memory KV cache client.

This module provides the primary entry point for clients to interact with
the Maru shared memory KV cache system.

Example:
    from maru import MaruConfig, MaruHandler

    config = MaruConfig(server_url="tcp://localhost:5555")
    with MaruHandler(config) as handler:
        # Zero-copy store: alloc → write to buf → store
        handle = handler.alloc(size=len(data))
        handle.buf[:len(data)] = data
        handler.store(key="12345", handle=handle)

        result = handler.retrieve(key="12345")  # returns MemoryInfo
"""

import inspect
import logging
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from maru_common import MaruConfig
from maru_shm import MaruHandle

from .memory import (
    AllocHandle,
    DaxMapper,
    MemoryInfo,
    OwnedRegionManager,
    PagedMemoryAllocator,
)
from .memory.types import MappingStatus
from .plugin import load_handler_plugins
from .rpc_client import RpcClient

logger = logging.getLogger(__name__)


class MaruHandler:
    """Main interface for Maru shared memory KV cache operations.

    This class handles:
    - Connection management to MaruServer
    - Memory mapping via DaxMapper
    - KV store/retrieve operations

    Thread-safety:
    - Read operations (exists, retrieve, batch_exists, batch_retrieve) are
      lock-free — they rely on RpcAsyncClient (already thread-safe) and
      DaxMapper's internal lock for lazy mapping.
    - Write operations (store, batch_store, delete) are serialized by
      ``_write_lock`` to guarantee atomicity of allocate-write-register.
    - ``close()`` sets ``_closing`` event to reject new operations, then
      acquires ``_write_lock`` to wait for in-flight writes before teardown.

    Architecture::

        MaruHandler
            ├── RpcClient (server communication, sole RPC owner)
            ├── DaxMapper (memory mapping via MaruShmClient, owns all mmap/munmap)
            ├── OwnedRegionManager (owned regions + allocation, no RPC)
            │   ├── OwnedRegion 1 (PagedMemoryAllocator)
            │   ├── OwnedRegion 2 (PagedMemoryAllocator)
            │   └── ...
            └── _key_to_location (key -> (region_id, page_index))
    """

    def __init__(self, config: MaruConfig | None = None) -> None:
        """Initialize MaruHandler.

        Args:
            config: Configuration object. If None, uses defaults.
        """
        self._config = config or MaruConfig()
        self._group_request_id: str | None = None
        self._group_reply_received = False
        self._group_cleanup_started = False
        self._require_cuda_pin = False
        self._callback_slot_size = False
        if self._config.use_async_rpc:
            from .rpc_async_client import RpcAsyncClient

            self._rpc = RpcAsyncClient(
                self._config.server_url,
                timeout_ms=self._config.timeout_ms,
                max_inflight=self._config.max_inflight,
            )
        else:
            self._rpc = RpcClient(
                self._config.server_url,
                timeout_ms=self._config.timeout_ms,
            )
        self._mapper: DaxMapper | None = None

        # Managers (initialized on connect)
        self._owned: OwnedRegionManager | None = None

        # Thread-safety
        self._write_lock = threading.Lock()
        self._closing = threading.Event()

        # Client-side timing for StatsManager reporting
        self._stats_buffer: list[dict] = []
        self._stats_lock = threading.Lock()
        self._stats_rpc: RpcClient | None = None  # dedicated connection for flush
        self._stats_flusher: threading.Thread | None = None

        # Connection state
        self._key_to_location: dict[str, tuple[int, int]] = {}
        self._connected = False

        # Region-added callback (set by CxlMemoryAdapter)
        self._on_region_added: Callable[..., None] | None = None
        self._on_region_removed: Callable[[int], None] | None = None
        self._staged_handles: dict[int, MaruHandle] = {}
        self._staged_ready: set[int] = set()
        self._staged_callbacks: set[int] = set()

        # Expansion policy
        self._auto_expand = self._config.auto_expand
        self._expand_size = self._config.expand_size or self._config.pool_size

        # Out-of-tree extension plugins (see maru_handler/plugin.py). Empty
        # unless a package registers a `maru.handler_plugins` entry point;
        # loading is soft-fail so a broken plugin never blocks construction.
        self._plugins = load_handler_plugins()

        logger.debug("Created MaruHandler with config: %s", self._config)

        self._dispatch_plugins("on_init", self)

    def _dispatch_plugins(self, hook: str, *args) -> None:
        """Invoke ``hook`` on every loaded plugin, isolating failures.

        Plugins implement any subset of the MaruHandlerPlugin protocol, so a
        missing hook is skipped. One plugin raising never affects the others
        or the surrounding core operation — the point of soft-fail loading.
        """
        for plugin in self._plugins:
            fn = getattr(plugin, hook, None)
            if fn is None:
                continue
            try:
                fn(*args)
            except Exception:
                logger.exception(
                    "handler plugin %s hook %s failed",
                    type(plugin).__name__,
                    hook,
                )

    # =========================================================================
    # Public Accessors
    # =========================================================================

    @property
    def mapper(self) -> DaxMapper:
        """Deprecated: Use get_buffer_view() instead."""
        return self._mapper

    def get_buffer_view(
        self, region_id: int, offset: int, size: int
    ) -> memoryview | None:
        """Get a memoryview slice from a mapped region.

        Args:
            region_id: The region ID (owned or shared).
            offset: Byte offset within the region.
            size: Number of bytes to view.

        Returns:
            Writable memoryview, or None if region not mapped.
        """
        return self._mapper.get_buffer_view(region_id, offset, size)

    def get_region_page_count(self, region_id: int) -> int | None:
        """Get page count for a region (owned or shared).

        Args:
            region_id: The region ID.

        Returns:
            Number of pages, or None if region not found.
        """
        if self._owned is not None:
            region = self._owned.get_owned_region(region_id)
            if region is not None:
                return region.allocator.page_count
        mapped = self._mapper.get_region(region_id)
        if mapped is None:
            return None
        return mapped.size // self._config.chunk_size_bytes

    def get_owned_region_ids(self) -> list[int]:
        """Get list of currently owned region IDs.

        Returns:
            List of region IDs. Empty if not connected.
        """
        if self._owned is None:
            return []
        return self._owned.get_region_ids()

    # --- Plugin accessor surface -------------------------------------------
    # STABLE PLUGIN API. These two methods, together with the MaruHandlerPlugin
    # hook signatures (maru_handler/plugin.py), are the public contract that
    # out-of-tree plugins (on independent release cycles) depend on. Do NOT
    # rename, remove, or change their behaviour without a deprecation cycle —
    # doing so silently breaks every installed plugin. A contract test in
    # tests/unit/test_plugin_loader.py fails CI if this surface drifts.
    # Kept intentionally small: everything a plugin needs about a batch arrives
    # via the on_batch_retrieve hook args; these cover only the region-mapping
    # state that isn't in those args.

    def is_region_mapped(self, region_id: int) -> bool:
        """Return True if the region is currently mmap'd. Stable plugin API."""
        return (
            self._mapper is not None and self._mapper.get_region(region_id) is not None
        )

    def get_region_dax_path(self, region_id: int) -> str | None:
        """Return the DAX device path for a mapped region, or None. Stable plugin API."""
        if self._mapper is None:
            return None
        return self._mapper.get_dax_path(region_id)

    def get_chunk_size(self) -> int:
        """Get the configured chunk size in bytes.

        Returns:
            Chunk size in bytes.
        """
        return self._config.chunk_size_bytes

    def set_on_region_added(self, callback: Callable[..., None] | None) -> None:
        """Register a callback after a region is added.

        New callbacks receive (region_id, page_count, slot_size_bytes). Legacy
        callbacks accepting only two arguments retain their original contract.

        On registration, replays callback for all existing owned regions
        so the caller doesn't need separate init-time logic.

        Args:
            callback: Three- or two-argument callback, or None to unregister.
        """
        # Inspect once; never mistake a TypeError inside the callback for arity.
        accepts_slot = False
        if callback is not None:
            signature = inspect.signature(callback)
            try:
                signature.bind(0, 0, 0)
                accepts_slot = True
            except TypeError:
                signature.bind(0, 0)
        self._callback_slot_size = accepts_slot
        self._on_region_added = callback
        if callback is not None and self._owned is not None:
            for rid in self._owned.get_region_ids():
                region = self._owned.get_owned_region(rid)
                if region is not None:
                    logger.debug(
                        "on_region_added replay: region=%d pages=%d",
                        rid,
                        region.allocator.page_count,
                    )
                    self._notify_region_added(rid, region.allocator.page_count)

    def set_on_region_removed(self, callback: Callable[[int], None] | None) -> None:
        """Set the idempotent view-cleanup callback for staged rollback.

        callback receives a region ID before its allocator/mapping is removed.
        It must raise if cleanup is unsafe. None unregisters it. No initial replay.
        Legacy connect, expansion, and close keep their existing behavior.
        """
        self._on_region_removed = callback

    def get_mapping_status(self, region_id: int) -> MappingStatus:
        """Return mapping/pin/user state for region_id, including staged regions."""
        if self._mapper is None:
            return MappingStatus(region_id, False, False, 0)
        return self._mapper.get_mapping_status(region_id)

    @contextmanager
    def hold_region(self, region_id: int) -> Iterator[None]:
        """Lease region_id against staged rollback until work completes.

        Keep the context open until GPU completion, including asynchronous copies.
        This does not change the existing Handler close contract.

        Raises:
            RuntimeError: If the mapper is unavailable.
            KeyError: If region_id is unmapped.
        """
        with self._write_lock:
            if self._mapper is None:
                raise RuntimeError("Mapper is not initialized")
            lease = self._mapper.hold_region(region_id)
            lease.__enter__()
        try:
            yield
        finally:
            lease.__exit__(None, None, None)

    def get_staged_region_ids(self) -> list[int]:
        """Return pending region IDs, including failed cleanup for explicit retry."""
        with self._write_lock:
            return list(self._staged_handles)

    def is_region_staged(self, region_id: int) -> bool:
        """Return whether region_id is pending preparation/cleanup; callback-safe."""
        return region_id in self._staged_handles

    def get_region_allocated_pages(self, region_id: int) -> int:
        """Return live owned page count for region_id; staged/unknown regions yield 0."""
        region = self._owned.get_owned_region(region_id) if self._owned else None
        return region.allocator.num_allocated if region else 0

    def prepare_regions(
        self,
        handles: list[MaruHandle],
        *,
        require_cuda_pin: bool = False,
        targets: dict[int, str] | None = None,
    ) -> list[int]:
        """Take ownership of newly allocated handles and stage them for commit.

        Maps every region and runs the existing add callback before publication.
        Callbacks may inspect mappings but must not allocate or reenter lifecycle
        APIs. An add callback requires a matching remove callback for rollback.
        Validation errors leave ownership with the caller. Once accepted, any
        preparation failure rolls back ALL supplied handles, including unattempted
        ones. Cleanup failures remain visible via get_staged_region_ids for retry.

        Args:
            handles: Fresh server allocations belonging to this instance.
            require_cuda_pin: Reject unpinned mappings only for this preparation.
            targets: Region ID to target ID; required by round-robin placement.

        Returns:
            Ordered IDs to pass to commit_regions or rollback_regions.

        Raises:
            ValueError: If IDs repeat or refer to existing mappings/owned regions.
            RuntimeError: If uninitialized, callbacks are unpaired, or pin is required.
            Exception: Preparation error, or ExceptionGroup if rollback also fails.
        """
        with self._write_lock:
            if self._closing.is_set():
                raise RuntimeError("Handler is closing")
            if self._owned is None or self._mapper is None:
                raise RuntimeError("Memory managers are not initialized")
            ids = [handle.region_id for handle in handles]
            if len(set(ids)) != len(ids) or any(
                rid in self._staged_handles
                or self._owned.is_owned(rid)
                or self._mapper.get_region(rid) is not None
                for rid in ids
            ):
                raise ValueError("Expected fresh, distinct region handles")
            if self._on_region_added is not None and self._on_region_removed is None:
                raise RuntimeError(
                    "Staged preparation requires a region removal callback"
                )
            self._staged_handles.update((h.region_id, h) for h in handles)
            try:
                for handle in handles:
                    region = self._owned.stage_region(
                        handle, target_id=(targets or {}).get(handle.region_id)
                    )
                    rid = handle.region_id
                    if (
                        require_cuda_pin
                        and not self.get_mapping_status(rid).cuda_pinned
                    ):
                        raise RuntimeError(
                            f"Region {rid} requires successful CUDA pinning"
                        )
                    if self._on_region_added is not None:
                        self._staged_callbacks.add(rid)
                        self._notify_region_added(rid, region.allocator.page_count)
                    self._staged_ready.add(rid)
            except Exception as error:
                failures = self._rollback_regions_locked(ids)
                if failures:
                    raise ExceptionGroup(
                        "Region preparation and rollback failed", [error, *failures]
                    ) from None
                raise
            return ids

    def commit_regions(self, region_ids: list[int]) -> None:
        """Publish prepared region_ids atomically, preserving their allocation order.

        Raises:
            ValueError: If any region is unprepared or cleanup has begun.
            RuntimeError: If memory managers are uninitialized.
        """
        with self._write_lock:
            if self._closing.is_set():
                raise RuntimeError("Handler is closing")
            if self._owned is None:
                raise RuntimeError("Memory managers are not initialized")
            if any(rid not in self._staged_ready for rid in region_ids):
                raise ValueError("Only fully prepared regions may be committed")
            self._owned.commit_regions(region_ids)
            for rid in region_ids:
                del self._staged_handles[rid]
                self._staged_ready.remove(rid)
                self._staged_callbacks.discard(rid)

    def rollback_regions(self, region_ids: list[int]) -> None:
        """Release staged region_ids: views, allocator, CUDA pin, mmap, then RPC.

        Repeating a successful rollback is a no-op. Committed/legacy owned regions
        are rejected. A failed cleanup is retained for retry and other requested
        regions are still cleaned. No server return occurs before local release.

        Raises:
            ValueError: If any requested region is already active.
            ExceptionGroup: If cleanup, active users, or server return block release.
        """
        with self._write_lock:
            if self._owned is not None and any(
                self._owned.is_owned(rid) for rid in region_ids
            ):
                raise ValueError("Cannot roll back active owned regions")
            failures = self._rollback_regions_locked(region_ids)
            if failures:
                raise ExceptionGroup(
                    "Region rollback failed; retry pending regions", failures
                )

    # =========================================================================
    # Connection Management
    # =========================================================================

    def connect(self, *, require_cuda_pin: bool = False) -> bool:
        """Connect to the server and request a memory allocation.

        Args:
            require_cuda_pin: ON only: reject unpinned initial/shared regions.
                Defaults to False for CPU correctness tests.

        Raises:
            ValueError: If strict pin validation is requested with OFF.

        Returns:
            True if successful
        """
        if self._group_cleanup_started:
            logger.error("Pending group cleanup: retry close() before connect()")
            return False
        if self._connected:
            return True
        if self._config.placement_policy == "chunk_round_robin":
            return self._connect_group(require_cuda_pin=require_cuda_pin)
        if require_cuda_pin:
            raise ValueError("Strict pin validation is only supported in ON mode")

        try:
            # 1. Connect RPC client
            self._rpc.connect()

            # 1b. Scan local devices and handshake with server
            from maru_shm.device_scanner import scan_dax_devices

            local_devices = scan_dax_devices()
            device_table = dict(local_devices)
            if device_table:
                logger.info("Scanned %d local DAX devices", len(device_table))
            else:
                logger.warning(
                    "No local DAX devices with UUID headers found. "
                    "Multi-node UUID resolution will not work."
                )

            try:
                handshake_resp = self._rpc.handshake()
                rm_address = handshake_resp.get("rm_address") or self._config.rm_address
            except Exception:
                logger.debug("Handshake failed, using config rm_address", exc_info=True)
                rm_address = self._config.rm_address
            self._mapper = DaxMapper(rm_address=rm_address, device_table=device_table)

            # 2. Initialize managers
            self._owned = OwnedRegionManager(
                mapper=self._mapper,
                chunk_size=self._config.chunk_size_bytes,
            )

            # 3. Request initial owned region via RPC
            try:
                response = self._rpc.request_alloc(
                    instance_id=self._config.instance_id,
                    size=self._config.pool_size,
                )
            except Exception:
                logger.error(
                    "RPC request_alloc failed during connect",
                    exc_info=True,
                )
                return False

            if not response.success or response.handle is None:
                logger.error(
                    "Initial allocation failed: %s",
                    getattr(response, "error", "unknown"),
                )
                if self._owned is not None:
                    self._owned.close()
                self._owned = None
                self._rpc.close()
                return False

            # 4. Add region to OwnedRegionManager (mmap + allocator)
            handle = response.handle
            try:
                self._owned.add_region(handle)
            except Exception:
                logger.error("Failed to init region", exc_info=True)
                try:
                    self._rpc.return_alloc(self._config.instance_id, handle.region_id)
                except Exception:
                    logger.debug(
                        "Failed to return allocation during cleanup",
                        exc_info=True,
                    )
                if self._owned is not None:
                    self._owned.close()
                self._owned = None
                self._rpc.close()
                return False

            self._connected = True

            # 5. Pre-map shared regions (eager mapping)
            if self._config.eager_map:
                self._premap_shared_regions()

            # 6. Start stats flush thread with dedicated RPC connection
            if self._config.enable_stats:
                self._stats_rpc = RpcClient(
                    server_url=self._config.server_url,
                    timeout_ms=self._config.timeout_ms,
                )
                self._stats_rpc.connect()
                self._stats_flusher = threading.Thread(
                    target=self._stats_flush_loop,
                    name="stats-flusher",
                    daemon=True,
                )
                self._stats_flusher.start()

            logger.info(
                "Connected: chunk_size=%d",
                self._config.chunk_size_bytes,
            )
            return True

        except Exception:
            logger.error("Failed to connect", exc_info=True)
            return False

    # =========================================================================
    # Timing
    # =========================================================================

    def _record_stats(
        self, op_type: str, size: int, latency_us: float, result: str = "none"
    ) -> None:
        """Buffer a timing entry. Flushed by background thread every 1s."""
        if not self._config.enable_stats or not self._connected:
            return
        entry = {
            "client_id": self._config.instance_id,
            "op_type": op_type,
            "result": result,
            "size": size,
            "latency_us": latency_us,
        }
        with self._stats_lock:
            self._stats_buffer.append(entry)

    def _flush_stats(self) -> None:
        """Send buffered stats via dedicated RPC. Drops on failure (best-effort)."""
        with self._stats_lock:
            buf = self._stats_buffer.copy()
            self._stats_buffer.clear()
        if buf and self._stats_rpc is not None:
            try:
                self._stats_rpc.report_stats(buf)
            except Exception:
                # Stats are best-effort monitoring data. Dropped entries only
                # affect interval accuracy, not application correctness.
                logger.warning("Failed to flush %d stats entries (dropped)", len(buf))

    def _stats_flush_loop(self) -> None:
        """Background loop: flush stats buffer every 1s."""
        while self._connected and not self._closing.is_set():
            self._closing.wait(timeout=1.0)
            self._flush_stats()

    def close(self) -> None:
        """Close the connection and return all allocations.

        Sets ``_closing`` event to reject new operations, then acquires
        ``_write_lock`` to wait for in-flight writes before teardown.

        Raises:
            RuntimeError, BufferError: In ON mode, local mapping or server group
                cleanup is incomplete. Release views/leases and retry close().
            ExceptionGroup: If pending staged rollback fails. The connection and
                existing owned regions remain available so cleanup can be retried.
        """
        if self._config.placement_policy == "chunk_round_robin":
            self._close_group()
            return
        if not self._connected and not self._staged_handles:
            return

        was_closing = self._closing.is_set()
        self._closing.set()  # reject new operations + wake flush thread
        try:
            # Wait for any preparation already in progress before taking the
            # rollback snapshot. New prepare calls now fail the closing check.
            with self._write_lock:
                failures = (
                    self._rollback_regions_locked(list(self._staged_handles))
                    if self._staged_handles
                    else []
                )
            if failures:
                raise ExceptionGroup(
                    "Pending region rollback failed during close", failures
                )
        except Exception:
            if not was_closing:
                self._closing.clear()
            raise
        if not self._connected:
            return

        # Stop stats: close dedicated RPC first (unblocks flush thread),
        # then join thread. Flush loop does one final flush before exiting.
        if self._stats_rpc is not None:
            self._stats_rpc.close()
        if self._stats_flusher is not None:
            self._stats_flusher.join(timeout=2.0)
            self._stats_flusher = None
        self._stats_rpc = None

        try:
            with self._write_lock:
                # 0. Let plugins release resources while regions are still
                #    mapped and the RPC connection is live (e.g. unpin device
                #    ranges) — must run before the unmap in step 3.
                if self._plugins:
                    self._dispatch_plugins("on_close", self)

                # 1. Close owned regions (allocator cleanup only) → get region_ids
                owned_region_ids: list[int] = []
                if self._owned is not None:
                    owned_region_ids = self._owned.close()

                # 2. Return allocations to server via RPC
                for rid in owned_region_ids:
                    try:
                        self._rpc.return_alloc(self._config.instance_id, rid)
                    except Exception:
                        logger.error("Failed to return region %d", rid, exc_info=True)

                # 3. Unmap all regions (owned + shared) via DaxMapper
                if self._mapper is not None:
                    self._mapper.close()

                # 4. Close RPC connection
                self._rpc.close()

        except Exception:
            logger.error("Error during close", exc_info=True)

        finally:
            self._connected = False
            self._owned = None
            self._key_to_location.clear()

    # =========================================================================
    # KV Operations
    # =========================================================================

    def alloc(self, size: int) -> AllocHandle:
        """Allocate a page and return a handle with a writable memoryview.

        The caller writes directly to ``handle.buf``, then passes the handle
        to ``store(key, handle)`` to register without copying.

        Args:
            size: Required bytes (must be <= chunk_size)

        Returns:
            AllocHandle with writable memoryview and allocation metadata

        Raises:
            RuntimeError: If not connected or closing
            ValueError: If size exceeds chunk_size or allocation fails
        """
        self._ensure_connected()
        t0 = time.monotonic()

        with self._write_lock:
            if self._closing.is_set():
                raise RuntimeError("Handler is closing")

            chunk_size = self._owned.get_chunk_size()
            if size > chunk_size:
                raise ValueError(
                    f"Requested size {size} exceeds chunk_size {chunk_size}"
                )

            result = self._owned.allocate()
            if result is None:
                if not self._expand_region():
                    if not self._auto_expand:
                        raise ValueError(
                            "Cannot allocate page: pool exhausted "
                            "and auto_expand is disabled"
                        )
                    raise ValueError(
                        "Cannot allocate page: pool exhausted after expansion attempt"
                    )
                result = self._owned.allocate()
                if result is None:
                    raise ValueError("Cannot allocate page after expansion")

            region_id, page_index = result

            buf = self._mapper.get_buffer_view(
                region_id,
                page_index * chunk_size,
                size,
            )
            if buf is None:
                self._owned.free(region_id, page_index)
                raise ValueError(f"Failed to get buffer view for region {region_id}")

            handle = AllocHandle(
                buf=buf,
                _region_id=region_id,
                _page_index=page_index,
                _size=size,
            )
            logger.debug(
                "alloc: size=%d, region=%d, page=%d",
                size,
                region_id,
                page_index,
            )

        self._record_stats("alloc", size, (time.monotonic() - t0) * 1e6)
        return handle

    def free(self, handle: AllocHandle) -> None:
        """Free a page previously obtained via alloc().

        Can be called before store() (discard) or after (eviction).

        Args:
            handle: AllocHandle from alloc()

        Raises:
            ValueError: If handle is not tracked (already freed or invalid)
        """
        self._ensure_connected()
        t0 = time.monotonic()

        with self._write_lock:
            region_id = handle._region_id
            page_index = handle._page_index

            # Find and remove the key mapping if stored
            key_to_remove = None
            for key, loc in self._key_to_location.items():
                if loc == (region_id, page_index):
                    key_to_remove = key
                    break

            if key_to_remove is not None:
                del self._key_to_location[key_to_remove]

            self._owned.free(region_id, page_index)

            logger.debug(
                "free: region=%d, page=%d, key=%s",
                region_id,
                page_index,
                key_to_remove,
            )

        self._record_stats("free", 0, (time.monotonic() - t0) * 1e6)

    def store(
        self,
        key: str,
        handle: AllocHandle,
    ) -> bool:
        """Register a pre-written page in the KV cache (zero-copy).

        Data must already be written to the page via ``handle.buf``.
        This method only performs duplicate check + metadata registration.

        Args:
            key: The chunk key string
            handle: AllocHandle from alloc()

        Returns:
            True if successful
        """
        self._ensure_connected()
        t0 = time.monotonic()
        store_size = 0
        store_ok = False

        with self._write_lock:
            if self._closing.is_set():
                raise RuntimeError("Handler is closing")

            # Duplicate skip
            if key in self._key_to_location:
                self._owned.free(handle._region_id, handle._page_index)
                logger.debug("store: key=%s already in local map, skipping", key)
                store_ok = True
            elif self._rpc.exists_kv(key):
                self._owned.free(handle._region_id, handle._page_index)
                logger.debug("store: key=%s already exists on server, skipping", key)
                store_ok = True
            else:
                region_id = handle._region_id
                page_index = handle._page_index
                offset = page_index * self._owned.get_chunk_size()
                total_size = handle._size
                store_size = total_size

                try:
                    is_new = self._rpc.register_kv(
                        key=key,
                        region_id=region_id,
                        kv_offset=offset,
                        kv_length=total_size,
                    )
                except Exception:
                    self._owned.free(region_id, page_index)
                    logger.error(
                        "store: register_kv RPC failed for key=%s, freed page (region=%d, page=%d)",
                        key,
                        region_id,
                        page_index,
                        exc_info=True,
                    )
                    store_size = total_size
                    # store_ok stays False
                else:
                    if not is_new:
                        self._owned.free(region_id, page_index)
                        logger.debug(
                            "store: key=%s lost register race, freed page (region=%d, page=%d)",
                            key,
                            region_id,
                            page_index,
                        )
                    else:
                        self._key_to_location[key] = (region_id, page_index)
                        logger.debug(
                            "store: key=%s, region=%d, page=%d, offset=%d, size=%d",
                            key,
                            region_id,
                            page_index,
                            offset,
                            total_size,
                        )
                    store_ok = True

        self._record_stats("store", store_size, (time.monotonic() - t0) * 1e6)
        return store_ok

    def retrieve(self, key: str) -> MemoryInfo | None:
        """Retrieve a zero-copy MemoryInfo from the KV cache.

        Returns a MemoryInfo with a memoryview slice of the mmap region.
        Works for both owned (RW) and shared (RO) regions.

        WARNING: The returned memoryview is only valid while the region
        remains mapped. Do not use after calling close().

        Args:
            key: The chunk key string

        Returns:
            MemoryInfo with memoryview, or None if not found
        """
        self._ensure_connected()
        t0 = time.monotonic()

        result = self._rpc.lookup_kv(key)
        if not result.found or result.handle is None:
            logger.debug("Key %s not found", key)
            self._record_stats(
                "retrieve", 0, (time.monotonic() - t0) * 1e6, result="miss"
            )
            return None

        handle = result.handle
        region_id = handle.region_id

        # Shared region: on-demand mapping
        if not self._owned.is_owned(region_id):
            if self._mapper.get_region(region_id) is None:
                try:
                    self._mapper.map_region(handle)
                except Exception:
                    logger.error(
                        "Failed to map shared region %d", region_id, exc_info=True
                    )
                    return None

        if (
            self._require_cuda_pin
            and not self.get_mapping_status(region_id).cuda_pinned
        ):
            return None

        buf = self._mapper.get_buffer_view(
            region_id, result.kv_offset, result.kv_length
        )
        if buf is None:
            logger.error("Region %d: get_buffer_view returned None", region_id)
            return None

        logger.debug(
            "retrieve: key=%s, region=%d, offset=%d, size=%d, readonly=%s, owned=%s",
            key,
            region_id,
            result.kv_offset,
            result.kv_length,
            buf.readonly,
            self._owned.is_owned(region_id),
        )
        chunk_size = self._owned.get_chunk_size()
        page_index = (
            -1
            if self._config.placement_policy == "chunk_round_robin"
            and not self._owned.is_owned(region_id)
            else result.kv_offset // chunk_size
        )
        self._record_stats(
            "retrieve", result.kv_length, (time.monotonic() - t0) * 1e6, result="hit"
        )
        return MemoryInfo(
            view=buf,
            region_id=region_id,
            page_index=page_index,
            kv_offset=result.kv_offset,
        )

    def exists(self, key: str) -> bool:
        """Check if a key exists.

        Args:
            key: The chunk key string

        Returns:
            True if exists
        """
        self._ensure_connected()
        t0 = time.monotonic()
        result = self._rpc.exists_kv(key)
        self._record_stats(
            "exists",
            0,
            (time.monotonic() - t0) * 1e6,
            result="hit" if result else "miss",
        )
        return result

    def pin(self, key: str) -> bool:
        """Check if a key exists and pin it atomically.

        If the key exists, increments pin_count to protect from eviction.

        Args:
            key: The chunk key string

        Returns:
            True if exists (and was pinned)
        """
        self._ensure_connected()
        t0 = time.monotonic()
        result = self._rpc.pin_kv(key)
        self._record_stats(
            "pin", 0, (time.monotonic() - t0) * 1e6, result="hit" if result else "miss"
        )
        return result

    def unpin(self, key: str) -> bool:
        """Unpin a KV entry, making it eligible for eviction.

        Args:
            key: The chunk key string

        Returns:
            True if unpinned successfully
        """
        self._ensure_connected()
        t0 = time.monotonic()
        result = self._rpc.unpin(key)
        self._record_stats(
            "unpin",
            0,
            (time.monotonic() - t0) * 1e6,
            result="hit" if result else "miss",
        )
        return result

    def delete(self, key: str) -> bool:
        """Delete a key and free the corresponding page.

        Args:
            key: The chunk key string

        Returns:
            True if deleted
        """
        self._ensure_connected()
        t0 = time.monotonic()

        with self._write_lock:
            if self._closing.is_set():
                raise RuntimeError("Handler is closing")

            # RPC first, then local free — prevents inconsistency on RPC failure
            result = self._rpc.delete_kv(key)
            if result:
                location = self._key_to_location.pop(key, None)
                if location is not None:
                    region_id, page_index = location
                    self._owned.free(region_id, page_index)
                logger.debug("Deleted key=%s", key)
            else:
                logger.debug("Delete key=%s: not found on server", key)

        self._record_stats(
            "delete",
            0,
            (time.monotonic() - t0) * 1e6,
            result="hit" if result else "miss",
        )
        return result

    def healthcheck(self) -> bool:
        """Check if the handler and MaruServer are healthy.

        Verifies local connection state and sends a heartbeat RPC
        to confirm the MaruServer is responsive.

        Returns:
            True if connected and server responded to heartbeat
        """
        if not self._connected or self._closing.is_set():
            return False

        try:
            return self._rpc.heartbeat()
        except Exception as e:
            logger.warning("Healthcheck failed: %s", e)
            return False

    def get_stats(self) -> dict:
        """Get server statistics."""
        self._ensure_connected()

        stats = self._rpc.get_stats()
        result = {
            "kv_manager": {
                "total_entries": stats.kv_manager.total_entries,
                "total_size": stats.kv_manager.total_size,
            },
            "allocation_manager": {
                "num_allocations": stats.allocation_manager.num_allocations,
                "total_allocated": stats.allocation_manager.total_allocated,
                "active_clients": stats.allocation_manager.active_clients,
            },
            "stats_manager": stats.stats_manager,
            "cxl_pool": stats.cxl_pool,
        }

        if self._owned is not None:
            store_stats = self._owned.get_stats()
            result["store_regions"] = store_stats
            # Backward compat: first region stats as "allocator"
            regions_list = store_stats.get("regions", [])
            if regions_list:
                result["allocator"] = regions_list[0]

        # Merge plugin-contributed stats under result["plugins"][<PluginClass>].
        for plugin in self._plugins:
            fn = getattr(plugin, "contribute_stats", None)
            if fn is None:
                continue
            try:
                contrib = fn()
            except Exception:
                logger.exception(
                    "handler plugin %s contribute_stats failed",
                    type(plugin).__name__,
                )
                continue
            if contrib:
                result.setdefault("plugins", {})[type(plugin).__name__] = contrib

        return result

    # =========================================================================
    # Batch Operations
    # =========================================================================

    def batch_retrieve(self, keys: list[str]) -> list[MemoryInfo | None]:
        """Retrieve multiple values as MemoryInfo in batch.

        Uses a single batch RPC call for lookup, returns zero-copy
        memoryview slices for both owned (RW) and shared (RO) regions.

        WARNING: Returned memoryviews are only valid while regions remain mapped.

        On RPC failure, allocated pages are freed but data already written to
        those pages is not zeroed. This is safe because the pages are never
        registered with the server and will be overwritten on reuse.

        Args:
            keys: List of chunk key strings

        Returns:
            List of MemoryInfo (None for keys not found)
        """
        self._ensure_connected()
        t0 = time.monotonic()

        try:
            batch_resp = self._rpc.batch_lookup_kv(keys)
        except Exception:
            logger.error("batch_retrieve RPC failed", exc_info=True)
            return [None] * len(keys)

        results: list[MemoryInfo | None] = []
        for i, entry in enumerate(batch_resp.entries):
            if not entry.found or entry.handle is None:
                results.append(None)
                continue

            handle = entry.handle
            region_id = handle.region_id

            # Ensure region is mapped
            if not self._owned.is_owned(region_id):
                if self._mapper.get_region(region_id) is None:
                    try:
                        self._mapper.map_region(handle)
                    except Exception:
                        logger.error(
                            "Failed to map shared region %d",
                            region_id,
                            exc_info=True,
                        )
                        results.append(None)
                        continue

            if (
                self._require_cuda_pin
                and not self.get_mapping_status(region_id).cuda_pinned
            ):
                results.append(None)
                continue

            buf = self._mapper.get_buffer_view(
                region_id, entry.kv_offset, entry.kv_length
            )
            if buf is None:
                logger.error("Region %d: get_buffer_view returned None", region_id)
                results.append(None)
                continue

            logger.debug(
                "batch_retrieve: key=%s, region=%d, offset=%d, size=%d, readonly=%s",
                keys[i],
                region_id,
                entry.kv_offset,
                entry.kv_length,
                buf.readonly,
            )
            chunk_size = self._owned.get_chunk_size()
            page_index = (
                -1
                if self._config.placement_policy == "chunk_round_robin"
                and not self._owned.is_owned(region_id)
                else entry.kv_offset // chunk_size
            )
            results.append(
                MemoryInfo(
                    view=buf,
                    region_id=region_id,
                    page_index=page_index,
                    kv_offset=entry.kv_offset,
                )
            )

        hits = sum(1 for r in results if r is not None)
        ro_count = sum(1 for r in results if r is not None and r.view.readonly)
        logger.debug(
            "batch_retrieve: %d/%d hits, %d readonly (shared), %d writable (owned)",
            hits,
            len(keys),
            ro_count,
            hits - ro_count,
        )
        # Plugins observe the mapped batch (e.g. issue prefetch/pin hints).
        # Guarded so the empty-plugin common case stays free on the hot path.
        if self._plugins:
            self._dispatch_plugins("on_batch_retrieve", self, keys, batch_resp)

        total_bytes = sum(
            entry.kv_length
            for i, entry in enumerate(batch_resp.entries)
            if results[i] is not None and entry.handle is not None
        )
        self._record_stats(
            "batch_retrieve",
            total_bytes,
            (time.monotonic() - t0) * 1e6,
            result="hit" if hits == len(keys) else ("partial" if hits > 0 else "miss"),
        )
        return results

    def batch_store(
        self,
        keys: list[str],
        handles: list[AllocHandle],
    ) -> list[bool]:
        """Register multiple pre-written pages in batch (zero-copy).

        Data must already be written to each page via ``handle.buf``.
        Uses a single batch RPC call for metadata registration.

        Args:
            keys: List of chunk key strings
            handles: List of AllocHandle from alloc()

        Returns:
            List of booleans indicating success for each key
        """
        self._ensure_connected()
        t0 = time.monotonic()

        if len(keys) != len(handles):
            raise ValueError("keys and handles must have the same length")

        with self._write_lock:
            if self._closing.is_set():
                raise RuntimeError("Handler is closing")

            chunk_size = self._owned.get_chunk_size()
            results = [True] * len(keys)
            register_entries = []
            allocations: dict[int, tuple[int, int]] = {}

            # Phase 1: Batch check which keys already exist
            try:
                exists_resp = self._rpc.batch_exists_kv(keys)
                exists_results = exists_resp.results
            except Exception:
                logger.error(
                    "batch_exists RPC failed, proceeding without check", exc_info=True
                )
                exists_results = [False] * len(keys)

            skipped = sum(exists_results)
            if skipped > 0:
                logger.debug(
                    "batch_store: %d/%d keys already exist, skipping",
                    skipped,
                    len(keys),
                )

            # Phase 2: Build register entries, free duplicates
            for i, (key, handle) in enumerate(zip(keys, handles, strict=True)):
                if key in self._key_to_location:
                    self._owned.free(handle._region_id, handle._page_index)
                    logger.debug(
                        "batch_store: key=%s already in local map, skipping", key
                    )
                    continue
                if exists_results[i]:
                    self._owned.free(handle._region_id, handle._page_index)
                    logger.debug(
                        "batch_store: key=%s already exists on server, skipping",
                        key,
                    )
                    continue

                region_id = handle._region_id
                page_index = handle._page_index
                allocations[i] = (region_id, page_index)
                offset = page_index * chunk_size
                register_entries.append((key, region_id, offset, handle._size))

            # Phase 3: Batch register
            if register_entries:
                try:
                    batch_resp = self._rpc.batch_register_kv(register_entries)
                except Exception:
                    logger.error("Batch register RPC failed", exc_info=True)
                    for _idx, (rid, pidx) in allocations.items():
                        self._owned.free(rid, pidx)
                    return [False] * len(keys)

                if not batch_resp.success:
                    logger.error("Batch register RPC failed")
                    for _idx, (rid, pidx) in allocations.items():
                        self._owned.free(rid, pidx)
                    return [False] * len(keys)

                batch_idx = 0
                for i in range(len(keys)):
                    if results[i] and i in allocations:
                        if batch_idx < len(batch_resp.results):
                            results[i] = batch_resp.results[batch_idx]
                        batch_idx += 1

            # Track
            for i, key in enumerate(keys):
                if results[i] and i in allocations:
                    self._key_to_location[key] = allocations[i]

            total_bytes = sum(handles[i]._size for i in range(len(keys)) if results[i])
            logger.debug(
                "batch_store: %d/%d succeeded, total_data=%d bytes",
                sum(results),
                len(keys),
                total_bytes,
            )

        self._record_stats("batch_store", total_bytes, (time.monotonic() - t0) * 1e6)
        return results

    def batch_exists(self, keys: list[str]) -> list[bool]:
        """Check if multiple keys exist.

        Uses a single batch RPC call instead of N individual calls.

        Args:
            keys: List of chunk key strings

        Returns:
            List of booleans indicating existence for each key
        """
        self._ensure_connected()
        t0 = time.monotonic()

        try:
            batch_resp = self._rpc.batch_exists_kv(keys)
        except Exception:
            logger.error("batch_exists RPC failed", exc_info=True)
            return [False] * len(keys)
        hits = sum(batch_resp.results)
        self._record_stats(
            "batch_exists",
            0,
            (time.monotonic() - t0) * 1e6,
            result="hit" if hits == len(keys) else ("partial" if hits > 0 else "miss"),
        )
        return batch_resp.results

    def batch_pin(self, keys: list[str]) -> list[bool]:
        """Check existence and pin multiple keys in a single RPC call.

        Args:
            keys: List of chunk key strings

        Returns:
            List of booleans — True if key exists (and was pinned).
        """
        self._ensure_connected()
        t0 = time.monotonic()
        results = self._rpc.batch_pin_kv(keys).results
        hits = sum(results)
        self._record_stats(
            "batch_pin",
            0,
            (time.monotonic() - t0) * 1e6,
            result="hit" if hits == len(keys) else ("partial" if hits > 0 else "miss"),
        )
        return results

    def batch_unpin(self, keys: list[str]) -> list[bool]:
        """Unpin multiple keys in a single RPC call.

        Args:
            keys: List of chunk key strings

        Returns:
            List of booleans — True if successfully unpinned.
        """
        self._ensure_connected()
        t0 = time.monotonic()
        results = self._rpc.batch_unpin(keys).results
        hits = sum(results)
        self._record_stats(
            "batch_unpin",
            0,
            (time.monotonic() - t0) * 1e6,
            result="hit" if hits == len(keys) else ("partial" if hits > 0 else "miss"),
        )
        return results

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def pool_handle(self) -> MaruHandle | None:
        """Get initial pool handle (backward compat)."""
        if self._owned is None:
            return None
        first_rid = self._owned.get_first_region_id()
        if first_rid is None:
            return None
        mapped = self._mapper.get_region(first_rid)
        return mapped.handle if mapped else None

    @property
    def allocator(self) -> PagedMemoryAllocator | None:
        """Get the first region's allocator (backward compat)."""
        if self._owned is None:
            return None
        return self._owned.get_first_allocator()

    @property
    def owned_region_manager(self) -> OwnedRegionManager | None:
        """Deprecated: Use get_owned_region_ids(), get_region_page_count() instead."""
        return self._owned

    @property
    def instance_id(self) -> str:
        """Get instance ID."""
        return self._config.instance_id

    @property
    def connected(self) -> bool:
        """Check if connected."""
        return self._connected

    # =========================================================================
    # Helpers
    # =========================================================================

    def get_placement_status(self) -> dict:
        """Return placement counters and pending group lifecycle state.

        cleanup_pending requires close() retry before any new connection.
        Group request IDs remain stable across lost replies and cleanup retries.
        """
        status = self._owned.get_placement_status() if self._owned else {}
        return {
            **status,
            "request_id": self._group_request_id,
            "cleanup_pending": self._group_cleanup_started,
        }

    def _notify_region_added(self, region_id: int, page_count: int) -> None:
        if self._on_region_added is not None:
            if self._callback_slot_size:
                self._on_region_added(region_id, page_count, self.get_chunk_size())
            else:
                self._on_region_added(region_id, page_count)

    def _connect_group(self, *, require_cuda_pin: bool) -> bool:
        """Connect the opt-in path; never fall back to single-region allocation."""
        from maru_shm.device_scanner import scan_dax_devices

        if self._group_cleanup_started:
            logger.error("Pending group cleanup: retry close() before connect()")
            return False
        self._closing.clear()
        self._require_cuda_pin = require_cuda_pin
        try:
            self._rpc.connect()
            handshake = self._rpc.handshake()
            if not handshake.get(
                "success"
            ) or "multi_pool_alloc_v1" not in handshake.get("capabilities", []):
                raise RuntimeError("Server lacks multi_pool_alloc_v1 capability")
            if self._mapper is None:
                self._mapper = DaxMapper(
                    rm_address=handshake.get("rm_address") or self._config.rm_address,
                    device_table=dict(scan_dax_devices()),
                )
                self._owned = OwnedRegionManager(
                    self._mapper, self.get_chunk_size(), "chunk_round_robin"
                )
            if self._group_request_id is None:
                self._group_request_id = str(uuid.uuid4())
                self._group_reply_received = False
            try:
                response = self._rpc.request_alloc_group(
                    self._config.instance_id,
                    self._group_request_id,
                    self._config.pool_size,
                    self.get_chunk_size(),
                )
            except Exception:
                # No handles received: replay this exact ID on connect() retry.
                # close() can still return the same group once the server sees it.
                logger.error(
                    "Group reply unavailable; retry connect or close", exc_info=True
                )
                return False
            self._group_reply_received = True
            if (
                not response.success
                and response.state == "failed"
                and not response.regions
                and not response.pending_region_ids
                and not response.outcome_unknown
            ):
                # Rejected before allocation, or all rollback already confirmed.
                self._group_request_id = None
            if not response.success or response.state != "active":
                raise RuntimeError(f"Group allocation failed: {response.error}")
            regions = response.regions
            ids = [region.handle.region_id for region in regions]
            if (
                response.request_id != self._group_request_id
                or not ids
                or len(ids) != len(set(ids))
                or any(
                    not region.target_id
                    or region.page_count
                    != region.handle.length // self.get_chunk_size()
                    for region in regions
                )
            ):
                raise ValueError("Invalid allocation group response")
            self.prepare_regions(
                [region.handle for region in regions],
                require_cuda_pin=require_cuda_pin,
                targets={
                    region.handle.region_id: region.target_id for region in regions
                },
            )
            self.commit_regions(ids)
            self._connected = True
            if self._config.eager_map:
                self._premap_shared_regions()
            if self._config.enable_stats:
                self._stats_rpc = RpcClient(
                    server_url=self._config.server_url,
                    timeout_ms=self._config.timeout_ms,
                )
                self._stats_rpc.connect()
                self._stats_flusher = threading.Thread(
                    target=self._stats_flush_loop, name="stats-flusher", daemon=True
                )
                self._stats_flusher.start()
            return True
        except Exception:
            logger.error("Group connection failed", exc_info=True)
        # Outside except: traceback references must not retain mapped buffers.
        try:
            self._close_group()
        except Exception:
            logger.error("Group cleanup pending; retry close()", exc_info=True)
        return False

    def _close_group(self) -> None:
        """Strict local teardown before releasing the server allocation group."""
        self._group_cleanup_started = True
        self._closing.set()
        if self._stats_rpc is not None:
            self._stats_rpc.close()
            self._stats_rpc = None
        if self._stats_flusher is not None:
            self._stats_flusher.join(timeout=2)
            self._stats_flusher = None
        with self._write_lock:
            if self._staged_handles:
                errors = self._rollback_regions_locked(list(self._staged_handles))
                if errors:
                    raise ExceptionGroup("Group preparation cleanup pending", errors)
            # Preserve unmap failures for retries. Do not return any remaining
            # server regions until all local mappings/views are gone.
            if self._mapper is not None:
                if self._plugins:
                    self._dispatch_plugins("on_close", self)
                for rid in self._mapper.get_region_ids():
                    if self.get_mapping_status(rid).active_users:
                        raise RuntimeError(f"Region {rid} has active users")
                if self._owned is not None:
                    self._owned.close()
                for rid in self._mapper.get_region_ids():
                    if self._on_region_removed is not None:
                        self._on_region_removed(rid)
                    self._mapper.release_region(rid)
            if self._group_request_id is not None:
                if not self._group_reply_received:
                    # A lost request/reply is resolved with the SAME ID before
                    # cancellation, including a request that never reached server.
                    allocation = self._rpc.request_alloc_group(
                        self._config.instance_id,
                        self._group_request_id,
                        self._config.pool_size,
                        self.get_chunk_size(),
                    )
                    self._group_reply_received = True
                    if (
                        not allocation.success
                        and allocation.state == "failed"
                        and not allocation.regions
                        and not allocation.pending_region_ids
                        and not allocation.outcome_unknown
                    ):
                        self._group_request_id = None
            if self._group_request_id is not None:
                response = self._rpc.return_alloc_group(
                    self._config.instance_id, self._group_request_id
                )
                if (
                    not response.success
                    or response.pending_region_ids
                    or response.outcome_unknown
                ):
                    raise RuntimeError("Server allocation group cleanup pending")
            if self._mapper is not None:
                self._mapper.close()
            self._rpc.close()
            self._mapper = None
            self._owned = None
            self._connected = False
            self._group_request_id = None
            self._group_reply_received = False
            self._group_cleanup_started = False
            self._key_to_location.clear()

    def _rollback_regions_locked(self, region_ids: list[int]) -> list[Exception]:
        """Clean accepted staged handles while holding the lifecycle/write lock."""
        failures: list[Exception] = []
        for rid in reversed(region_ids):
            if rid not in self._staged_handles:
                continue
            self._staged_ready.discard(rid)
            try:
                if self._owned is None or self._mapper is None:
                    raise RuntimeError("Memory managers are not initialized")
                instance_id = self._config.instance_id
                if instance_id is None:
                    raise RuntimeError("Instance ID is not initialized")
                if self.get_mapping_status(rid).active_users:
                    raise RuntimeError(f"Region {rid} has active users")
                if rid in self._staged_callbacks:
                    if self._on_region_removed is None:
                        raise RuntimeError(f"Missing removal callback for region {rid}")
                    self._on_region_removed(rid)
                    self._staged_callbacks.remove(rid)
                self._owned.remove_region(rid)
                self._mapper.release_region(rid)
                if not self._rpc.return_alloc(instance_id, rid):
                    raise RuntimeError(f"Server return failed for region {rid}")
                del self._staged_handles[rid]
            except Exception as error:
                failures.append(error)
        return failures

    def _expand_region(self) -> bool:
        """Request a new store region from the server and add it.

        Gated by ``auto_expand`` config.

        Returns:
            True if expansion succeeded.
        """
        if self._config.placement_policy == "chunk_round_robin":
            return False
        if not self._auto_expand:
            logger.warning(
                "Pool exhausted but auto_expand is disabled. "
                "Set auto_expand=True in MaruConfig to enable."
            )
            return False

        try:
            response = self._rpc.request_alloc(
                instance_id=self._config.instance_id,
                size=self._expand_size,
            )
        except Exception:
            logger.error("RPC request_alloc failed during expand", exc_info=True)
            return False

        if not response.success or response.handle is None:
            logger.warning(
                "Region expansion refused: %s",
                getattr(response, "error", "unknown"),
            )
            return False

        handle = response.handle
        try:
            region = self._owned.add_region(handle)
            logger.info("Expanded: new store region %d", handle.region_id)
            if self._on_region_added is not None:
                logger.debug(
                    "on_region_added fire: region=%d pages=%d",
                    handle.region_id,
                    region.allocator.page_count,
                )
                self._notify_region_added(handle.region_id, region.allocator.page_count)
            return True
        except Exception:
            logger.error("Failed to init expanded region", exc_info=True)
            try:
                self._rpc.return_alloc(self._config.instance_id, handle.region_id)
            except Exception:
                logger.debug(
                    "Failed to return allocation during expansion cleanup",
                    exc_info=True,
                )
            return False

    def _premap_shared_regions(self) -> None:
        """Pre-map all existing shared regions from other instances.

        Called during connect() to eliminate mmap from the retrieve hot path.
        Failures are logged but do not block connection — lazy fallback
        remains as safety net in retrieve().
        """
        try:
            response = self._rpc.list_allocations(
                exclude_instance_id=self._config.instance_id
            )
        except Exception as e:
            logger.warning("Failed to list allocations for pre-map: %s", e)
            return

        if not response.success:
            logger.warning(
                "list_allocations failed: %s",
                response.error or "unknown",
            )
            return

        # NOTE: Race window exists between list_allocations() and map_region().
        # A region owner may disconnect between these calls, making the handle
        # stale. This is safe — map_region() failure is caught below and lazy
        # fallback in retrieve() handles it.
        mapped_count = 0
        for handle in response.allocations:
            if self._mapper.get_region(handle.region_id) is not None:
                continue  # already mapped (own region)
            try:
                self._mapper.map_region(handle, prefault=False)
                mapped_count += 1
            except Exception as e:
                logger.warning(
                    "Failed to pre-map shared region %d: %s",
                    handle.region_id,
                    e,
                )

        logger.info(
            "Pre-mapped %d shared regions (%d total from server)",
            mapped_count,
            len(response.allocations),
        )

    def _ensure_connected(self) -> None:
        """Ensure connected, raise if not or if closing."""
        if self._closing.is_set():
            raise RuntimeError("Handler is closing")
        if not self._connected or self._owned is None:
            raise RuntimeError("Not connected. Call connect() first.")

    # =========================================================================
    # Context Manager
    # =========================================================================

    def __enter__(self) -> "MaruHandler":
        """Context manager entry."""
        if self._config.auto_connect:
            self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

"""RpcHandlerMixin - Shared message dispatch and handler methods for RPC servers."""

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from maru_common import MessageType
from maru_common.protocol import HEADER_SIZE, MS_MESSAGE_CLASSES
from maru_common.serializer import DecodedRequest, MessageDecodeError, Serializer
from maru_common.storage_types import Status

if TYPE_CHECKING:
    from .server import MaruServer

logger = logging.getLogger(__name__)

_MS_CODES = frozenset(int(code) for code in MS_MESSAGE_CLASSES)


def is_ms_frame(frame: bytes) -> bool:
    """A multi-storage request (0x40-0x5F), by its header's msg_type byte."""
    return len(frame) >= HEADER_SIZE and frame[3] in _MS_CODES


class RpcHandlerMixin:
    """
    Mixin providing shared message dispatch and handler methods for RPC servers.

    Requires the subclass to define ``self._server`` of type ``MaruServer``.
    """

    _server: "MaruServer"
    _handlers: dict[int, Callable[[Any], dict]] | None = None
    _ms_handlers: dict[int, Callable[[Any, dict], Any]] | None = None

    def _get_handlers(self) -> dict[int, Callable[[Any], dict]]:
        """Return cached handler dispatch dict (built once per instance)."""
        if self._handlers is None:
            self._handlers = {
                MessageType.STORAGE.value: self._handle_storage,
                MessageType.REQUEST_ALLOC.value: self._handle_request_alloc,
                MessageType.RETURN_ALLOC.value: self._handle_return_alloc,
                MessageType.LIST_ALLOCATIONS.value: self._handle_list_allocations,
                MessageType.REGISTER_KV.value: self._handle_register_kv,
                MessageType.LOOKUP_KV.value: self._handle_lookup_kv,
                MessageType.EXISTS_KV.value: self._handle_exists_kv,
                MessageType.DELETE_KV.value: self._handle_delete_kv,
                MessageType.PIN_KV.value: self._handle_pin_kv,
                MessageType.UNPIN_KV.value: self._handle_unpin_kv,
                # Batch operations
                MessageType.BATCH_REGISTER_KV.value: self._handle_batch_register_kv,
                MessageType.BATCH_LOOKUP_KV.value: self._handle_batch_lookup_kv,
                MessageType.BATCH_EXISTS_KV.value: self._handle_batch_exists_kv,
                MessageType.BATCH_PIN_KV.value: self._handle_batch_pin_kv,
                MessageType.BATCH_UNPIN_KV.value: self._handle_batch_unpin_kv,
                # Admin
                MessageType.GET_STATS.value: self._handle_get_stats,
                MessageType.GET_USAGE.value: self._handle_get_usage,
                MessageType.HEARTBEAT.value: self._handle_heartbeat,
                MessageType.REPORT_STATS.value: self._handle_report_stats,
                MessageType.HANDSHAKE.value: self._handle_handshake,
            }
        return self._handlers

    def _handle_message(self, msg_type: int, request: Any) -> dict:
        """Dispatch message to appropriate handler."""
        handler = self._get_handlers().get(msg_type)
        if handler is None:
            return {"error": f"Unknown message type: {msg_type}"}

        return handler(request)

    # =========================================================================
    # Multi-storage (0x40 - 0x5F)
    # =========================================================================

    def _get_ms_handlers(self) -> dict[int, Callable[[Any, dict], Any]]:
        """Handlers take (request, raw payload) and return the response."""
        if self._ms_handlers is None:
            ms = self._server.ms_service
            self._ms_handlers = {
                MessageType.NODE_OPEN.value: ms.node_open,
                MessageType.NODE_HEARTBEAT.value: ms.node_heartbeat,
                MessageType.NODE_CLOSE.value: ms.node_close,
                MessageType.NODE_RECOVER_LIST.value: ms.node_recover_list,
                MessageType.WORKER_EXIT.value: ms.worker_exit,
                MessageType.ADMIN_DELETE.value: ms.admin_delete,
                MessageType.WORKER_OPEN.value: ms.worker_open,
                MessageType.WORKER_HEARTBEAT.value: ms.worker_heartbeat,
                MessageType.WORKER_CLOSE.value: ms.worker_close,
                MessageType.PROBE.value: ms.probe,
                MessageType.LOOKUP_AND_PIN.value: ms.lookup_and_pin,
                MessageType.UNPIN.value: ms.unpin,
                MessageType.SOURCE_PIN.value: ms.source_pin,
                MessageType.COMMIT.value: ms.commit,
                MessageType.RETIRE.value: ms.retire,
                MessageType.FINALIZE_MOVE.value: ms.finalize_move,
                MessageType.FETCH_COMPLETE.value: ms.fetch_complete,
            }
        return self._ms_handlers

    def _handle_ms_message(self, decoded: DecodedRequest) -> Any:
        self._server.ms_service.ensure_started()
        handler = self._get_ms_handlers()[decoded.header.msg_type]
        return handler(decoded.request, decoded.payload)

    def _process_ms_frame(self, serializer: Serializer, raw_data: bytes) -> bytes:
        """Strict decode; a malformed payload or a failed handler gets the
        request's response with REJECTED. Raises only for an
        unreadable header."""
        try:
            decoded = serializer.decode_request_payload(raw_data)
        except MessageDecodeError as e:
            if e.header is None or e.header.msg_type not in _MS_CODES:
                raise
            logger.debug("rejecting a malformed request: %s", e)
            _, response_cls = MS_MESSAGE_CLASSES[MessageType(e.header.msg_type)]
            return serializer.encode_response(
                e.header, response_cls(status=Status.REJECTED, error=str(e))
            )
        try:
            response = self._handle_ms_message(decoded)
        except Exception as e:
            logger.error(
                "multi-storage request 0x%02X failed",
                decoded.header.msg_type,
                exc_info=True,
            )
            _, response_cls = MS_MESSAGE_CLASSES[MessageType(decoded.header.msg_type)]
            response = response_cls(
                status=Status.REJECTED, error=str(e) or "internal server error"
            )
        return serializer.encode_response(decoded.header, response)

    # =========================================================================
    # Allocation Handlers
    # =========================================================================

    def _handle_request_alloc(self, req: Any) -> dict:
        handle = self._server.request_alloc(
            instance_id=req.instance_id,
            size=req.size,
        )
        if handle is None:
            logger.debug(
                "[REQUEST_ALLOC] instance=%s, size=%d -> FAILED",
                req.instance_id,
                req.size,
            )
            return {"success": False, "error": "Allocation failed"}
        logger.debug(
            "[REQUEST_ALLOC] instance=%s, size=%d -> region_id=%d",
            req.instance_id,
            req.size,
            handle.region_id,
        )
        return {"success": True, "handle": handle.to_dict()}

    def _handle_return_alloc(self, req: Any) -> dict:
        success = self._server.return_alloc(
            instance_id=req.instance_id,
            region_id=req.region_id,
        )
        return {"success": success}

    def _handle_list_allocations(self, req: Any) -> dict:
        handles = self._server.list_allocations(
            exclude_instance_id=req.exclude_instance_id,
        )
        logger.debug(
            "[LIST_ALLOCATIONS] exclude=%s -> %d regions",
            req.exclude_instance_id,
            len(handles),
        )
        return {
            "success": True,
            "allocations": [h.to_dict() for h in handles],
        }

    # =========================================================================
    # KV Handlers
    # =========================================================================

    def _handle_register_kv(self, req: Any) -> dict:
        logger.debug(
            "[PUT] key=%s, region_id=%d, kv_offset=%d, kv_length=%d",
            req.key,
            req.region_id,
            req.kv_offset,
            req.kv_length,
        )
        is_new = self._server.register_kv(
            key=req.key,
            region_id=req.region_id,
            kv_offset=req.kv_offset,
            kv_length=req.kv_length,
        )
        return {"success": True, "is_new": is_new}

    def _handle_lookup_kv(self, req: Any) -> dict:
        result = self._server.lookup_kv(key=req.key)
        if result is None:
            logger.debug("[GET] key=%s -> NOT FOUND", req.key)
            return {"found": False}
        logger.debug(
            "[GET] key=%s -> region_id=%d, kv_offset=%d, kv_length=%d",
            req.key,
            result["handle"].region_id,
            result["kv_offset"],
            result["kv_length"],
        )
        return {
            "found": True,
            "handle": result["handle"].to_dict(),
            "kv_offset": result["kv_offset"],
            "kv_length": result["kv_length"],
        }

    def _handle_exists_kv(self, req: Any) -> dict:
        exists = self._server.exists_kv(key=req.key)
        return {"exists": exists}

    def _handle_delete_kv(self, req: Any) -> dict:
        success = self._server.delete_kv(key=req.key)
        return {"success": success}

    def _handle_pin_kv(self, req: Any) -> dict:
        exists = self._server.pin_kv(key=req.key)
        logger.debug("[PIN] key=%s -> %s", req.key, exists)
        return {"exists": exists}

    def _handle_unpin_kv(self, req: Any) -> dict:
        success = self._server.unpin(key=req.key)
        logger.debug("[UNPIN] key=%s -> %s", req.key, success)
        return {"success": success}

    # =========================================================================
    # Batch KV Handlers
    # =========================================================================

    def _handle_batch_register_kv(self, req: Any) -> dict:
        """Handle batch register KV request."""
        entries = [(e.key, e.region_id, e.kv_offset, e.kv_length) for e in req.entries]
        logger.debug("[BATCH_PUT] %d entries", len(entries))
        for e in req.entries:
            logger.debug(
                "[BATCH_PUT] key=%s, region_id=%d, kv_offset=%d, kv_length=%d",
                e.key,
                e.region_id,
                e.kv_offset,
                e.kv_length,
            )
        results = self._server.batch_register_kv(entries)
        return {"success": True, "results": results}

    def _handle_batch_lookup_kv(self, req: Any) -> dict:
        """Handle batch lookup KV request."""
        keys = req.keys
        logger.debug("[BATCH_GET] %d keys", len(keys))
        results = self._server.batch_lookup_kv(keys)

        entries = []
        for result in results:
            if result is None:
                entries.append({"found": False})
            else:
                logger.debug(
                    "[BATCH_GET] region_id=%d, kv_offset=%d, kv_length=%d",
                    result["handle"].region_id,
                    result["kv_offset"],
                    result["kv_length"],
                )
                entries.append(
                    {
                        "found": True,
                        "handle": result["handle"].to_dict(),
                        "kv_offset": result["kv_offset"],
                        "kv_length": result["kv_length"],
                    }
                )
        return {"entries": entries}

    def _handle_batch_exists_kv(self, req: Any) -> dict:
        """Handle batch exists KV request."""
        keys = req.keys
        results = self._server.batch_exists_kv(keys)
        hits = sum(results)
        logger.debug("[BATCH_EXISTS] %d keys, %d hits", len(keys), hits)
        return {"results": results}

    def _handle_batch_pin_kv(self, req: Any) -> dict:
        """Handle batch pin KV request."""
        keys = req.keys
        results = self._server.batch_pin_kv(keys)
        hits = sum(results)
        logger.debug(
            "[BATCH_PIN] %d keys, %d pinned (prefix-stop)",
            len(keys),
            hits,
        )
        return {"results": results}

    def _handle_batch_unpin_kv(self, req: Any) -> dict:
        """Handle batch unpin KV request."""
        keys = req.keys
        results = self._server.batch_unpin(keys)
        ok = sum(results)
        logger.debug("[BATCH_UNPIN] %d keys, %d ok", len(keys), ok)
        return {"results": results}

    # =========================================================================
    # Admin Handlers
    # =========================================================================

    def _handle_get_stats(self, _req: Any) -> dict:
        stats = self._server.get_stats()
        return stats

    def _handle_get_usage(self, _req: Any) -> dict:
        usage = self._server.get_usage()
        logger.debug(
            "[GET_USAGE] %d instances, pool_free=%d/%d",
            len(usage.get("instances", [])),
            usage.get("pool_free", 0),
            usage.get("pool_total", 0),
        )
        return usage

    def _handle_report_stats(self, req: Any) -> dict:
        self._server.report_stats(req.entries)
        return {"success": True}

    def _handle_heartbeat(self, _req: Any) -> dict:
        return {}

    def _handle_handshake(self, req: Any) -> dict:
        from maru_common.storage_types import (
            MIXED_STORAGE_CAPABILITY,
            REMOTE_CXL_STORAGE_CAPABILITY,
            STORAGE_CAPABILITY,
        )

        cxl = self._server.replica_directory.supports_mixed
        remote_cxl_agents = list(getattr(self._server, "remote_cxl_agents", []))
        return {
            "success": True,
            "rm_address": self._server.rm_address,
            "remote_cxl_agents": remote_cxl_agents,
            "capabilities": [STORAGE_CAPABILITY]
            + ([MIXED_STORAGE_CAPABILITY] if cxl else [])
            + ([REMOTE_CXL_STORAGE_CAPABILITY] if cxl and remote_cxl_agents else []),
            "server_epoch": self._server.replica_directory.server_epoch,
        }

    def _handle_storage(self, req: Any) -> dict:
        return self._server.replica_directory.execute(req.action, req.payload)

# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Message serialization for RPC protocol.

Binary-only serialization using MessagePack with protocol header.
"""

import hashlib
import itertools
import logging
import threading
from dataclasses import asdict, dataclass, is_dataclass
from enum import Enum
from typing import Any, TypeVar

import msgpack
from dacite import Config, DaciteError, from_dict

from maru_shm.types import MaruHandle, MaruPoolInfo

from .protocol import (
    HEADER_SIZE,
    MESSAGE_CLASSES,
    MS_MESSAGE_CLASSES,
    PROTOCOL_MAGIC,
    PROTOCOL_VERSION,
    MessageFlags,
    MessageHeader,
    MessageType,
)

T = TypeVar("T")

logger = logging.getLogger(__name__)

# A response's msg_type is its request's code with this bit set, on every
# endpoint (MaruServer and StorageService alike).
RESPONSE_BIT = int(MessageType.RESPONSE)
OP_DIGEST_SIZE = 16
# Fields that say who sends and under which session, not what is asked; a
# resend after reopening a session must give the same digest.
ENVELOPE_FIELDS = ("op", "session", "server_epoch", "worker_session")
# The header sequence is 32 bits; 0 is kept for NOTIFY.
_SEQ_MOD = 1 << 32


class MessageDecodeError(ValueError):
    """A multi-storage message whose payload does not fit its type.

    ``header`` is the parsed header, or None when the header itself was
    broken, so that the receiver can answer REJECTED to the right request.
    """

    def __init__(self, message: str, header: MessageHeader | None = None):
        super().__init__(message)
        self.header = header


@dataclass(frozen=True)
class DecodedRequest:
    """A request with its header and the raw payload it was decoded from;
    the receiver computes op digests from ``payload`` (see op_digest)."""

    header: MessageHeader
    request: Any
    payload: dict[str, Any]


def _reject_bool(value: Any) -> Any:
    if isinstance(value, bool):
        raise ValueError(f"expected a number, got {value!r}")
    return value


def _number_as_float(value: Any) -> Any:
    _reject_bool(value)
    return float(value) if isinstance(value, int) else value


def _to_serializable(obj: Any) -> Any:
    """Convert object to serializable format, handling nested objects with to_dict()."""
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    elif is_dataclass(obj) and not isinstance(obj, type):
        return {k: _to_serializable(v) for k, v in asdict(obj).items()}
    elif isinstance(obj, dict):
        return {k: _to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list | tuple):
        return [_to_serializable(item) for item in obj]
    else:
        return obj


def _canonical(obj: Any) -> Any:
    """Plain values with dict keys sorted at every level, so that a payload
    and its decoded copy encode to the same bytes."""
    obj = _to_serializable(obj)
    if isinstance(obj, dict):
        return {k: _canonical(obj[k]) for k in sorted(obj)}
    if isinstance(obj, list):
        return [_canonical(item) for item in obj]
    if isinstance(obj, Enum):
        return obj.value
    return obj


def op_digest(request: Any) -> bytes:
    """Digest of what a request asks (spec chapter 1, op).

    BLAKE2b-128 of the MessagePack encoding of the request with the envelope
    fields (op, session, server_epoch, worker_session) removed, dict keys
    sorted at every level, enums as their values and tuples as lists. The
    sender computes it from the request it builds. The receiver computes it
    from the raw payload (DecodedRequest.payload), never from the decoded
    dataclass: fields one side does not know, or defaults it filled in, would
    make the two differ.
    """
    payload = _canonical(request)
    if not isinstance(payload, dict):
        raise TypeError(f"op_digest needs a request, got {type(request).__name__}")
    for name in ENVELOPE_FIELDS:
        payload.pop(name, None)
    packed = msgpack.packb(payload, use_bin_type=True)
    return hashlib.blake2b(packed, digest_size=OP_DIGEST_SIZE).digest()


class Serializer:
    """
    Binary message serializer using MessagePack.

    Message format::

        [Header 16 bytes][MessagePack payload]

    ``message_classes`` maps each request code of one endpoint to its request
    and response dataclasses; the default is the MaruServer table.
    ``strict_codes`` are the codes decoded with type checks (wrong types,
    unknown enum values and non-map payloads raise MessageDecodeError). By
    default these are the multi-storage codes of the MaruServer table, or
    every code of a table passed in; legacy messages keep the lenient decode.
    """

    def __init__(
        self,
        message_classes: dict | None = None,
        strict_codes: Any | None = None,
    ):
        """Initialize serializer."""
        self._seq_counter = itertools.count(1)
        self._seq_lock = threading.Lock()
        self._message_classes = (
            MESSAGE_CLASSES if message_classes is None else message_classes
        )
        if strict_codes is None:
            strict_codes = (
                MS_MESSAGE_CLASSES if message_classes is None else message_classes
            )
        self._strict_codes = {int(c) for c in strict_codes}
        maru_shm_hooks = {
            MaruHandle: lambda d: MaruHandle.from_dict(d) if isinstance(d, dict) else d,
            MaruPoolInfo: lambda d: (
                MaruPoolInfo.from_dict(d) if isinstance(d, dict) else d
            ),
        }
        # dacite config for nested dataclass conversion with type hooks for maru_shm types
        self._dacite_config = Config(check_types=False, type_hooks=maru_shm_hooks)
        # Multi-storage messages: enums travel as their values and tuples as
        # lists; every field is type checked, and an int is a valid float
        self._strict_config = Config(
            check_types=True,
            cast=[Enum, tuple],
            type_hooks={
                **maru_shm_hooks,
                float: _number_as_float,
                int: _reject_bool,
            },
        )

    def _next_seq(self) -> int:
        """Next header sequence: wraps at 32 bits and skips 0 (NOTIFY)."""
        with self._seq_lock:
            seq = next(self._seq_counter) % _SEQ_MOD
            if seq == 0:
                seq = next(self._seq_counter) % _SEQ_MOD
            return seq

    def _from_payload(self, header: MessageHeader, cls: type, payload: Any) -> Any:
        if header.msg_type & ~RESPONSE_BIT not in self._strict_codes:
            return from_dict(data_class=cls, data=payload, config=self._dacite_config)
        if not isinstance(payload, dict):
            raise MessageDecodeError(
                f"{cls.__name__}: payload is {type(payload).__name__}, not a map",
                header,
            )
        try:
            return from_dict(data_class=cls, data=payload, config=self._strict_config)
        except (DaciteError, ValueError, TypeError) as e:
            raise MessageDecodeError(f"{cls.__name__}: {e}", header) from e

    # =========================================================================
    # Encoding
    # =========================================================================

    def encode(
        self,
        msg_type: MessageType,
        data: Any,
        flags: int = MessageFlags.NONE,
        seq: int | None = None,
    ) -> bytes:
        """
        Encode a message to bytes.

        Args:
            msg_type: Message type enum
            data: Payload data (dict or dataclass)
            flags: Message flags
            seq: Sequence number (auto-generated if None)

        Returns:
            Encoded message bytes (header + msgpack payload)
        """
        # Convert to serializable dict
        payload_dict = _to_serializable(data)
        if not isinstance(payload_dict, dict):
            payload_dict = {"value": payload_dict}

        # Encode payload
        payload = msgpack.packb(payload_dict, use_bin_type=True)

        # Create header
        header = MessageHeader(
            magic=PROTOCOL_MAGIC,
            version=PROTOCOL_VERSION,
            msg_type=msg_type,
            flags=flags,
            sequence=seq if seq is not None else self._next_seq(),
            payload_length=len(payload),
        )

        return header.pack() + payload

    # =========================================================================
    # Decoding
    # =========================================================================

    def decode(self, data: bytes) -> tuple[MessageHeader, dict[str, Any]]:
        """
        Decode message bytes to dict payload (low-level).

        Args:
            data: Raw message bytes

        Returns:
            Tuple of (header, payload_dict)

        Raises:
            ValueError: If message format is invalid
        """
        if len(data) < HEADER_SIZE:
            raise ValueError(f"Message too short: {len(data)} bytes")

        # Parse header
        header = MessageHeader.unpack(data[:HEADER_SIZE])

        if not header.validate():
            raise ValueError(f"Invalid magic number: 0x{header.magic:04X}")

        # Parse payload
        payload_data = data[HEADER_SIZE : HEADER_SIZE + header.payload_length]
        if len(payload_data) < header.payload_length:
            raise ValueError(
                f"Payload truncated: {len(payload_data)} < {header.payload_length}"
            )

        payload = msgpack.unpackb(payload_data, raw=False)

        return header, payload

    def decode_request(self, data: bytes) -> tuple[MessageHeader, Any]:
        """
        Decode message bytes to typed request dataclass.

        Args:
            data: Raw message bytes

        Returns:
            Tuple of (header, request_dataclass)

        Raises:
            ValueError: If message format is invalid or unknown message type
        """
        header, payload = self.decode(data)

        msg_type = header.msg_type
        if msg_type not in self._message_classes:
            raise ValueError(f"Unknown message type: 0x{msg_type:02X}")

        request_cls, _ = self._message_classes[msg_type]
        return header, self._from_payload(header, request_cls, payload)

    def decode_request_payload(self, data: bytes) -> DecodedRequest:
        """Decode a request and keep its raw payload, for op digests.

        Raises MessageDecodeError (with the header when it was readable) for
        anything that is not a valid request of this endpoint.
        """
        try:
            header, payload = self.decode(data)
        except (ValueError, msgpack.UnpackException) as e:
            raise MessageDecodeError(f"undecodable message: {e}") from e
        if header.msg_type not in self._message_classes:
            raise MessageDecodeError(
                f"Unknown message type: 0x{header.msg_type:02X}", header
            )
        request_cls, _ = self._message_classes[header.msg_type]
        request = self._from_payload(header, request_cls, payload)
        return DecodedRequest(header=header, request=request, payload=payload)

    def decode_response(self, data: bytes) -> tuple[MessageHeader, Any]:
        """
        Decode message bytes to typed response dataclass.

        Args:
            data: Raw message bytes

        Returns:
            Tuple of (header, response_dataclass)

        Raises:
            ValueError: If message format is invalid or unknown message type
        """
        header, payload = self.decode(data)

        msg_type = header.msg_type

        # First try the raw msg_type (handles admin messages where 0x80 bit is already set)
        if msg_type in self._message_classes:
            _, response_cls = self._message_classes[msg_type]
        # Then try extracting request type from response type (for regular messages)
        elif msg_type & RESPONSE_BIT:
            request_type = msg_type & ~RESPONSE_BIT
            if request_type in self._message_classes:
                _, response_cls = self._message_classes[request_type]
            else:
                raise ValueError(f"Unknown message type: 0x{msg_type:02X}")
        else:
            raise ValueError(f"Unknown message type: 0x{msg_type:02X}")

        return header, self._from_payload(header, response_cls, payload)

    def decode_as(self, data: bytes, data_class: type[T]) -> tuple[MessageHeader, T]:
        """
        Decode message bytes to specified dataclass type.

        Args:
            data: Raw message bytes
            data_class: Target dataclass type

        Returns:
            Tuple of (header, dataclass_instance)
        """
        header, payload = self.decode(data)
        result = from_dict(
            data_class=data_class, data=payload, config=self._dacite_config
        )
        return header, result

    # =========================================================================
    # Response Encoding
    # =========================================================================

    def encode_response(
        self,
        request_header: MessageHeader,
        data: Any,
    ) -> bytes:
        """
        Encode a response message.

        Args:
            request_header: Original request header (for seq matching)
            data: Response payload

        Returns:
            Encoded response bytes
        """
        # Use int for response type (request_type | RESPONSE flag)
        response_type = request_header.msg_type | RESPONSE_BIT
        return self._encode_raw(
            msg_type=response_type,
            data=data,
            flags=request_header.flags,
            seq=request_header.sequence,
        )

    def _encode_raw(
        self,
        msg_type: int,
        data: Any,
        flags: int = MessageFlags.NONE,
        seq: int | None = None,
    ) -> bytes:
        """Internal encode with raw int msg_type."""
        # Convert to serializable dict
        payload_dict = _to_serializable(data)
        if not isinstance(payload_dict, dict):
            payload_dict = {"value": payload_dict}

        # Encode payload
        payload = msgpack.packb(payload_dict, use_bin_type=True)

        # Create header
        header = MessageHeader(
            magic=PROTOCOL_MAGIC,
            version=PROTOCOL_VERSION,
            msg_type=msg_type,
            flags=flags,
            sequence=seq if seq is not None else self._next_seq(),
            payload_length=len(payload),
        )

        return header.pack() + payload


# =============================================================================
# Convenience functions
# =============================================================================


def create_serializer(message_classes: dict | None = None) -> Serializer:
    """Create a serializer instance."""
    return Serializer(message_classes)

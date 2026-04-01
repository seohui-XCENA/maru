/* SPDX-License-Identifier: Apache-2.0
 * Copyright 2026 XCENA Inc.
 *
 * CXL-RPC shared memory layout — THE canonical definition.
 *
 * This header is the single source of truth for the CXL-RPC channel
 * memory layout. All implementations (C++ RM server, Python MetaServer,
 * Python client) must match these definitions byte-for-byte.
 *
 * Python side: maru_common/cxl_rpc.py imports constants from here
 * via a generated or manually-synced copy.
 *
 * Architecture:
 *   Handler (Python) ↔ MetaServer (Python)  — both use cxl_rpc.py
 *   MetaServer (Python) ↔ RM (C++)         — Python client, C++ server
 *
 *   All share the same Channel layout defined here.
 */

#pragma once

#include <cstddef>
#include <cstdint>

namespace maru {
namespace cxl_rpc {

/* =========================================================================
 * Constants
 * ========================================================================= */

static constexpr int CACHELINE_SIZE = 64;

/** Maximum payload per request/response (4 KB).
 *  Covers protocol header (16B) + MessagePack payload for most operations
 *  including batch ops. */
static constexpr int PAYLOAD_MAX = 4096;

/** Total size of one Channel in bytes.
 *  = Header(64) + ReqCtl(64) + ReqPayload(4096) + RespCtl(64) + RespPayload(4096) */
static constexpr int CHANNEL_SIZE = 3 * CACHELINE_SIZE + 2 * PAYLOAD_MAX;  // 8384

/** Slot status values — shared between client (writer) and server (reader). */
static constexpr uint32_t STATUS_IDLE      = 0;
static constexpr uint32_t STATUS_REQ_READY = 1;
static constexpr uint32_t STATUS_RESP_READY = 2;

/* =========================================================================
 * Channel layout — byte offsets within a Channel
 * ========================================================================= */

/** Header: heartbeat + process identification.
 *  Written by client (ntstore), read by server/reaper (clflush+load). */
static constexpr int OFF_HEADER         = 0;      // [0, 64)

/** Request control: status + seq_num + payload_len.
 *  Written by client (ntstore), polled by server (clflush+load). */
static constexpr int OFF_REQ_CTL        = CACHELINE_SIZE;  // [64, 128)

/** Request payload: raw message bytes.
 *  Written by client (ntstore), read by server after REQ_READY. */
static constexpr int OFF_REQ_PAYLOAD    = 2 * CACHELINE_SIZE;  // [128, 4224)

/** Response control: status + seq_num + payload_len.
 *  Written by server (ntstore), polled by client (clflush+load). */
static constexpr int OFF_RESP_CTL       = 2 * CACHELINE_SIZE + PAYLOAD_MAX;  // [4224, 4288)

/** Response payload: raw message bytes.
 *  Written by server (ntstore), read by client after RESP_READY. */
static constexpr int OFF_RESP_PAYLOAD   = 3 * CACHELINE_SIZE + PAYLOAD_MAX;  // [4288, 8384)

/* =========================================================================
 * Struct definitions — packed to match exact byte layout
 * ========================================================================= */

/** Channel header: heartbeat and client identification.
 *  Occupies 1 cacheline (64 bytes). */
struct ChannelHeader {
    uint64_t lastHeartbeat;    ///< Client's monotonic timestamp (nanoseconds)
    uint32_t pid;              ///< Client PID (for local reaper kill() check)
    uint32_t nodeId;           ///< Node identifier (for local vs remote distinction)
    char     clientId[48];     ///< Null-terminated client identifier string
} __attribute__((aligned(CACHELINE_SIZE)));

/** Control slot: request or response metadata.
 *  Occupies 1 cacheline (64 bytes).
 *  The status field is polled — keeping it in its own cacheline
 *  prevents false sharing with the payload. */
struct ControlSlot {
    uint32_t status;           ///< IDLE / REQ_READY / RESP_READY
    uint32_t seqNum;           ///< Sequence number for request/response matching
    uint32_t payloadLen;       ///< Actual payload size in bytes (≤ PAYLOAD_MAX)
    uint8_t  _reserved[52];    ///< Padding to fill cacheline
} __attribute__((aligned(CACHELINE_SIZE)));

/** Complete channel: header + request + response.
 *  Each client gets one Channel. Writer is always single
 *  (client writes request, server writes response), so no locks needed. */
struct Channel {
    ChannelHeader header;                     // [0, 64)
    ControlSlot   reqCtl;                     // [64, 128)
    uint8_t       reqPayload[PAYLOAD_MAX];    // [128, 4224)
    ControlSlot   respCtl;                    // [4224, 4288)
    uint8_t       respPayload[PAYLOAD_MAX];   // [4288, 8384)
};

/* =========================================================================
 * Compile-time assertions — catch layout mismatches at build time
 * ========================================================================= */

static_assert(sizeof(ChannelHeader) == CACHELINE_SIZE,
              "ChannelHeader must be exactly 1 cacheline (64 bytes)");
static_assert(sizeof(ControlSlot) == CACHELINE_SIZE,
              "ControlSlot must be exactly 1 cacheline (64 bytes)");
static_assert(sizeof(Channel) == CHANNEL_SIZE,
              "Channel must be exactly 8384 bytes");

/* Offset consistency: these constexpr values must match the struct layout.
 * Verified by the formulas:
 *   OFF_HEADER       = 0
 *   OFF_REQ_CTL      = sizeof(ChannelHeader)                          = 64
 *   OFF_REQ_PAYLOAD  = OFF_REQ_CTL + sizeof(ControlSlot)              = 128
 *   OFF_RESP_CTL     = OFF_REQ_PAYLOAD + PAYLOAD_MAX                  = 4224
 *   OFF_RESP_PAYLOAD = OFF_RESP_CTL + sizeof(ControlSlot)             = 4288
 *   CHANNEL_SIZE     = OFF_RESP_PAYLOAD + PAYLOAD_MAX                 = 8384
 */
static_assert(OFF_REQ_CTL      == sizeof(ChannelHeader),
              "OFF_REQ_CTL must follow ChannelHeader");
static_assert(OFF_REQ_PAYLOAD  == OFF_REQ_CTL + sizeof(ControlSlot),
              "OFF_REQ_PAYLOAD must follow reqCtl");
static_assert(OFF_RESP_CTL     == OFF_REQ_PAYLOAD + PAYLOAD_MAX,
              "OFF_RESP_CTL must follow reqPayload");
static_assert(OFF_RESP_PAYLOAD == OFF_RESP_CTL + sizeof(ControlSlot),
              "OFF_RESP_PAYLOAD must follow respCtl");
static_assert(CHANNEL_SIZE     == OFF_RESP_PAYLOAD + PAYLOAD_MAX,
              "CHANNEL_SIZE must cover full channel");

}  // namespace cxl_rpc
}  // namespace maru

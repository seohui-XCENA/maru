#include "cxl_rpc_server.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <thread>

#include <emmintrin.h>  // _mm_stream_si128, _mm_sfence
#include <xmmintrin.h>  // _mm_clflush

#include "ipc.h"
#include "ipc_serialize.h"
#include "log.h"
#include "util.h"

namespace maru {

using namespace cxl_rpc;

// =============================================================================
// ntstore / clflush — x86 cache-bypass primitives
// =============================================================================

void CxlRpcServer::ntstore(void *dst, const void *src, size_t len) {
    auto *d = static_cast<uint8_t *>(dst);
    const auto *s = static_cast<const uint8_t *>(src);
    size_t i = 0;

    // 16-byte aligned non-temporal stores
    size_t head = (16 - (reinterpret_cast<uintptr_t>(d) & 15)) & 15;
    if (head > len) head = len;
    if (head > 0) {
        std::memcpy(d, s, head);
        _mm_clflush(d);
        i = head;
    }
    for (; i + 16 <= len; i += 16) {
        __m128i v;
        std::memcpy(&v, s + i, 16);
        _mm_stream_si128(reinterpret_cast<__m128i *>(d + i), v);
    }
    for (; i + 4 <= len; i += 4) {
        uint32_t val;
        std::memcpy(&val, s + i, 4);
        _mm_stream_si32(reinterpret_cast<int *>(d + i), static_cast<int>(val));
    }
    if (i < len) {
        std::memcpy(d + i, s + i, len - i);
        _mm_clflush(d + i);
    }
    _mm_sfence();
}

void CxlRpcServer::clflush(const void *addr, size_t len) {
    auto start = reinterpret_cast<uintptr_t>(addr) & ~(CACHELINE_SIZE - 1UL);
    auto end = reinterpret_cast<uintptr_t>(addr) + len;
    for (uintptr_t p = start; p < end; p += CACHELINE_SIZE) {
        _mm_clflush(reinterpret_cast<const void *>(p));
    }
    _mm_mfence();
}

// =============================================================================
// Construction / destruction
// =============================================================================

CxlRpcServer::CxlRpcServer(PoolManager &pm, const std::string &daxPath,
                             int maxChannels)
    : pm_(pm), handler_(pm), daxPath_(daxPath), maxChannels_(maxChannels) {
    channels_.resize(maxChannels);
}

CxlRpcServer::~CxlRpcServer() {
    stop();
    if (mmapBase_ && mmapBase_ != MAP_FAILED) {
        ::munmap(mmapBase_, mmapSize_);
    }
    if (daxFd_ >= 0) {
        ::close(daxFd_);
    }
}

// =============================================================================
// Channel access
// =============================================================================

Channel *CxlRpcServer::channel(int id) {
    auto *base = static_cast<uint8_t *>(mmapBase_);
    return reinterpret_cast<Channel *>(base + id * CHANNEL_SIZE);
}

const Channel *CxlRpcServer::channel(int id) const {
    auto *base = static_cast<const uint8_t *>(mmapBase_);
    return reinterpret_cast<const Channel *>(base + id * CHANNEL_SIZE);
}

// =============================================================================
// Lifecycle
// =============================================================================

int CxlRpcServer::start() {
    int rc = initSecret(pm_.stateDir(), pm_.hasExistingAllocations());
    if (rc != 0) {
        logf(LogLevel::Error, "Failed to init secret: %d", rc);
        return rc;
    }

    daxFd_ = ::open(daxPath_.c_str(), O_RDWR);
    if (daxFd_ < 0) {
        logf(LogLevel::Error, "Failed to open DAX device %s: %s",
             daxPath_.c_str(), std::strerror(errno));
        return -errno;
    }

    mmapSize_ = static_cast<size_t>(maxChannels_) * CHANNEL_SIZE;
    mmapBase_ = ::mmap(nullptr, mmapSize_, PROT_READ | PROT_WRITE,
                       MAP_SHARED, daxFd_, 0);
    if (mmapBase_ == MAP_FAILED) {
        logf(LogLevel::Error, "Failed to mmap DAX device %s (%zu bytes): %s",
             daxPath_.c_str(), mmapSize_, std::strerror(errno));
        ::close(daxFd_);
        daxFd_ = -1;
        return -errno;
    }

    // Zero out all channels on startup
    std::memset(mmapBase_, 0, mmapSize_);

    running_ = true;
    pollThread_ = std::thread(&CxlRpcServer::pollLoop, this);

    logf(LogLevel::Info, "CXL-RPC server started on %s (%d channels)",
         daxPath_.c_str(), maxChannels_);
    return 0;
}

void CxlRpcServer::stop() {
    running_ = false;
    if (pollThread_.joinable()) {
        pollThread_.join();
    }
}

// =============================================================================
// Reaper support
// =============================================================================

uint64_t CxlRpcServer::getLastHeartbeat(int channelId) const {
    if (channelId < 0 || channelId >= maxChannels_) return 0;
    const auto *ch = channel(channelId);
    clflush(&ch->header.lastHeartbeat, sizeof(uint64_t));
    return ch->header.lastHeartbeat;
}

const char *CxlRpcServer::getClientId(int channelId) const {
    if (channelId < 0 || channelId >= maxChannels_) return "";
    return channel(channelId)->header.clientId;
}

// =============================================================================
// Poll loop — main server loop
// =============================================================================

void CxlRpcServer::pollLoop() {
    while (running_) {
        bool didWork = false;

        for (int i = 0; i < maxChannels_; ++i) {
            if (!channels_[i].active) continue;

            auto *ch = channel(i);

            // Check if request is ready
            clflush(&ch->reqCtl.status, sizeof(uint32_t));
            if (ch->reqCtl.status != STATUS_REQ_READY) continue;

            didWork = true;
            handleOneRequest(i);
        }

        // If no work was done, yield to avoid 100% CPU burn
        if (!didWork) {
            std::this_thread::yield();
        }
    }
}

bool CxlRpcServer::handleOneRequest(int channelId) {
    auto *ch = channel(channelId);

    // Read request control
    clflush(&ch->reqCtl, sizeof(ControlSlot));
    uint32_t seq = ch->reqCtl.seqNum;
    uint32_t payloadLen = ch->reqCtl.payloadLen;

    if (payloadLen > PAYLOAD_MAX) {
        logf(LogLevel::Warn, "CXL-RPC ch=%d: payload too large (%u)",
             channelId, payloadLen);
        // Clear request, send error
        uint32_t idle = STATUS_IDLE;
        ntstore(&ch->reqCtl.status, &idle, sizeof(idle));
        return false;
    }

    // Read request payload
    clflush(ch->reqPayload, payloadLen);

    // Parse the RM binary IPC message (same format as TCP)
    // The payload contains: MsgHeader(12B) + request body
    if (payloadLen < sizeof(MsgHeader)) {
        uint32_t idle = STATUS_IDLE;
        ntstore(&ch->reqCtl.status, &idle, sizeof(idle));
        return false;
    }

    MsgHeader hdr{};
    std::memcpy(&hdr, ch->reqPayload, sizeof(hdr));
    if (hdr.magic != kMagic || hdr.version != kVersion) {
        logf(LogLevel::Warn, "CXL-RPC ch=%d: bad header", channelId);
        uint32_t idle = STATUS_IDLE;
        ntstore(&ch->reqCtl.status, &idle, sizeof(idle));
        return false;
    }

    const uint8_t *body = ch->reqPayload + sizeof(MsgHeader);
    uint32_t bodyLen = hdr.payloadLen;
    MsgType type = static_cast<MsgType>(hdr.type);

    // Dispatch to RequestHandler (same logic as TcpServer)
    std::vector<uint8_t> respPayload;
    MsgType respType;

    // Extract client_id from channel header for this request
    std::string cid(ch->header.clientId);
    if (!channels_[channelId].clientId.empty()) {
        cid = channels_[channelId].clientId;
    } else if (!cid.empty()) {
        channels_[channelId].clientId = cid;
        channels_[channelId].active = true;
        pm_.clientReconnected(cid);
    }

    RequestContext ctx{cid};

    if (type == MsgType::ALLOC_REQ && bodyLen >= sizeof(AllocReq)) {
        AllocReq req{};
        std::memcpy(&req, body, sizeof(req));
        auto result = handler_.handleAlloc(req, ctx);
        auto serialized = serializeAllocResp(result.resp, result.devicePath);
        respType = MsgType::ALLOC_RESP;
        respPayload = std::move(serialized);

    } else if (type == MsgType::FREE_REQ && bodyLen >= sizeof(FreeReq)) {
        FreeReq req{};
        std::memcpy(&req, body, sizeof(req));
        auto result = handler_.handleFree(req, ctx);
        respType = MsgType::FREE_RESP;
        respPayload.resize(sizeof(result.resp));
        std::memcpy(respPayload.data(), &result.resp, sizeof(result.resp));

    } else if (type == MsgType::GET_ACCESS_REQ && bodyLen >= sizeof(GetAccessReq)) {
        GetAccessReq req{};
        std::memcpy(&req, body, sizeof(req));
        auto result = handler_.handleGetAccess(req, ctx);
        auto serialized = serializeGetAccessResp(
            result.status, result.devicePath, result.offset, result.length);
        respType = MsgType::GET_ACCESS_RESP;
        respPayload = std::move(serialized);

    } else if (type == MsgType::STATS_REQ) {
        auto result = handler_.handleStats();
        respType = MsgType::STATS_RESP;
        respPayload = std::move(result.payload);

    } else {
        // Unknown or malformed request
        respType = MsgType::ERROR_RESP;
        ErrorResp er{};
        er.status = -ENOSYS;
        er.msgLen = 0;
        respPayload.resize(sizeof(er));
        std::memcpy(respPayload.data(), &er, sizeof(er));
    }

    // Build response: MsgHeader + payload
    MsgHeader respHdr{};
    respHdr.magic = kMagic;
    respHdr.version = kVersion;
    respHdr.type = static_cast<uint16_t>(respType);
    respHdr.payloadLen = respPayload.size();

    std::vector<uint8_t> fullResp(sizeof(respHdr) + respPayload.size());
    std::memcpy(fullResp.data(), &respHdr, sizeof(respHdr));
    if (!respPayload.empty()) {
        std::memcpy(fullResp.data() + sizeof(respHdr),
                    respPayload.data(), respPayload.size());
    }

    // Write response via ntstore
    // Clear request BEFORE marking response ready (prevents race condition)
    uint32_t idle = STATUS_IDLE;
    ntstore(&ch->reqCtl.status, &idle, sizeof(idle));

    ntstore(ch->respPayload, fullResp.data(), fullResp.size());

    ControlSlot respCtl{};
    respCtl.status = STATUS_RESP_READY;
    respCtl.seqNum = seq;
    respCtl.payloadLen = fullResp.size();
    ntstore(&ch->respCtl, &respCtl, sizeof(respCtl));

    return true;
}

}  // namespace maru

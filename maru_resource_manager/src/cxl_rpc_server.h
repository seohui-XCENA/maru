#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include "cxl_rpc_layout.h"
#include "pool_manager.h"
#include "request_handler.h"

namespace maru {

/// CXL-RPC server for the Resource Manager.
///
/// Polls CXL shared memory channels for requests and dispatches them
/// to RequestHandler — the same business logic used by TcpServer.
/// Coexists with TcpServer; main.cpp selects one based on config.
class CxlRpcServer {
public:
    CxlRpcServer(PoolManager &pm, const std::string &daxPath,
                 int maxChannels = 64);
    ~CxlRpcServer();

    /// Open DAX device, mmap RPC region, start poll loop in background thread.
    int start();
    void stop();

    /// Reaper support: read heartbeat from a channel header.
    uint64_t getLastHeartbeat(int channelId) const;
    const char *getClientId(int channelId) const;

private:
    void pollLoop();
    bool handleOneRequest(int channelId);

    /// ntstore helper: write to CXL memory bypassing cache.
    static void ntstore(void *dst, const void *src, size_t len);
    /// clflush helper: invalidate cache lines before reading CXL memory.
    static void clflush(const void *addr, size_t len);

    /// Get pointer to channel struct at given index.
    cxl_rpc::Channel *channel(int id);
    const cxl_rpc::Channel *channel(int id) const;

    PoolManager &pm_;
    RequestHandler handler_;
    std::string daxPath_;
    int maxChannels_;

    int daxFd_{-1};
    void *mmapBase_{nullptr};
    size_t mmapSize_{0};

    struct ChannelState {
        bool active{false};
        std::string clientId;
    };
    std::vector<ChannelState> channels_;

    std::atomic<bool> running_{false};
    std::thread pollThread_;
};

}  // namespace maru

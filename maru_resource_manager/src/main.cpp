#include <unistd.h>

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <memory>
#include <thread>

#include "cxl_rpc_server.h"
#include "log.h"
#include "pool_manager.h"
#include "reaper.h"
#include "server_config.h"
#include "tcp_server.h"
#include "util.h"

static volatile std::sig_atomic_t gStop = 0;
static volatile std::sig_atomic_t gRescan = 0;

static void onSignal(int) { gStop = 1; }
static void onRescan(int) { gRescan = 1; }

int main(int argc, char **argv) {
    maru::ServerConfig cfg = maru::parseArgs(argc, argv);
    maru::setLogLevel(cfg.logLevel);

    // Ensure state directory exists
    maru::ensureDirExists(cfg.stateDir);

    // Persist config for reference
    maru::writeConfigFile(cfg);

    // Startup banner
    maru::logf(maru::LogLevel::Info, "maru-resource-manager starting");
    maru::logf(maru::LogLevel::Info, "  listen     : %s:%u", cfg.host.c_str(), cfg.port);
    maru::logf(maru::LogLevel::Info, "  state dir  : %s", cfg.stateDir.c_str());
    maru::logf(maru::LogLevel::Info, "  log level  : %s", maru::logLevelStr(cfg.logLevel));
    maru::logf(maru::LogLevel::Info, "  workers    : %d", cfg.numWorkers);
    maru::logf(maru::LogLevel::Info, "  grace period: %ds", cfg.gracePeriodSec);
    maru::logf(maru::LogLevel::Info, "  max clients : %d", cfg.maxClients);
    maru::logf(maru::LogLevel::Info, "  transport  : %s", cfg.transport.c_str());

    // Signal handlers
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    std::signal(SIGHUP, onRescan);
    std::signal(SIGPIPE, SIG_IGN);

    // Initialize components with explicit config injection
    maru::PoolManager pm(cfg.stateDir, cfg.gracePeriodSec);
    int rc = pm.loadPools();
    if (rc != 0) {
        maru::logf(maru::LogLevel::Warn,
                    "no CXL/DAX devices found — starting with empty pool");
    }

    // Start server transport based on config
    std::unique_ptr<maru::TcpServer> tcpServer;
    std::unique_ptr<maru::CxlRpcServer> cxlServer;

    if (cfg.transport == "cxl-rpc") {
        cxlServer = std::make_unique<maru::CxlRpcServer>(
            pm, cfg.daxPath, cfg.maxChannels);
        rc = cxlServer->start();
        if (rc != 0) {
            maru::logf(maru::LogLevel::Error,
                        "failed to start CXL-RPC server on %s: %s",
                        cfg.daxPath.c_str(), std::strerror(-rc));
            return 1;
        }
    } else {
        tcpServer = std::make_unique<maru::TcpServer>(
            pm, cfg.host, cfg.port, cfg.numWorkers, cfg.maxClients);
        rc = tcpServer->start();
        if (rc != 0) {
            if (rc == -EADDRINUSE) {
                maru::logf(maru::LogLevel::Error,
                            "port %u is already in use — "
                            "another maru-resource-manager may be running. "
                            "Use --port to specify a different port.",
                            cfg.port);
            } else {
                maru::logf(maru::LogLevel::Error,
                            "failed to start server on %s:%u: %s",
                            cfg.host.c_str(), cfg.port, std::strerror(-rc));
            }
            return 1;
        }
        maru::logf(maru::LogLevel::Info, "ready — listening on %s:%u",
                    cfg.host.c_str(), cfg.port);
    }

    maru::Reaper reaper(pm);
    reaper.start();

    // Main loop — runs until SIGINT/SIGTERM
    while (!gStop) {
        if (gRescan) {
            gRescan = 0;
            pm.rescanDevices();
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Graceful shutdown
    maru::logf(maru::LogLevel::Info, "shutting down...");
    reaper.stop();
    if (tcpServer) tcpServer->stop();
    if (cxlServer) cxlServer->stop();
    pm.checkpoint();
    maru::logf(maru::LogLevel::Info, "shutdown complete");
    return 0;
}

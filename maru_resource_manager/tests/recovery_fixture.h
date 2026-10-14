// Shared fixture for recovery tests: a private fake /dev and sysfs under
// fakeRoot. The linker wraps filesystem calls (test_header_recovery.cpp),
// so these tests never open real DAX devices.
#pragma once

#include <gtest/gtest.h>

#include <algorithm>
#include <cerrno>
#include <cstdarg>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <ostream>
#include <string>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <dirent.h>
#include <unistd.h>

#include "device_header.h"
#include "checkpoint_fault.h"
#include "metadata.h"
#include "pool_validation.h"
#include "util.h"
#include "wal.h"

namespace fs = std::filesystem;
using namespace maru;

// Root of the fake /dev and /sys for the running test.
extern std::string fakeRoot;
// Every path the code under test passed to open(), for access checks.
extern std::vector<std::string> openedPaths;

namespace maru {
class PoolManagerTestPeer {
public:
    static void inject(PoolManager &pm, PoolState pool, const Allocation &a) {
        pm.pools_.push_back(std::move(pool));
        if (a.allocLength) pm.allocations_[a.handle.regionId] = a;
    }
    static std::string checkpointBlock(const PoolManager &pm) {
        return pm.checkpointBlock_;
    }
    // FS_DAX discovery reads /proc/mounts, so tests drive the sweep directly.
    static void removeOrphanFsDaxFiles(PoolManager &pm, uint64_t nextRegionId,
                                       std::string unavailable = "") {
        pm.nextRegionId_ = nextRegionId;
        pm.unavailablePools_ = std::move(unavailable);
        for (const auto &pool : pm.pools_) pm.removeOrphanFsDaxFiles(pool);
    }
};
} // namespace maru

class HeaderRecoveryTest : public ::testing::Test {
protected:
    static constexpr uint64_t H = 2ULL << 20;
    static constexpr uint64_t B = 16ULL << 20;
    std::string state;
    void SetUp() override {
        char path[] = "/tmp/maru-header-recovery-XXXXXX";
        ASSERT_NE(::mkdtemp(path), nullptr);
        fakeRoot = path;
        state = fakeRoot + "/state";
        fs::create_directories(state);
        fs::create_directories(fakeRoot + "/dev");
        fs::create_directories(fakeRoot + "/sys/bus/dax/devices");
        fs::create_directories(fakeRoot + "/sys/class/block");
        ASSERT_EQ(initSecret(state, false), 0);
    }
    void TearDown() override {
        fs::remove_all(fakeRoot);
        fakeRoot.clear();
    }
    std::string dev(uint32_t id) { return "/dev/dax" + std::to_string(id) + ".0"; }
    void addDevice(uint32_t id, bool header = true) {
        const auto name = "dax" + std::to_string(id) + ".0";
        const auto sys = fakeRoot + "/sys/bus/dax/devices/" + name;
        fs::create_directories(sys);
        fs::create_directories(fakeRoot + "/sys/class/dax/" + name);
        fs::create_symlink("/sys/bus/dax/drivers/device_dax", sys + "/driver");
        std::ofstream(sys + "/align") << H;
        std::ofstream(fakeRoot + "/sys/class/dax/" + name + "/size") << B;
        int fd = ::open(dev(id).c_str(), O_CREAT | O_RDWR, 0600);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(::ftruncate(fd, B), 0);
        ::close(fd);
        if (header) {
            DeviceHeader hdr{};
            initDeviceHeader(hdr);
            ASSERT_EQ(writeDeviceHeader(dev(id), hdr, H), 0);
        }
    }
    PoolState pool(uint32_t id = 0) {
        PoolState p{};
        p.poolId = id;
        p.devPath = dev(id);
        p.type = DaxType::DEV_DAX;
        p.alignBytes = p.dataBegin = H;
        p.dataEnd = B;
        p.totalSize = p.freeSize = B - H;
        p.freeList = {{H, B - H}};
        return p;
    }
    Allocation allocation(uint32_t id = 0, uint64_t offset = H) {
        Allocation a{};
        a.poolId = id;
        a.handle.regionId = 42;
        a.handle.offset = a.realOffset = offset;
        a.handle.length = a.allocLength = a.requestedSize = H;
        a.nonce = 17;
        std::strcpy(a.clientId, "offline-test-owner");
        a.handle.authToken = computeAuthToken(a.handle, a.nonce, a.clientId);
        return a;
    }
    void save(PoolState p, const std::map<uint64_t, Allocation> &a = {}) {
        MetadataStore m(state);
        ASSERT_EQ(m.save(p), 0);
        ASSERT_EQ(m.saveGlobal(a, 43), 0);
    }
    std::string bytes(const std::string &path) {
        std::ifstream in(path, std::ios::binary);
        return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
    }
    int allocate(PoolManager &pm, Handle &h, uint32_t id = 0) {
        std::string path, uuid;
        uint64_t requested;
        return pm.alloc(H, "offline-test-owner", h, path, uuid, dev(id), requested);
    }
};

#pragma once

#include <string>

#include "pool_manager.h"

namespace maru {

enum class WalRecordType : uint32_t { ALLOC = 1, FREE = 2 };

class WalStore {
public:
  explicit WalStore(const std::string &stateDir);
  ~WalStore();

  // Non-copyable
  WalStore(const WalStore &) = delete;
  WalStore &operator=(const WalStore &) = delete;

  int appendAlloc(const Allocation &alloc);
  int appendFree(uint64_t regionId);

  int replay(std::vector<PoolState> &pools,
             std::map<uint64_t, Allocation> &allocations,
             uint64_t &nextRegionId);

  int checkpoint(const std::vector<PoolState> &pools,
                 class MetadataStore &metadata,
                 const std::map<uint64_t, Allocation> &allocations,
                 uint64_t nextRegionId);

private:
  std::string stateDir_;
  int walFd_{-1};

  std::string walPath() const;
  int ensureOpen();
  int appendRecord(WalRecordType type, const void *payload, uint32_t len);
};

} // namespace maru

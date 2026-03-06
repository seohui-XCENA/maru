#pragma once

#include <string>
#include <vector>

#include "pool_manager.h"

namespace maru {

class MetadataStore {
public:
  explicit MetadataStore(const std::string &stateDir);

  int load(uint32_t poolId, PoolState &pool);
  int save(const PoolState &pool);

  // Global state management
  int loadGlobal(std::map<uint64_t, Allocation> &allocations,
                 uint64_t &nextRegionId);
  int saveGlobal(const std::map<uint64_t, Allocation> &allocations,
                 uint64_t nextRegionId);

private:
  std::string stateDir_;
  std::string metaPath(uint32_t poolId) const;
  std::string globalMetaPath() const;
};

} // namespace maru

#pragma once

#include "maru_backend.h"

namespace maru
{

/// PoolBackend for file-backed pools — FS_DAX (pmem + ext4 with dax option)
/// and MARUFS mounts. Each allocation creates a per-region file inside the
/// mount point; clients `open()` and `mmap()` the file directly.
class MaruFsBackend : public ExtentAllocatorBackend
{
public:
    std::string dataPath(const PoolState &pool,
                         uint64_t regionId) const override;

protected:
    int postCarve(PoolState &pool, uint64_t regionId,
                  uint64_t aligned, uint64_t alignedSize,
                  AllocOutcome &out) override;
    void preMerge(PoolState &pool, uint64_t regionId) override;
};

}  // namespace maru

#pragma once

#include "pool_backend.h"

namespace maru
{

/// Abstract base that implements free-list first-fit extent allocation.
///
/// Subclasses supply backend-specific side effects and outcome fields via
/// protected hooks. The `allocate` / `freeAlloc` template methods own the
/// generic carve / merge logic so concrete subclasses don't duplicate it.
///
/// Stateless — subclasses should also be stateless so one instance can be
/// shared across all pools of the given backend kind.
class ExtentAllocatorBackend : public PoolBackend
{
public:
    /// First-fit + 2 MiB alignment carve from `pool.freeList`. On success
    /// calls `postCarve` to let the subclass finish populating `out` and
    /// perform any side effects (file creation, ioctl, etc.).
    int allocate(PoolState &pool, uint64_t requestedSize,
                 uint64_t regionId, AllocOutcome &out) final;

    /// Calls `preMerge` (for subclass-specific cleanup) then merges the
    /// extent back into `pool.freeList`.
    int freeAlloc(PoolState &pool, uint64_t regionId,
                  uint64_t realOffset, uint64_t allocLength) final;

protected:
    /// Called after the extent has been carved but before the outcome is
    /// published. Subclass performs any side effects (e.g. creating a file
    /// inside a mount) and fills in `out.handleOffset` / `out.realOffset` /
    /// `out.allocLength`. Returning non-zero aborts the allocation.
    ///
    /// Note: on non-zero return the carved extent is NOT rolled back from
    /// `pool.freeList` — this matches pre-refactor behavior and is safe
    /// because the caller (PoolManager) does not commit any state on
    /// failure either.
    virtual int postCarve(PoolState &pool, uint64_t regionId,
                          uint64_t aligned, uint64_t alignedSize,
                          AllocOutcome &out) = 0;

    /// Called before the freed extent is merged back. Subclass performs
    /// any cleanup (e.g. unlinking a file). Best-effort — failure is not
    /// propagated to the caller.
    virtual void preMerge(PoolState &pool, uint64_t regionId) = 0;
};

/// PoolBackend for bare DEV_DAX devices (`/dev/daxN.M`).
///
/// RM owns the offset allocator here — the client `mmap`s the device
/// directly at the returned offset. No file creation, no ACL.
class MaruBackend : public ExtentAllocatorBackend
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

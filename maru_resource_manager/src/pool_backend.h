#pragma once

#include <cstdint>
#include <string>

namespace maru
{

// Forward declaration — defined in pool_manager.h.
struct PoolState;

/// Result of a successful allocation attempt.
///
/// Populated by PoolBackend::allocate(). Consumed by PoolManager to build
/// the Handle returned to the client and to record the extent for later
/// free/replay.
struct AllocOutcome
{
    /// Value to place in Handle.offset.
    /// - DEV_DAX: aligned real offset into the device.
    /// - FS-style (FS_DAX, MARUFS): 0 (client opens a fresh file per region).
    uint64_t handleOffset;

    /// Actual extent offset within the pool (for freeAlloc / WAL replay).
    /// Same as handleOffset for DEV_DAX; typically 0 for FS-style but the
    /// backend is free to assign any tracking value.
    uint64_t realOffset;

    /// Aligned allocation length (requested size rounded up to pool.alignBytes).
    uint64_t allocLength;
};

/// Strategy interface for per-pool allocation semantics.
///
/// A single PoolBackend instance is owned by a PoolState. The backend
/// operates on pool.freeList (bookkeeping) and, for FS-style pools, on
/// the filesystem (creating/unlinking per-region files).
///
/// Cross-cutting concerns — auth tokens, the global allocations_ table,
/// reaper tracking, WAL — stay in PoolManager and are NOT the backend's
/// responsibility.
///
/// Error convention: methods return 0 on success and a negative errno
/// value (e.g. -ENOMEM, -EIO) on failure.
class PoolBackend
{
public:
    virtual ~PoolBackend() = default;

    /// Attempt to allocate `requestedSize` bytes from `pool`.
    ///
    /// On success: updates `pool.freeList` to remove the chosen extent,
    /// performs any side effects (file create for FS-style), and populates
    /// `out` with the resulting extent metadata. Returns 0.
    ///
    /// On failure: returns negative errno. `pool.freeList` and the
    /// filesystem are left untouched.
    ///
    /// `regionId` is assigned externally by PoolManager and is used to
    /// derive per-region filenames for FS-style backends.
    virtual int allocate(PoolState &pool, uint64_t requestedSize,
                         uint64_t regionId, AllocOutcome &out) = 0;

    /// Return an extent to the pool.
    ///
    /// Updates `pool.freeList` (merging adjacent extents). For FS-style
    /// backends, unlinks the region file. Returns 0 on success, negative
    /// errno on failure. Best-effort — even on partial failure, the
    /// backend should leave the pool in a consistent state.
    virtual int freeAlloc(PoolState &pool, uint64_t regionId,
                          uint64_t realOffset, uint64_t allocLength) = 0;

    /// Compute the path that the client opens for mmap.
    ///
    /// - DEV_DAX: pool.devPath (e.g. "/dev/dax0.0").
    /// - FS-style: file path inside the mount (e.g.
    ///   "/mnt/marufs_test/maru_5.dat").
    virtual std::string dataPath(const PoolState &pool,
                                  uint64_t regionId) const = 0;
};

}  // namespace maru

#include "maru_backend.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>

#include "pool_manager.h"

namespace maru
{

namespace
{

uint64_t alignUp(uint64_t v, uint64_t align)
{
    if (align == 0)
    {
        return v;
    }
    uint64_t rem = v % align;
    if (rem == 0)
    {
        return v;
    }
    uint64_t result = v + (align - rem);
    if (result < v)
    {
        return UINT64_MAX;  // overflow guard
    }
    return result;
}

void insertExtentSorted(PoolState &pool, uint64_t offset, uint64_t length)
{
    auto it = std::lower_bound(
        pool.freeList.begin(),
        pool.freeList.end(),
        offset,
        [](const Extent &e, uint64_t off) { return e.offset < off; });

    if (it != pool.freeList.begin())
    {
        auto prev = std::prev(it);
        if (prev->offset + prev->length >= offset)
        {
            uint64_t newEnd =
                std::max(prev->offset + prev->length, offset + length);
            prev->length = newEnd - prev->offset;
            if (it != pool.freeList.end() &&
                prev->offset + prev->length >= it->offset)
            {
                uint64_t end = std::max(prev->offset + prev->length,
                                         it->offset + it->length);
                prev->length = end - prev->offset;
                pool.freeList.erase(it);
            }
            return;
        }
    }

    if (it != pool.freeList.end() && offset + length >= it->offset)
    {
        uint64_t newEnd = std::max(offset + length, it->offset + it->length);
        it->offset = offset;
        it->length = newEnd - offset;
        return;
    }

    pool.freeList.insert(it, Extent{offset, length});
}

}  // namespace

// ------------------------------------------------------------------------
// ExtentAllocatorBackend — template methods for free-list carve / merge
// ------------------------------------------------------------------------

int ExtentAllocatorBackend::allocate(PoolState &pool, uint64_t requestedSize,
                                      uint64_t regionId, AllocOutcome &out)
{
    uint64_t alignedSize = alignUp(requestedSize, pool.alignBytes);
    for (size_t i = 0; i < pool.freeList.size(); ++i)
    {
        Extent ex = pool.freeList[i];
        uint64_t aligned = alignUp(ex.offset, pool.alignBytes);
        uint64_t end = aligned + alignedSize;
        if (end > ex.offset + ex.length)
        {
            continue;
        }

        bool hasFront = (aligned > ex.offset);
        bool hasBack = (end < ex.offset + ex.length);

        if (hasFront && hasBack)
        {
            // Split: keep front fragment in place, insert back after it
            pool.freeList[i] = Extent{ex.offset, aligned - ex.offset};
            pool.freeList.insert(
                pool.freeList.begin() + static_cast<std::ptrdiff_t>(i) + 1,
                Extent{end, (ex.offset + ex.length) - end});
        }
        else if (hasFront)
        {
            pool.freeList[i] = Extent{ex.offset, aligned - ex.offset};
        }
        else if (hasBack)
        {
            pool.freeList[i] = Extent{end, (ex.offset + ex.length) - end};
        }
        else
        {
            pool.freeList.erase(pool.freeList.begin() +
                                static_cast<std::ptrdiff_t>(i));
        }

        return postCarve(pool, regionId, aligned, alignedSize, out);
    }
    return -ENOMEM;
}

int ExtentAllocatorBackend::freeAlloc(PoolState &pool, uint64_t regionId,
                                       uint64_t realOffset,
                                       uint64_t allocLength)
{
    preMerge(pool, regionId);
    insertExtentSorted(pool, realOffset, allocLength);
    return 0;
}

// ------------------------------------------------------------------------
// MaruBackend — DEV_DAX concrete
// ------------------------------------------------------------------------

int MaruBackend::postCarve(PoolState & /*pool*/, uint64_t /*regionId*/,
                            uint64_t aligned, uint64_t alignedSize,
                            AllocOutcome &out)
{
    // DEV_DAX: client mmaps the raw device at the carved offset.
    out.handleOffset = aligned;
    out.realOffset = aligned;
    out.allocLength = alignedSize;
    return 0;
}

void MaruBackend::preMerge(PoolState & /*pool*/, uint64_t /*regionId*/)
{
    // DEV_DAX: no filesystem object to clean up.
}

std::string MaruBackend::dataPath(const PoolState &pool,
                                    uint64_t /*regionId*/) const
{
    return pool.devPath;
}

}  // namespace maru

#include "maru_fs_backend.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>

#include "pool_manager.h"

namespace maru
{

namespace
{

std::string makeFsDaxFilePath(const std::string &mountPoint, uint64_t regionId)
{
    char filename[512];
    std::snprintf(filename, sizeof(filename), "%s/maru_%llu.dat",
                  mountPoint.c_str(), (unsigned long long)regionId);
    return std::string(filename);
}

int createFsDaxFile(const std::string &mountPoint, uint64_t regionId,
                    uint64_t size)
{
    std::string filename = makeFsDaxFilePath(mountPoint, regionId);
    int fd = ::open(filename.c_str(), O_CREAT | O_RDWR | O_EXCL, 0600);
    if (fd < 0)
    {
        return -errno;
    }
    if (::ftruncate(fd, static_cast<off_t>(size)) != 0)
    {
        int err = errno;
        ::close(fd);
        ::unlink(filename.c_str());
        return -err;
    }
    ::close(fd);
    return 0;
}

void deleteFsDaxFile(const std::string &mountPoint, uint64_t regionId)
{
    std::string filename = makeFsDaxFilePath(mountPoint, regionId);
    ::unlink(filename.c_str());
}

}  // namespace

int MaruFsBackend::postCarve(PoolState &pool, uint64_t regionId,
                              uint64_t aligned, uint64_t alignedSize,
                              AllocOutcome &out)
{
    int rc = createFsDaxFile(pool.devPath, regionId, alignedSize);
    if (rc != 0)
    {
        return rc;
    }
    // File-backed: client opens the per-region file and mmap starts at
    // offset 0 (the whole file is the region).
    out.handleOffset = 0;
    out.realOffset = aligned;
    out.allocLength = alignedSize;
    return 0;
}

void MaruFsBackend::preMerge(PoolState &pool, uint64_t regionId)
{
    deleteFsDaxFile(pool.devPath, regionId);
}

std::string MaruFsBackend::dataPath(const PoolState &pool,
                                      uint64_t regionId) const
{
    return makeFsDaxFilePath(pool.devPath, regionId);
}

}  // namespace maru

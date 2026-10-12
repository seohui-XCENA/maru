#include "checkpoint_fault.h"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <limits.h>

namespace checkpoint_fault {
namespace {
std::string target;
Operation operation = Operation::Write;
int failureError = EIO;
size_t remainingBytes = 0;
unsigned failureCount = 0;
unsigned shortWriteCount = 0;

bool matches(int fd, Operation requested) {
    if (target.empty() || operation != requested) return false;
    char path[64];
    std::snprintf(path, sizeof(path), "/proc/self/fd/%d", fd);
    char resolved[PATH_MAX];
    const ssize_t length = ::readlink(path, resolved, sizeof(resolved));
    return length > 0 && std::string(resolved, static_cast<size_t>(length)) == target;
}

int fail() {
    ++failureCount;
    std::fprintf(stderr, "Injected checkpoint %s failure: %s (errno=%d)\n",
                 operation == Operation::Write ? "write" : "fsync",
                 target.c_str(), failureError);
    errno = failureError;
    return -1;
}
} // namespace

void arm(const std::string &path, Operation op, int error, size_t bytesBeforeFailure) {
    target = path;
    operation = op;
    failureError = error;
    remainingBytes = bytesBeforeFailure;
    failureCount = shortWriteCount = 0;
}

void clear() { target.clear(); }
unsigned failures() { return failureCount; }
unsigned shortWrites() { return shortWriteCount; }
} // namespace checkpoint_fault

extern "C" ssize_t __real_write(int, const void *, size_t);
extern "C" int __real_fsync(int);

extern "C" ssize_t __wrap_write(int fd, const void *buf, size_t count) {
    using namespace checkpoint_fault;
    if (!matches(fd, Operation::Write)) return __real_write(fd, buf, count);
    if (remainingBytes == 0) return fail();
    const size_t allowed = std::min(count, remainingBytes);
    const ssize_t written = __real_write(fd, buf, allowed);
    if (written > 0) {
        remainingBytes -= static_cast<size_t>(written);
        if (static_cast<size_t>(written) < count) ++shortWriteCount;
    }
    return written;
}

extern "C" int __wrap_fsync(int fd) {
    using namespace checkpoint_fault;
    if (matches(fd, Operation::Fsync)) return fail();
    return __real_fsync(fd);
}

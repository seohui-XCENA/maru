#pragma once

#include <cstddef>
#include <string>

// Test-only syscall interception. The target must be the exact temporary
// checkpoint pathname, so unrelated writes (including WAL appends) pass through.
namespace checkpoint_fault {
enum class Operation { Write, Fsync };

void arm(const std::string &path, Operation operation, int error,
         size_t bytesBeforeFailure = 0);
void clear();
unsigned failures();
unsigned shortWrites();
} // namespace checkpoint_fault

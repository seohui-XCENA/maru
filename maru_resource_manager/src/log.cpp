#include "log.h"

#include <atomic>
#include <cstdarg>
#include <cstdio>

namespace maru {

static std::atomic<int> gLogLevel{0};

void setLogLevel(LogLevel level) { gLogLevel.store(static_cast<int>(level)); }

void logf(LogLevel level, const char *fmt, ...) {
  if (static_cast<int>(level) > gLogLevel.load()) {
    return;
  }
  va_list ap;
  va_start(ap, fmt);
  std::vfprintf(stderr, fmt, ap);
  std::fprintf(stderr, "\n");
  va_end(ap);
}

} // namespace maru

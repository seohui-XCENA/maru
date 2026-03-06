#include "reaper.h"

#include <chrono>

namespace maru {

Reaper::Reaper(PoolManager &pm) : pm_(pm) {}

Reaper::~Reaper() { stop(); }

void Reaper::start() {
  stop_ = false;
  th_ = std::thread(&Reaper::run, this);
}

void Reaper::stop() {
  stop_ = true;
  if (th_.joinable()) {
    th_.join();
  }
}

void Reaper::run() {
  while (!stop_) {
    uint64_t reaped = 0;
    pm_.reapExpired(reaped);
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

} // namespace maru

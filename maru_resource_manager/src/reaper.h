#pragma once

#include <atomic>
#include <thread>

#include "pool_manager.h"

namespace maru {

class Reaper {
public:
  explicit Reaper(PoolManager &pm);
  ~Reaper();

  void start();
  void stop();

private:
  void run();

  PoolManager &pm_;
  std::atomic<bool> stop_{false};
  std::thread th_;
};

} // namespace maru

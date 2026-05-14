// Copyright 2018 Netflix, Inc.
// Copyright 2026 RowKeyDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

#include "limen/average_sample_window.h"
#include <cstdint>
#include <limits>

namespace limen {

void AverageSampleWindow::AddSample(int64_t rtt_ns, int inflight,
                                    bool did_drop) {
  sample_count_.fetch_add(1, std::memory_order_relaxed);
  sum_rtt_ns_.fetch_add(rtt_ns, std::memory_order_relaxed);

  int64_t current_min = min_rtt_ns_.load(std::memory_order_relaxed);
  while (rtt_ns < current_min &&
         !min_rtt_ns_.compare_exchange_weak(current_min, rtt_ns,
                                            std::memory_order_relaxed,
                                            std::memory_order_relaxed)) {
  }

  int current_max = max_inflight_.load(std::memory_order_relaxed);
  while (inflight > current_max &&
         !max_inflight_.compare_exchange_weak(current_max, inflight,
                                              std::memory_order_relaxed,
                                              std::memory_order_relaxed)) {
  }

  // The drop flag is a sticky OR-aggregate: once set, it stays set
  // until the next `Reset`. Setting unconditionally to `true` (when
  // the sample reports a drop) avoids a CAS loop without changing
  // semantics — every writer wanting to set the flag is writing the
  // same value.
  if (did_drop) {
    did_drop_.store(true, std::memory_order_relaxed);
  }
}

int64_t AverageSampleWindow::TrackedRttNs() const {
  int const count = sample_count_.load(std::memory_order_relaxed);
  if (count == 0) {
    return 0;
  }
  int64_t const sum = sum_rtt_ns_.load(std::memory_order_relaxed);
  return sum / count;
}

void AverageSampleWindow::Reset() {
  sample_count_.store(0, std::memory_order_relaxed);
  sum_rtt_ns_.store(0, std::memory_order_relaxed);
  min_rtt_ns_.store(std::numeric_limits<int64_t>::max(),
                    std::memory_order_relaxed);
  max_inflight_.store(0, std::memory_order_relaxed);
  did_drop_.store(false, std::memory_order_relaxed);
}

}  // namespace limen

// Copyright 2019 Netflix, Inc.
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

#include "limen/percentile_sample_window.h"
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>

namespace limen {

PercentileSampleWindow::PercentileSampleWindow(double percentile,
                                               int max_samples_per_window)
    : percentile_(percentile),
      max_samples_(max_samples_per_window),
      rtts_(std::make_unique<std::atomic<int64_t>[]>(max_samples_per_window)) {
  // Each `std::atomic<int64_t>` in the array is value-initialised to
  // zero by `make_unique<T[]>`, so the array is safe to read after
  // construction without an explicit zero-fill.
  //
  // Reserve the sort buffer to its maximum capacity so subsequent
  // `clear()` + `push_back()` cycles in `TrackedRttNs` never grow
  // the underlying allocation.
  sort_buffer_.reserve(max_samples_per_window);
}

void PercentileSampleWindow::AddSample(int64_t rtt_ns, int inflight,
                                       bool did_drop) {
  int const index = sample_count_.fetch_add(1, std::memory_order_relaxed);
  if (index < max_samples_) {
    rtts_[index].store(rtt_ns, std::memory_order_relaxed);
  }
  // The sample's RTT contributes to the minimum even when the array
  // is full; the array bound only affects which samples participate
  // in the percentile calculation.
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

  if (did_drop) {
    did_drop_.store(true, std::memory_order_relaxed);
  }
}

int64_t PercentileSampleWindow::TrackedRttNs() const {
  int const observed = sample_count_.load(std::memory_order_relaxed);
  if (observed == 0) {
    return 0;
  }
  int const usable = std::min(observed, max_samples_);

  // Snapshot the array into the pre-allocated sort buffer. The
  // buffer's capacity was set to `max_samples_` at construction so
  // `clear` + `push_back` never reallocates.
  sort_buffer_.clear();
  for (int i = 0; i < usable; ++i) {
    sort_buffer_.push_back(rtts_[i].load(std::memory_order_relaxed));
  }
  std::sort(sort_buffer_.begin(), sort_buffer_.end());

  // Upstream picks the element at `round(sample_count * percentile)`
  // and converts to a zero-based index. Mirror that exactly so
  // applications can compare Limen and Netflix outputs.
  int rtt_index =
      static_cast<int>(std::lround(static_cast<double>(usable) * percentile_));
  int const zero_based = rtt_index - 1;
  int const clamped = std::max(
      0, std::min(zero_based, static_cast<int>(sort_buffer_.size()) - 1));
  return sort_buffer_[clamped];
}

void PercentileSampleWindow::Reset() {
  sample_count_.store(0, std::memory_order_relaxed);
  min_rtt_ns_.store(std::numeric_limits<int64_t>::max(),
                    std::memory_order_relaxed);
  max_inflight_.store(0, std::memory_order_relaxed);
  did_drop_.store(false, std::memory_order_relaxed);
  // The RTT array is intentionally not zeroed: the next window
  // overwrites the entries it needs as it fills. Old samples past
  // the next `sample_count_` are never read.
}

}  // namespace limen

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

#ifndef LIMEN_PERCENTILE_SAMPLE_WINDOW_H_
#define LIMEN_PERCENTILE_SAMPLE_WINDOW_H_

#include "limen/sample_window.h"
#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

namespace limen {

// Sample window that reports a configured percentile of the RTTs
// observed since the last `Reset`.
//
// Stores every individual RTT in a pre-allocated atomic array of
// fixed size. A request thread claims an array index via
// `fetch_add` on `sample_count_` and (if the index is within bounds)
// stores its RTT at that index. The array is allocated once at
// construction; no allocation occurs on any path after.
//
// At boundary time, the boundary thread reads `sample_count_`, then
// reads the array up to `min(sample_count_, max_samples_per_window)`,
// sorts the snapshot, and selects the element at the configured
// percentile. The array's stale entries are left in place after
// `Reset`; the next window overwrites them as it fills.
//
// Auxiliary fields — minimum RTT, maximum in-flight, drop-occurred —
// use the same atomic patterns as `AverageSampleWindow`.
//
// Ported from Netflix's `ImmutablePercentileSampleWindow.java`.
class PercentileSampleWindow final : public SampleWindow {
 public:
  // `percentile` in (0.0, 1.0]. `max_samples_per_window` bounds the
  // pre-allocated RTT array.
  PercentileSampleWindow(double percentile, int max_samples_per_window);

  void AddSample(int64_t rtt_ns, int inflight, bool did_drop) override;

  int64_t CandidateRttNs() const override {
    return min_rtt_ns_.load(std::memory_order_relaxed);
  }

  // Sorts the captured RTT array and returns the value at the
  // configured percentile. Computed on demand; not amortised across
  // samples. Returns 0 when no samples have been observed.
  int64_t TrackedRttNs() const override;

  int MaxInflight() const override {
    return max_inflight_.load(std::memory_order_relaxed);
  }

  int SampleCount() const override {
    return sample_count_.load(std::memory_order_relaxed);
  }

  bool DidDrop() const override {
    return did_drop_.load(std::memory_order_relaxed);
  }

  void Reset() override;

 private:
  double const percentile_;
  int const max_samples_;
  // Pre-allocated at construction. `std::atomic<int64_t>` is not
  // movable, so we hold the array indirectly through a unique_ptr to
  // an array of atomics.
  std::unique_ptr<std::atomic<int64_t>[]> rtts_;
  // Pre-allocated sort buffer for `TrackedRttNs`. Capacity is sized
  // to `max_samples_per_window` at construction, so the per-boundary
  // percentile computation does not allocate. Mutable because
  // `TrackedRttNs` is logically const but rewrites this buffer; safe
  // because the boundary thread holds the WindowedLimit try-lock
  // when it calls `TrackedRttNs`.
  mutable std::vector<int64_t> sort_buffer_;
  std::atomic<int> sample_count_{0};
  std::atomic<int64_t> min_rtt_ns_{std::numeric_limits<int64_t>::max()};
  std::atomic<int> max_inflight_{0};
  std::atomic<bool> did_drop_{false};
};

}  // namespace limen

#endif  // LIMEN_PERCENTILE_SAMPLE_WINDOW_H_

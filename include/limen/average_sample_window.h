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

#ifndef LIMEN_AVERAGE_SAMPLE_WINDOW_H_
#define LIMEN_AVERAGE_SAMPLE_WINDOW_H_

#include "limen/sample_window.h"
#include <atomic>
#include <cstdint>
#include <limits>

namespace limen {

// Sample window that reports the arithmetic mean RTT over the
// samples folded in since construction or the last `Reset`.
//
// All aggregate state is held in independent atomics. `AddSample`
// uses a `fetch_add` on the count and the sum, a compare-and-swap
// loop on the running minimum and the running maximum-inflight, and
// a plain conditional store on the drop-occurred flag. There is no
// lock and no allocation on any path.
//
// Ported from Netflix's `ImmutableAverageSampleWindow.java`. Where
// upstream allocates a fresh window on every sample, Limen folds
// samples into a single mutable slot whose lifetime spans the entire
// `WindowedLimit`.
class AverageSampleWindow final : public SampleWindow {
 public:
  AverageSampleWindow() = default;

  void AddSample(int64_t rtt_ns, int inflight, bool did_drop) override;

  int64_t CandidateRttNs() const override {
    return min_rtt_ns_.load(std::memory_order_relaxed);
  }

  // Arithmetic mean RTT. Returns 0 when no samples have been seen.
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
  std::atomic<int> sample_count_{0};
  std::atomic<int64_t> sum_rtt_ns_{0};
  std::atomic<int64_t> min_rtt_ns_{std::numeric_limits<int64_t>::max()};
  std::atomic<int> max_inflight_{0};
  std::atomic<bool> did_drop_{false};
};

}  // namespace limen

#endif  // LIMEN_AVERAGE_SAMPLE_WINDOW_H_

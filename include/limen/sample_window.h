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

#ifndef LIMEN_SAMPLE_WINDOW_H_
#define LIMEN_SAMPLE_WINDOW_H_

#include <cstdint>

namespace limen {

// Aggregates per-window sample data for `WindowedLimit`.
//
// The wider design uses a fixed pair of pre-allocated `SampleWindow`
// instances and an atomic index that selects which one is active.
// Each implementation is therefore mutable: samples are folded into
// the live atomic fields via relaxed updates, and the boundary thread
// reads the aggregate, hands it to the wrapped algorithm, and resets
// the slot. There is no per-sample allocation.
//
// Two implementations ship with v0.1.0:
// - `AverageSampleWindow` tracks a running sum and reports the
//   arithmetic mean RTT.
// - `PercentileSampleWindow` keeps every RTT in a fixed-size atomic
//   array and reports the configured percentile at boundary time.
//
// The interface mirrors Netflix's
// `com.netflix.concurrency.limits.limit.window.SampleWindow`.
class SampleWindow {
 public:
  SampleWindow() = default;
  virtual ~SampleWindow() = default;

  SampleWindow(SampleWindow const&) = delete;
  SampleWindow& operator=(SampleWindow const&) = delete;
  SampleWindow(SampleWindow&&) = delete;
  SampleWindow& operator=(SampleWindow&&) = delete;

  // Records one sample. Called from any request thread without
  // external synchronisation; implementations use relaxed atomics.
  virtual void AddSample(int64_t rtt_ns, int inflight, bool did_drop) = 0;

  // Candidate RTT for adaptive window sizing. Implementations return
  // the smallest RTT seen so far (`int64_t` max if none).
  virtual int64_t CandidateRttNs() const = 0;

  // Tracked RTT — the value the wrapped algorithm consumes. For the
  // average window this is the arithmetic mean; for the percentile
  // window this is the configured percentile sorted at boundary time.
  virtual int64_t TrackedRttNs() const = 0;

  // Maximum in-flight count reported across all samples in the
  // window. Driven by the caller's `inflight` argument on each
  // `AddSample` call.
  virtual int MaxInflight() const = 0;

  // Number of samples folded into the window. The percentile window
  // can be configured to drop samples past its array bound, but the
  // sample count still increments past the bound.
  virtual int SampleCount() const = 0;

  // True if any sample in the window reported `did_drop=true`.
  virtual bool DidDrop() const = 0;

  // Resets the aggregate back to its initial state. Called by the
  // boundary thread after reading the slot and before the slot can
  // become active again. Implementations must leave the slot safe to
  // receive concurrent `AddSample` calls from racing request threads
  // that captured the old active index just before the swap.
  virtual void Reset() = 0;
};

}  // namespace limen

#endif  // LIMEN_SAMPLE_WINDOW_H_

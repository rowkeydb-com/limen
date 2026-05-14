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

#include "limen/windowed_limit.h"
#include "limen/average_sample_window.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

namespace limen {

absl::StatusOr<std::unique_ptr<WindowedLimit>> WindowedLimit::Builder::Build(
    std::unique_ptr<Limit> delegate) {
  // Validate the accumulated configuration. Upstream Java enforces
  // additional minimums (>= 100 ms window time, >= 10 sample
  // window) via runtime exceptions; Limen does not, because
  // (1) `-fno-exceptions` rules out the upstream mechanism, and
  // (2) sub-millisecond windows are useful in tests. Operators
  // should still follow the upstream guidance for production
  // deployments; the recommended ranges are documented on each
  // setter above. A library does not crash the host process on
  // bad input — the application picks the response.
  if (min_window_time_ns_ <= 0) {
    return absl::InvalidArgumentError(
        "WindowedLimit::Builder::MinWindowTimeNs must be positive");
  }
  if (max_window_time_ns_ <= 0) {
    return absl::InvalidArgumentError(
        "WindowedLimit::Builder::MaxWindowTimeNs must be positive");
  }
  if (min_window_time_ns_ > max_window_time_ns_) {
    return absl::InvalidArgumentError(
        "WindowedLimit::Builder::MinWindowTimeNs must not exceed "
        "MaxWindowTimeNs");
  }
  if (window_size_ < 1) {
    return absl::InvalidArgumentError(
        "WindowedLimit::Builder::WindowSize must be at least 1");
  }
  if (min_rtt_threshold_ns_ < 0) {
    return absl::InvalidArgumentError(
        "WindowedLimit::Builder::MinRttThresholdNs must be non-negative");
  }

  // If the application did not supply a factory, default to the
  // average sample window — that's the upstream default too.
  auto factory = sample_window_factory_
                     ? sample_window_factory_
                     : []() -> std::unique_ptr<SampleWindow> {
    return std::make_unique<AverageSampleWindow>();
  };
  Params params{
      std::move(delegate), {factory(), factory()}, min_window_time_ns_,
      max_window_time_ns_, window_size_,           min_rtt_threshold_ns_,
  };
  return std::make_unique<WindowedLimit>(PrivateTag{}, std::move(params));
}

WindowedLimit::WindowedLimit(PrivateTag, Params params)
    : delegate_(std::move(params.delegate)),
      slots_(std::move(params.slots)),
      min_window_time_ns_(params.min_window_time_ns),
      max_window_time_ns_(params.max_window_time_ns),
      window_size_(params.window_size),
      min_rtt_threshold_ns_(params.min_rtt_threshold_ns) {}

void WindowedLimit::OnSample(int64_t start_time_ns, int64_t rtt_ns,
                             int inflight, bool did_drop) {
  // Drop noise. Upstream `WindowedLimit` does the same filter at the
  // top of `onSample`.
  if (rtt_ns < min_rtt_threshold_ns_) {
    return;
  }
  int64_t const end_time_ns = start_time_ns + rtt_ns;

  // Snapshot the active index, then fold the sample into that slot.
  // A boundary swap that happens between the load and the AddSample
  // call is the documented race the design accepts.
  int const idx = active_index_.load(std::memory_order_acquire);
  slots_[idx]->AddSample(rtt_ns, inflight, did_drop);

  int64_t const next_due = next_update_time_ns_.load(std::memory_order_relaxed);
  if (end_time_ns <= next_due) {
    return;
  }

  // Boundary crossed. Serialise across racing threads via a
  // try-lock — only one thread does the swap, the others continue
  // accumulating against the new active buffer.
  if (!boundary_mu_.try_lock()) {
    return;
  }

  // Re-check under the lock: another thread may have already
  // performed the swap between our load above and our lock here.
  int64_t const due_now = next_update_time_ns_.load(std::memory_order_relaxed);
  if (end_time_ns <= due_now) {
    boundary_mu_.unlock();
    return;
  }

  int const old_idx = active_index_.load(std::memory_order_relaxed);
  int const new_idx = 1 - old_idx;
  SampleWindow& completed = *slots_[old_idx];

  // Read the aggregate the algorithm will see BEFORE flipping the
  // active index, so a late writer that captured the old index has
  // a chance to land its update in the aggregate.
  int64_t const candidate_rtt = completed.CandidateRttNs();
  int64_t const tracked_rtt = completed.TrackedRttNs();
  int const max_inflight = completed.MaxInflight();
  int const sample_count = completed.SampleCount();
  bool const did_drop_in_window = completed.DidDrop();

  // Flip the active buffer. New samples now land in `new_idx`.
  active_index_.store(new_idx, std::memory_order_release);

  // Schedule the next boundary. Upstream clamps
  // `candidate * 2` to `[min_window_time, max_window_time]`; the
  // default `min == max == 1s` pins the window to one second.
  int64_t adaptive_ns;
  if (candidate_rtt == std::numeric_limits<int64_t>::max()) {
    adaptive_ns = min_window_time_ns_;
  } else {
    adaptive_ns = std::min(std::max(candidate_rtt * 2, min_window_time_ns_),
                           max_window_time_ns_);
  }
  next_update_time_ns_.store(end_time_ns + adaptive_ns,
                             std::memory_order_relaxed);

  // Drive the delegate only when the just-closed window collected
  // enough usable samples (`candidate < int64-max` means at least
  // one sample, `sample_count >= window_size` rejects underloaded
  // windows).
  bool const window_ready =
      candidate_rtt != std::numeric_limits<int64_t>::max() &&
      sample_count >= window_size_;
  if (window_ready) {
    delegate_->OnSample(start_time_ns, tracked_rtt, max_inflight,
                        did_drop_in_window);
  }

  // Reset the now-inactive slot so it is fresh next time it
  // becomes active. Late writers that captured `old_idx` before
  // the swap may still land updates in this slot; the design
  // accepts the small loss in those interleavings.
  completed.Reset();

  boundary_mu_.unlock();
}

}  // namespace limen

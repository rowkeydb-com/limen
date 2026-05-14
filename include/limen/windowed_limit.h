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

#ifndef LIMEN_WINDOWED_LIMIT_H_
#define LIMEN_WINDOWED_LIMIT_H_

#include "limen/limit.h"
#include "limen/sample_window.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>

namespace limen {

// Decorator that aggregates per-request samples into time windows and
// drives the wrapped `Limit` at most once per window boundary.
//
// Internally holds two pre-allocated `SampleWindow` slots and an
// atomic index selecting which one is active. Request threads fold
// their samples into the active slot via relaxed atomic operations.
// At a window boundary the thread that wins a try-lock on
// `boundary_mu_` flips the active index, reads the now-inactive
// slot, hands the aggregate to the wrapped limit, and resets the
// slot for its next use. Other threads that observe the try-lock
// already taken skip the boundary work and continue accumulating
// in the new active slot.
//
// The window duration adapts to the observed RTT. Each boundary,
// the next-update time is set to `end_time + clamp(candidate * 2,
// min_window_time, max_window_time)`. With the defaults
// (`min_window_time == max_window_time == 1s`) the clamp pins the
// window to one second; setting the two to different values opts
// into adaptive sizing.
//
// Ported from Netflix's `WindowedLimit.java`, with the deliberate
// pre-allocated-slot design that avoids the per-sample allocation
// upstream relies on Java's GC to absorb.
class WindowedLimit final : public Limit {
 public:
  // Constructs a `SampleWindow` slot. Called twice at `Build` time.
  // Default produces an `AverageSampleWindow`; the percentile
  // window factory is provided directly by the application.
  using WindowFactory = std::function<std::unique_ptr<SampleWindow>()>;

  class Builder {
   public:
    Builder() = default;

    // Minimum window duration. Default 1 second. Production
    // operators are encouraged to keep this at or above 100 ms (the
    // upstream Java enforced minimum); tests may use shorter values
    // to drive boundaries cheaply.
    Builder& MinWindowTimeNs(int64_t v) {
      min_window_time_ns_ = v;
      return *this;
    }

    // Maximum window duration. Default 1 second. Production
    // operators are encouraged to keep this at or above 100 ms; tests
    // may use shorter values to drive boundaries cheaply.
    Builder& MaxWindowTimeNs(int64_t v) {
      max_window_time_ns_ = v;
      return *this;
    }

    // Minimum sample count before a boundary drives the delegate.
    // Default 10. Production operators are encouraged to keep this
    // at or above 10 (the upstream Java enforced minimum).
    Builder& WindowSize(int v) {
      window_size_ = v;
      return *this;
    }

    // Samples with RTT below this threshold are dropped on entry
    // (treated as noise). Default 100 microseconds in nanoseconds.
    Builder& MinRttThresholdNs(int64_t v) {
      min_rtt_threshold_ns_ = v;
      return *this;
    }

    // Replaces the default `AverageSampleWindow` factory. The
    // factory is invoked twice at `Build` time, once per slot.
    Builder& SampleWindowFactory(WindowFactory factory) {
      sample_window_factory_ = std::move(factory);
      return *this;
    }

    // Builds the limiter and transfers ownership of the delegate.
    // Returns `InvalidArgumentError` when the accumulated
    // configuration is broken (non-positive window times, inverted
    // min/max, sample count below one, negative RTT threshold). A
    // library never crashes the host process on bad input; the
    // application picks the response.
    absl::StatusOr<std::unique_ptr<WindowedLimit>> Build(
        std::unique_ptr<Limit> delegate);

   private:
    int64_t min_window_time_ns_ = 1'000'000'000;
    int64_t max_window_time_ns_ = 1'000'000'000;
    int window_size_ = 10;
    int64_t min_rtt_threshold_ns_ = 100'000;
    WindowFactory sample_window_factory_;
  };

  int GetLimit() const final { return delegate_->GetLimit(); }

  void OnSample(int64_t start_time_ns, int64_t rtt_ns, int inflight,
                bool did_drop) final;

  void NotifyOnChange(ChangeCallback callback) final {
    delegate_->NotifyOnChange(std::move(callback));
  }

 private:
  // Construction goes through the inner Builder. The PrivateTag is
  // only constructible by Builder, so the public ctor cannot be
  // called from elsewhere — but `std::make_unique` can call it,
  // which keeps Build() free of raw `new`.
  struct PrivateTag {
   private:
    PrivateTag() = default;
    friend class Builder;
  };

  struct Params {
    std::unique_ptr<Limit> delegate;
    std::array<std::unique_ptr<SampleWindow>, 2> slots;
    int64_t min_window_time_ns;
    int64_t max_window_time_ns;
    int window_size;
    int64_t min_rtt_threshold_ns;
  };

 public:
  WindowedLimit(PrivateTag, Params params);

 private:
  std::unique_ptr<Limit> delegate_;
  std::array<std::unique_ptr<SampleWindow>, 2> slots_;
  std::atomic<int> active_index_{0};
  std::atomic<int64_t> next_update_time_ns_{0};
  absl::Mutex boundary_mu_;

  int64_t const min_window_time_ns_;
  int64_t const max_window_time_ns_;
  int const window_size_;
  int64_t const min_rtt_threshold_ns_;
};

}  // namespace limen

#endif  // LIMEN_WINDOWED_LIMIT_H_

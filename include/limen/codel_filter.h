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

#ifndef LIMEN_CODEL_FILTER_H_
#define LIMEN_CODEL_FILTER_H_

#include "absl/base/thread_annotations.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "opentelemetry/metrics/async_instruments.h"
#include "opentelemetry/metrics/observer_result.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace limen {

// `CodelFilter` is a transport-agnostic Controlled Delay (CoDel)
// admission filter. It implements the full RFC 8289 algorithm:
// the application stamps an `enqueue_time` when it puts a unit of
// work onto its own queue, and at dequeue (or wherever the
// application makes the next "should I do this work?" decision)
// it calls `ShouldDrop(enqueue_time)`. The filter returns true
// when the algorithm has decided the item has sat in the queue
// too long and should be dropped. The application responds by
// failing the request with whatever its protocol-level
// "overloaded" status is (`RESOURCE_EXHAUSTED` for gRPC).
//
// CoDel watches the *minimum sojourn time* over a sliding window
// of `interval` (default 100 ms). If that minimum exceeds
// `target` (default 5 ms), the filter enters drop mode and drops
// items at a rate that increases by an inverse-square-root law
// (`interval / sqrt(count)`). It exits drop mode the moment
// sojourn drops below target. Brief bursts that resolve within
// target are not dropped; sustained overload is.
//
// Per RFC 8289 §5, the state machine has five variables
// (`first_above_time_`, `drop_next_`, `count_`, `lastcount_`,
// `dropping_`). The implementation lives inline in
// `src/codel_filter.cc`; this header pins the public surface.
//
// One filter per queue. The filter does not own the queue, does
// not enforce a queue-length cap, and does not know how many
// items are queued — those are the application's concern.
// Concurrent `ShouldDrop` calls against one filter are
// serialised through `absl::Mutex mu_` (short critical section).
//
// Thread-safe. Non-copyable, non-movable; held by
// `std::unique_ptr` for its full lifetime, same pattern as every
// other Limen limiter.
//
// Implements RFC 8289 (Nichols, Jacobson, McGregor, Iyengar,
// January 2018).
class CodelFilter final {
 public:
  // Callable that returns "now". Default at Build() is
  // `&absl::Now`. Tests inject a mock clock so assertions hold
  // under any schedule — same pattern as
  // `AbstractPartitionedLimiter`'s `SleepFn`.
  using ClockFn = std::function<absl::Time()>;

  class Builder {
   public:
    Builder() = default;

    // Sojourn threshold. The algorithm enters drop mode when
    // the minimum sojourn over `Interval` exceeds this value.
    // Default 5 ms (RFC 8289 §5.3). Must be positive.
    Builder& Target(absl::Duration v) {
      target_ = v;
      return *this;
    }

    // Sliding-window duration. The algorithm starts dropping
    // when sojourn has exceeded `Target` for at least this
    // long. Default 100 ms (RFC 8289 §5.3). Must be positive.
    Builder& Interval(absl::Duration v) {
      interval_ = v;
      return *this;
    }

    // Time source. Default at Build() is `&absl::Now`. Tests
    // pass a mock clock for determinism.
    Builder& Clock(ClockFn clock) {
      clock_ = std::move(clock);
      return *this;
    }

    // Label value emitted on every metric the filter records.
    Builder& Id(std::string id) {
      id_ = std::move(id);
      return *this;
    }

    // Optional OpenTelemetry SDK MeterProvider. When supplied,
    // the filter registers `limen.codel.drops` (observable
    // counter) and `limen.codel.dropping` (observable up-down
    // counter, 0 or 1). Without a MeterProvider, the
    // observability handles are null and no observability work
    // runs.
    //
    // The SDK type (not the API type) is required for
    // consistency with the rest of Limen.
    Builder& MeterProvider(
        std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> p) {
      meter_provider_ = std::move(p);
      return *this;
    }

    // Validates the accumulated configuration and constructs
    // the filter. Returns `InvalidArgumentError` when:
    // - Target is not positive.
    // - Interval is not positive.
    // A library never crashes the host process on bad input;
    // the application picks the response (log and abort, fall
    // back to a default, fail the startup health check, etc.).
    absl::StatusOr<std::unique_ptr<CodelFilter>> Build();

   private:
    absl::Duration target_ = absl::Milliseconds(5);
    absl::Duration interval_ = absl::Milliseconds(100);
    ClockFn clock_ = nullptr;  // installed by Build() if null.
    std::string id_;
    std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> meter_provider_;
  };

  // The single hot-path entry. Computes sojourn from the
  // supplied `enqueue_time` and the configured clock, runs the
  // RFC 8289 state machine, and returns true when the caller
  // should drop the item. Thread-safe via `absl::Mutex`.
  //
  // Clock-skew guard: if `enqueue_time` is in the future (mock
  // clock test, scheduler racing the timestamp, etc.), sojourn
  // is clamped at zero; the filter never sees a negative
  // duration.
  bool ShouldDrop(absl::Time enqueue_time) ABSL_LOCKS_EXCLUDED(mu_);

  // Whether the filter is currently in drop mode. Cheap
  // read; acquires `mu_` briefly.
  bool IsDropping() const ABSL_LOCKS_EXCLUDED(mu_);

  // Cumulative drops since the filter was built. Reads
  // `drop_count_` with relaxed atomic ordering; does NOT
  // acquire `mu_`, so it is cheap to call from any thread and
  // from the OTel `limen.codel.drops` callback.
  int64_t DropCount() const;

  // Current drops-in-burst counter (RFC 8289 §5.5 `count_`).
  // Acquires `mu_` briefly. Useful as a diagnostic accessor
  // for operators and as a test hook for asserting the RFC's
  // count-adjustment hysteresis behaviour.
  uint32_t DropEpisodeCount() const ABSL_LOCKS_EXCLUDED(mu_);

  // Time of the next scheduled drop in drop mode (RFC 8289
  // §5.5 `drop_next_`). Acquires `mu_` briefly. Diagnostic
  // accessor; tests use it to pin the inverse-sqrt schedule
  // produced by `control_law`.
  absl::Time DropNext() const ABSL_LOCKS_EXCLUDED(mu_);

  // Non-copyable, non-movable. Held by std::unique_ptr.
  CodelFilter(CodelFilter const&) = delete;
  CodelFilter& operator=(CodelFilter const&) = delete;
  CodelFilter(CodelFilter&&) = delete;
  CodelFilter& operator=(CodelFilter&&) = delete;

 private:
  struct PrivateTag {
   private:
    PrivateTag() = default;
    friend class Builder;
  };

  struct Params {
    absl::Duration target;
    absl::Duration interval;
    ClockFn clock;
    std::string id;
    std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> meter_provider;
  };

 public:
  CodelFilter(PrivateTag, Params params);

 private:
  // Runs the RFC 8289 §5.6 `dodequeue` predicate. Mutates
  // `first_above_time_`. Caller holds `mu_`.
  bool OkToDrop(absl::Duration sojourn, absl::Time now)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // RFC 8289 §5.6 `control_law`. Pure; no state access beyond
  // the const `interval_`.
  absl::Time ControlLaw(absl::Time t, uint32_t count) const;

  void RegisterObservableInstruments();
  static void ObserveDrops(opentelemetry::metrics::ObserverResult result,
                           void* state) noexcept;
  static void ObserveDropping(opentelemetry::metrics::ObserverResult result,
                              void* state) noexcept;

  // State, RFC 8289 §5.2 naming and initial values. All
  // mutated only under `mu_`. `absl::UnixEpoch()` is Limen's
  // translation of the RFC's `time_t = 0` sentinel for "unset":
  // any wall-clock or mock-clock value used in production is
  // strictly greater, so equality tests against the sentinel
  // and "recent drop" comparisons (`now - drop_next_ < 16 *
  // interval_`) work the same as the RFC's scalar-0 tests.
  mutable absl::Mutex mu_;
  absl::Time first_above_time_ ABSL_GUARDED_BY(mu_) = absl::UnixEpoch();
  absl::Time drop_next_ ABSL_GUARDED_BY(mu_) = absl::UnixEpoch();
  uint32_t count_ ABSL_GUARDED_BY(mu_) = 0;
  uint32_t lastcount_ ABSL_GUARDED_BY(mu_) = 0;
  bool dropping_ ABSL_GUARDED_BY(mu_) = false;

  // Configuration. Set once at construction; never mutated.
  absl::Duration const target_;
  absl::Duration const interval_;
  ClockFn const clock_;
  std::string const id_;

  // Cumulative drops. Incremented on every drop decision with
  // relaxed atomic ordering. Read by the `limen.codel.drops`
  // OTel callback off the hot path; also surfaced via
  // `DropCount()` for tests and direct application use.
  std::atomic<int64_t> drop_count_{0};

  // OpenTelemetry handles. Null when no MeterProvider was
  // supplied at build time. Same shape as
  // `AbstractLimiter`'s observable instruments.
  std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> meter_provider_;
  std::shared_ptr<opentelemetry::metrics::ObservableInstrument>
      drops_instrument_;
  std::shared_ptr<opentelemetry::metrics::ObservableInstrument>
      dropping_instrument_;
};

}  // namespace limen

#endif  // LIMEN_CODEL_FILTER_H_

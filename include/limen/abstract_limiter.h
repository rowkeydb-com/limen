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

#ifndef LIMEN_ABSTRACT_LIMITER_H_
#define LIMEN_ABSTRACT_LIMITER_H_

#include "limen/limit.h"
#include "limen/limiter.h"
#include "opentelemetry/metrics/async_instruments.h"
#include "opentelemetry/metrics/observer_result.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace limen {

// Base class for concurrency limiters.
//
// Owns:
// - The in-flight atomic counter that the wait-free request hot
//   path increments and decrements.
// - One atomic counter per `(id, status)` admission-outcome pair
//   where status is one of success, rejected, dropped, ignored,
//   bypassed.
// - Three OpenTelemetry observable instruments — `limen.limit`
//   (gauge), `limen.inflight` (up-down counter), and `limen.call`
//   (counter) — whose callbacks read the atomics above. The
//   per-request path makes no synchronous OpenTelemetry calls.
//
// The bypass predicate, when configured, short-circuits the
// admission decision: if `bypass_predicate(context)` returns true,
// the limiter admits the call without touching the in-flight
// counter and counts the outcome as `bypassed`. The wrapped
// algorithm sees no sample for a bypassed call.
//
// Concrete subclasses implement `DoAcquire`, which decides whether
// to admit a non-bypass call. The base class's `TryAcquire`
// handles the bypass branch, the counter bookkeeping, and the
// SlotGuard construction; subclasses only own the gate-check
// policy.
//
// Ported from Netflix's `AbstractLimiter.java`. Concrete subclass
// `SimpleLimiter` implements a compare-and-swap gate on the
// in-flight atomic.
class AbstractLimiter : public Limiter {
 public:
  // Meter name under which all limen metrics are registered. Used
  // by AbstractLimiter and by partitioned subclasses that register
  // additional instruments on the same meter.
  static constexpr char kMeterName[] = "limen";

  // Five mutually-exclusive outcomes for one admission decision.
  enum class Status : int {
    kSuccess = 0,
    kRejected = 1,
    kDropped = 2,
    kIgnored = 3,
    kBypassed = 4,
  };
  // Cardinality of the Status enum. Used to size the outcome
  // counter array. Kept outside the enum so callers cannot pass a
  // sentinel into a Status-typed parameter by mistake.
  static constexpr int kStatusCount = 5;

  using BypassPredicate = std::function<bool(std::string_view context)>;

  std::optional<SlotGuard> TryAcquire(std::string_view context = {}) final;

  // Wait-free reads of the limiter's current state. Safe to call
  // from any thread without coordination.
  int GetLimit() const { return limit_->GetLimit(); }
  int InflightCount() const {
    return inflight_.load(std::memory_order_relaxed);
  }
  int64_t OutcomeCount(Status status) const {
    return outcome_counts_[static_cast<int>(status)].load(
        std::memory_order_relaxed);
  }

 protected:
  struct Params {
    std::unique_ptr<Limit> limit;
    std::string id;
    BypassPredicate bypass_predicate;
    std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> meter_provider;
  };

  // Return value of `DoAcquire` on admission. `inflight_post` is
  // the post-increment global in-flight value; `partition_index`
  // identifies the partition slot that was reserved (or -1 if the
  // limiter is not partitioned). The partition slot, when set,
  // travels through SlotGuard to OnSlotComplete so the partition
  // counter can be released alongside the global one.
  struct AcquireResult {
    int inflight_post;
    int partition_index;
  };

  explicit AbstractLimiter(Params params);

  // Subclasses implement the per-call admission gate. On admission,
  // the override must atomically reserve a slot — that is, leave
  // the in-flight counter incremented by exactly one — and return
  // an AcquireResult carrying the post-increment in-flight value
  // and (for partitioned limiters) the resolved partition index.
  // On rejection, return std::nullopt without touching the counter.
  // The base implementation admits unconditionally with a plain
  // fetch_add and returns a -1 partition index, which is useful
  // for tests that only need to exercise the bookkeeping machinery.
  // SimpleLimiter overrides this with a compare-and-swap loop;
  // AbstractPartitionedLimiter overrides it with a partition-aware
  // gate.
  virtual std::optional<AcquireResult> DoAcquire(std::string_view context);

  // Hook called by the SlotGuard on completion. Decrements
  // in-flight (for non-bypass calls), increments the appropriate
  // outcome counter, feeds a sample to the wrapped algorithm when
  // the outcome warrants one, and calls `OnReleasePartition` so a
  // partitioned subclass can release its per-partition counter.
  // Bypass calls already had their counter incremented at acquire
  // time; the bypass SlotGuard's completion is a no-op for
  // accounting purposes.
  void OnSlotComplete(CompletionStatus status, int64_t start_time_ns,
                      int inflight_at_acquire, bool bypassed,
                      int partition_index) final;

  // Partitioned subclasses override to decrement the per-partition
  // in-flight counter. The base implementation is a no-op. Called
  // from OnSlotComplete with the partition index recorded at
  // acquire time, only for non-bypass completions.
  virtual void OnReleasePartition(int /*partition_index*/) {}

  // Read-only accessors for partitioned subclasses that need to
  // wire their own observable instruments onto the same meter and
  // identify the limiter in metric label sets.
  std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> const&
  GetMeterProvider() const {
    return meter_provider_;
  }
  std::string const& GetId() const { return id_; }

  // Register a callback invoked when the wrapped Limit's cap
  // changes (a `SettableLimit::SetLimit` override or a Gradient2
  // adaptive update). Partitioned subclasses use this to recompute
  // per-partition quotas without exposing the wrapped Limit
  // pointer itself.
  void RegisterLimitChangeCallback(Limit::ChangeCallback callback);

  // Emission hook for the `limen.inflight` observable up-down
  // counter. The default implementation emits one point with just
  // the `id` label (the global in-flight value). Partitioned
  // subclasses override to emit additional per-partition points
  // on the same instrument so an operator can see global and
  // per-partition counts under the same metric name.
  virtual void OnObserveInflight(
      opentelemetry::metrics::ObserverResult const& result) const;

  // Emit one observation row through the OpenTelemetry collector.
  // Wrapped here so partitioned subclasses can format their own
  // per-partition rows without duplicating the attribute-view
  // boilerplate.
  static void EmitObservation(
      opentelemetry::metrics::ObserverResult const& result, int64_t value,
      std::map<std::string, std::string> const& attributes);

  // In-flight atomic. Exposed to subclasses so they can implement
  // their own compare-and-swap-based admission gate against it.
  std::atomic<int> inflight_{0};

 private:
  void RegisterObservableInstruments();
  static void ObserveLimit(opentelemetry::metrics::ObserverResult result,
                           void* state) noexcept;
  static void ObserveInflight(opentelemetry::metrics::ObserverResult result,
                              void* state) noexcept;
  static void ObserveCall(opentelemetry::metrics::ObserverResult result,
                          void* state) noexcept;

  std::unique_ptr<Limit> limit_;
  std::string const id_;
  BypassPredicate bypass_predicate_;
  std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> meter_provider_;

  // One counter per Status value. Indexed by static_cast<int>(status).
  std::atomic<int64_t> outcome_counts_[kStatusCount] = {};

  // OpenTelemetry observable instrument handles. Null when no
  // MeterProvider was supplied. Kept alive for the limiter's
  // lifetime so the SDK's collection thread can read the atomics
  // above through the callbacks below.
  std::shared_ptr<opentelemetry::metrics::ObservableInstrument> limit_gauge_;
  std::shared_ptr<opentelemetry::metrics::ObservableInstrument>
      inflight_counter_;
  std::shared_ptr<opentelemetry::metrics::ObservableInstrument> call_counter_;
};

}  // namespace limen

#endif  // LIMEN_ABSTRACT_LIMITER_H_

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
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
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
// Listener construction; subclasses only own the gate-check policy.
//
// Ported from Netflix's `AbstractLimiter.java`. Concrete subclass
// `SimpleLimiter` (lands in commit 6) implements a compare-and-swap
// gate on the in-flight atomic.
class AbstractLimiter : public Limiter {
 public:
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

  std::unique_ptr<Listener> TryAcquire(std::string_view context = {}) final;

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

  explicit AbstractLimiter(Params params);

  // Subclasses implement the per-call admission gate. Returning
  // true admits and the base class produces a Listener; returning
  // false rejects (the base class increments the rejected counter
  // and returns null). The base implementation admits
  // unconditionally — useful for tests that only need to exercise
  // the listener machinery.
  virtual bool DoAcquire(std::string_view context);

  // Called by the per-call Listener when the work completes. The
  // base class decrements the in-flight counter, increments the
  // outcome counter, and feeds a sample to the wrapped algorithm
  // when the outcome warrants one. Bypass listeners never call
  // this method — they are accounted for at acquire time.
  void RecordOutcome(Status status, int64_t start_time_ns,
                     int inflight_at_acquire);

 private:
  // Two private Listener implementations defined in the .cc. As
  // nested classes they have full access to the enclosing class's
  // protected `RecordOutcome` method without a friend declaration.
  class CallListener;
  class BypassListener;

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

  std::atomic<int> inflight_{0};
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

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

  explicit AbstractLimiter(Params params);

  // Subclasses implement the per-call admission gate. On admission,
  // the override must atomically reserve a slot — that is, leave
  // the in-flight counter incremented by exactly one — and return
  // the post-increment value. On rejection, return std::nullopt
  // without touching the counter. The base implementation admits
  // unconditionally with a plain fetch_add, which is useful for
  // tests that only need to exercise the bookkeeping machinery.
  // SimpleLimiter overrides this with a compare-and-swap loop
  // that checks the in-flight value against the cap before
  // committing the increment.
  virtual std::optional<int> DoAcquire(std::string_view context);

  // Hook called by the SlotGuard on completion. Decrements
  // in-flight (for non-bypass calls), increments the appropriate
  // outcome counter, and feeds a sample to the wrapped algorithm
  // when the outcome warrants one. Bypass calls already had their
  // counter incremented at acquire time; the bypass SlotGuard's
  // completion is a no-op for accounting purposes.
  void OnSlotComplete(CompletionStatus status, int64_t start_time_ns,
                      int inflight_at_acquire, bool bypassed) final;

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

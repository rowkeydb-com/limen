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

#include "limen/abstract_limiter.h"
#include "opentelemetry/common/key_value_iterable_view.h"
#include "opentelemetry/metrics/meter.h"
#include "opentelemetry/metrics/observer_result.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <utility>

namespace limen {
namespace {

constexpr char kMeterName[] = "limen";

int64_t NowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

// String label for each Status value. Order must match the enum's
// integer values; the array is indexed by static_cast<int>(status).
constexpr char const* kStatusLabels[] = {
    "success", "rejected", "dropped", "ignored", "bypassed",
};
static_assert(std::size(kStatusLabels) == AbstractLimiter::kStatusCount,
              "kStatusLabels must have one entry per Status value");

}  // namespace

// Concrete Listener for an admitted call. Captures per-call state
// at admission (start timestamp, in-flight count at acquire) and
// the back-reference to the limiter. The completion methods
// delegate to `AbstractLimiter::RecordOutcome`. Defined as a
// private nested class so it inherits access to the enclosing
// class's protected members.
class AbstractLimiter::CallListener final : public Limiter::Listener {
 public:
  CallListener(AbstractLimiter* limiter, int64_t start_time_ns,
               int inflight_at_acquire)
      : limiter_(limiter),
        start_time_ns_(start_time_ns),
        inflight_at_acquire_(inflight_at_acquire) {}

  void OnSuccess() override {
    limiter_->RecordOutcome(AbstractLimiter::Status::kSuccess, start_time_ns_,
                            inflight_at_acquire_);
  }

  void OnIgnore() override {
    limiter_->RecordOutcome(AbstractLimiter::Status::kIgnored, start_time_ns_,
                            inflight_at_acquire_);
  }

  void OnDropped() override {
    limiter_->RecordOutcome(AbstractLimiter::Status::kDropped, start_time_ns_,
                            inflight_at_acquire_);
  }

 private:
  AbstractLimiter* limiter_;
  int64_t start_time_ns_;
  int inflight_at_acquire_;
};

// Listener returned by the bypass branch. The bypassed counter is
// incremented at acquire time (in TryAcquire), so the completion
// methods are pure no-ops: the call neither touched the in-flight
// gate nor signals real system load, so there is nothing to
// release and no sample to feed.
class AbstractLimiter::BypassListener final : public Limiter::Listener {
 public:
  BypassListener() = default;
  void OnSuccess() override {}
  void OnIgnore() override {}
  void OnDropped() override {}
};

AbstractLimiter::AbstractLimiter(Params params)
    : limit_(std::move(params.limit)),
      id_(std::move(params.id)),
      bypass_predicate_(std::move(params.bypass_predicate)),
      meter_provider_(std::move(params.meter_provider)) {
  RegisterObservableInstruments();
}

std::unique_ptr<Limiter::Listener> AbstractLimiter::TryAcquire(
    std::string_view context) {
  // Bypass: the call opts out of admission control entirely. The
  // bypassed counter is incremented here at acquire time (matching
  // upstream Java's createBypassListener behaviour); the returned
  // BypassListener does nothing on completion, so a bypassed call
  // whose listener is destructed without an explicit On* call
  // still contributes to the counter.
  if (bypass_predicate_ && bypass_predicate_(context)) {
    outcome_counts_[static_cast<int>(Status::kBypassed)].fetch_add(
        1, std::memory_order_relaxed);
    return std::make_unique<BypassListener>();
  }

  // Non-bypass: delegate to the subclass's gate. The base
  // implementation admits unconditionally; SimpleLimiter (commit 6)
  // overrides to do the compare-and-swap on the in-flight atomic.
  if (!DoAcquire(context)) {
    outcome_counts_[static_cast<int>(Status::kRejected)].fetch_add(
        1, std::memory_order_relaxed);
    return nullptr;
  }

  // Admitted. Increment the in-flight counter (the gate already
  // checked admission against the cap if the subclass implements
  // one). Capture the value AFTER our increment so the listener's
  // completion can pass a stable count to the algorithm.
  int64_t const start_time_ns = NowNs();
  int const inflight_at_acquire =
      inflight_.fetch_add(1, std::memory_order_relaxed) + 1;
  return std::make_unique<CallListener>(this, start_time_ns,
                                        inflight_at_acquire);
}

bool AbstractLimiter::DoAcquire(std::string_view /*context*/) { return true; }

void AbstractLimiter::RecordOutcome(Status status, int64_t start_time_ns,
                                    int inflight_at_acquire) {
  inflight_.fetch_sub(1, std::memory_order_relaxed);
  outcome_counts_[static_cast<int>(status)].fetch_add(
      1, std::memory_order_relaxed);

  // Feed a sample to the wrapped algorithm for the two completion
  // outcomes that carry latency information. Ignored calls do not
  // signal real system load and so contribute no sample.
  if (status == Status::kSuccess || status == Status::kDropped) {
    int64_t const rtt_ns = NowNs() - start_time_ns;
    bool const did_drop = (status == Status::kDropped);
    limit_->OnSample(start_time_ns, rtt_ns, inflight_at_acquire, did_drop);
  }
}

void AbstractLimiter::RegisterObservableInstruments() {
  if (meter_provider_ == nullptr) {
    return;
  }
  auto meter =
      meter_provider_->GetMeter(kMeterName, /*version=*/"", /*schema=*/"");
  limit_gauge_ = meter->CreateInt64ObservableGauge(
      "limen.limit", "Current concurrency cap.", /*unit=*/"1");
  limit_gauge_->AddCallback(&AbstractLimiter::ObserveLimit, this);

  inflight_counter_ = meter->CreateInt64ObservableUpDownCounter(
      "limen.inflight", "Requests currently in progress.", /*unit=*/"1");
  inflight_counter_->AddCallback(&AbstractLimiter::ObserveInflight, this);

  call_counter_ = meter->CreateInt64ObservableCounter(
      "limen.call", "Cumulative outcomes of admission decisions.",
      /*unit=*/"1");
  call_counter_->AddCallback(&AbstractLimiter::ObserveCall, this);
}

namespace {

void ObserveOne(opentelemetry::metrics::ObserverResult const& result,
                int64_t value,
                std::map<std::string, std::string> const& attrs) {
  opentelemetry::common::KeyValueIterableView attr_view(attrs);
  auto int_observer =
      opentelemetry::nostd::get<opentelemetry::nostd::shared_ptr<
          opentelemetry::metrics::ObserverResultT<int64_t>>>(result);
  int_observer->Observe(value, attr_view);
}

}  // namespace

void AbstractLimiter::ObserveLimit(
    opentelemetry::metrics::ObserverResult result, void* state) noexcept {
  auto* self = static_cast<AbstractLimiter*>(state);
  ObserveOne(result, static_cast<int64_t>(self->limit_->GetLimit()),
             {{"id", self->id_}});
}

void AbstractLimiter::ObserveInflight(
    opentelemetry::metrics::ObserverResult result, void* state) noexcept {
  auto* self = static_cast<AbstractLimiter*>(state);
  ObserveOne(
      result,
      static_cast<int64_t>(self->inflight_.load(std::memory_order_relaxed)),
      {{"id", self->id_}});
}

void AbstractLimiter::ObserveCall(opentelemetry::metrics::ObserverResult result,
                                  void* state) noexcept {
  auto* self = static_cast<AbstractLimiter*>(state);
  for (int i = 0; i < AbstractLimiter::kStatusCount; ++i) {
    int64_t const value =
        self->outcome_counts_[i].load(std::memory_order_relaxed);
    std::map<std::string, std::string> attrs{{"id", self->id_},
                                             {"status", kStatusLabels[i]}};
    ObserveOne(result, value, attrs);
  }
}

}  // namespace limen

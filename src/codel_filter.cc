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

#include "limen/codel_filter.h"
#include "absl/status/status.h"
#include "absl/time/clock.h"
#include "opentelemetry/common/key_value_iterable_view.h"
#include "opentelemetry/metrics/meter.h"
#include "opentelemetry/nostd/string_view.h"
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>

namespace limen {
namespace {

// All Limen metrics live under one meter name. Same string used
// by `AbstractLimiter::kMeterName`; duplicated here so CodelFilter
// has no compile-time dependency on AbstractLimiter.
constexpr char kMeterName[] = "limen";

// Emit one observation row through the OpenTelemetry collector
// callback. Mirrors `AbstractLimiter::EmitObservation` (see
// `src/abstract_limiter.cc`) so the format of every `limen.*`
// metric stays consistent.
void EmitObservation(opentelemetry::metrics::ObserverResult const& result,
                     int64_t value,
                     std::map<std::string, std::string> const& attributes) {
  opentelemetry::common::KeyValueIterableView attr_view(attributes);
  auto int_observer =
      opentelemetry::nostd::get<opentelemetry::nostd::shared_ptr<
          opentelemetry::metrics::ObserverResultT<int64_t>>>(result);
  int_observer->Observe(value, attr_view);
}

}  // namespace

absl::StatusOr<std::unique_ptr<CodelFilter>> CodelFilter::Builder::Build() {
  if (target_ <= absl::ZeroDuration()) {
    return absl::InvalidArgumentError(
        "CodelFilter::Builder::Target must be a positive duration");
  }
  if (interval_ <= absl::ZeroDuration()) {
    return absl::InvalidArgumentError(
        "CodelFilter::Builder::Interval must be a positive duration");
  }

  Params params{
      target_,
      interval_,
      clock_ ? std::move(clock_)
             : CodelFilter::ClockFn([] { return absl::Now(); }),
      std::move(id_),
      std::move(meter_provider_),
  };
  return std::make_unique<CodelFilter>(PrivateTag{}, std::move(params));
}

CodelFilter::CodelFilter(PrivateTag, Params params)
    : target_(params.target),
      interval_(params.interval),
      clock_(std::move(params.clock)),
      id_(std::move(params.id)),
      meter_provider_(std::move(params.meter_provider)) {
  RegisterObservableInstruments();
}

bool CodelFilter::ShouldDrop(absl::Time enqueue_time) {
  absl::MutexLock lock(mu_);
  absl::Time const now = clock_();
  // Clock-skew clamp: enqueue_time in the future yields zero
  // sojourn rather than a negative duration. Defensive; the
  // RFC's reference does not need this because its enqueue
  // stamps `now`, but Limen accepts the timestamp from the
  // application.
  absl::Duration const sojourn =
      std::max(absl::ZeroDuration(), now - enqueue_time);
  bool const ok_to_drop = OkToDrop(sojourn, now);

  if (dropping_) {
    if (!ok_to_drop) {
      // RFC 8289 §5.5: sojourn fell below TARGET → leave drop state.
      dropping_ = false;
      return false;
    }
    if (now >= drop_next_) {
      // RFC 8289 §5.5: scheduled drop time has arrived. Drop
      // this item, advance count_, schedule the next.
      ++count_;
      drop_next_ = ControlLaw(drop_next_, count_);
      drop_count_.fetch_add(1, std::memory_order_relaxed);
      return true;
    }
    return false;
  }

  if (ok_to_drop) {
    // RFC 8289 §5.5: enter drop state, drop this item, install
    // the first drop schedule.
    dropping_ = true;
    // Recent-episode hysteresis (RFC 8289 §5.5, Linux variant):
    // if a previous drop episode happened within 16 intervals,
    // resume with that episode's growth (`delta`) rather than
    // restarting at count=1.
    uint32_t const delta = count_ - lastcount_;
    count_ = 1;
    if (delta > 1 && (now - drop_next_) < 16 * interval_) {
      count_ = delta;
    }
    drop_next_ = ControlLaw(now, count_);
    lastcount_ = count_;
    drop_count_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }
  return false;
}

bool CodelFilter::OkToDrop(absl::Duration sojourn, absl::Time now) {
  // Deviation from RFC 8289 §5.6: the RFC's first test is
  // `if (sojourn_time_ < TARGET || bytes() <= maxpacket_)`. The
  // `bytes() <= maxpacket_` term protects against draining a
  // near-empty queue too aggressively. Limen does not own the
  // queue (the application stamps `enqueue_time` per item and
  // controls its own queue depth), so there is no `bytes()` to
  // consult and no `maxpacket_` to compare against. Dropping
  // that term has no relevant effect under the API contract
  // (the filter is only consulted on items that are actually
  // queued).
  if (sojourn < target_) {
    // Went below target — stay below for at least INTERVAL.
    first_above_time_ = absl::UnixEpoch();
    return false;
  }
  if (first_above_time_ == absl::UnixEpoch()) {
    // Just went above from below. If still above at
    // first_above_time_, we'll say it's ok to drop.
    first_above_time_ = now + interval_;
    return false;
  }
  if (now >= first_above_time_) {
    return true;
  }
  return false;
}

absl::Time CodelFilter::ControlLaw(absl::Time t, uint32_t count) const {
  // RFC 8289 §5.6: control_law(t, count) = t + INTERVAL / sqrt(count).
  return t + interval_ / std::sqrt(static_cast<double>(count));
}

bool CodelFilter::IsDropping() const {
  absl::MutexLock lock(mu_);
  return dropping_;
}

int64_t CodelFilter::DropCount() const {
  return drop_count_.load(std::memory_order_relaxed);
}

uint32_t CodelFilter::DropEpisodeCount() const {
  absl::MutexLock lock(mu_);
  return count_;
}

absl::Time CodelFilter::DropNext() const {
  absl::MutexLock lock(mu_);
  return drop_next_;
}

void CodelFilter::RegisterObservableInstruments() {
  if (meter_provider_ == nullptr) {
    return;
  }
  auto meter =
      meter_provider_->GetMeter(kMeterName, /*version=*/"", /*schema=*/"");
  drops_instrument_ = meter->CreateInt64ObservableCounter(
      "limen.codel.drops",
      "Cumulative count of items the CoDel filter has dropped.",
      /*unit=*/"1");
  drops_instrument_->AddCallback(&CodelFilter::ObserveDrops, this);

  dropping_instrument_ = meter->CreateInt64ObservableUpDownCounter(
      "limen.codel.dropping",
      "Whether the CoDel filter is currently in drop mode (0 or 1).",
      /*unit=*/"1");
  dropping_instrument_->AddCallback(&CodelFilter::ObserveDropping, this);
}

void CodelFilter::ObserveDrops(opentelemetry::metrics::ObserverResult result,
                               void* state) noexcept {
  auto* self = static_cast<CodelFilter*>(state);
  int64_t const value = self->drop_count_.load(std::memory_order_relaxed);
  EmitObservation(result, value, {{"id", self->id_}});
}

void CodelFilter::ObserveDropping(opentelemetry::metrics::ObserverResult result,
                                  void* state) noexcept {
  auto* self = static_cast<CodelFilter*>(state);
  int64_t value;
  {
    absl::MutexLock lock(self->mu_);
    value = self->dropping_ ? 1 : 0;
  }
  EmitObservation(result, value, {{"id", self->id_}});
}

}  // namespace limen

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

#include "limen/gradient2_limit.h"
#include "limen/abstract_limiter.h"
#include "opentelemetry/common/attribute_value.h"
#include "opentelemetry/common/key_value_iterable_view.h"
#include "opentelemetry/context/context.h"
#include "opentelemetry/metrics/meter.h"
#include "opentelemetry/sdk/metrics/instruments.h"
#include "opentelemetry/sdk/metrics/view/instrument_selector.h"
#include "opentelemetry/sdk/metrics/view/meter_selector.h"
#include "opentelemetry/sdk/metrics/view/view.h"
#include <algorithm>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace limen {
namespace {

namespace sdk_metrics = opentelemetry::sdk::metrics;

// Bucket boundaries for the two RTT histograms, expressed in
// nanoseconds (the unit of the value Limen records). The design
// defaults: 1µs, 10µs, 100µs, 1ms, 10ms, 100ms, 1s, 10s.
std::vector<double> LatencyBucketsNs() {
  return {1000.0,     10000.0,     100000.0,     1000000.0,
          10000000.0, 100000000.0, 1000000000.0, 10000000000.0};
}

// Bucket boundaries for the queue-size histogram. Dimensionless
// integer counts; the typical value is the configured `queue_size`
// constant (default 4). The boundaries below cover both the static
// constant case and the dynamic queue-size functions Netflix's
// upstream supports.
std::vector<double> QueueSizeBuckets() {
  return {1.0, 4.0, 10.0, 50.0, 100.0, 500.0, 1000.0};
}

// Registers an explicit histogram View on the supplied MeterProvider
// for `instrument_name` with the given bucket boundaries.
void RegisterHistogramView(sdk_metrics::MeterProvider& provider,
                           std::string const& instrument_name,
                           std::string const& description,
                           std::string const& unit,
                           std::vector<double> boundaries) {
  auto instrument_selector = std::make_unique<sdk_metrics::InstrumentSelector>(
      sdk_metrics::InstrumentType::kHistogram, instrument_name, unit);
  auto meter_selector = std::make_unique<sdk_metrics::MeterSelector>(
      AbstractLimiter::kMeterName, /*version=*/"", /*schema=*/"");

  auto aggregation_config =
      std::make_shared<sdk_metrics::HistogramAggregationConfig>();
  aggregation_config->boundaries_ = std::move(boundaries);

  // OTel C++ 1.24's `View` constructor does not accept a unit; the
  // unit is carried by the instrument and by the InstrumentSelector
  // that matches the View to its instrument.
  auto view = std::make_unique<sdk_metrics::View>(
      instrument_name, description, sdk_metrics::AggregationType::kHistogram,
      std::move(aggregation_config));

  provider.AddView(std::move(instrument_selector), std::move(meter_selector),
                   std::move(view));
}

}  // namespace

std::unique_ptr<Gradient2Limit> Gradient2Limit::Builder::Build() {
  // Copy out of the Builder rather than moving, so a second `Build()`
  // call on the same Builder yields an identically-configured second
  // limiter rather than one with an empty id and a null MeterProvider.
  Params params{
      initial_limit_,  min_limit_, max_concurrency_, queue_size_,
      rtt_tolerance_,  smoothing_, long_window_,     id_,
      meter_provider_,
  };
  return std::unique_ptr<Gradient2Limit>(new Gradient2Limit(std::move(params)));
}

Gradient2Limit::Gradient2Limit(Params params)
    : AbstractLimit(params.initial_limit),
      estimated_limit_(params.initial_limit),
      // Upstream Gradient2Limit instantiates ExpAvgMeasurement with a
      // hard-coded warmup window of 10 samples; Limen matches that so
      // the algorithm's behaviour is identical for the first ten
      // sample windows.
      long_rtt_(params.long_window, /*warmup_window=*/10),
      min_limit_(params.min_limit),
      max_limit_(params.max_concurrency),
      queue_size_constant_(params.queue_size),
      smoothing_(params.smoothing),
      tolerance_(params.rtt_tolerance),
      id_(std::move(params.id)) {
  if (params.meter_provider == nullptr) {
    return;
  }

  // Register one explicit View per histogram before creating the
  // instruments. Doing it in this order means the SDK applies the
  // Limen-provided defaults; if the application registers its own
  // views on the same MeterProvider later, those take precedence.
  RegisterHistogramView(*params.meter_provider, "limen.min_rtt",
                        "Long-term baseline RTT, per window.", "ns",
                        LatencyBucketsNs());
  RegisterHistogramView(*params.meter_provider, "limen.min_window_rtt",
                        "Short-window RTT measurement, per window.", "ns",
                        LatencyBucketsNs());
  RegisterHistogramView(
      *params.meter_provider, "limen.queue_size",
      "Algorithm queue-size constant (additive-increase amount), per window.",
      "1", QueueSizeBuckets());

  auto meter = params.meter_provider->GetMeter(AbstractLimiter::kMeterName,
                                               /*version=*/"", /*schema=*/"");
  long_rtt_hist_ = meter->CreateDoubleHistogram(
      "limen.min_rtt", "Long-term baseline RTT, per window.", "ns");
  short_rtt_hist_ = meter->CreateDoubleHistogram(
      "limen.min_window_rtt", "Short-window RTT measurement, per window.",
      "ns");
  queue_size_hist_ = meter->CreateDoubleHistogram(
      "limen.queue_size",
      "Algorithm queue-size constant (additive-increase amount), per window.",
      "1");
}

int Gradient2Limit::Update(int64_t /*start_time_ns*/, int64_t rtt_ns,
                           int inflight, bool /*did_drop*/) {
  // Defensive zero-RTT short-circuit. Upstream divides by `shortRtt`
  // in the gradient calculation; a zero RTT would produce a division
  // by zero and an unbounded gradient that the clamp to 1.0 then
  // swallows, but the queue-size addition still raises the cap. Limen
  // returns the current cap unchanged so a sample with no measured
  // latency cannot move the cap in either direction.
  if (rtt_ns <= 0) {
    return static_cast<int>(estimated_limit_);
  }

  double const short_rtt = static_cast<double>(rtt_ns);
  double const long_rtt = long_rtt_.Add(short_rtt);
  double const queue_size = static_cast<double>(queue_size_constant_);

  RecordHistograms(short_rtt, long_rtt, queue_size);

  // Drift correction: when the smoothed long-term RTT is more than
  // twice the short-window RTT, compress the long-term value by 5
  // percent. This is the "recovering from a load spike" branch.
  if (long_rtt / short_rtt > 2.0) {
    long_rtt_.Update([](double v) { return v * 0.95; });
  }

  // App-limited skip: a server using less than half of its current
  // cap tells us nothing useful about its real capacity. Leave the
  // cap alone.
  if (static_cast<double>(inflight) < estimated_limit_ / 2.0) {
    return static_cast<int>(estimated_limit_);
  }

  // Gradient, clamped to [0.5, 1.0]. The lower bound prevents
  // aggressive load shedding on a single outlier; the upper bound
  // covers the case where the smoothed long RTT lags above the
  // short-window value (no queuing implied).
  double const gradient =
      std::max(0.5, std::min(1.0, tolerance_ * long_rtt / short_rtt));

  double new_limit = estimated_limit_ * gradient + queue_size;
  new_limit = estimated_limit_ * (1.0 - smoothing_) + new_limit * smoothing_;
  new_limit = std::max(static_cast<double>(min_limit_),
                       std::min(static_cast<double>(max_limit_), new_limit));

  estimated_limit_ = new_limit;
  return static_cast<int>(new_limit);
}

void Gradient2Limit::RecordHistograms(double short_rtt_ns, double long_rtt_ns,
                                      double queue_size) {
  if (long_rtt_hist_ == nullptr) {
    return;
  }
  // `AttributeValue` is an OTel variant that holds `string_view`,
  // `const char*`, and primitive types — but not `std::string`. We
  // hand it a view into `id_`, which outlives the call.
  std::map<std::string, opentelemetry::common::AttributeValue> attrs;
  attrs["id"] = opentelemetry::nostd::string_view(id_);
  opentelemetry::common::KeyValueIterableView attr_view(attrs);
  opentelemetry::context::Context ctx;
  long_rtt_hist_->Record(long_rtt_ns, attr_view, ctx);
  short_rtt_hist_->Record(short_rtt_ns, attr_view, ctx);
  queue_size_hist_->Record(queue_size, attr_view, ctx);
}

}  // namespace limen

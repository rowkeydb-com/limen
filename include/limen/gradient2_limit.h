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

#ifndef LIMEN_GRADIENT2_LIMIT_H_
#define LIMEN_GRADIENT2_LIMIT_H_

#include "limen/abstract_limit.h"
#include "limen/exp_avg_measurement.h"
#include "opentelemetry/metrics/sync_instruments.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace limen {

// Gradient2 algorithm. Every window, computes the gradient between
// the short-window RTT and a long-term exponentially smoothed RTT,
// scales the current cap by that gradient, adds a small queue-size
// constant, and smooths the result against the previous cap. Three
// refinements make this robust in practice:
//
// - A drift-correction step that compresses the long-term RTT by 5%
//   per window when the long-term value is more than twice the
//   short-window RTT (recovery from a load spike).
// - An app-limited skip: if in-flight is below half the current cap,
//   the cap is left alone because the measured latency does not
//   reflect real load.
// - A defensive zero-RTT short-circuit: upstream divides by the
//   short RTT in the gradient calculation. Limen returns the current
//   cap unchanged when the sample is zero, avoiding a division by
//   zero and the resulting unbounded gradient. This is a Limen-side
//   correctness fix, not an upstream port.
//
// The additive-increase amount the algorithm adds to the cap each
// window is supplied by a callable, `QueueSizeFn`, of the current
// cap. Upstream Java's `Gradient2Limit` carries the same shape
// (`Function<Integer, Integer> queueSize`); the upstream default is
// the constant `4`. Limen's default is `ceil(sqrt(current_limit))`
// (floored at 1), which scales the probe rate sublinearly with cap
// — at cap=100 the probe is ~10 per window; at cap=10000 it is
// ~100. Constant-burst behaviour remains available through the
// `QueueSize(int)` builder overload, which the constant-form
// upstream default uses.
//
// Construction goes through the inner `Builder` class. The builder
// also accepts an OpenTelemetry SDK MeterProvider; if one is
// supplied, the limiter registers three histograms with sane default
// bucket boundaries (latency buckets for the two RTT histograms, a
// count-based bucket set for the queue-size histogram). Applications
// can layer their own views on top of these defaults.
//
// Thread-safety: `OnSample` takes the base class's mutex (and so
// does the implicit `Update` call below it). `GetLimit` is wait-free.
// All mutable state lives inside `Update`'s exclusive critical
// section.
//
// Ported from Netflix's `Gradient2Limit.java`.
class Gradient2Limit final : public AbstractLimit {
 public:
  // Callable that computes the additive-increase amount from the
  // current cap. Invoked once per window-update call.
  using QueueSizeFn = std::function<int(int /*current_limit*/)>;

  class Builder {
   public:
    Builder() = default;

    // Starting cap before any samples arrive. Default 20.
    Builder& InitialLimit(int v) {
      initial_limit_ = v;
      return *this;
    }

    // Floor on the cap. The algorithm never drives the cap below
    // this. Default 20.
    Builder& MinLimit(int v) {
      min_limit_ = v;
      return *this;
    }

    // Ceiling on the cap. The algorithm never drives the cap above
    // this. Default 200.
    Builder& MaxConcurrency(int v) {
      max_concurrency_ = v;
      return *this;
    }

    // Constant additive-increase amount per window. Matches Netflix
    // upstream's `Gradient2Limit.Builder.queueSize(int)` shortcut
    // and the upstream default of `4`. Stored as the constant-
    // valued function `[v](int) { return v; }`.
    Builder& QueueSize(int v) {
      queue_size_fn_ = [v](int /*cap*/) { return v; };
      return *this;
    }

    // Functional additive-increase amount per window: a callable
    // from the current cap to the burst term used in
    // `new_limit = current_limit * gradient + QueueSize(current_limit)`.
    // The default if neither overload is called is
    // `ceil(sqrt(current_limit))`, floored at 1 — a sublinear probe
    // rate that scales naturally with server capacity (~10% at
    // cap=100, ~1% at cap=10000).
    //
    // Concurrent `OnSample` calls against one limiter serialise
    // through the limiter's internal mutex, so the callable is
    // never invoked concurrently against the same limiter. A
    // stateful callable that touches shared state outside the
    // limiter remains the caller's responsibility.
    //
    // A default-constructed or `nullptr`-wrapping `QueueSizeFn`
    // would normally throw on first invocation; with
    // `-fno-exceptions` that becomes `std::terminate`. A library
    // does not crash the host process on user-supplied input, so
    // an empty function is replaced silently with the default
    // (`DefaultSqrtQueueSize`).
    Builder& QueueSize(QueueSizeFn fn) {
      queue_size_fn_ = fn ? std::move(fn) : QueueSizeFn(DefaultSqrtQueueSize);
      return *this;
    }

    // How much higher the short RTT must climb above the long-term
    // average before the algorithm shrinks the cap. Must be >= 1.0.
    // Default 1.5 (a 50 percent latency increase is forgiven).
    Builder& RttTolerance(double v) {
      rtt_tolerance_ = v;
      return *this;
    }

    // Weight given to the freshly-computed cap when blending against
    // the previous cap. Must be in [0.0, 1.0]. Default 0.2.
    Builder& Smoothing(double v) {
      smoothing_ = v;
      return *this;
    }

    // Sample count over which the long-term RTT is smoothed. Default
    // 600 (a ten-minute window at one sample per second).
    Builder& LongWindow(int v) {
      long_window_ = v;
      return *this;
    }

    // Label value emitted on every metric the limiter records. This
    // is the name an operator uses on a dashboard to distinguish one
    // limiter from another in a process with many.
    Builder& Id(std::string id) {
      id_ = std::move(id);
      return *this;
    }

    // OpenTelemetry SDK MeterProvider. When supplied, the limiter
    // registers views for its three histograms with sane default
    // bucket boundaries before creating the histogram instruments.
    // Applications that want to override the boundaries should
    // register their own views on the same MeterProvider after this
    // builder runs.
    //
    // The SDK type (not the API type) is required because view
    // registration is an SDK concern; the API layer exposes only
    // `GetMeter`.
    Builder& MeterProvider(
        std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> p) {
      meter_provider_ = std::move(p);
      return *this;
    }

    // Constructs the limiter. The returned pointer owns its
    // histogram handles and the long-term measurement; destroying
    // the limiter releases both.
    std::unique_ptr<Gradient2Limit> Build();

   private:
    int initial_limit_ = 20;
    int min_limit_ = 20;
    int max_concurrency_ = 200;
    // Default sublinear probe: at cap=100 ⇒ 10 (10% probe), at
    // cap=10000 ⇒ 100 (1% probe). Floored at 1 so a cap of 0..1
    // never produces a zero-valued additive-increase term.
    QueueSizeFn queue_size_fn_ = DefaultSqrtQueueSize;
    double rtt_tolerance_ = 1.5;
    double smoothing_ = 0.2;
    int long_window_ = 600;
    std::string id_;
    std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> meter_provider_;
  };

  // The Builder's default `QueueSizeFn`. Exposed publicly so tests
  // and consumers can refer to the exact same function the Builder
  // installs. Returns `max(1, ceil(sqrt(current_limit)))`.
  static int DefaultSqrtQueueSize(int current_limit);

 protected:
  int Update(int64_t start_time_ns, int64_t rtt_ns, int inflight,
             bool did_drop) override;

 private:
  friend class Builder;

  struct Params {
    int initial_limit;
    int min_limit;
    int max_concurrency;
    QueueSizeFn queue_size_fn;
    double rtt_tolerance;
    double smoothing;
    int long_window;
    std::string id;
    std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> meter_provider;
  };

  explicit Gradient2Limit(Params params);

  // Records the per-window samples into the three histograms.
  // No-op when no MeterProvider was supplied at build time.
  void RecordHistograms(double short_rtt_ns, double long_rtt_ns,
                        double queue_size);

  // Mutable per-window state. Accessed exclusively from `Update`,
  // which the base class invokes under the per-limiter mutex.
  double estimated_limit_;
  ExpAvgMeasurement long_rtt_;

  // Configuration. Set once at construction; never mutated.
  int const min_limit_;
  int const max_limit_;
  QueueSizeFn const queue_size_fn_;
  double const smoothing_;
  double const tolerance_;
  std::string const id_;

  // OpenTelemetry handles. Null when no MeterProvider was supplied.
  std::shared_ptr<opentelemetry::metrics::Histogram<double>> long_rtt_hist_;
  std::shared_ptr<opentelemetry::metrics::Histogram<double>> short_rtt_hist_;
  std::shared_ptr<opentelemetry::metrics::Histogram<double>> queue_size_hist_;
};

}  // namespace limen

#endif  // LIMEN_GRADIENT2_LIMIT_H_

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
#include "gtest/gtest.h"
#include "opentelemetry/sdk/metrics/data/metric_data.h"
#include "opentelemetry/sdk/metrics/data/point_data.h"
#include "opentelemetry/sdk/metrics/export/metric_producer.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include "opentelemetry/sdk/metrics/metric_reader.h"
#include "opentelemetry/sdk/metrics/push_metric_exporter.h"
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace limen {
namespace {

namespace sdk_metrics = opentelemetry::sdk::metrics;

// A push-style metric exporter that captures every batch of metrics
// pushed into it. The Gradient2Limit tests force collection through
// `MetricReader::Collect` and then read back the captured data.
class CapturingExporter final : public sdk_metrics::PushMetricExporter {
 public:
  sdk_metrics::AggregationTemporality GetAggregationTemporality(
      sdk_metrics::InstrumentType /*instrument_type*/) const noexcept override {
    return sdk_metrics::AggregationTemporality::kCumulative;
  }

  opentelemetry::sdk::common::ExportResult Export(
      sdk_metrics::ResourceMetrics const& data) noexcept override {
    captured_.push_back(data);
    return opentelemetry::sdk::common::ExportResult::kSuccess;
  }

  bool ForceFlush(std::chrono::microseconds /*timeout*/ =
                      std::chrono::microseconds::max()) noexcept override {
    return true;
  }

  bool Shutdown(std::chrono::microseconds /*timeout*/ =
                    std::chrono::microseconds::max()) noexcept override {
    return true;
  }

  std::vector<sdk_metrics::ResourceMetrics> const& captured() const {
    return captured_;
  }

 private:
  std::vector<sdk_metrics::ResourceMetrics> captured_;
};

// A reader that triggers collection on demand. Wraps the
// capturing exporter so tests can drive samples, force a collect,
// and inspect what came out.
class ManualReader final : public sdk_metrics::MetricReader {
 public:
  explicit ManualReader(std::shared_ptr<CapturingExporter> exporter)
      : exporter_(std::move(exporter)) {}

  sdk_metrics::AggregationTemporality GetAggregationTemporality(
      sdk_metrics::InstrumentType instrument_type) const noexcept override {
    return exporter_->GetAggregationTemporality(instrument_type);
  }

  // Triggers a collect and forwards the result to the capturing
  // exporter. Returns the number of metric streams seen.
  int Collect() {
    int streams = 0;
    MetricReader::Collect([&](sdk_metrics::ResourceMetrics const& data) {
      ++streams;
      (void)exporter_->Export(data);
      return true;
    });
    return streams;
  }

 protected:
  bool OnForceFlush(std::chrono::microseconds /*timeout*/) noexcept override {
    return true;
  }

  bool OnShutDown(std::chrono::microseconds /*timeout*/) noexcept override {
    return true;
  }

  void OnInitialized() noexcept override {}

 private:
  std::shared_ptr<CapturingExporter> exporter_;
};

// Looks up a named metric in the most recently captured batch and
// returns a pointer to its HistogramPointData. Returns nullptr if
// the metric is missing or has no points.
sdk_metrics::HistogramPointData const* FindHistogramPoint(
    CapturingExporter const& exporter, std::string const& metric_name) {
  for (auto const& batch : exporter.captured()) {
    for (auto const& scope : batch.scope_metric_data_) {
      for (auto const& metric : scope.metric_data_) {
        if (metric.instrument_descriptor.name_ != metric_name) {
          continue;
        }
        for (auto const& point : metric.point_data_attr_) {
          auto const* hist =
              opentelemetry::nostd::get_if<sdk_metrics::HistogramPointData>(
                  &point.point_data);
          if (hist != nullptr) {
            return hist;
          }
        }
      }
    }
  }
  return nullptr;
}

TEST(Gradient2LimitTest, InitialState) {
  auto limit = Gradient2Limit::Builder().InitialLimit(50).Build();
  EXPECT_EQ(limit->GetLimit(), 50);
}

TEST(Gradient2LimitTest, SteadyStateRampsByQueueSize) {
  auto limit = Gradient2Limit::Builder()
                   .InitialLimit(20)
                   .MaxConcurrency(200)
                   .QueueSize(4)
                   .Build();

  int const start_cap = limit->GetLimit();
  // Drive identical RTT samples with inflight at the cap so the
  // app-limited skip does not fire. Each window the long-term
  // EWMA tracks the short-window value (with warmup = 10 samples
  // of running mean), so the gradient stays clamped to 1.0 and
  // the cap grows by the smoothed queue-size addend each window.
  int last = start_cap;
  bool ever_grew = false;
  bool ever_hit_max = false;
  for (int i = 0; i < 1000; ++i) {
    limit->OnSample(/*start_time_ns=*/0, /*rtt_ns=*/1'000'000,
                    /*inflight=*/limit->GetLimit(),
                    /*did_drop=*/false);
    int const cap = limit->GetLimit();
    EXPECT_GE(cap, last);
    if (cap > last) ever_grew = true;
    if (cap == 200) ever_hit_max = true;
    last = cap;
  }
  EXPECT_TRUE(ever_grew);
  EXPECT_TRUE(ever_hit_max);
}

TEST(Gradient2LimitTest, LatencySpikeShrinks) {
  auto limit = Gradient2Limit::Builder()
                   .InitialLimit(50)
                   .MinLimit(1)
                   .MaxConcurrency(200)
                   .Build();

  // Establish a steady-state baseline at 1ms RTT.
  for (int i = 0; i < 50; ++i) {
    limit->OnSample(0, 1'000'000, limit->GetLimit(), false);
  }
  int const pre_spike = limit->GetLimit();

  // Feed a sample where the short RTT is ten times the long-term
  // baseline. The gradient clamps to 0.5, the smoothed new cap is
  // strictly below the previous cap.
  limit->OnSample(0, 10'000'000, limit->GetLimit(), false);
  EXPECT_LT(limit->GetLimit(), pre_spike);
}

TEST(Gradient2LimitTest, RecoveryShrinksLongRtt) {
  auto limit = Gradient2Limit::Builder()
                   .InitialLimit(50)
                   .MinLimit(1)
                   .MaxConcurrency(200)
                   .LongWindow(50)
                   .Build();

  // Pull the long-term RTT up with a sustained spike.
  for (int i = 0; i < 100; ++i) {
    limit->OnSample(0, 10'000'000, limit->GetLimit(), false);
  }
  int const post_spike = limit->GetLimit();

  // Return RTT to baseline. The drift-correction branch fires
  // each window where `long_rtt > 2 × short_rtt`, multiplying the
  // long-term value by 0.95 until the gradient stabilises. With
  // a 50-sample long window the recovery is observably faster
  // than the natural EWMA would deliver alone; the cap should
  // climb back above the post-spike floor inside a few dozen
  // recovery windows.
  bool cap_recovered = false;
  for (int i = 0; i < 200; ++i) {
    limit->OnSample(0, 1'000'000, limit->GetLimit(), false);
    if (limit->GetLimit() > post_spike) {
      cap_recovered = true;
      break;
    }
  }
  EXPECT_TRUE(cap_recovered);
}

TEST(Gradient2LimitTest, AppLimitedSkipPreservesLimit) {
  auto limit = Gradient2Limit::Builder()
                   .InitialLimit(50)
                   .MinLimit(1)
                   .MaxConcurrency(200)
                   .Build();

  // Sample with inflight below half the current cap. Even though
  // the short RTT is well above the long-term value (which would
  // normally drag the cap down), the algorithm leaves the cap
  // alone because the latency signal is not meaningful at this
  // utilisation.
  int const before = limit->GetLimit();
  limit->OnSample(0, 100'000'000, /*inflight=*/before / 4, false);
  EXPECT_EQ(limit->GetLimit(), before);
}

TEST(Gradient2LimitTest, MinLimitFloor) {
  auto limit = Gradient2Limit::Builder()
                   .InitialLimit(50)
                   .MinLimit(10)
                   .MaxConcurrency(200)
                   .LongWindow(20)
                   .Build();

  // Drive sustained latency at twenty times the warmup baseline so
  // the gradient sits at the 0.5 floor for an extended period. The
  // cap should drop but never below `MinLimit`.
  for (int i = 0; i < 20; ++i) {
    limit->OnSample(0, 1'000'000, limit->GetLimit(), false);
  }
  for (int i = 0; i < 500; ++i) {
    limit->OnSample(0, 20'000'000, limit->GetLimit(), false);
    EXPECT_GE(limit->GetLimit(), 10);
  }
}

TEST(Gradient2LimitTest, MaxConcurrencyCeiling) {
  auto limit = Gradient2Limit::Builder()
                   .InitialLimit(20)
                   .MaxConcurrency(100)
                   .QueueSize(50)
                   .Build();

  // A large queue-size constant drives the cap up rapidly. The
  // ceiling must hold even though each window's raw computation
  // would otherwise exceed it.
  for (int i = 0; i < 200; ++i) {
    limit->OnSample(0, 1'000'000, limit->GetLimit(), false);
    EXPECT_LE(limit->GetLimit(), 100);
  }
}

TEST(Gradient2LimitTest, SmoothingDampsTheShift) {
  // Pick smoothing values that produce easily distinguishable
  // damping. With smoothing 1.0 the new cap is taken verbatim;
  // with smoothing 0.1 only a tenth of the shift is applied.
  //
  // Pin both caps at 100 during warmup by setting MaxConcurrency
  // equal to InitialLimit, so the cap can't drift up before the
  // spike and the post-spike comparison sees the same starting
  // value for both limiters.
  auto fast = Gradient2Limit::Builder()
                  .InitialLimit(100)
                  .MinLimit(1)
                  .MaxConcurrency(100)
                  .Smoothing(1.0)
                  .Build();
  auto slow = Gradient2Limit::Builder()
                  .InitialLimit(100)
                  .MinLimit(1)
                  .MaxConcurrency(100)
                  .Smoothing(0.1)
                  .Build();

  // Establish identical baselines. Both caps stay at 100.
  for (int i = 0; i < 20; ++i) {
    fast->OnSample(0, 1'000'000, fast->GetLimit(), false);
    slow->OnSample(0, 1'000'000, slow->GetLimit(), false);
  }
  ASSERT_EQ(fast->GetLimit(), 100);
  ASSERT_EQ(slow->GetLimit(), 100);

  // Feed an identical latency spike to both. The faster-smoothed
  // limiter should drop further than the slow-smoothed one for
  // the same input.
  fast->OnSample(0, 10'000'000, fast->GetLimit(), false);
  slow->OnSample(0, 10'000'000, slow->GetLimit(), false);

  EXPECT_LT(fast->GetLimit(), slow->GetLimit());
}

TEST(Gradient2LimitTest, ToleranceForgivesSmallIncreases) {
  auto limit = Gradient2Limit::Builder()
                   .InitialLimit(50)
                   .MinLimit(1)
                   .MaxConcurrency(200)
                   .RttTolerance(1.5)
                   .Build();

  // Warm up the long-term baseline.
  for (int i = 0; i < 30; ++i) {
    limit->OnSample(0, 1'000'000, limit->GetLimit(), false);
  }
  int const pre = limit->GetLimit();

  // Short RTT 1.4× the long-term baseline. With `tolerance = 1.5`
  // the product `tolerance * long_rtt / short_rtt` is approximately
  // 1.5 / 1.4 ≈ 1.07, clamped to 1.0. The cap should not shrink.
  limit->OnSample(0, 1'400'000, limit->GetLimit(), false);
  EXPECT_GE(limit->GetLimit(), pre);
}

TEST(Gradient2LimitTest, HandlesZeroRttGracefully) {
  auto limit = Gradient2Limit::Builder()
                   .InitialLimit(50)
                   .MinLimit(1)
                   .MaxConcurrency(200)
                   .Build();

  int const before = limit->GetLimit();
  limit->OnSample(0, 0, limit->GetLimit(), false);
  EXPECT_EQ(limit->GetLimit(), before);

  // Following a real sample, the cap can move again. The
  // zero-RTT short-circuit must not poison subsequent updates.
  for (int i = 0; i < 30; ++i) {
    limit->OnSample(0, 1'000'000, limit->GetLimit(), false);
  }
  EXPECT_GT(limit->GetLimit(), before);
}

TEST(Gradient2LimitTest, ChangeCallbackFiresOnNewLimit) {
  auto limit = Gradient2Limit::Builder()
                   .InitialLimit(20)
                   .MaxConcurrency(200)
                   .QueueSize(4)
                   .Build();

  std::vector<int> seen;
  limit->NotifyOnChange([&](int new_limit) { seen.push_back(new_limit); });

  int last = limit->GetLimit();
  for (int i = 0; i < 200; ++i) {
    limit->OnSample(0, 1'000'000, limit->GetLimit(), false);
    int const now = limit->GetLimit();
    if (now != last) {
      ASSERT_FALSE(seen.empty());
      EXPECT_EQ(seen.back(), now);
      last = now;
    }
  }
  EXPECT_FALSE(seen.empty());
}

TEST(Gradient2LimitTest, HistogramBucketsMatchDesign) {
  auto exporter = std::make_shared<CapturingExporter>();
  auto reader = std::make_unique<ManualReader>(exporter);
  auto* reader_ptr = reader.get();

  auto provider = std::make_shared<sdk_metrics::MeterProvider>();
  provider->AddMetricReader(std::move(reader));

  auto limit = Gradient2Limit::Builder()
                   .InitialLimit(50)
                   .MaxConcurrency(200)
                   .Id("test-limiter")
                   .MeterProvider(provider)
                   .Build();

  // Force the algorithm into the gradient-calculation branch so
  // all three histograms record (inflight >= cap/2, rtt > 0).
  limit->OnSample(0, 1'000'000, limit->GetLimit(), false);
  reader_ptr->Collect();

  auto const* long_rtt = FindHistogramPoint(*exporter, "limen.min_rtt");
  ASSERT_NE(long_rtt, nullptr);
  std::vector<double> const latency_buckets = {
      1000.0,     10000.0,     100000.0,     1000000.0,
      10000000.0, 100000000.0, 1000000000.0, 10000000000.0};
  EXPECT_EQ(long_rtt->boundaries_, latency_buckets);

  auto const* short_rtt = FindHistogramPoint(*exporter, "limen.min_window_rtt");
  ASSERT_NE(short_rtt, nullptr);
  EXPECT_EQ(short_rtt->boundaries_, latency_buckets);

  auto const* queue = FindHistogramPoint(*exporter, "limen.queue_size");
  ASSERT_NE(queue, nullptr);
  std::vector<double> const count_buckets = {1.0,   4.0,   10.0,  50.0,
                                             100.0, 500.0, 1000.0};
  EXPECT_EQ(queue->boundaries_, count_buckets);
}

TEST(Gradient2LimitTest, HistogramsRecordOncePerUpdateCall) {
  auto exporter = std::make_shared<CapturingExporter>();
  auto reader = std::make_unique<ManualReader>(exporter);
  auto* reader_ptr = reader.get();

  auto provider = std::make_shared<sdk_metrics::MeterProvider>();
  provider->AddMetricReader(std::move(reader));

  auto limit = Gradient2Limit::Builder()
                   .InitialLimit(50)
                   .MaxConcurrency(200)
                   .Id("test-limiter")
                   .MeterProvider(provider)
                   .Build();

  for (int i = 0; i < 3; ++i) {
    limit->OnSample(0, 1'000'000, limit->GetLimit(), false);
  }
  reader_ptr->Collect();

  // Each histogram's cumulative point count equals the number of
  // `Record` calls — three for three OnSample invocations.
  auto const* long_rtt = FindHistogramPoint(*exporter, "limen.min_rtt");
  ASSERT_NE(long_rtt, nullptr);
  EXPECT_EQ(long_rtt->count_, 3u);

  auto const* short_rtt = FindHistogramPoint(*exporter, "limen.min_window_rtt");
  ASSERT_NE(short_rtt, nullptr);
  EXPECT_EQ(short_rtt->count_, 3u);

  auto const* queue = FindHistogramPoint(*exporter, "limen.queue_size");
  ASSERT_NE(queue, nullptr);
  EXPECT_EQ(queue->count_, 3u);
}

TEST(Gradient2LimitTest, ZeroRttSampleDoesNotRecord) {
  // The zero-RTT short-circuit at the top of `Update` returns the
  // current cap unchanged without computing a gradient or recording
  // to any histogram. Pin that contract: feed a single zero-RTT
  // sample after construction and assert no histogram receives any
  // record.
  auto exporter = std::make_shared<CapturingExporter>();
  auto reader = std::make_unique<ManualReader>(exporter);
  auto* reader_ptr = reader.get();

  auto provider = std::make_shared<sdk_metrics::MeterProvider>();
  provider->AddMetricReader(std::move(reader));

  auto limit = Gradient2Limit::Builder()
                   .InitialLimit(50)
                   .MaxConcurrency(200)
                   .Id("test-limiter")
                   .MeterProvider(provider)
                   .Build();

  limit->OnSample(0, /*rtt_ns=*/0, limit->GetLimit(), false);
  reader_ptr->Collect();

  // No data points should have flowed for any of the three
  // histograms.
  EXPECT_EQ(FindHistogramPoint(*exporter, "limen.min_rtt"), nullptr);
  EXPECT_EQ(FindHistogramPoint(*exporter, "limen.min_window_rtt"), nullptr);
  EXPECT_EQ(FindHistogramPoint(*exporter, "limen.queue_size"), nullptr);
}

}  // namespace
}  // namespace limen

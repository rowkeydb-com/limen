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

// Observability end-to-end tests. Each test stands up an OTel
// MeterProvider with a manual-collect reader, builds one or more
// Limen components against it, drives known traffic, triggers a
// collect, and inspects the captured metric streams.
//
// Per-class observability tests (in the abstract_limiter,
// abstract_partitioned_limiter, gradient2_limit, and codel_filter
// test files) already verify per-component contracts. The tests
// here are deliberately cross-cutting: they verify properties
// that span the whole emitted catalog — `limen.` prefix
// everywhere, `id` label everywhere, every catalog member
// present, no cross-id contamination, hot-path-no-exporter-
// activity, all five outcome statuses emit, partition attribute
// flows, and cumulative counters accumulate across collects.

#include "limen/abstract_limiter.h"
#include "limen/abstract_partitioned_limiter.h"
#include "limen/codel_filter.h"
#include "limen/fixed_limit.h"
#include "limen/gradient2_limit.h"
#include "limen/limiter.h"
#include "limen/simple_limiter.h"
#include "limen/windowed_limit.h"
#include "absl/status/status.h"
#include "absl/time/time.h"
#include "gtest/gtest.h"
#include "opentelemetry/sdk/metrics/data/metric_data.h"
#include "opentelemetry/sdk/metrics/data/point_data.h"
#include "opentelemetry/sdk/metrics/export/metric_producer.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include "opentelemetry/sdk/metrics/metric_reader.h"
#include "opentelemetry/sdk/metrics/push_metric_exporter.h"
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace limen {
namespace {

namespace sdk_metrics = opentelemetry::sdk::metrics;

// Same push-style harness used by other observability tests in
// abstract_partitioned_limiter_test.cc and codel_filter_test.cc.
// Tests call `ManualReader::Collect()` explicitly and inspect the
// captured ResourceMetrics.
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

class ManualReader final : public sdk_metrics::MetricReader {
 public:
  explicit ManualReader(std::shared_ptr<CapturingExporter> exporter)
      : exporter_(std::move(exporter)) {}
  sdk_metrics::AggregationTemporality GetAggregationTemporality(
      sdk_metrics::InstrumentType instrument_type) const noexcept override {
    return exporter_->GetAggregationTemporality(instrument_type);
  }
  void Collect() {
    MetricReader::Collect([&](sdk_metrics::ResourceMetrics const& data) {
      (void)exporter_->Export(data);
      return true;
    });
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

// One captured data point. Holds enough information to assert on
// name, attributes, and (for the point types Limen emits) the
// numeric value.
struct CapturedPoint {
  std::string metric_name;
  std::map<std::string, std::string> attributes;
  // For SumPointData and LastValuePointData. For histograms,
  // -1; histogram bucket boundary checks go through a dedicated
  // accessor in the existing per-class tests.
  int64_t value = -1;
  bool is_histogram = false;
};

// Flatten the exporter's captured ResourceMetrics into a list of
// CapturedPoint records. Walks every batch, every scope, every
// metric, every point.
std::vector<CapturedPoint> CollectAllPoints(CapturingExporter const& exporter) {
  std::vector<CapturedPoint> points;
  for (auto const& batch : exporter.captured()) {
    for (auto const& scope : batch.scope_metric_data_) {
      for (auto const& metric : scope.metric_data_) {
        for (auto const& point : metric.point_data_attr_) {
          CapturedPoint cp;
          cp.metric_name = metric.instrument_descriptor.name_;
          for (auto const& [k, v] : point.attributes) {
            if (auto const* s = opentelemetry::nostd::get_if<std::string>(&v)) {
              cp.attributes[k] = *s;
            }
          }
          if (auto const* sum =
                  opentelemetry::nostd::get_if<sdk_metrics::SumPointData>(
                      &point.point_data)) {
            cp.value = opentelemetry::nostd::get<int64_t>(sum->value_);
          } else if (auto const* lv = opentelemetry::nostd::get_if<
                         sdk_metrics::LastValuePointData>(&point.point_data)) {
            cp.value = opentelemetry::nostd::get<int64_t>(lv->value_);
          } else if (opentelemetry::nostd::get_if<
                         sdk_metrics::HistogramPointData>(&point.point_data) !=
                     nullptr) {
            cp.is_histogram = true;
          }
          points.push_back(std::move(cp));
        }
      }
    }
  }
  return points;
}

// Returns the set of distinct metric names seen in the captured
// stream.
std::set<std::string> DistinctMetricNames(CapturingExporter const& exporter) {
  std::set<std::string> names;
  for (auto const& batch : exporter.captured()) {
    for (auto const& scope : batch.scope_metric_data_) {
      for (auto const& metric : scope.metric_data_) {
        names.insert(metric.instrument_descriptor.name_);
      }
    }
  }
  return names;
}

// Returns the `(name, unit)` map for every metric in the captured
// stream. Used by AllMetricInstrumentsHaveExpectedUnit.
std::map<std::string, std::string> MetricUnits(
    CapturingExporter const& exporter) {
  std::map<std::string, std::string> units;
  for (auto const& batch : exporter.captured()) {
    for (auto const& scope : batch.scope_metric_data_) {
      for (auto const& metric : scope.metric_data_) {
        units[metric.instrument_descriptor.name_] =
            metric.instrument_descriptor.unit_;
      }
    }
  }
  return units;
}

// Builds a fully-configured Limen stack against one shared
// MeterProvider. Four components, four metric sources:
//
//   - `simple` — `SimpleLimiter` over `FixedLimit`. Sources
//     `limen.limit`, `limen.inflight`, `limen.call`.
//   - `gradient` — standalone `Gradient2Limit`. Sources
//     `limen.min_rtt`, `limen.min_window_rtt`,
//     `limen.queue_size`. Driven via direct `OnSample` calls with
//     synthetic non-zero `rtt_ns`, so the histograms record
//     deterministically (unlike a real admit/release pipeline,
//     which would be at the mercy of host clock resolution and
//     could leave `rtt_ns == 0` and skip the histogram path).
//   - `partitioned` — `AbstractPartitionedLimiter` over
//     `FixedLimit`. Sources `limen.limit.partition` and the
//     per-partition rows on `limen.inflight`.
//   - `codel` — `CodelFilter`. Sources `limen.codel.drops` and
//     `limen.codel.dropping`.
struct FullStack {
  std::shared_ptr<CapturingExporter> exporter;
  std::shared_ptr<sdk_metrics::MeterProvider> provider;
  ManualReader* reader_ptr;
  std::unique_ptr<SimpleLimiter> simple;
  std::unique_ptr<Gradient2Limit> gradient;
  std::unique_ptr<AbstractPartitionedLimiter> partitioned;
  std::unique_ptr<CodelFilter> codel;
};

AbstractPartitionedLimiter::PartitionResolver MatchExact(
    std::string target, std::string partition_name) {
  return
      [target = std::move(target), partition_name = std::move(partition_name)](
          std::string_view ctx) -> std::optional<std::string> {
        if (ctx == target) return partition_name;
        return std::nullopt;
      };
}

FullStack BuildFullStack() {
  FullStack stack;
  stack.exporter = std::make_shared<CapturingExporter>();
  auto reader = std::make_unique<ManualReader>(stack.exporter);
  stack.reader_ptr = reader.get();
  stack.provider = std::make_shared<sdk_metrics::MeterProvider>();
  stack.provider->AddMetricReader(std::move(reader));

  // SimpleLimiter over a FixedLimit. Provides the three core
  // observable instruments (`limen.limit`, `limen.inflight`,
  // `limen.call`) without dragging the algorithm in.
  stack.simple = SimpleLimiter::Builder()
                     .Limit(FixedLimit::Of(4))
                     .Id("simple")
                     .MeterProvider(stack.provider)
                     .Build()
                     .value();

  // Standalone Gradient2Limit. We never wrap it in a SimpleLimiter
  // because that would force the histograms to depend on a real
  // admit/release pipeline whose `rtt_ns` can be zero on fast
  // hosts (steady_clock collision), skipping the histogram path
  // via Gradient2Limit::Update's `rtt_ns <= 0` short-circuit.
  // Driving the algorithm directly with `OnSample` and an
  // explicit non-zero rtt makes the histogram recording
  // deterministic.
  //
  // InitialLimit=10 with inflight=10 keeps the app-limited skip
  // (`inflight < estimated_limit / 2`) inactive on the first
  // sample so the gradient branch — and therefore the histograms
  // — actually fires.
  stack.gradient = Gradient2Limit::Builder()
                       .InitialLimit(10)
                       .MinLimit(1)
                       .MaxConcurrency(200)
                       .Id("gradient")
                       .MeterProvider(stack.provider)
                       .Build();

  // Partitioned limiter with two partitions over a FixedLimit.
  stack.partitioned = AbstractPartitionedLimiter::Builder()
                          .Limit(FixedLimit::Of(10))
                          .Id("partitioned")
                          .Partition("p1", 0.5)
                          .Partition("p2", 0.5)
                          .PartitionResolver(MatchExact("p1", "p1"))
                          .PartitionResolver(MatchExact("p2", "p2"))
                          .MeterProvider(stack.provider)
                          .Build()
                          .value();

  // CodelFilter.
  stack.codel = CodelFilter::Builder()
                    .Id("codel")
                    .MeterProvider(stack.provider)
                    .Build()
                    .value();

  return stack;
}

// Drive each component just enough to populate the captured
// stream on the next `Collect`.
void DriveBaselineTraffic(FullStack& stack) {
  // SimpleLimiter: one admit/release.
  {
    auto slot = stack.simple->TryAcquire();
    ASSERT_TRUE(slot);
    slot->OnSuccess();
  }
  // Gradient2Limit: one direct OnSample with a synthetic 1 ms
  // RTT and inflight at the configured InitialLimit. With
  // `rtt_ns = 1'000'000 > 0` the zero-RTT short-circuit doesn't
  // fire, and with `inflight = 10` against `estimated_limit_ = 10`
  // the app-limited skip doesn't fire either, so all three
  // histograms record deterministically.
  stack.gradient->OnSample(/*start_time_ns=*/0, /*rtt_ns=*/1'000'000,
                           /*inflight=*/10, /*did_drop=*/false);
  // Partitioned limiter: one admit/release per partition so the
  // partitioned in-flight callback has a per-partition value to
  // emit even if it's zero post-release.
  {
    auto slot = stack.partitioned->TryAcquire("p1");
    ASSERT_TRUE(slot);
    slot->OnSuccess();
  }
  {
    auto slot = stack.partitioned->TryAcquire("p2");
    ASSERT_TRUE(slot);
    slot->OnSuccess();
  }
  // CodelFilter: one ShouldDrop call. The filter's observable
  // callbacks fire on Collect regardless of whether we drove a
  // drop; this call is just for completeness.
  (void)stack.codel->ShouldDrop(absl::Now());
}

// =====================================================================
// Cross-cutting observability tests.
// =====================================================================

TEST(LimenObservabilityE2ETest, AllEmittedMetricsHaveLimenPrefix) {
  // Every metric Limen emits, across every class, must start with
  // `limen.` so an operator can find them all on one dashboard
  // with a single prefix filter.
  FullStack stack = BuildFullStack();
  DriveBaselineTraffic(stack);
  stack.reader_ptr->Collect();

  auto const names = DistinctMetricNames(*stack.exporter);
  ASSERT_FALSE(names.empty()) << "No metrics captured";
  for (auto const& name : names) {
    EXPECT_EQ(name.rfind("limen.", 0), 0u)
        << "Metric name does not start with limen.: " << name;
  }
}

TEST(LimenObservabilityE2ETest, AllEmittedPointsCarryIdLabel) {
  // Every captured data point must carry an `id` attribute so an
  // operator can distinguish multiple limiter instances of the
  // same type. (The `id` value comes from the application's
  // `.Id(...)` on the builder.)
  FullStack stack = BuildFullStack();
  DriveBaselineTraffic(stack);
  stack.reader_ptr->Collect();

  auto const points = CollectAllPoints(*stack.exporter);
  ASSERT_FALSE(points.empty()) << "No points captured";
  for (auto const& p : points) {
    EXPECT_TRUE(p.attributes.count("id") > 0)
        << "Point on metric " << p.metric_name << " is missing the `id` "
        << "attribute (attributes: " << p.attributes.size() << ")";
  }
}

TEST(LimenObservabilityE2ETest, MetricCatalogContainsAllExpectedNames) {
  // The full advertised catalog must appear in the captured
  // stream when every component has been exercised at least
  // once. A regression that silently drops one of these metric
  // names — registration glue not run, View not applied, callback
  // not added — fails here.
  FullStack stack = BuildFullStack();
  DriveBaselineTraffic(stack);
  stack.reader_ptr->Collect();

  auto const names = DistinctMetricNames(*stack.exporter);

  std::set<std::string> const expected_names = {
      "limen.limit",           "limen.inflight",       "limen.call",
      "limen.min_rtt",         "limen.min_window_rtt", "limen.queue_size",
      "limen.limit.partition", "limen.codel.drops",    "limen.codel.dropping",
  };

  for (auto const& expected : expected_names) {
    EXPECT_TRUE(names.count(expected) > 0)
        << "Catalog metric missing from captured stream: " << expected;
  }
}

TEST(LimenObservabilityE2ETest, DifferentIdsDoNotCrossContaminate) {
  // Two SimpleLimiters with distinct `Id`s on the same
  // MeterProvider. The outcome counter values for one must
  // reflect only that limiter's traffic.
  auto exporter = std::make_shared<CapturingExporter>();
  auto reader = std::make_unique<ManualReader>(exporter);
  auto* reader_ptr = reader.get();
  auto provider = std::make_shared<sdk_metrics::MeterProvider>();
  provider->AddMetricReader(std::move(reader));

  auto limiter_a = SimpleLimiter::Builder()
                       .Limit(FixedLimit::Of(4))
                       .Id("a")
                       .MeterProvider(provider)
                       .Build()
                       .value();
  auto limiter_b = SimpleLimiter::Builder()
                       .Limit(FixedLimit::Of(4))
                       .Id("b")
                       .MeterProvider(provider)
                       .Build()
                       .value();

  // 3 successes through `a`, 5 successes through `b`. Counters
  // for each id should reflect those exact values.
  for (int i = 0; i < 3; ++i) {
    auto slot = limiter_a->TryAcquire();
    ASSERT_TRUE(slot);
    slot->OnSuccess();
  }
  for (int i = 0; i < 5; ++i) {
    auto slot = limiter_b->TryAcquire();
    ASSERT_TRUE(slot);
    slot->OnSuccess();
  }

  reader_ptr->Collect();

  auto const points = CollectAllPoints(*exporter);
  int64_t a_success = -1;
  int64_t b_success = -1;
  for (auto const& p : points) {
    if (p.metric_name != "limen.call") continue;
    auto const status_it = p.attributes.find("status");
    if (status_it == p.attributes.end() || status_it->second != "success") {
      continue;
    }
    auto const id_it = p.attributes.find("id");
    if (id_it == p.attributes.end()) continue;
    if (id_it->second == "a") a_success = p.value;
    if (id_it->second == "b") b_success = p.value;
  }
  EXPECT_EQ(a_success, 3) << "limen.call{id=a,status=success}";
  EXPECT_EQ(b_success, 5) << "limen.call{id=b,status=success}";
}

TEST(LimenObservabilityE2ETest, HotPathDoesNoExporterActivityBeforeCollect) {
  // No Limen component's hot path may push to the exporter
  // before an explicit `Collect()` call. The captured batch list
  // must remain empty after thousands of TryAcquire / OnSuccess
  // and ShouldDrop calls.
  FullStack stack = BuildFullStack();

  for (int i = 0; i < 1'000; ++i) {
    auto slot = stack.simple->TryAcquire();
    if (slot) slot->OnSuccess();
  }
  for (int i = 0; i < 1'000; ++i) {
    stack.gradient->OnSample(0, 1'000'000, 10, false);
  }
  for (int i = 0; i < 1'000; ++i) {
    auto slot = stack.partitioned->TryAcquire("p1");
    if (slot) slot->OnSuccess();
  }
  for (int i = 0; i < 1'000; ++i) {
    (void)stack.codel->ShouldDrop(absl::Now());
  }

  EXPECT_TRUE(stack.exporter->captured().empty())
      << "Hot path pushed to exporter before Collect; captured.size()="
      << stack.exporter->captured().size();
}

TEST(LimenObservabilityE2ETest, AllOutcomeStatusesEmitOnCallCounter) {
  // The `limen.call` observable counter must emit a data point
  // for every one of the five Status values (success, rejected,
  // dropped, ignored, bypassed) even when the count for that
  // status is zero. Drive at least one of each so the
  // assertion catches a wiring regression in any branch.
  auto exporter = std::make_shared<CapturingExporter>();
  auto reader = std::make_unique<ManualReader>(exporter);
  auto* reader_ptr = reader.get();
  auto provider = std::make_shared<sdk_metrics::MeterProvider>();
  provider->AddMetricReader(std::move(reader));

  auto limiter =
      SimpleLimiter::Builder()
          .Limit(FixedLimit::Of(1))
          .Id("statuses")
          .BypassPredicate([](std::string_view ctx) { return ctx == "skip"; })
          .MeterProvider(provider)
          .Build()
          .value();

  // success.
  {
    auto slot = limiter->TryAcquire();
    ASSERT_TRUE(slot);
    slot->OnSuccess();
  }
  // dropped.
  {
    auto slot = limiter->TryAcquire();
    ASSERT_TRUE(slot);
    slot->OnDropped();
  }
  // ignored (destructor without explicit completion).
  {
    auto slot = limiter->TryAcquire();
    ASSERT_TRUE(slot);
    // No completion call; SlotGuard destructor fires OnIgnore.
  }
  // bypassed.
  {
    auto slot = limiter->TryAcquire("skip");
    ASSERT_TRUE(slot);
    slot->OnSuccess();
  }
  // rejected: saturate at cap=1.
  {
    auto holder = limiter->TryAcquire();
    ASSERT_TRUE(holder);
    auto rejected = limiter->TryAcquire();
    EXPECT_FALSE(rejected);
    holder->OnSuccess();
  }

  reader_ptr->Collect();

  auto const points = CollectAllPoints(*exporter);
  std::set<std::string> seen_statuses;
  for (auto const& p : points) {
    if (p.metric_name != "limen.call") continue;
    auto const it = p.attributes.find("status");
    if (it != p.attributes.end()) seen_statuses.insert(it->second);
  }

  for (auto const& expected :
       {"success", "rejected", "dropped", "ignored", "bypassed"}) {
    EXPECT_TRUE(seen_statuses.count(expected) > 0)
        << "limen.call status missing from captured stream: " << expected;
  }
}

TEST(LimenObservabilityE2ETest,
     PartitionAttributeFlowsThroughInflightAndLimit) {
  // The partitioned limiter contractually emits per-partition
  // rows on TWO instruments: `limen.inflight` (one row per
  // partition via the `OnObserveInflight` override, in addition
  // to the global row) AND `limen.limit.partition` (one row per
  // partition, by definition of that instrument). Both must
  // carry the `partition` attribute set to the configured name.
  // A regression that emits only the aggregate, omits the
  // attribute, or wires only one of the two instruments fails
  // here.
  auto exporter = std::make_shared<CapturingExporter>();
  auto reader = std::make_unique<ManualReader>(exporter);
  auto* reader_ptr = reader.get();
  auto provider = std::make_shared<sdk_metrics::MeterProvider>();
  provider->AddMetricReader(std::move(reader));

  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(10))
                     .Id("partitioned-inflight")
                     .Partition("alpha", 0.5)
                     .Partition("beta", 0.5)
                     .PartitionResolver(MatchExact("alpha", "alpha"))
                     .PartitionResolver(MatchExact("beta", "beta"))
                     .MeterProvider(provider)
                     .Build()
                     .value();
  // Drive one admit/release per partition so the busy counts
  // have been touched.
  {
    auto slot = limiter->TryAcquire("alpha");
    ASSERT_TRUE(slot);
    slot->OnSuccess();
  }
  {
    auto slot = limiter->TryAcquire("beta");
    ASSERT_TRUE(slot);
    slot->OnSuccess();
  }

  reader_ptr->Collect();

  auto const points = CollectAllPoints(*exporter);
  std::set<std::string> inflight_partitions;
  std::set<std::string> limit_partitions;
  for (auto const& p : points) {
    auto const it = p.attributes.find("partition");
    if (it == p.attributes.end()) continue;
    if (p.metric_name == "limen.inflight") {
      inflight_partitions.insert(it->second);
    } else if (p.metric_name == "limen.limit.partition") {
      limit_partitions.insert(it->second);
    }
  }
  EXPECT_TRUE(inflight_partitions.count("alpha") > 0)
      << "limen.inflight is missing a `partition=alpha` data point";
  EXPECT_TRUE(inflight_partitions.count("beta") > 0)
      << "limen.inflight is missing a `partition=beta` data point";
  EXPECT_TRUE(limit_partitions.count("alpha") > 0)
      << "limen.limit.partition is missing a `partition=alpha` data point";
  EXPECT_TRUE(limit_partitions.count("beta") > 0)
      << "limen.limit.partition is missing a `partition=beta` data point";
}

TEST(LimenObservabilityE2ETest, AllMetricInstrumentsHaveExpectedUnit) {
  // Every metric Limen emits has a contractually-defined OTel
  // `unit`. Operators and collector pipelines route by unit, so
  // a regression that ships the wrong unit (e.g. recording the
  // RTT histograms with unit "1" instead of "ns") silently
  // breaks any consumer that filters or converts by unit. Pin
  // the full unit catalog here.
  FullStack stack = BuildFullStack();
  DriveBaselineTraffic(stack);
  stack.reader_ptr->Collect();

  auto const units = MetricUnits(*stack.exporter);

  struct ExpectedUnit {
    std::string name;
    std::string unit;
  };
  std::vector<ExpectedUnit> const expected = {
      {"limen.limit", "1"},
      {"limen.inflight", "1"},
      {"limen.call", "1"},
      {"limen.min_rtt", "ns"},
      {"limen.min_window_rtt", "ns"},
      {"limen.queue_size", "1"},
      {"limen.limit.partition", "1"},
      {"limen.codel.drops", "1"},
      {"limen.codel.dropping", "1"},
  };
  for (auto const& [name, expected_unit] : expected) {
    auto const it = units.find(name);
    ASSERT_NE(it, units.end()) << "Metric not captured: " << name;
    EXPECT_EQ(it->second, expected_unit)
        << "Wrong unit for " << name << ": got '" << it->second
        << "', expected '" << expected_unit << "'";
  }
}

TEST(LimenObservabilityE2ETest, CumulativeCountersAccumulateAcrossCollects) {
  // `limen.call` is an observable counter with kCumulative
  // temporality. Driving more traffic then collecting again must
  // produce a strictly-greater cumulative count, not reset.
  auto exporter = std::make_shared<CapturingExporter>();
  auto reader = std::make_unique<ManualReader>(exporter);
  auto* reader_ptr = reader.get();
  auto provider = std::make_shared<sdk_metrics::MeterProvider>();
  provider->AddMetricReader(std::move(reader));

  auto limiter = SimpleLimiter::Builder()
                     .Limit(FixedLimit::Of(4))
                     .Id("cumulative")
                     .MeterProvider(provider)
                     .Build()
                     .value();

  // Drive 3 successes, collect, read the success count.
  for (int i = 0; i < 3; ++i) {
    auto slot = limiter->TryAcquire();
    ASSERT_TRUE(slot);
    slot->OnSuccess();
  }
  reader_ptr->Collect();
  size_t const captured_after_first = exporter->captured().size();
  ASSERT_GT(captured_after_first, 0u);

  // Drive 4 more successes, collect, read again.
  for (int i = 0; i < 4; ++i) {
    auto slot = limiter->TryAcquire();
    ASSERT_TRUE(slot);
    slot->OnSuccess();
  }
  reader_ptr->Collect();

  // Find the most recent success count for id=cumulative.
  // Iterate the captured points in order; the highest captured
  // value is the latest cumulative count.
  auto const points = CollectAllPoints(*exporter);
  int64_t highest_seen = -1;
  for (auto const& p : points) {
    if (p.metric_name != "limen.call") continue;
    auto status_it = p.attributes.find("status");
    auto id_it = p.attributes.find("id");
    if (status_it == p.attributes.end() || id_it == p.attributes.end()) {
      continue;
    }
    if (status_it->second != "success" || id_it->second != "cumulative") {
      continue;
    }
    if (p.value > highest_seen) highest_seen = p.value;
  }
  EXPECT_EQ(highest_seen, 7)
      << "limen.call{id=cumulative,status=success} cumulative count must "
         "equal total successful admits (3 + 4 = 7)";
}

}  // namespace
}  // namespace limen

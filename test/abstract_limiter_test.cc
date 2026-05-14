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
#include "limen/limit.h"
#include "limen/settable_limit.h"
#include "gtest/gtest.h"
#include "opentelemetry/sdk/metrics/data/metric_data.h"
#include "opentelemetry/sdk/metrics/data/point_data.h"
#include "opentelemetry/sdk/metrics/export/metric_producer.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include "opentelemetry/sdk/metrics/metric_reader.h"
#include "opentelemetry/sdk/metrics/push_metric_exporter.h"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

// Allocation counter for `NoSynchronousOtelCallOnHotPath`. The
// `tracking` flag is toggled around the timed loop so unrelated
// allocations from gtest, the SDK constructor, etc. do not pollute
// the count. The replacements cover only the sized, non-aligned
// forms; no type reachable from `TryAcquire` / completion methods
// is currently over-aligned.
std::atomic<int> g_alloc_count{0};
std::atomic<bool> g_alloc_tracking{false};

}  // namespace

void* operator new(std::size_t size) {
  if (g_alloc_tracking.load(std::memory_order_relaxed)) {
    g_alloc_count.fetch_add(1, std::memory_order_relaxed);
  }
  void* p = std::malloc(size);
  if (p == nullptr) {
    std::abort();
  }
  return p;
}

void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t /*size*/) noexcept { std::free(p); }

void* operator new[](std::size_t size) {
  if (g_alloc_tracking.load(std::memory_order_relaxed)) {
    g_alloc_count.fetch_add(1, std::memory_order_relaxed);
  }
  void* p = std::malloc(size);
  if (p == nullptr) {
    std::abort();
  }
  return p;
}

void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t /*size*/) noexcept { std::free(p); }

namespace limen {
namespace {

namespace sdk_metrics = opentelemetry::sdk::metrics;

// Concrete subclass that publicises the protected constructor so
// tests can build an AbstractLimiter with the base's default gate.
class TestLimiter : public AbstractLimiter {
 public:
  static std::unique_ptr<TestLimiter> Create(
      std::unique_ptr<Limit> limit, std::string id,
      BypassPredicate bypass_predicate = nullptr,
      std::shared_ptr<sdk_metrics::MeterProvider> meter_provider = nullptr) {
    Params params{std::move(limit), std::move(id), std::move(bypass_predicate),
                  std::move(meter_provider)};
    return std::unique_ptr<TestLimiter>(new TestLimiter(std::move(params)));
  }

 private:
  explicit TestLimiter(Params params) : AbstractLimiter(std::move(params)) {}
};

// Subclass whose gate always rejects. Used to exercise the
// rejected-status counter path in
// `ObservableCounterEnumeratesAllStatuses`.
class RejectingLimiter : public AbstractLimiter {
 public:
  static std::unique_ptr<RejectingLimiter> Create(
      std::unique_ptr<Limit> limit, std::string id,
      std::shared_ptr<sdk_metrics::MeterProvider> meter_provider = nullptr) {
    Params params{std::move(limit), std::move(id),
                  /*bypass_predicate=*/nullptr, std::move(meter_provider)};
    return std::unique_ptr<RejectingLimiter>(
        new RejectingLimiter(std::move(params)));
  }

 protected:
  std::optional<AcquireResult> DoAcquire(
      std::string_view /*context*/) override {
    return std::nullopt;
  }

 private:
  explicit RejectingLimiter(Params params)
      : AbstractLimiter(std::move(params)) {}
};

// Limit decorator that records every OnSample call so tests can
// assert the sample fed to the wrapped algorithm. Forwards
// GetLimit and NotifyOnChange to a SettableLimit so the cap can
// be changed mid-test.
class RecordingLimit final : public Limit {
 public:
  explicit RecordingLimit(int initial_limit)
      : inner_(SettableLimit::StartingAt(initial_limit)) {}

  int GetLimit() const override { return inner_->GetLimit(); }

  void OnSample(int64_t start_time_ns, int64_t rtt_ns, int inflight,
                bool did_drop) override {
    ++sample_count;
    last_start_time_ns = start_time_ns;
    last_rtt_ns = rtt_ns;
    last_inflight = inflight;
    last_did_drop = did_drop;
    inner_->OnSample(start_time_ns, rtt_ns, inflight, did_drop);
  }

  void NotifyOnChange(ChangeCallback callback) override {
    inner_->NotifyOnChange(std::move(callback));
  }

  void SetLimit(int new_limit) { inner_->SetLimit(new_limit); }

  int sample_count = 0;
  int64_t last_start_time_ns = 0;
  int64_t last_rtt_ns = 0;
  int last_inflight = 0;
  bool last_did_drop = false;

 private:
  std::unique_ptr<SettableLimit> inner_;
};

// Push-style exporter that captures every metric batch. Tests
// trigger collection through `ManualReader::Collect`.
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

  void Clear() { captured_.clear(); }

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

// Looks up the latest int64 point for the named metric. Returns
// nullopt if the metric is absent. Uses the LAST data-point that
// matches; the caller is responsible for filtering by attribute
// when more than one stream shares the metric name.
struct Int64Point {
  int64_t value;
  std::map<std::string, std::string> attributes;
};

std::vector<Int64Point> Int64PointsFor(CapturingExporter const& exporter,
                                       std::string const& metric_name) {
  std::vector<Int64Point> points;
  for (auto const& batch : exporter.captured()) {
    for (auto const& scope : batch.scope_metric_data_) {
      for (auto const& metric : scope.metric_data_) {
        if (metric.instrument_descriptor.name_ != metric_name) {
          continue;
        }
        for (auto const& point : metric.point_data_attr_) {
          int64_t value = 0;
          if (auto const* sum =
                  opentelemetry::nostd::get_if<sdk_metrics::SumPointData>(
                      &point.point_data)) {
            value = opentelemetry::nostd::get<int64_t>(sum->value_);
          } else if (auto const* last = opentelemetry::nostd::get_if<
                         sdk_metrics::LastValuePointData>(&point.point_data)) {
            value = opentelemetry::nostd::get<int64_t>(last->value_);
          } else {
            continue;
          }
          std::map<std::string, std::string> attrs;
          for (auto const& [k, v] : point.attributes) {
            if (auto const* s = opentelemetry::nostd::get_if<std::string>(&v)) {
              attrs[k] = *s;
            }
          }
          points.push_back({value, std::move(attrs)});
        }
      }
    }
  }
  return points;
}

struct OtelHarness {
  std::shared_ptr<CapturingExporter> exporter =
      std::make_shared<CapturingExporter>();
  std::shared_ptr<sdk_metrics::MeterProvider> provider =
      std::make_shared<sdk_metrics::MeterProvider>();
  ManualReader* reader = nullptr;

  OtelHarness() {
    auto owned_reader = std::make_unique<ManualReader>(exporter);
    reader = owned_reader.get();
    provider->AddMetricReader(std::move(owned_reader));
  }
};

TEST(AbstractLimiterTest, ObservableGaugeReturnsLiveLimit) {
  OtelHarness h;
  auto wrapped = std::make_unique<RecordingLimit>(50);
  auto* recorder = wrapped.get();
  auto limiter =
      TestLimiter::Create(std::move(wrapped), "test", nullptr, h.provider);

  recorder->SetLimit(75);
  h.reader->Collect();
  auto points = Int64PointsFor(*h.exporter, "limen.limit");
  ASSERT_FALSE(points.empty());
  EXPECT_EQ(points.back().value, 75);
}

TEST(AbstractLimiterTest, ObservableUpDownCounterReturnsLiveInflight) {
  OtelHarness h;
  auto wrapped = std::make_unique<RecordingLimit>(50);
  auto limiter =
      TestLimiter::Create(std::move(wrapped), "test", nullptr, h.provider);

  auto a = limiter->TryAcquire();
  auto b = limiter->TryAcquire();
  auto c = limiter->TryAcquire();
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  ASSERT_TRUE(c);

  h.reader->Collect();
  auto points = Int64PointsFor(*h.exporter, "limen.inflight");
  ASSERT_FALSE(points.empty());
  EXPECT_EQ(points.back().value, 3);

  a->OnSuccess();
  h.exporter->Clear();
  h.reader->Collect();
  points = Int64PointsFor(*h.exporter, "limen.inflight");
  ASSERT_FALSE(points.empty());
  EXPECT_EQ(points.back().value, 2);
}

TEST(AbstractLimiterTest, ObservableCounterEnumeratesAllStatuses) {
  OtelHarness h;
  // Bypass predicate matches when context starts with "bypass".
  auto bypass = [](std::string_view ctx) { return ctx == "bypass"; };
  auto wrapped = std::make_unique<RecordingLimit>(50);
  auto limiter =
      TestLimiter::Create(std::move(wrapped), "test", bypass, h.provider);

  // One success.
  auto s = limiter->TryAcquire();
  ASSERT_TRUE(s);
  s->OnSuccess();

  // One ignored.
  auto i = limiter->TryAcquire();
  ASSERT_TRUE(i);
  i->OnIgnore();

  // One dropped.
  auto d = limiter->TryAcquire();
  ASSERT_TRUE(d);
  d->OnDropped();

  // One bypassed (admitted via bypass predicate, completed
  // successfully).
  auto b = limiter->TryAcquire("bypass");
  ASSERT_TRUE(b);
  b->OnSuccess();

  // The base limiter admits unconditionally, so a "rejected"
  // outcome requires a subclass that overrides DoAcquire. Use the
  // file-scope RejectingLimiter helper.
  auto rejecting_wrapped = std::make_unique<RecordingLimit>(50);
  auto rejecting = RejectingLimiter::Create(std::move(rejecting_wrapped),
                                            "reject", h.provider);
  auto r = rejecting->TryAcquire();
  EXPECT_FALSE(r);

  h.reader->Collect();
  // "rejected" is owned by the RejectingLimiter's MeterProvider
  // (the same provider in this test). Each limiter publishes
  // separate `(id, status)` data points. Filter by id.
  auto const points = Int64PointsFor(*h.exporter, "limen.call");
  auto find_count = [&](std::string const& id, std::string const& status) {
    for (auto const& p : points) {
      auto it_id = p.attributes.find("id");
      auto it_status = p.attributes.find("status");
      if (it_id != p.attributes.end() && it_id->second == id &&
          it_status != p.attributes.end() && it_status->second == status) {
        return p.value;
      }
    }
    return int64_t{0};
  };
  // One real success; the bypassed-then-OnSuccess call lands in
  // the bypassed bucket exclusively. The taxonomy is mutually
  // exclusive — bypassed wins over the nominal completion.
  EXPECT_EQ(find_count("test", "success"), 1);
  EXPECT_EQ(find_count("test", "ignored"), 1);
  EXPECT_EQ(find_count("test", "dropped"), 1);
  EXPECT_EQ(find_count("test", "bypassed"), 1);
  EXPECT_EQ(find_count("test", "rejected"), 0);
  EXPECT_EQ(find_count("reject", "rejected"), 1);
}

TEST(AbstractLimiterTest,
     BypassPredicateCountsBypassedAndDoesNotTouchInflight) {
  auto bypass = [](std::string_view ctx) { return ctx == "skip"; };
  auto wrapped = std::make_unique<RecordingLimit>(50);
  auto* recorder = wrapped.get();
  auto limiter = TestLimiter::Create(std::move(wrapped), "test", bypass,
                                     /*meter_provider=*/nullptr);

  EXPECT_EQ(limiter->InflightCount(), 0);
  auto listener = limiter->TryAcquire("skip");
  ASSERT_TRUE(listener);
  EXPECT_EQ(limiter->InflightCount(), 0)
      << "Bypass must not touch the in-flight counter";
  listener->OnSuccess();
  EXPECT_EQ(limiter->InflightCount(), 0);

  EXPECT_EQ(recorder->sample_count, 0)
      << "Bypassed calls must not feed a sample to the algorithm";
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kBypassed), 1);
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kSuccess), 0);
}

TEST(AbstractLimiterTest, ListenerOnSuccessReleasesAndCounts) {
  auto wrapped = std::make_unique<RecordingLimit>(50);
  auto* recorder = wrapped.get();
  auto limiter = TestLimiter::Create(std::move(wrapped), "test");

  auto listener = limiter->TryAcquire();
  ASSERT_TRUE(listener);
  EXPECT_EQ(limiter->InflightCount(), 1);
  listener->OnSuccess();
  EXPECT_EQ(limiter->InflightCount(), 0);
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kSuccess), 1);
  EXPECT_EQ(recorder->sample_count, 1);
  EXPECT_FALSE(recorder->last_did_drop);
  EXPECT_EQ(recorder->last_inflight, 1);
}

TEST(AbstractLimiterTest, ListenerOnIgnoreReleasesWithoutSample) {
  auto wrapped = std::make_unique<RecordingLimit>(50);
  auto* recorder = wrapped.get();
  auto limiter = TestLimiter::Create(std::move(wrapped), "test");

  auto listener = limiter->TryAcquire();
  ASSERT_TRUE(listener);
  EXPECT_EQ(limiter->InflightCount(), 1);
  listener->OnIgnore();
  EXPECT_EQ(limiter->InflightCount(), 0);
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kIgnored), 1);
  EXPECT_EQ(recorder->sample_count, 0);
}

TEST(AbstractLimiterTest, ListenerOnDroppedReleasesAndCounts) {
  auto wrapped = std::make_unique<RecordingLimit>(50);
  auto* recorder = wrapped.get();
  auto limiter = TestLimiter::Create(std::move(wrapped), "test");

  auto listener = limiter->TryAcquire();
  ASSERT_TRUE(listener);
  EXPECT_EQ(limiter->InflightCount(), 1);
  listener->OnDropped();
  EXPECT_EQ(limiter->InflightCount(), 0);
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kDropped), 1);
  EXPECT_EQ(recorder->sample_count, 1);
  EXPECT_TRUE(recorder->last_did_drop);
}

TEST(AbstractLimiterTest, NoAllocationOnHotPath) {
  // The per-request acquire and complete path is allocation-free
  // by design. TryAcquire returns a SlotGuard by value through
  // std::optional, so the slot lives on the caller's stack with
  // no heap involvement. The observable instruments are
  // registered at construction; their callbacks fire only when
  // the SDK's collection thread polls them, not from a request
  // thread. Drive a tight acquire / OnSuccess loop and assert
  // zero heap allocations across the run.
  OtelHarness h;
  auto limiter =
      TestLimiter::Create(std::make_unique<RecordingLimit>(50), "otel",
                          /*bypass=*/nullptr, h.provider);

  constexpr int kIterations = 1000;
  g_alloc_count.store(0, std::memory_order_relaxed);
  g_alloc_tracking.store(true, std::memory_order_relaxed);
  for (int i = 0; i < kIterations; ++i) {
    auto l = limiter->TryAcquire();
    ASSERT_TRUE(l);
    l->OnSuccess();
  }
  g_alloc_tracking.store(false, std::memory_order_relaxed);

  EXPECT_EQ(g_alloc_count.load(std::memory_order_relaxed), 0);
}

}  // namespace
}  // namespace limen

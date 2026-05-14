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

#include "limen/abstract_partitioned_limiter.h"
#include "limen/fixed_limit.h"
#include "limen/settable_limit.h"
#include "absl/status/status.h"
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
#include <latch>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

// Allocation counter for the hot-path no-allocation test.
std::atomic<int> g_alloc_count{0};
std::atomic<bool> g_alloc_tracking{false};

// RAII guard: turns allocation tracking on for its lifetime and
// guarantees the flag is cleared even if the enclosing test
// returns early. Prevents one test leaking the tracking flag
// into another test in the same binary.
class AllocTrackingScope {
 public:
  AllocTrackingScope() {
    g_alloc_count.store(0, std::memory_order_relaxed);
    g_alloc_tracking.store(true, std::memory_order_relaxed);
  }
  ~AllocTrackingScope() {
    g_alloc_tracking.store(false, std::memory_order_relaxed);
  }
  AllocTrackingScope(AllocTrackingScope const&) = delete;
  AllocTrackingScope& operator=(AllocTrackingScope const&) = delete;
};

}  // namespace

void* operator new(std::size_t size) {
  if (g_alloc_tracking.load(std::memory_order_relaxed)) {
    g_alloc_count.fetch_add(1, std::memory_order_relaxed);
  }
  void* p = std::malloc(size);
  if (p == nullptr) std::abort();
  return p;
}

void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t /*size*/) noexcept { std::free(p); }

void* operator new[](std::size_t size) {
  if (g_alloc_tracking.load(std::memory_order_relaxed)) {
    g_alloc_count.fetch_add(1, std::memory_order_relaxed);
  }
  void* p = std::malloc(size);
  if (p == nullptr) std::abort();
  return p;
}

void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t /*size*/) noexcept { std::free(p); }

namespace limen {
namespace {

namespace sdk_metrics = opentelemetry::sdk::metrics;

// Push-style OTel exporter and manual reader, mirroring the
// harness in abstract_limiter_test.cc. Tests trigger collection
// through `ManualReader::Collect()` and assert against the
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
        if (metric.instrument_descriptor.name_ != metric_name) continue;
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

// Convenience: build a resolver that matches when the context
// equals the given target string.
AbstractPartitionedLimiter::PartitionResolver MatchExact(
    std::string target, std::string partition_name) {
  return
      [target = std::move(target), partition_name = std::move(partition_name)](
          std::string_view ctx) -> std::optional<std::string> {
        if (ctx == target) return partition_name;
        return std::nullopt;
      };
}

TEST(AbstractPartitionedLimiterTest, BuildRejectsMissingLimit) {
  auto result = AbstractPartitionedLimiter::Builder()
                    .Id("test")
                    .Partition("live", 1.0)
                    .PartitionResolver(MatchExact("live", "live"))
                    .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("Limit()"), std::string::npos);
}

TEST(AbstractPartitionedLimiterTest, BuildRejectsNoPartitions) {
  auto result = AbstractPartitionedLimiter::Builder()
                    .Limit(FixedLimit::Of(10))
                    .Id("test")
                    .PartitionResolver(MatchExact("live", "live"))
                    .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("Partition()"), std::string::npos);
}

TEST(AbstractPartitionedLimiterTest, BuildRejectsNoResolvers) {
  auto result = AbstractPartitionedLimiter::Builder()
                    .Limit(FixedLimit::Of(10))
                    .Id("test")
                    .Partition("live", 1.0)
                    .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("PartitionResolver"),
            std::string::npos);
}

TEST(AbstractPartitionedLimiterTest, BuildRejectsNegativeMaxDelayedThreads) {
  auto result = AbstractPartitionedLimiter::Builder()
                    .Limit(FixedLimit::Of(10))
                    .Id("test")
                    .Partition("live", 1.0)
                    .PartitionResolver(MatchExact("live", "live"))
                    .MaxDelayedThreads(-1)
                    .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("MaxDelayedThreads"),
            std::string::npos);
}

TEST(AbstractPartitionedLimiterTest, BuildRejectsEmptyPartitionName) {
  auto result = AbstractPartitionedLimiter::Builder()
                    .Limit(FixedLimit::Of(10))
                    .Id("test")
                    .Partition("", 0.5)
                    .PartitionResolver(MatchExact("live", "live"))
                    .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("empty"), std::string::npos);
}

TEST(AbstractPartitionedLimiterTest, BuildRejectsReservedUnknownName) {
  auto result = AbstractPartitionedLimiter::Builder()
                    .Limit(FixedLimit::Of(10))
                    .Id("test")
                    .Partition("unknown", 0.5)
                    .PartitionResolver(MatchExact("live", "live"))
                    .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("reserved"), std::string::npos);
}

TEST(AbstractPartitionedLimiterTest, BuildRejectsPercentageBelowZero) {
  auto result = AbstractPartitionedLimiter::Builder()
                    .Limit(FixedLimit::Of(10))
                    .Id("test")
                    .Partition("live", -0.1)
                    .PartitionResolver(MatchExact("live", "live"))
                    .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("in [0, 1]"), std::string::npos);
}

TEST(AbstractPartitionedLimiterTest, BuildRejectsPercentageAboveOne) {
  auto result = AbstractPartitionedLimiter::Builder()
                    .Limit(FixedLimit::Of(10))
                    .Id("test")
                    .Partition("live", 1.5)
                    .PartitionResolver(MatchExact("live", "live"))
                    .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("in [0, 1]"), std::string::npos);
}

TEST(AbstractPartitionedLimiterTest, BuildRejectsPercentageSumOverOne) {
  auto result = AbstractPartitionedLimiter::Builder()
                    .Limit(FixedLimit::Of(10))
                    .Id("test")
                    .Partition("live", 0.7)
                    .Partition("batch", 0.5)
                    .PartitionResolver(MatchExact("live", "live"))
                    .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("Sum of partition percentages"),
            std::string::npos);
}

TEST(AbstractPartitionedLimiterTest,
     BuildRejectsRejectDelayForUndeclaredPartition) {
  auto result = AbstractPartitionedLimiter::Builder()
                    .Limit(FixedLimit::Of(10))
                    .Id("test")
                    .Partition("live", 1.0)
                    .PartitionRejectDelay("typo", std::chrono::milliseconds(50))
                    .PartitionResolver(MatchExact("live", "live"))
                    .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("\"typo\""), std::string::npos);
}

TEST(AbstractPartitionedLimiterTest, BuildRejectsNullResolver) {
  auto result =
      AbstractPartitionedLimiter::Builder()
          .Limit(FixedLimit::Of(10))
          .Id("test")
          .Partition("live", 1.0)
          .PartitionResolver(AbstractPartitionedLimiter::PartitionResolver{})
          .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("null"), std::string::npos);
}

TEST(AbstractPartitionedLimiterTest, QuotaCeilingRounding) {
  // cap = 10. live = 0.31 → ceil(3.1) = 4. batch = 0.55 → ceil(5.5) = 6.
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(10))
                     .Id("test")
                     .Partition("live", 0.31)
                     .Partition("batch", 0.55)
                     .PartitionResolver(MatchExact("live", "live"))
                     .Build()
                     .value();
  EXPECT_EQ(limiter->PartitionLimitFor("live"), 4);
  EXPECT_EQ(limiter->PartitionLimitFor("batch"), 6);
}

TEST(AbstractPartitionedLimiterTest, QuotaMinimumOneSlot) {
  // cap = 1, percent = 0.01 → ceil(0.01) = 1 (clamped by max(1, ...)).
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(1))
                     .Id("test")
                     .Partition("tiny", 0.01)
                     .PartitionResolver(MatchExact("tiny", "tiny"))
                     .Build()
                     .value();
  EXPECT_EQ(limiter->PartitionLimitFor("tiny"), 1);
  // The synthetic unknown partition also rounds up to 1.
  EXPECT_EQ(limiter->PartitionLimitFor("unknown"), 1);
}

TEST(AbstractPartitionedLimiterTest, QuotaRecomputeOnCapChange) {
  auto settable = SettableLimit::StartingAt(10);
  auto* settable_ptr = settable.get();
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(std::move(settable))
                     .Id("test")
                     .Partition("live", 0.8)
                     .Partition("batch", 0.2)
                     .PartitionResolver(MatchExact("live", "live"))
                     .Build()
                     .value();
  EXPECT_EQ(limiter->PartitionLimitFor("live"), 8);
  EXPECT_EQ(limiter->PartitionLimitFor("batch"), 2);

  // Raise cap.
  settable_ptr->SetLimit(20);
  EXPECT_EQ(limiter->PartitionLimitFor("live"), 16);
  EXPECT_EQ(limiter->PartitionLimitFor("batch"), 4);

  // Lower cap.
  settable_ptr->SetLimit(5);
  EXPECT_EQ(limiter->PartitionLimitFor("live"), 4);
  EXPECT_EQ(limiter->PartitionLimitFor("batch"), 1);
}

TEST(AbstractPartitionedLimiterTest, ResolverChainFirstNonNullWins) {
  // First resolver matches "alpha"; second resolver matches
  // "beta". A request tagged "alpha" must land in "live"; a
  // request tagged "beta" must land in "batch".
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(10))
                     .Id("test")
                     .Partition("live", 0.5)
                     .Partition("batch", 0.5)
                     .PartitionResolver(MatchExact("alpha", "live"))
                     .PartitionResolver(MatchExact("beta", "batch"))
                     .Build()
                     .value();

  auto a = limiter->TryAcquire("alpha");
  ASSERT_TRUE(a);
  EXPECT_EQ(limiter->PartitionInflightCount("live"), 1);
  EXPECT_EQ(limiter->PartitionInflightCount("batch"), 0);

  auto b = limiter->TryAcquire("beta");
  ASSERT_TRUE(b);
  EXPECT_EQ(limiter->PartitionInflightCount("live"), 1);
  EXPECT_EQ(limiter->PartitionInflightCount("batch"), 1);
}

TEST(AbstractPartitionedLimiterTest, UnknownPartitionWhenNoResolverMatches) {
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(10))
                     .Id("test")
                     .Partition("live", 1.0)
                     .PartitionResolver(MatchExact("live", "live"))
                     .Build()
                     .value();

  auto guard = limiter->TryAcquire("does-not-match");
  ASSERT_TRUE(guard);
  EXPECT_EQ(limiter->PartitionInflightCount("unknown"), 1);
  EXPECT_EQ(limiter->PartitionInflightCount("live"), 0);
}

TEST(AbstractPartitionedLimiterTest, BurstIntoIdleWithinGlobalCap) {
  // cap = 10, live = 0.2 (quota 2). With the global cap not
  // exhausted, live can take more than its 2-slot quota.
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(10))
                     .Id("test")
                     .Partition("live", 0.2)
                     .Partition("batch", 0.8)
                     .PartitionResolver(MatchExact("live", "live"))
                     .Build()
                     .value();
  EXPECT_EQ(limiter->PartitionLimitFor("live"), 2);

  std::vector<std::optional<Limiter::SlotGuard>> slots;
  for (int i = 0; i < 5; ++i) {
    slots.push_back(limiter->TryAcquire("live"));
    ASSERT_TRUE(slots.back()) << "slot " << i;
  }
  EXPECT_EQ(limiter->PartitionInflightCount("live"), 5)
      << "Live exceeded its quota because global has unused capacity";
  EXPECT_EQ(limiter->InflightCount(), 5);
}

TEST(AbstractPartitionedLimiterTest, HardCapWhenGlobalExhausted) {
  // cap = 4, live = 0.5 (quota 2), batch = 0.5 (quota 2).
  // Saturate global with 4 requests across both partitions, then
  // try one more live request — must be rejected because live is
  // at quota AND global is full.
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(4))
                     .Id("test")
                     .Partition("live", 0.5)
                     .Partition("batch", 0.5)
                     .PartitionResolver(MatchExact("live", "live"))
                     .PartitionResolver(MatchExact("batch", "batch"))
                     .Build()
                     .value();
  ASSERT_EQ(limiter->PartitionLimitFor("live"), 2);

  auto l1 = limiter->TryAcquire("live");
  auto l2 = limiter->TryAcquire("live");
  auto b1 = limiter->TryAcquire("batch");
  auto b2 = limiter->TryAcquire("batch");
  ASSERT_TRUE(l1);
  ASSERT_TRUE(l2);
  ASSERT_TRUE(b1);
  ASSERT_TRUE(b2);
  EXPECT_EQ(limiter->InflightCount(), 4);

  // Live is at quota (2) and global is at cap (4). The next live
  // request must be rejected.
  auto extra = limiter->TryAcquire("live");
  EXPECT_FALSE(extra);
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kRejected), 1);
}

TEST(AbstractPartitionedLimiterTest, BypassPredicateMatchedBypasses) {
  auto bypass = [](std::string_view ctx) { return ctx == "skip"; };
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(1))
                     .Id("test")
                     .BypassPredicate(bypass)
                     .Partition("live", 1.0)
                     .PartitionResolver(MatchExact("live", "live"))
                     .Build()
                     .value();

  // Saturate.
  auto blocker = limiter->TryAcquire("live");
  ASSERT_TRUE(blocker);
  EXPECT_EQ(limiter->InflightCount(), 1);
  EXPECT_EQ(limiter->PartitionInflightCount("live"), 1);

  // Bypass-matching call must admit despite the cap being full
  // and must not touch any counter.
  auto bypassed = limiter->TryAcquire("skip");
  ASSERT_TRUE(bypassed);
  EXPECT_EQ(limiter->InflightCount(), 1)
      << "Bypass must not increment the global counter";
  EXPECT_EQ(limiter->PartitionInflightCount("live"), 1)
      << "Bypass must not touch the partition counter";
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kBypassed), 1);
}

TEST(AbstractPartitionedLimiterTest, ConcurrentConservation) {
  // Drive many threads through admission + completion. Post-join,
  // every partition's busy counter and the global in-flight
  // counter must be exactly zero. The conservation invariant
  // holds under every legal schedule.
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(20))
                     .Id("test")
                     .Partition("live", 0.5)
                     .Partition("batch", 0.5)
                     .PartitionResolver(MatchExact("live", "live"))
                     .PartitionResolver(MatchExact("batch", "batch"))
                     .Build()
                     .value();

  constexpr int kThreads = 32;
  constexpr int kIterationsPerThread = 500;
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      std::string_view const ctx = (t % 2 == 0) ? "live" : "batch";
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire(ctx);
        if (slot) slot->OnSuccess();
      }
    });
  }
  for (auto& thr : threads) thr.join();

  EXPECT_EQ(limiter->InflightCount(), 0)
      << "Global in-flight did not return to zero";
  EXPECT_EQ(limiter->PartitionInflightCount("live"), 0)
      << "Live partition did not return to zero";
  EXPECT_EQ(limiter->PartitionInflightCount("batch"), 0)
      << "Batch partition did not return to zero";
  EXPECT_EQ(limiter->PartitionInflightCount("unknown"), 0);
}

TEST(AbstractPartitionedLimiterTest,
     RejectDelaySleepHookCalledWithExactDuration) {
  // The reject-delay path calls the injected SleepFn with the
  // partition's configured duration. Single-threaded test, so the
  // fake records without synchronisation.
  std::vector<std::chrono::milliseconds> recorded;
  auto recording_sleep = [&](std::chrono::milliseconds d) {
    recorded.push_back(d);
  };

  // cap = 1, live = 1.0 with a 50 ms reject delay. Saturate live
  // (one slot), then issue a second live request — partition cap
  // is hit, the reject path triggers the sleep hook.
  auto limiter =
      AbstractPartitionedLimiter::Builder()
          .Limit(FixedLimit::Of(1))
          .Id("test")
          .Partition("live", 1.0)
          .PartitionRejectDelay("live", std::chrono::milliseconds(50))
          .PartitionResolver(MatchExact("live", "live"))
          .SleepFor(recording_sleep)
          .Build()
          .value();

  auto blocker = limiter->TryAcquire("live");
  ASSERT_TRUE(blocker);

  auto rejected = limiter->TryAcquire("live");
  EXPECT_FALSE(rejected);
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kRejected), 1);

  ASSERT_EQ(recorded.size(), 1u);
  EXPECT_EQ(recorded[0], std::chrono::milliseconds(50));
}

TEST(AbstractPartitionedLimiterTest, RejectDelayZeroDelayNeverSleeps) {
  std::atomic<int> sleep_calls{0};
  auto recording_sleep = [&](std::chrono::milliseconds /*d*/) {
    sleep_calls.fetch_add(1);
  };

  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(1))
                     .Id("test")
                     .Partition("live", 1.0)
                     .PartitionResolver(MatchExact("live", "live"))
                     .SleepFor(recording_sleep)
                     .Build()
                     .value();

  auto blocker = limiter->TryAcquire("live");
  ASSERT_TRUE(blocker);
  auto rejected = limiter->TryAcquire("live");
  EXPECT_FALSE(rejected);
  EXPECT_EQ(sleep_calls.load(), 0)
      << "A partition with no reject delay must never sleep";
}

TEST(AbstractPartitionedLimiterTest, RejectDelayCappedByMaxDelayedThreads) {
  // Configure max_delayed_threads = 2. Drive 8 concurrent
  // rejected requests through the partition. The sleep hook
  // blocks until released so we can observe the cap. At most 2
  // threads may be sleeping at any moment.
  std::atomic<int> inside_sleep{0};
  std::atomic<int> peak_inside{0};
  // The cap is two; exactly two threads enter `gated_sleep`. The
  // `parked_latch` counts down once per thread on entry so the
  // main thread can wait deterministically until both are parked.
  // `release_sleepers` is a separate latch the main thread fires
  // to let the parked threads finish.
  std::latch parked_latch{2};
  std::latch release_sleepers{1};
  auto gated_sleep = [&](std::chrono::milliseconds /*d*/) {
    int const live = inside_sleep.fetch_add(1) + 1;
    int prev = peak_inside.load();
    while (live > prev && !peak_inside.compare_exchange_weak(prev, live)) {
    }
    parked_latch.count_down();
    release_sleepers.wait();
    inside_sleep.fetch_sub(1);
  };

  auto limiter =
      AbstractPartitionedLimiter::Builder()
          .Limit(FixedLimit::Of(1))
          .Id("test")
          .Partition("live", 1.0)
          .PartitionRejectDelay("live", std::chrono::milliseconds(50))
          .PartitionResolver(MatchExact("live", "live"))
          .MaxDelayedThreads(2)
          .SleepFor(gated_sleep)
          .Build()
          .value();

  // Saturate the partition.
  auto blocker = limiter->TryAcquire("live");
  ASSERT_TRUE(blocker);

  // Fire 8 concurrent rejected requests. Two should be parked
  // inside the sleep hook; the other six should have returned
  // immediately because delayed_threads_ reached its cap.
  constexpr int kThreads = 8;
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      auto r = limiter->TryAcquire("live");
      EXPECT_FALSE(r);
    });
  }

  // Wait deterministically for both parked sleepers to land in
  // `gated_sleep`. Each thread counts down `parked_latch` after
  // updating `peak_inside`; when both have, `wait()` returns and
  // the steady-state peak is the value we assert on. Futex-backed,
  // no busy-wait under TSan instrumentation.
  parked_latch.wait();

  release_sleepers.count_down();
  for (auto& thr : threads) thr.join();

  EXPECT_EQ(peak_inside.load(), 2)
      << "Sleeper count exceeded MaxDelayedThreads cap";
  EXPECT_EQ(limiter->DelayedThreadCount(), 0)
      << "All sleepers must have unwound the counter";
}

TEST(AbstractPartitionedLimiterTest, ObservablePartitionLimitGauge) {
  auto exporter = std::make_shared<CapturingExporter>();
  auto provider = std::make_shared<sdk_metrics::MeterProvider>();
  auto owned_reader = std::make_unique<ManualReader>(exporter);
  auto* reader = owned_reader.get();
  provider->AddMetricReader(std::move(owned_reader));

  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(10))
                     .Id("api")
                     .MeterProvider(provider)
                     .Partition("live", 0.7)
                     .Partition("batch", 0.3)
                     .PartitionResolver(MatchExact("live", "live"))
                     .Build()
                     .value();

  reader->Collect();
  auto points = Int64PointsFor(*exporter, "limen.limit.partition");
  ASSERT_FALSE(points.empty());

  // Expect one row per declared partition plus the synthetic
  // unknown. Look up by partition label.
  auto find_value = [&](std::string const& partition_name) -> int64_t {
    for (auto const& p : points) {
      auto it = p.attributes.find("partition");
      if (it != p.attributes.end() && it->second == partition_name) {
        return p.value;
      }
    }
    return -1;
  };
  EXPECT_EQ(find_value("live"), 7);
  EXPECT_EQ(find_value("batch"), 3);
  EXPECT_EQ(find_value("unknown"), 1);
}

TEST(AbstractPartitionedLimiterTest, ObservableInflightCarriesPartitionRows) {
  auto exporter = std::make_shared<CapturingExporter>();
  auto provider = std::make_shared<sdk_metrics::MeterProvider>();
  auto owned_reader = std::make_unique<ManualReader>(exporter);
  auto* reader = owned_reader.get();
  provider->AddMetricReader(std::move(owned_reader));

  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(10))
                     .Id("api")
                     .MeterProvider(provider)
                     .Partition("live", 0.6)
                     .Partition("batch", 0.4)
                     .PartitionResolver(MatchExact("live", "live"))
                     .PartitionResolver(MatchExact("batch", "batch"))
                     .Build()
                     .value();

  auto a = limiter->TryAcquire("live");
  auto b = limiter->TryAcquire("live");
  auto c = limiter->TryAcquire("batch");
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  ASSERT_TRUE(c);

  reader->Collect();
  auto points = Int64PointsFor(*exporter, "limen.inflight");

  int64_t global_value = -1;
  int64_t live_value = -1;
  int64_t batch_value = -1;
  for (auto const& p : points) {
    auto it_part = p.attributes.find("partition");
    if (it_part == p.attributes.end()) {
      global_value = p.value;
    } else if (it_part->second == "live") {
      live_value = p.value;
    } else if (it_part->second == "batch") {
      batch_value = p.value;
    }
  }
  EXPECT_EQ(global_value, 3);
  EXPECT_EQ(live_value, 2);
  EXPECT_EQ(batch_value, 1);
}

TEST(AbstractPartitionedLimiterTest, NoAllocationOnAcquireRelease) {
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(100))
                     .Id("test")
                     .Partition("live", 1.0)
                     .PartitionResolver(MatchExact("live", "live"))
                     .Build()
                     .value();

  constexpr int kIterations = 10000;
  {
    AllocTrackingScope tracking;
    for (int i = 0; i < kIterations; ++i) {
      auto slot = limiter->TryAcquire("live");
      if (slot) slot->OnSuccess();
    }
  }
  EXPECT_EQ(g_alloc_count.load(std::memory_order_relaxed), 0);
}

}  // namespace
}  // namespace limen

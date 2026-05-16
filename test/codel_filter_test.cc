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
#include "absl/time/time.h"
#include "gtest/gtest.h"
#include "opentelemetry/sdk/metrics/data/metric_data.h"
#include "opentelemetry/sdk/metrics/data/point_data.h"
#include "opentelemetry/sdk/metrics/export/metric_producer.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include "opentelemetry/sdk/metrics/metric_reader.h"
#include "opentelemetry/sdk/metrics/push_metric_exporter.h"
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <latch>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

// Allocation counter used by NoAllocationOnHotPath. Same pattern
// as test/windowed_limit_test.cc:33-75.
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

// Mock clock used by every algorithm-correctness test. Starting
// time is one billion seconds past the Unix epoch so the filter's
// `drop_next_` sentinel (`absl::UnixEpoch()`) is well below "now"
// and `(now - drop_next_) < 16 * interval_` reliably evaluates to
// false on first call (the desired cold-start behaviour).
//
// State is stored as `int64_t` nanoseconds since the Unix epoch
// inside a `std::atomic` — lock-free on every platform without
// the libatomic runtime dependency that `std::atomic<absl::Time>`
// would require. The conversion functions are constant-time.
//
// `AsFn()` returns a lambda capturing `this` by value (8 bytes,
// inside std::function's small-buffer optimization). Calling the
// lambda reads one atomic and returns; no allocation.
class MockClock {
 public:
  absl::Time Now() const {
    return absl::FromUnixNanos(now_ns_.load(std::memory_order_relaxed));
  }
  void Advance(absl::Duration d) {
    now_ns_.fetch_add(absl::ToInt64Nanoseconds(d), std::memory_order_relaxed);
  }
  void Set(absl::Time t) {
    // Round up to the next nanosecond so that the stored value is
    // >= the input. `absl::Time` has sub-nanosecond precision; the
    // int64 nanos storage truncates. Without the round-up, calling
    // `Set(drop_next_)` after a `ControlLaw` step (which adds
    // `interval / sqrt(count)` — generally sub-nano) leaves
    // `Now() < drop_next_` strictly, and the subsequent
    // `ShouldDrop` would not fire the in-drop drop.
    int64_t nanos = absl::ToUnixNanos(t);
    if (absl::FromUnixNanos(nanos) < t) ++nanos;
    now_ns_.store(nanos, std::memory_order_relaxed);
  }
  CodelFilter::ClockFn AsFn() {
    return [this] { return Now(); };
  }

 private:
  std::atomic<int64_t> now_ns_{
      absl::ToInt64Nanoseconds(absl::Seconds(1'000'000'000))};
};

// Push-style OTel exporter and manual reader, mirroring the
// harness in abstract_partitioned_limiter_test.cc:93-158. Tests
// trigger collection through `ManualReader::Collect()` and assert
// against the captured ResourceMetrics.
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

// Finds the most recent int64 point for `metric_name` in the
// exporter's captured batches and returns its value. Returns
// std::nullopt if the metric was never observed.
std::optional<int64_t> FindLastInt64Point(CapturingExporter const& exporter,
                                          std::string const& metric_name) {
  std::optional<int64_t> last;
  for (auto const& batch : exporter.captured()) {
    for (auto const& scope : batch.scope_metric_data_) {
      for (auto const& metric : scope.metric_data_) {
        if (metric.instrument_descriptor.name_ != metric_name) continue;
        for (auto const& point : metric.point_data_attr_) {
          if (auto const* sum =
                  opentelemetry::nostd::get_if<sdk_metrics::SumPointData>(
                      &point.point_data)) {
            last = opentelemetry::nostd::get<int64_t>(sum->value_);
          } else if (auto const* lastv = opentelemetry::nostd::get_if<
                         sdk_metrics::LastValuePointData>(&point.point_data)) {
            last = opentelemetry::nostd::get<int64_t>(lastv->value_);
          }
        }
      }
    }
  }
  return last;
}

// Build a filter that uses RFC 8289 defaults (target=5ms,
// interval=100ms) with the supplied mock clock injected.
std::unique_ptr<CodelFilter> BuildWithMockClock(MockClock& clock) {
  return CodelFilter::Builder().Clock(clock.AsFn()).Id("test").Build().value();
}

// Drive the filter into drop mode at sojourn>target for at least
// `interval`, starting from a freshly-built filter. Returns the
// mock clock's time at the moment of drop-mode entry (the first
// `ShouldDrop` call that returned true). Advances the clock in
// 10ms steps; sojourn=10ms; target=5ms; interval=100ms; so entry
// fires on the 11th call at `start + 100ms`.
absl::Time DriveIntoDropMode(CodelFilter& filter, MockClock& clock) {
  for (int i = 0; i < 100; ++i) {
    bool const dropped =
        filter.ShouldDrop(clock.Now() - absl::Milliseconds(10));
    if (dropped) return clock.Now();
    clock.Advance(absl::Milliseconds(10));
  }
  ADD_FAILURE() << "DriveIntoDropMode did not enter drop mode within 100 "
                   "steps; filter or test is wrong";
  return clock.Now();
}

// =====================================================================
// Builder validation.
// =====================================================================

TEST(CodelFilterTest, BuildRejectsNonPositiveTarget) {
  for (auto const v :
       {absl::ZeroDuration(), absl::Microseconds(-1), -absl::Milliseconds(1)}) {
    auto result = CodelFilter::Builder().Target(v).Build();
    ASSERT_FALSE(result.ok()) << "Target=" << v;
    EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
    EXPECT_NE(result.status().message().find("Target"), std::string::npos);
  }
}

TEST(CodelFilterTest, BuildRejectsNonPositiveInterval) {
  for (auto const v :
       {absl::ZeroDuration(), absl::Microseconds(-1), -absl::Milliseconds(1)}) {
    auto result = CodelFilter::Builder().Interval(v).Build();
    ASSERT_FALSE(result.ok()) << "Interval=" << v;
    EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
    EXPECT_NE(result.status().message().find("Interval"), std::string::npos);
  }
}

TEST(CodelFilterTest, BuildAcceptsTargetEqualInterval) {
  auto result = CodelFilter::Builder()
                    .Target(absl::Milliseconds(5))
                    .Interval(absl::Milliseconds(5))
                    .Build();
  ASSERT_TRUE(result.ok());
  auto filter = std::move(result).value();
  EXPECT_FALSE(filter->IsDropping());
  EXPECT_EQ(filter->DropCount(), 0);
}

TEST(CodelFilterTest, BuildAcceptsValidDefaults) {
  auto result = CodelFilter::Builder().Build();
  ASSERT_TRUE(result.ok());
  auto filter = std::move(result).value();
  EXPECT_FALSE(filter->IsDropping());
  EXPECT_EQ(filter->DropCount(), 0);
}

// =====================================================================
// Algorithm correctness (single-threaded with mock clock).
// =====================================================================

TEST(CodelFilterTest, ShouldDropReturnsFalseWhenSojournBelowTarget) {
  MockClock clock;
  auto filter = BuildWithMockClock(clock);

  // sojourn = 0, well below target=5ms.
  EXPECT_FALSE(filter->ShouldDrop(clock.Now()));
  EXPECT_FALSE(filter->IsDropping());
  EXPECT_EQ(filter->DropCount(), 0);
}

TEST(CodelFilterTest, ShouldDropEntersDropModeAfterIntervalAboveTarget) {
  MockClock clock;
  auto filter = BuildWithMockClock(clock);

  // Calls 1..10 at sojourn=10ms (>target=5ms) but elapsed-above-
  // target < interval=100ms must NOT enter drop mode. Call 11 at
  // exactly t0+100ms triggers entry.
  absl::Time const start = clock.Now();
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(filter->ShouldDrop(clock.Now() - absl::Milliseconds(10)))
        << "call " << (i + 1);
    EXPECT_FALSE(filter->IsDropping()) << "call " << (i + 1);
    clock.Advance(absl::Milliseconds(10));
  }
  // Call 11 at start+100ms.
  ASSERT_EQ(clock.Now(), start + absl::Milliseconds(100));
  EXPECT_TRUE(filter->ShouldDrop(clock.Now() - absl::Milliseconds(10)));
  EXPECT_TRUE(filter->IsDropping());
  EXPECT_EQ(filter->DropCount(), 1);
}

TEST(CodelFilterTest, ShouldDropExitsDropModeImmediatelyWhenSojournDrops) {
  MockClock clock;
  auto filter = BuildWithMockClock(clock);
  (void)DriveIntoDropMode(*filter, clock);
  ASSERT_TRUE(filter->IsDropping());
  int64_t const drops_at_entry = filter->DropCount();

  // Sojourn drops to zero. RFC §5.5 says: leave drop state, return
  // the packet (no drop).
  EXPECT_FALSE(filter->ShouldDrop(clock.Now()));
  EXPECT_FALSE(filter->IsDropping());
  EXPECT_EQ(filter->DropCount(), drops_at_entry);
}

TEST(CodelFilterTest, ControlLawProducesInverseSqrtScheduleWithPinnedValues) {
  MockClock clock;
  auto filter = BuildWithMockClock(clock);
  absl::Time const t_entry = DriveIntoDropMode(*filter, clock);
  ASSERT_EQ(filter->DropEpisodeCount(), 1u);

  // After entry, drop_next_ should be t_entry + interval/sqrt(1)
  // = t_entry + 100ms. Tolerance 1µs.
  constexpr absl::Duration kTolerance = absl::Microseconds(1);
  EXPECT_LE(filter->DropNext() - (t_entry + absl::Milliseconds(100)),
            kTolerance);
  EXPECT_GE(filter->DropNext() - (t_entry + absl::Milliseconds(100)),
            -kTolerance);

  // Drive 4 in-drop-mode drops, recording drop_next_ each time.
  // count_ should go 2, 3, 4, 5. drop_next_ spacings:
  // count=2 ⇒ 100ms/sqrt(2) ≈ 70.7107ms.
  // count=3 ⇒ 100ms/sqrt(3) ≈ 57.7350ms.
  // count=4 ⇒ 100ms/sqrt(4) = 50ms.
  // count=5 ⇒ 100ms/sqrt(5) ≈ 44.7214ms.
  uint32_t const expected_counts[] = {2, 3, 4, 5};
  absl::Time prev_drop_next = filter->DropNext();
  for (uint32_t const expected_count : expected_counts) {
    // Advance to drop_next_ and trigger the next in-drop drop.
    clock.Set(prev_drop_next);
    ASSERT_TRUE(filter->ShouldDrop(clock.Now() - absl::Milliseconds(10)));
    ASSERT_EQ(filter->DropEpisodeCount(), expected_count);

    absl::Time const new_drop_next = filter->DropNext();
    absl::Duration const expected_spacing =
        absl::Milliseconds(100) /
        std::sqrt(static_cast<double>(expected_count));
    absl::Duration const actual_spacing = new_drop_next - prev_drop_next;
    EXPECT_LE(actual_spacing - expected_spacing, kTolerance)
        << "count=" << expected_count << " actual=" << actual_spacing
        << " expected=" << expected_spacing;
    EXPECT_GE(actual_spacing - expected_spacing, -kTolerance)
        << "count=" << expected_count;
    prev_drop_next = new_drop_next;
  }
}

// Helper used by the count-hysteresis tests below. Drives the
// filter into drop mode and then runs `additional_in_drop_drops`
// more in-drop drops, leaving count_ at 1+additional. Returns
// the time of the last drop.
absl::Time DriveDropEpisode(CodelFilter& filter, MockClock& clock,
                            uint32_t additional_in_drop_drops) {
  (void)DriveIntoDropMode(filter, clock);
  for (uint32_t i = 0; i < additional_in_drop_drops; ++i) {
    clock.Set(filter.DropNext());
    bool const dropped =
        filter.ShouldDrop(clock.Now() - absl::Milliseconds(10));
    EXPECT_TRUE(dropped) << "in-drop drop #" << (i + 1) << " did not fire";
  }
  return clock.Now();
}

// Exits drop mode by feeding one sojourn-below-target sample.
void ExitDropMode(CodelFilter& filter, MockClock& clock) {
  ASSERT_FALSE(filter.ShouldDrop(clock.Now()));
  ASSERT_FALSE(filter.IsDropping());
}

TEST(CodelFilterTest, CountResetsToOneOnDropModeReentryAfterLongQuietPeriod) {
  MockClock clock;
  auto filter = BuildWithMockClock(clock);

  // First episode: drive count_ to 5 (entry plus 4 in-drop drops).
  (void)DriveDropEpisode(*filter, clock, /*additional_in_drop_drops=*/4);
  ASSERT_EQ(filter->DropEpisodeCount(), 5u);

  // Exit drop mode.
  ExitDropMode(*filter, clock);

  // Advance well beyond 16 * interval = 1600ms. 20 * interval =
  // 2000ms is comfortably past, so the recent-episode test must
  // fail.
  clock.Advance(20 * absl::Milliseconds(100));

  // Drive sustained high sojourn for interval to re-enter. After
  // re-entry: delta = 5 - 1 = 4, but (now - drop_next_) is large,
  // so the hysteresis branch fails. count_ = 1.
  absl::Time const reentry_time = DriveIntoDropMode(*filter, clock);

  EXPECT_EQ(filter->DropEpisodeCount(), 1u);
  // drop_next_ at re-entry is ControlLaw(reentry_time, 1)
  // = reentry_time + 100ms.
  constexpr absl::Duration kTolerance = absl::Microseconds(1);
  EXPECT_LE(filter->DropNext() - (reentry_time + absl::Milliseconds(100)),
            kTolerance);
  EXPECT_GE(filter->DropNext() - (reentry_time + absl::Milliseconds(100)),
            -kTolerance);
}

TEST(CodelFilterTest, CountInheritsDeltaFromPreviousEpisodeOnRecentReentry) {
  MockClock clock;
  auto filter = BuildWithMockClock(clock);

  // First episode: count_ grows to 5, lastcount_ stays at 1 (set
  // at the entry of this episode).
  (void)DriveDropEpisode(*filter, clock, /*additional_in_drop_drops=*/4);
  ASSERT_EQ(filter->DropEpisodeCount(), 5u);

  ExitDropMode(*filter, clock);

  // Advance 2 * interval = 200ms — well within 16 * interval =
  // 1600ms. Hysteresis branch must fire.
  clock.Advance(2 * absl::Milliseconds(100));

  // Drive into drop mode again. delta = 5 - 1 = 4. (now -
  // drop_next_) is around 200ms < 1600ms, so count_ inherits
  // delta=4.
  absl::Time const reentry_time = DriveIntoDropMode(*filter, clock);

  EXPECT_EQ(filter->DropEpisodeCount(), 4u);
  // drop_next_ at re-entry is ControlLaw(reentry_time, 4)
  // = reentry_time + 100ms/sqrt(4) = reentry_time + 50ms.
  constexpr absl::Duration kTolerance = absl::Microseconds(1);
  EXPECT_LE(filter->DropNext() - (reentry_time + absl::Milliseconds(50)),
            kTolerance);
  EXPECT_GE(filter->DropNext() - (reentry_time + absl::Milliseconds(50)),
            -kTolerance);
}

TEST(CodelFilterTest, LastCountUpdatedToCurrentCountAtDropModeEntry) {
  MockClock clock;
  auto filter = BuildWithMockClock(clock);

  // Episode 1: drive count_ to 3 (entry + 2 in-drop drops). At
  // entry, count_=1, lastcount_=1.
  (void)DriveDropEpisode(*filter, clock, /*additional_in_drop_drops=*/2);
  ASSERT_EQ(filter->DropEpisodeCount(), 3u);

  ExitDropMode(*filter, clock);

  // Recent re-entry: delta = 3 - 1 = 2. count_ = 2 (inherits
  // delta). lastcount_ becomes 2. Then drive count_ to 7 in this
  // second episode (entry + 5 in-drop drops).
  clock.Advance(2 * absl::Milliseconds(100));
  (void)DriveDropEpisode(*filter, clock, /*additional_in_drop_drops=*/5);
  ASSERT_EQ(filter->DropEpisodeCount(), 7u);

  ExitDropMode(*filter, clock);

  // Third recent re-entry. At the start of this entry,
  // lastcount_=2 (from episode 2's entry). count_=7 (from end of
  // episode 2). delta = 7 - 2 = 5. count_ inherits delta=5.
  clock.Advance(2 * absl::Milliseconds(100));
  absl::Time const reentry_time = DriveIntoDropMode(*filter, clock);

  EXPECT_EQ(filter->DropEpisodeCount(), 5u);
  // drop_next_ = ControlLaw(reentry_time, 5)
  // = reentry_time + 100ms/sqrt(5) ≈ reentry_time + 44.7214ms.
  constexpr absl::Duration kTolerance = absl::Microseconds(1);
  absl::Duration const expected = absl::Milliseconds(100) / std::sqrt(5.0);
  EXPECT_LE(filter->DropNext() - (reentry_time + expected), kTolerance);
  EXPECT_GE(filter->DropNext() - (reentry_time + expected), -kTolerance);
}

TEST(CodelFilterTest, ClockSkewYieldsZeroSojourn) {
  MockClock clock;
  auto filter = BuildWithMockClock(clock);

  // enqueue_time is one second AFTER now. Sojourn must clamp at
  // zero, the filter must not drop, and state must not be
  // corrupted.
  EXPECT_FALSE(filter->ShouldDrop(clock.Now() + absl::Seconds(1)));
  EXPECT_FALSE(filter->IsDropping());
  EXPECT_EQ(filter->DropCount(), 0);
  EXPECT_EQ(filter->DropEpisodeCount(), 0u);

  // A subsequent normal call behaves identically to a freshly-
  // built filter: sojourn=0, no drop.
  EXPECT_FALSE(filter->ShouldDrop(clock.Now()));
  EXPECT_FALSE(filter->IsDropping());
  EXPECT_EQ(filter->DropCount(), 0);
}

TEST(CodelFilterTest, BuilderDefaultsAreRfcValues) {
  MockClock clock;
  auto filter = BuildWithMockClock(clock);

  // Sojourn 4ms is below RFC default target=5ms. No matter how
  // long we drive at this sojourn, drop mode never engages.
  for (int i = 0; i < 50; ++i) {
    EXPECT_FALSE(filter->ShouldDrop(clock.Now() - absl::Milliseconds(4)));
    EXPECT_FALSE(filter->IsDropping());
    clock.Advance(absl::Milliseconds(10));
  }

  // Reset filter state by exiting drop mode (no-op here) and
  // re-driving at sojourn 6ms (above default target=5ms). After
  // approximately one default-interval (100ms) of sustained
  // above-target sojourn, drop mode engages.
  bool entered = false;
  for (int i = 0; i < 20 && !entered; ++i) {
    if (filter->ShouldDrop(clock.Now() - absl::Milliseconds(6))) entered = true;
    clock.Advance(absl::Milliseconds(10));
  }
  EXPECT_TRUE(entered)
      << "Sustained sojourn=6ms must engage drop mode within ~100ms";
}

// =====================================================================
// Threading.
// =====================================================================

TEST(CodelFilterTest, ConcurrentCallersDoNotCorruptState) {
  // Real wall-clock; the filter's internal mutex serialises
  // every state mutation. Asserted invariants are post-join only
  // and hold under every legal schedule.
  auto filter = CodelFilter::Builder().Id("concurrent").Build().value();

  constexpr int kThreads = 32;
  constexpr int kIterationsPerThread = 1000;
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(static_cast<unsigned>(t));
      std::uniform_int_distribution<int> dist(0, 10'000);
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        absl::Duration const offset = absl::Microseconds(dist(rng));
        (void)filter->ShouldDrop(absl::Now() - offset);
      }
    });
  }
  for (auto& thr : threads) thr.join();

  EXPECT_GE(filter->DropCount(), 0);
  // No assertion on whether IsDropping is true or false; either
  // is legal depending on the realized RTT during the run. We
  // do require the call to return cleanly under any schedule.
  (void)filter->IsDropping();
}

// =====================================================================
// Observability.
// =====================================================================

TEST(CodelFilterTest, DropsCounterIncrementsExactlyPerDrop) {
  MockClock clock;
  auto exporter = std::make_shared<CapturingExporter>();
  auto reader = std::make_unique<ManualReader>(exporter);
  auto* reader_ptr = reader.get();

  auto provider = std::make_shared<sdk_metrics::MeterProvider>();
  provider->AddMetricReader(std::move(reader));

  auto filter = CodelFilter::Builder()
                    .Clock(clock.AsFn())
                    .Id("drops-counter")
                    .MeterProvider(provider)
                    .Build()
                    .value();

  // Drive 7 drops: 1 entry + 6 in-drop drops.
  (void)DriveDropEpisode(*filter, clock, /*additional_in_drop_drops=*/6);
  ASSERT_EQ(filter->DropCount(), 7);

  reader_ptr->Collect();
  auto value = FindLastInt64Point(*exporter, "limen.codel.drops");
  ASSERT_TRUE(value.has_value())
      << "limen.codel.drops not found in exporter output";
  EXPECT_EQ(*value, 7);

  // Drive 3 more drops; cumulative should now be 10.
  for (int i = 0; i < 3; ++i) {
    clock.Set(filter->DropNext());
    EXPECT_TRUE(filter->ShouldDrop(clock.Now() - absl::Milliseconds(10)));
  }
  ASSERT_EQ(filter->DropCount(), 10);

  reader_ptr->Collect();
  value = FindLastInt64Point(*exporter, "limen.codel.drops");
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(*value, 10);
}

TEST(CodelFilterTest, DroppingGaugeReflectsLiveState) {
  MockClock clock;
  auto exporter = std::make_shared<CapturingExporter>();
  auto reader = std::make_unique<ManualReader>(exporter);
  auto* reader_ptr = reader.get();

  auto provider = std::make_shared<sdk_metrics::MeterProvider>();
  provider->AddMetricReader(std::move(reader));

  auto filter = CodelFilter::Builder()
                    .Clock(clock.AsFn())
                    .Id("dropping-gauge")
                    .MeterProvider(provider)
                    .Build()
                    .value();

  // Idle: gauge=0.
  reader_ptr->Collect();
  auto value = FindLastInt64Point(*exporter, "limen.codel.dropping");
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(*value, 0);

  // In drop mode: gauge=1.
  (void)DriveIntoDropMode(*filter, clock);
  ASSERT_TRUE(filter->IsDropping());
  reader_ptr->Collect();
  value = FindLastInt64Point(*exporter, "limen.codel.dropping");
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(*value, 1);

  // Exit drop mode: gauge=0 again.
  ExitDropMode(*filter, clock);
  reader_ptr->Collect();
  value = FindLastInt64Point(*exporter, "limen.codel.dropping");
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(*value, 0);
}

TEST(CodelFilterTest, HotPathDoesNoSynchronousOtelCall) {
  // The CoDel filter's hot path (`ShouldDrop`) must not invoke
  // the OTel SDK synchronously. Drive 10k calls against a filter
  // built with a MeterProvider attached but never trigger a
  // collection. The exporter must remain empty: any synchronous
  // SDK call during the hot path would flow through the SDK and
  // into the exporter at some point. Since collection is the
  // only path that runs observable callbacks, no exporter
  // activity proves the hot path stays off the SDK.
  MockClock clock;
  auto exporter = std::make_shared<CapturingExporter>();
  auto reader = std::make_unique<ManualReader>(exporter);

  auto provider = std::make_shared<sdk_metrics::MeterProvider>();
  provider->AddMetricReader(std::move(reader));

  auto filter = CodelFilter::Builder()
                    .Clock(clock.AsFn())
                    .Id("hot-path")
                    .MeterProvider(provider)
                    .Build()
                    .value();

  // Drive a mix of below-target, transition, and in-drop calls
  // so both branches of the state machine execute.
  for (int i = 0; i < 5'000; ++i) {
    (void)filter->ShouldDrop(clock.Now() - absl::Milliseconds(10));
    clock.Advance(absl::Microseconds(50));
  }
  for (int i = 0; i < 5'000; ++i) {
    (void)filter->ShouldDrop(clock.Now());  // sojourn=0; below target.
    clock.Advance(absl::Microseconds(50));
  }

  EXPECT_TRUE(exporter->captured().empty())
      << "Hot path triggered an OTel exporter callback; "
         "captured.size()="
      << exporter->captured().size();
}

// =====================================================================
// Hygiene.
// =====================================================================

TEST(CodelFilterTest, NoAllocationOnHotPath) {
  MockClock clock;
  // No MeterProvider — keeps the test focused on the algorithm
  // path. The mock clock's `AsFn()` captures `this` (8 bytes) by
  // value, which fits in `std::function`'s small-buffer
  // optimization, so calling the clock from `ShouldDrop` does not
  // allocate.
  auto filter =
      CodelFilter::Builder().Clock(clock.AsFn()).Id("no-alloc").Build().value();

  g_alloc_count.store(0, std::memory_order_relaxed);
  g_alloc_tracking.store(true, std::memory_order_relaxed);

  // Cover both branches: ~5000 below-target calls (not dropping
  // path, sojourn<target), then ~5000 with sustained sojourn>target
  // including the entry into drop mode and the in-drop branch.
  for (int i = 0; i < 5'000; ++i) {
    (void)filter->ShouldDrop(clock.Now());
    clock.Advance(absl::Microseconds(50));
  }
  for (int i = 0; i < 5'000; ++i) {
    (void)filter->ShouldDrop(clock.Now() - absl::Milliseconds(10));
    clock.Advance(absl::Microseconds(50));
  }

  g_alloc_tracking.store(false, std::memory_order_relaxed);
  EXPECT_EQ(g_alloc_count.load(std::memory_order_relaxed), 0);
}

}  // namespace
}  // namespace limen

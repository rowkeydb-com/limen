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

#include "limen/windowed_limit.h"
#include "limen/limit.h"
#include "absl/status/status.h"
#include "gtest/gtest.h"
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

// Allocation counter for `NoAllocationAfterConstruction`. The
// `tracking` flag is set after the limiter is constructed and reset
// before joining the test; while it is set every global `new` /
// `new[]` call increments the count.
//
// The replacements cover only the sized, non-aligned forms. None of
// the types reachable from `OnSample` are over-aligned, so the
// aligned overloads are never selected by the compiler today. If a
// future change adds an `alignas(N > __STDCPP_DEFAULT_NEW_ALIGNMENT__)`
// member, the aligned `operator new(size_t, std::align_val_t)`
// overload must be added here too or this test will silently
// stop counting that allocation.
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

// Test-only Limit that only counts calls. Useful for tests where
// the delegate must not allocate (it never pushes onto a vector).
class NoAllocLimit final : public Limit {
 public:
  int GetLimit() const override { return 100; }

  void OnSample(int64_t /*start_time_ns*/, int64_t /*rtt_ns*/, int /*inflight*/,
                bool /*did_drop*/) override {
    ++count;
  }

  void NotifyOnChange(ChangeCallback /*callback*/) override {}

  int count = 0;
};

// Test-only Limit that records every OnSample call so tests can
// assert what the wrapped algorithm sees.
class CountingLimit final : public Limit {
 public:
  int GetLimit() const override { return 100; }

  void OnSample(int64_t start_time_ns, int64_t rtt_ns, int inflight,
                bool did_drop) override {
    ++count;
    last_start_time_ns = start_time_ns;
    last_rtt_ns = rtt_ns;
    last_inflight = inflight;
    last_did_drop = did_drop;
    fire_start_times.push_back(start_time_ns);
  }

  void NotifyOnChange(ChangeCallback /*callback*/) override {}

  int count = 0;
  int64_t last_start_time_ns = 0;
  int64_t last_rtt_ns = 0;
  int last_inflight = 0;
  bool last_did_drop = false;
  std::vector<int64_t> fire_start_times;
};

TEST(WindowedLimitTest, BuildRejectsNonPositiveMinWindowTime) {
  auto delegate = std::make_unique<CountingLimit>();
  auto result = WindowedLimit::Builder()
                    .MinWindowTimeNs(0)
                    .MaxWindowTimeNs(1'000'000)
                    .Build(std::move(delegate));
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("MinWindowTimeNs"),
            std::string::npos);
}

TEST(WindowedLimitTest, BuildRejectsNonPositiveMaxWindowTime) {
  auto delegate = std::make_unique<CountingLimit>();
  auto result = WindowedLimit::Builder()
                    .MinWindowTimeNs(1'000'000)
                    .MaxWindowTimeNs(0)
                    .Build(std::move(delegate));
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("MaxWindowTimeNs"),
            std::string::npos);
}

TEST(WindowedLimitTest, BuildRejectsInvertedMinMaxWindowTime) {
  auto delegate = std::make_unique<CountingLimit>();
  auto result = WindowedLimit::Builder()
                    .MinWindowTimeNs(2'000'000)
                    .MaxWindowTimeNs(1'000'000)
                    .Build(std::move(delegate));
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("must not exceed"),
            std::string::npos);
}

TEST(WindowedLimitTest, BuildRejectsZeroWindowSize) {
  auto delegate = std::make_unique<CountingLimit>();
  auto result = WindowedLimit::Builder()
                    .MinWindowTimeNs(1'000'000)
                    .MaxWindowTimeNs(1'000'000)
                    .WindowSize(0)
                    .Build(std::move(delegate));
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("WindowSize"), std::string::npos);
}

TEST(WindowedLimitTest, BuildRejectsNegativeMinRttThreshold) {
  auto delegate = std::make_unique<CountingLimit>();
  auto result = WindowedLimit::Builder()
                    .MinWindowTimeNs(1'000'000)
                    .MaxWindowTimeNs(1'000'000)
                    .MinRttThresholdNs(-1)
                    .Build(std::move(delegate));
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("MinRttThresholdNs"),
            std::string::npos);
}

TEST(WindowedLimitTest, SamplesBelowMinRttThresholdAreDropped) {
  auto delegate = std::make_unique<CountingLimit>();
  auto* counting = delegate.get();
  auto windowed = WindowedLimit::Builder()
                      .MinWindowTimeNs(1'000'000)
                      .MaxWindowTimeNs(1'000'000)
                      .WindowSize(5)
                      .Build(std::move(delegate))
                      .value();

  // Sub-threshold samples (default threshold is 100µs).
  for (int i = 0; i < 200; ++i) {
    windowed->OnSample(static_cast<int64_t>(i) * 50'000, 50'000, 1, false);
  }
  EXPECT_EQ(counting->count, 0);
}

TEST(WindowedLimitTest, WindowNotReadyUntilWindowSizeReached) {
  auto delegate = std::make_unique<CountingLimit>();
  auto* counting = delegate.get();
  auto windowed = WindowedLimit::Builder()
                      .MinWindowTimeNs(1'000'000)
                      .MaxWindowTimeNs(1'000'000)
                      .WindowSize(20)
                      .Build(std::move(delegate))
                      .value();

  for (int i = 0; i < 5; ++i) {
    windowed->OnSample(static_cast<int64_t>(i) * 200'000, 500'000, 1, false);
  }
  windowed->OnSample(2'000'000, 500'000, 1, false);
  EXPECT_EQ(counting->count, 0);
}

TEST(WindowedLimitTest, WindowReadyAfterSamplesAndTime) {
  auto delegate = std::make_unique<CountingLimit>();
  auto* counting = delegate.get();
  auto windowed = WindowedLimit::Builder()
                      .MinWindowTimeNs(1'000'000)
                      .MaxWindowTimeNs(1'000'000)
                      .WindowSize(10)
                      .Build(std::move(delegate))
                      .value();

  // Sample 0 triggers a warmup boundary (1 sample, not ready); the
  // active index flips. Samples 1..11 land in the new active slot,
  // and sample 11 crosses the 1ms boundary. At that boundary the
  // slot has 11 samples, exceeds window_size=10, and the delegate
  // fires exactly once. The remaining samples and the final force
  // sample fall short of window_size on the next slot.
  for (int i = 0; i < 20; ++i) {
    windowed->OnSample(static_cast<int64_t>(i) * 100'000, 500'000, 1, false);
  }
  windowed->OnSample(3'000'000, 500'000, 1, false);
  EXPECT_EQ(counting->count, 1);
  EXPECT_EQ(counting->last_rtt_ns, 500'000);
  EXPECT_EQ(counting->last_inflight, 1);
  EXPECT_FALSE(counting->last_did_drop);
}

TEST(WindowedLimitTest, BoundarySwapResetsInactive) {
  auto delegate = std::make_unique<CountingLimit>();
  auto* counting = delegate.get();
  auto windowed = WindowedLimit::Builder()
                      .MinWindowTimeNs(1'000'000)
                      .MaxWindowTimeNs(1'000'000)
                      .WindowSize(5)
                      .Build(std::move(delegate))
                      .value();

  for (int i = 0; i < 10; ++i) {
    windowed->OnSample(static_cast<int64_t>(i) * 100'000, 100'000, 1, false);
  }
  windowed->OnSample(3'000'000, 100'000, 1, false);
  EXPECT_EQ(counting->count, 1);
  EXPECT_EQ(counting->last_rtt_ns, 100'000);

  // Fill the second window with a different RTT. If the first
  // window's samples leaked over (slot not reset), the average
  // would come out between the two values. The second-window mean
  // equals the new sample RTT exactly because the inactive slot
  // was reset.
  for (int i = 0; i < 10; ++i) {
    windowed->OnSample(3'100'000 + static_cast<int64_t>(i) * 100'000, 800'000,
                       1, false);
  }
  windowed->OnSample(6'000'000, 800'000, 1, false);
  EXPECT_EQ(counting->count, 2);
  EXPECT_EQ(counting->last_rtt_ns, 800'000);
}

TEST(WindowedLimitTest, AdaptiveWindowSize) {
  // Two limiters with identical adaptive bounds but driven at
  // different RTTs. The adaptive window formula is
  // `clamp(candidate * 2, min, max)`. With RTT=2ms the window
  // settles at 4ms; with RTT=10ms the window stretches to the
  // max=10ms cap. Over the same total drive time, the high-RTT
  // limiter must produce fewer delegate fires than the low-RTT
  // one.
  auto fast_delegate = std::make_unique<CountingLimit>();
  auto* fast = fast_delegate.get();
  auto fast_limiter = WindowedLimit::Builder()
                          .MinWindowTimeNs(1'000'000)
                          .MaxWindowTimeNs(10'000'000)
                          .WindowSize(10)
                          .Build(std::move(fast_delegate))
                          .value();
  auto slow_delegate = std::make_unique<CountingLimit>();
  auto* slow = slow_delegate.get();
  auto slow_limiter = WindowedLimit::Builder()
                          .MinWindowTimeNs(1'000'000)
                          .MaxWindowTimeNs(10'000'000)
                          .WindowSize(10)
                          .Build(std::move(slow_delegate))
                          .value();

  for (int i = 0; i < 1000; ++i) {
    int64_t const t = static_cast<int64_t>(i) * 100'000;
    fast_limiter->OnSample(t, 2'000'000, 1, false);
    slow_limiter->OnSample(t, 10'000'000, 1, false);
  }
  EXPECT_GT(fast->count, slow->count);

  // The high-RTT window saturates against the max-window-time cap.
  // The low-RTT window does not. Verify the low-RTT window's actual
  // observed inter-fire spacing is below the high-RTT one's, which
  // is the operational signature of adaptive sizing.
  ASSERT_GE(fast->fire_start_times.size(), 2u);
  ASSERT_GE(slow->fire_start_times.size(), 2u);
  int64_t const fast_gap =
      fast->fire_start_times[1] - fast->fire_start_times[0];
  int64_t const slow_gap =
      slow->fire_start_times[1] - slow->fire_start_times[0];
  EXPECT_LT(fast_gap, slow_gap);
}

TEST(WindowedLimitTest, GetLimitDelegates) {
  auto delegate = std::make_unique<CountingLimit>();
  auto windowed = WindowedLimit::Builder().Build(std::move(delegate)).value();
  EXPECT_EQ(windowed->GetLimit(), 100);
}

TEST(WindowedLimitTest, NoAllocationAfterConstruction) {
  // Pins the design's allocation-free contract for the average
  // sample window path. Use the NoAllocLimit delegate (which
  // doesn't grow any container) so any allocation observed by the
  // counter has to come from WindowedLimit or AverageSampleWindow.
  // Build the limiter, switch the global allocation counter on,
  // drive ten thousand samples through many boundary crossings,
  // switch the counter off, and verify zero heap allocations
  // occurred while the counter was live.
  auto delegate = std::make_unique<NoAllocLimit>();
  auto windowed = WindowedLimit::Builder()
                      .MinWindowTimeNs(1'000'000)
                      .MaxWindowTimeNs(1'000'000)
                      .WindowSize(5)
                      .Build(std::move(delegate))
                      .value();

  g_alloc_count.store(0, std::memory_order_relaxed);
  g_alloc_tracking.store(true, std::memory_order_relaxed);

  for (int i = 0; i < 10'000; ++i) {
    windowed->OnSample(static_cast<int64_t>(i) * 100, 500'000, 1, false);
  }

  g_alloc_tracking.store(false, std::memory_order_relaxed);
  EXPECT_EQ(g_alloc_count.load(std::memory_order_relaxed), 0);
}

TEST(WindowedLimitTest, ConcurrentSamplesAcrossBoundary) {
  // Many threads send samples continuously while the boundary
  // fires repeatedly. The design accepts a narrow race-loss
  // window where a late writer's sample is overwritten by the
  // boundary thread's reset; the loss is statistically tiny.
  // Drive 8 threads × 2000 samples = 16'000 calls and verify the
  // run completes cleanly under TSan. The boundary count is
  // unbounded but should be well above zero.
  auto delegate = std::make_unique<CountingLimit>();
  auto* counting = delegate.get();
  auto windowed = WindowedLimit::Builder()
                      .MinWindowTimeNs(1'000'000)
                      .MaxWindowTimeNs(1'000'000)
                      .WindowSize(5)
                      .Build(std::move(delegate))
                      .value();

  constexpr int kThreads = 8;
  constexpr int kSamplesPerThread = 2000;
  std::atomic<int> ready{0};
  std::atomic<int64_t> time_base{0};

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      ready.fetch_add(1);
      while (ready.load() < kThreads) {
      }
      for (int i = 0; i < kSamplesPerThread; ++i) {
        int64_t const t_ns =
            time_base.fetch_add(1000, std::memory_order_relaxed);
        windowed->OnSample(t_ns, 500'000, 1, false);
      }
    });
  }
  for (auto& thr : threads) {
    thr.join();
  }

  // Several boundaries should have fired. The exact count depends
  // on scheduling; the key invariant is that the run completed and
  // some boundaries delivered the aggregate to the delegate.
  EXPECT_GT(counting->count, 0);
}

TEST(WindowedLimitTest, BoundaryThreadWinsTryLockExclusively) {
  // Many threads cross the same boundary simultaneously. The
  // try-lock + double-check pattern in OnSample must guarantee
  // that the wrapped algorithm sees exactly one OnSample call per
  // boundary, not one per crossing thread.
  auto delegate = std::make_unique<CountingLimit>();
  auto* counting = delegate.get();
  auto windowed = WindowedLimit::Builder()
                      .MinWindowTimeNs(1'000'000)
                      .MaxWindowTimeNs(1'000'000)
                      .WindowSize(5)
                      .Build(std::move(delegate))
                      .value();

  // Warm up the active slot with enough samples that a boundary
  // crossing will find it ready (>= window_size = 5). Sample 0
  // crosses the very first boundary (end_time=500'000 > initial
  // next_update_time=0); slot 0 holds one sample at that point,
  // which is below window_size, so the delegate does not fire.
  // The boundary thread flips active to slot 1 and resets slot 0.
  // Samples 1..9 then land in slot 1; their end_times stay below
  // the next scheduled boundary at 1'500'000, so no further
  // boundary fires during warmup. Slot 1 ends with 9 samples.
  for (int i = 0; i < 10; ++i) {
    windowed->OnSample(static_cast<int64_t>(i) * 50'000, 500'000, 1, false);
  }
  ASSERT_EQ(counting->count, 0) << "Warmup must not have driven the delegate";

  // Now N threads all cross the 1ms boundary with the same end
  // time. Only one of them can win the try-lock; the others
  // observe the lock held and bail out without driving the
  // delegate.
  constexpr int kThreads = 16;
  std::atomic<int> ready{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&] {
      ready.fetch_add(1);
      while (ready.load() < kThreads) {
      }
      windowed->OnSample(/*start_time_ns=*/5'000'000, /*rtt_ns=*/500'000, 1,
                         false);
    });
  }
  for (auto& thr : threads) {
    thr.join();
  }

  // Exactly one delegate fire for this boundary crossing.
  EXPECT_EQ(counting->count, 1);
}

}  // namespace
}  // namespace limen

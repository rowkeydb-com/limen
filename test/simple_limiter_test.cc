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

#include "limen/simple_limiter.h"
#include "limen/fixed_limit.h"
#include "limen/settable_limit.h"
#include "gtest/gtest.h"
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <latch>
#include <memory>
#include <optional>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

// Allocation counter for `NoAllocationOnAcquireRelease`. Toggled
// around the timed loop so unrelated allocations from gtest, the
// SDK constructor, etc. do not pollute the count. The replacements
// cover only the sized, non-aligned forms; no type reachable from
// `TryAcquire` / SlotGuard completion is currently over-aligned.
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

TEST(SimpleLimiterTest, AcquiresUpToCap) {
  auto limiter =
      SimpleLimiter::Builder().Limit(FixedLimit::Of(3)).Id("test").Build();
  auto a = limiter->TryAcquire();
  auto b = limiter->TryAcquire();
  auto c = limiter->TryAcquire();
  auto d = limiter->TryAcquire();
  EXPECT_TRUE(a);
  EXPECT_TRUE(b);
  EXPECT_TRUE(c);
  EXPECT_FALSE(d);
}

TEST(SimpleLimiterTest, ReleaseReturnsSlot) {
  auto limiter =
      SimpleLimiter::Builder().Limit(FixedLimit::Of(2)).Id("test").Build();
  auto a = limiter->TryAcquire();
  auto b = limiter->TryAcquire();
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  EXPECT_FALSE(limiter->TryAcquire());

  a->OnSuccess();
  auto c = limiter->TryAcquire();
  EXPECT_TRUE(c);
}

TEST(SimpleLimiterTest, SlotGuardDestructorDefaultsToOnIgnore) {
  auto limiter =
      SimpleLimiter::Builder().Limit(FixedLimit::Of(5)).Id("test").Build();
  {
    auto slot = limiter->TryAcquire();
    ASSERT_TRUE(slot);
    EXPECT_EQ(limiter->InflightCount(), 1);
    // Slot falls out of scope without an explicit completion call.
  }
  EXPECT_EQ(limiter->InflightCount(), 0);
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kIgnored), 1);
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kSuccess), 0);
}

TEST(SimpleLimiterTest, CounterConservation) {
  auto limiter =
      SimpleLimiter::Builder().Limit(FixedLimit::Of(20)).Id("test").Build();
  constexpr int kThreads = 8;
  constexpr int kIterationsPerThread = 1000;

  // std::latch is futex-backed on Linux. Using a busy-wait spinlock
  // here interacts pathologically with TSan: every spin iteration
  // is an instrumented atomic load that walks the vector clock,
  // and a thread parked on the spin holds the core off the
  // not-yet-scheduled threads that would advance the count.
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire();
        if (slot) {
          slot->OnSuccess();
        }
      }
    });
  }
  for (auto& thr : threads) {
    thr.join();
  }
  EXPECT_EQ(limiter->InflightCount(), 0)
      << "All admitted slots must release back to zero";
}

TEST(SimpleLimiterTest, NeverExceedsCapUnderContention) {
  constexpr int kCap = 5;
  auto limiter =
      SimpleLimiter::Builder().Limit(FixedLimit::Of(kCap)).Id("test").Build();
  // The contention surface is min(kThreads, kCap + 1) — only that
  // many threads can be racing on the CAS gate at any one moment.
  // 32 threads against a cap of 5 is plenty of pressure to exercise
  // the no-transient-overshoot property under TSan.
  constexpr int kThreads = 32;
  constexpr int kIterationsPerThread = 500;

  std::atomic<int> peak_observed{0};
  std::atomic<bool> overshoot_detected{false};

  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire();
        if (slot) {
          int const live = limiter->InflightCount();
          if (live > kCap) {
            overshoot_detected.store(true);
          }
          int prev = peak_observed.load();
          while (live > prev &&
                 !peak_observed.compare_exchange_weak(prev, live)) {
          }
          slot->OnSuccess();
        }
      }
    });
  }
  for (auto& thr : threads) {
    thr.join();
  }
  EXPECT_FALSE(overshoot_detected.load())
      << "In-flight count exceeded the cap; CAS gate is broken";
  EXPECT_LE(peak_observed.load(), kCap);
  EXPECT_EQ(limiter->InflightCount(), 0);
}

TEST(SimpleLimiterTest, BypassPredicateMatchedBypasses) {
  auto bypass = [](std::string_view ctx) { return ctx == "skip"; };
  auto limiter = SimpleLimiter::Builder()
                     .Limit(FixedLimit::Of(1))
                     .Id("test")
                     .BypassPredicate(bypass)
                     .Build();
  // Saturate the gate with a normal admission.
  auto blocker = limiter->TryAcquire("normal");
  ASSERT_TRUE(blocker);
  EXPECT_EQ(limiter->InflightCount(), 1);

  // A bypass-tagged call admits despite the cap being full.
  auto bypassed = limiter->TryAcquire("skip");
  ASSERT_TRUE(bypassed);
  EXPECT_EQ(limiter->InflightCount(), 1)
      << "Bypass must not increment the in-flight counter";

  bypassed->OnSuccess();
  EXPECT_EQ(limiter->InflightCount(), 1);
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kBypassed), 1);
}

TEST(SimpleLimiterTest, BypassPredicateUnmatchedAdmitsNormally) {
  auto bypass = [](std::string_view ctx) { return ctx == "skip"; };
  auto limiter = SimpleLimiter::Builder()
                     .Limit(FixedLimit::Of(1))
                     .Id("test")
                     .BypassPredicate(bypass)
                     .Build();
  auto a = limiter->TryAcquire("normal");
  EXPECT_TRUE(a);
  EXPECT_EQ(limiter->InflightCount(), 1);

  // Bypass predicate doesn't match; gate enforces the cap.
  auto b = limiter->TryAcquire("normal");
  EXPECT_FALSE(b);
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kRejected), 1);
}

TEST(SimpleLimiterTest, ManyThreadsManyIterations) {
  // Stress test ported from upstream's testConcurrentSimple
  // (upstream uses 8 threads x 1000 iterations against cap=10).
  // 32 x 500 here exceeds upstream's coverage at a thread count
  // that TSan's quadratic instrumentation cost can still handle.
  constexpr int kCap = 10;
  auto limiter =
      SimpleLimiter::Builder().Limit(FixedLimit::Of(kCap)).Id("test").Build();
  constexpr int kThreads = 32;
  constexpr int kIterationsPerThread = 500;

  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire();
        if (slot) {
          slot->OnSuccess();
        }
      }
    });
  }
  for (auto& thr : threads) {
    thr.join();
  }
  EXPECT_EQ(limiter->InflightCount(), 0);
}

TEST(SimpleLimiterTest, NoAllocationOnAcquireRelease) {
  auto limiter =
      SimpleLimiter::Builder().Limit(FixedLimit::Of(100)).Id("test").Build();

  constexpr int kIterations = 10000;
  g_alloc_count.store(0, std::memory_order_relaxed);
  g_alloc_tracking.store(true, std::memory_order_relaxed);
  for (int i = 0; i < kIterations; ++i) {
    auto slot = limiter->TryAcquire();
    if (slot) {
      slot->OnSuccess();
    }
  }
  g_alloc_tracking.store(false, std::memory_order_relaxed);
  EXPECT_EQ(g_alloc_count.load(std::memory_order_relaxed), 0);
}

TEST(SimpleLimiterTest, AdaptsToLimitChange) {
  auto wrapped = SettableLimit::StartingAt(5);
  auto* settable = wrapped.get();
  auto limiter =
      SimpleLimiter::Builder().Limit(std::move(wrapped)).Id("test").Build();

  // Acquire up to the initial cap.
  std::vector<std::optional<Limiter::SlotGuard>> slots;
  slots.reserve(5);
  for (int i = 0; i < 5; ++i) {
    slots.push_back(limiter->TryAcquire());
    ASSERT_TRUE(slots.back());
  }
  EXPECT_FALSE(limiter->TryAcquire());

  // Raise the cap; the next acquire succeeds without releasing.
  settable->SetLimit(10);
  auto extra = limiter->TryAcquire();
  EXPECT_TRUE(extra);

  // Lower the cap below current in-flight; new acquires reject
  // until enough slots release.
  settable->SetLimit(2);
  EXPECT_FALSE(limiter->TryAcquire());
}

}  // namespace
}  // namespace limen

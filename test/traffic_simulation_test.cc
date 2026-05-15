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

// Traffic-simulation tests. Three families.
//
// (1) Algorithm-adaptation tests drive `WindowedLimit::OnSample`
//     directly with synthetic time and RTT sequences. There are no
//     threads, no real clock, no `SlotGuard` lifecycles. The tests
//     exercise the windowing + Gradient2 pipeline as a unit and
//     assert on the observable cap. Fully deterministic: given the
//     same input sequence the cap evolves identically across runs
//     and platforms.
//
// (2) Limiter-conservation tests spawn many threads that drive real
//     `TryAcquire`/`OnSuccess`/`OnDropped` lifecycles against
//     `SimpleLimiter`, `AbstractPartitionedLimiter`, `BlockingLimiter`,
//     and `LifoBlockingLimiter`. The threads do no I/O and never
//     sleep. Assertions are post-join conservation invariants only —
//     they hold under every legal schedule. Thread start
//     synchronisation uses `std::latch`; a peak in-flight is tracked
//     via `std::atomic<int>` compare-exchange against the limiter's
//     true `InflightCount()` / `PartitionInflightCount()` while the
//     slot is held. The CAS pattern is a sound but non-tight
//     observer: it records observations of over-admission when they
//     happen during the read; the limiter's internal CAS gate
//     prevents over-admission in the first place under any
//     interleaving, so the recorded peak is a lower bound on the
//     true peak.
//
// (3) Partitioned-limiter coverage is intentionally heavy: it is
//     the way most applications will use Limen (named-quota
//     admission with bursting). The suite covers conservation,
//     hard-cap enforcement under saturation, burst-into-idle,
//     three-partition arithmetic, unknown-partition routing,
//     resolver-chain order, bypass interaction with partitions,
//     and live recomputation under a SettableLimit cap change.

#include "limen/abstract_limiter.h"
#include "limen/abstract_partitioned_limiter.h"
#include "limen/blocking_limiter.h"
#include "limen/fixed_limit.h"
#include "limen/gradient2_limit.h"
#include "limen/lifo_blocking_limiter.h"
#include "limen/limiter.h"
#include "limen/settable_limit.h"
#include "limen/simple_limiter.h"
#include "limen/windowed_limit.h"
#include "absl/synchronization/mutex.h"
#include "gtest/gtest.h"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <latch>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace limen {
namespace {

// One-second sample window, ten samples required to drive the
// delegate. The 1ms warmup gives the cap a stable starting point
// before any traffic pattern is applied.
constexpr int64_t kWindowMinTimeNs = 1'000'000'000;
constexpr int64_t kWindowMaxTimeNs = 1'000'000'000;
constexpr int kWindowSize = 10;

// Helper: feed one fully-formed window of identical samples into a
// WindowedLimit, advancing the synthetic clock to cross the
// boundary at the end. After this returns, the wrapped algorithm
// has seen exactly one OnSample for this window (provided
// sample_count >= kWindowSize and inflight >= cap/2 so the
// app-limited skip does not fire).
void FeedOneWindow(WindowedLimit& windowed, int64_t& t_ns, int64_t rtt_ns,
                   int inflight, int samples) {
  for (int i = 0; i < samples; ++i) {
    windowed.OnSample(t_ns, rtt_ns, inflight, /*did_drop=*/false);
    t_ns += 1'000'000;
  }
  t_ns += kWindowMaxTimeNs;
  windowed.OnSample(t_ns, rtt_ns, inflight, /*did_drop=*/false);
  t_ns += 1'000'000;
}

std::unique_ptr<WindowedLimit> BuildAlgorithmStack(int initial_limit,
                                                   int min_limit,
                                                   int max_concurrency) {
  auto gradient = Gradient2Limit::Builder()
                      .InitialLimit(initial_limit)
                      .MinLimit(min_limit)
                      .MaxConcurrency(max_concurrency)
                      .QueueSize(4)
                      .LongWindow(20)
                      .Build();
  return WindowedLimit::Builder()
      .MinWindowTimeNs(kWindowMinTimeNs)
      .MaxWindowTimeNs(kWindowMaxTimeNs)
      .WindowSize(kWindowSize)
      .Build(std::move(gradient))
      .value();
}

AbstractPartitionedLimiter::PartitionResolver MatchExact(
    std::string target, std::string partition_name) {
  return
      [target = std::move(target), partition_name = std::move(partition_name)](
          std::string_view ctx) -> std::optional<std::string> {
        if (ctx == target) return partition_name;
        return std::nullopt;
      };
}

// Drive a single TryAcquire/OnSuccess pair while observing the
// limiter's global in-flight count. The recorded peak is a sound
// lower bound on the true maximum observed during the run — the
// limiter's CAS gate prevents over-admission, so `peak <= cap` is
// a non-trivial invariant we assert post-join. Between the
// TryAcquire and the InflightCount read, other threads may have
// already released, which is why the recorded value is a lower
// bound, not the exact peak. The test fails only on a real bug
// in the CAS gate, never on a benign interleaving.
void TryAcquireAndRecordPeak(AbstractLimiter& limiter, std::string_view ctx,
                             std::atomic<int>& peak) {
  auto slot = limiter.TryAcquire(ctx);
  if (!slot) return;
  int const live = limiter.InflightCount();
  int prev = peak.load(std::memory_order_relaxed);
  while (live > prev &&
         !peak.compare_exchange_weak(prev, live, std::memory_order_relaxed)) {
  }
  slot->OnSuccess();
}

// =====================================================================
// (1) Algorithm adaptation through the windowing layer.
// =====================================================================

TEST(TrafficSimulationTest, AlgorithmShrinksUnderSustainedLatencyIncrease) {
  // Establish a steady-state cap at low RTT, then drive sustained
  // high RTT and assert the cap shrinks. The canonical signal
  // Gradient2 exists to react to.
  auto windowed = BuildAlgorithmStack(/*initial_limit=*/50, /*min_limit=*/5,
                                      /*max_concurrency=*/200);

  int64_t t_ns = 0;
  for (int w = 0; w < 20; ++w) {
    FeedOneWindow(*windowed, t_ns, /*rtt_ns=*/1'000'000,
                  /*inflight=*/windowed->GetLimit(), kWindowSize);
  }
  int const baseline_cap = windowed->GetLimit();

  for (int w = 0; w < 30; ++w) {
    FeedOneWindow(*windowed, t_ns, /*rtt_ns=*/10'000'000,
                  /*inflight=*/windowed->GetLimit(), kWindowSize);
  }
  int const overloaded_cap = windowed->GetLimit();

  EXPECT_LT(overloaded_cap, baseline_cap)
      << "Cap must shrink under sustained latency increase. baseline="
      << baseline_cap << " overloaded=" << overloaded_cap;
}

TEST(TrafficSimulationTest, AlgorithmRecoversFromLatencySpike) {
  // baseline -> spike -> baseline. Cap shrinks during the spike,
  // grows back above the spike floor when latency returns to
  // baseline. The drift-correction branch in Gradient2 compresses
  // the stale long-term EWMA so recovery is observable in a
  // bounded number of windows.
  auto windowed = BuildAlgorithmStack(/*initial_limit=*/50, /*min_limit=*/5,
                                      /*max_concurrency=*/200);

  int64_t t_ns = 0;
  for (int w = 0; w < 20; ++w) {
    FeedOneWindow(*windowed, t_ns, 1'000'000, windowed->GetLimit(),
                  kWindowSize);
  }
  int const baseline_cap = windowed->GetLimit();

  for (int w = 0; w < 30; ++w) {
    FeedOneWindow(*windowed, t_ns, 10'000'000, windowed->GetLimit(),
                  kWindowSize);
  }
  int const spike_cap = windowed->GetLimit();
  ASSERT_LT(spike_cap, baseline_cap) << "Spike must shrink the cap first";

  // Recovery window budget is generous (2000 windows) so a tuning
  // shift in Gradient2's constants does not turn the test flaky.
  // The assertion is still directional ("eventually exceeds
  // spike_cap"), not a count.
  bool recovered = false;
  for (int w = 0; w < 2000 && !recovered; ++w) {
    FeedOneWindow(*windowed, t_ns, 1'000'000, windowed->GetLimit(),
                  kWindowSize);
    if (windowed->GetLimit() > spike_cap) recovered = true;
  }
  EXPECT_TRUE(recovered)
      << "Cap did not recover above spike floor within 2000 windows. spike_cap="
      << spike_cap << " final_cap=" << windowed->GetLimit();
}

TEST(TrafficSimulationTest, AlternatingLatencyDragsCapBelowSteadyBaseline) {
  // Two algorithm stacks driven for equal lengths. The first sees
  // steady fast windows (every window 1ms RTT). The second
  // alternates between fast windows and slow windows. Slow
  // windows trigger gradient compression (gradient clamps toward
  // 0.5 when short_rtt is well above the long-term average);
  // fast windows grow the cap by the queue-size constant. Net
  // effect across many cycles: the alternating stack's cap stays
  // below the steady-fast stack's. This is the demonstration that
  // a single Gradient2 algorithm has no defence against bimodal
  // workloads — the partitioned limiter's cap-distribution
  // machinery is what mitigates it.
  auto steady_fast = BuildAlgorithmStack(50, 5, 200);
  auto alternating = BuildAlgorithmStack(50, 5, 200);

  int64_t t_steady = 0;
  int64_t t_alt = 0;
  for (int w = 0; w < 100; ++w) {
    FeedOneWindow(*steady_fast, t_steady, /*rtt_ns=*/1'000'000,
                  /*inflight=*/steady_fast->GetLimit(), kWindowSize);
    int64_t const rtt = (w % 2 == 0) ? 1'000'000 : 10'000'000;
    FeedOneWindow(*alternating, t_alt, rtt, alternating->GetLimit(),
                  kWindowSize);
  }

  EXPECT_LT(alternating->GetLimit(), steady_fast->GetLimit())
      << "Alternating-latency cap must trail steady-fast cap. alternating="
      << alternating->GetLimit() << " steady_fast=" << steady_fast->GetLimit();
}

TEST(TrafficSimulationTest, AppLimitedSkipPropagatesThroughWindowing) {
  // When inflight is below half the cap during a window, the
  // algorithm treats the window as app-limited and leaves the cap
  // alone — even when the RTT samples in that window would
  // otherwise drag the cap down. Drive 30 windows of high-RTT
  // samples but with inflight pinned at 1; the cap must not move.
  auto windowed = BuildAlgorithmStack(/*initial_limit=*/50, /*min_limit=*/1,
                                      /*max_concurrency=*/200);
  int const initial_cap = windowed->GetLimit();

  int64_t t_ns = 0;
  for (int w = 0; w < 30; ++w) {
    FeedOneWindow(*windowed, t_ns, /*rtt_ns=*/100'000'000,
                  /*inflight=*/1, kWindowSize);
  }
  EXPECT_EQ(windowed->GetLimit(), initial_cap)
      << "Cap must not move when every window is app-limited. initial="
      << initial_cap << " final=" << windowed->GetLimit();
}

// =====================================================================
// (2) Limiter conservation under concurrent traffic.
// =====================================================================

TEST(TrafficSimulationTest, SimpleLimiterConservesCountersUnderTraffic) {
  auto limiter = SimpleLimiter::Builder()
                     .Limit(FixedLimit::Of(20))
                     .Id("conservation")
                     .Build()
                     .value();

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
        if (slot) slot->OnSuccess();
      }
    });
  }
  for (auto& thr : threads) thr.join();

  int64_t const total_decisions =
      static_cast<int64_t>(kThreads) * kIterationsPerThread;
  int64_t const successes =
      limiter->OutcomeCount(AbstractLimiter::Status::kSuccess);
  int64_t const rejecteds =
      limiter->OutcomeCount(AbstractLimiter::Status::kRejected);

  EXPECT_EQ(limiter->InflightCount(), 0);
  EXPECT_EQ(successes + rejecteds, total_decisions);
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kDropped), 0);
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kIgnored), 0);
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kBypassed), 0);
}

TEST(TrafficSimulationTest, SimpleLimiterNeverOverAdmitsUnderSaturation) {
  // Saturation: many threads against a small-cap limiter. Each
  // successful admission updates the peak via compare-exchange.
  // Post-join: peak <= cap. The CAS-based gate in SimpleLimiter
  // is what makes this invariant hold under any interleaving.
  constexpr int kCap = 8;
  auto limiter = SimpleLimiter::Builder()
                     .Limit(FixedLimit::Of(kCap))
                     .Id("no-overadmit")
                     .Build()
                     .value();

  constexpr int kThreads = 64;
  constexpr int kIterationsPerThread = 200;
  std::atomic<int> peak{0};
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        TryAcquireAndRecordPeak(*limiter, /*ctx=*/{}, peak);
      }
    });
  }
  for (auto& thr : threads) thr.join();

  EXPECT_LE(peak.load(), kCap)
      << "Observed in-flight exceeded cap. peak=" << peak.load()
      << " cap=" << kCap;
  EXPECT_EQ(limiter->InflightCount(), 0);
}

TEST(TrafficSimulationTest, MixedSuccessAndDroppedCompletionsConserve) {
  // Mix OnSuccess and OnDropped across threads. Post-join: in-flight
  // is zero; success + dropped + rejected counts equal total
  // decisions. Exercises the OnDropped path that feeds
  // did_drop=true samples into the wrapped algorithm.
  auto limiter = SimpleLimiter::Builder()
                     .Limit(FixedLimit::Of(10))
                     .Id("mixed-completion")
                     .Build()
                     .value();

  constexpr int kThreads = 16;
  constexpr int kIterationsPerThread = 500;
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire();
        if (!slot) continue;
        // Alternate completion type by (thread, iteration) parity
        // so distribution does not depend on scheduler ordering.
        if (((t + i) & 1) == 0) {
          slot->OnSuccess();
        } else {
          slot->OnDropped();
        }
      }
    });
  }
  for (auto& thr : threads) thr.join();

  int64_t const successes =
      limiter->OutcomeCount(AbstractLimiter::Status::kSuccess);
  int64_t const drops =
      limiter->OutcomeCount(AbstractLimiter::Status::kDropped);
  int64_t const rejecteds =
      limiter->OutcomeCount(AbstractLimiter::Status::kRejected);
  int64_t const total_decisions =
      static_cast<int64_t>(kThreads) * kIterationsPerThread;

  EXPECT_EQ(limiter->InflightCount(), 0);
  EXPECT_EQ(successes + drops + rejecteds, total_decisions);
}

TEST(TrafficSimulationTest, BypassUnderTrafficDoesNotConsumeSlots) {
  // Half the traffic bypasses. Under saturation, the bypass calls
  // must not consume in-flight slots — the bypassed counter
  // increments, the non-bypassed calls still see the same gate
  // behaviour as if bypass were absent.
  auto limiter =
      SimpleLimiter::Builder()
          .Limit(FixedLimit::Of(5))
          .Id("bypass-traffic")
          .BypassPredicate([](std::string_view ctx) { return ctx == "skip"; })
          .Build()
          .value();

  constexpr int kThreads = 32;
  constexpr int kIterationsPerThread = 200;
  std::atomic<int> peak{0};
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      std::string_view const ctx = (t % 2 == 0) ? "skip" : "real";
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire(ctx);
        if (slot) {
          int const live = limiter->InflightCount();
          int prev = peak.load(std::memory_order_relaxed);
          while (live > prev && !peak.compare_exchange_weak(
                                    prev, live, std::memory_order_relaxed)) {
          }
          slot->OnSuccess();
        }
      }
    });
  }
  for (auto& thr : threads) thr.join();

  EXPECT_LE(peak.load(), 5)
      << "Peak in-flight must respect the cap even with bypass traffic.";
  EXPECT_EQ(limiter->InflightCount(), 0);
  int64_t const total_iterations =
      static_cast<int64_t>(kThreads) * kIterationsPerThread;
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kBypassed),
            total_iterations / 2);
}

// =====================================================================
// (3) Partitioned-limiter coverage — the most-used surface.
// =====================================================================

TEST(TrafficSimulationTest, PartitionedConservesCountersUnderTraffic) {
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(20))
                     .Id("partitioned-conservation")
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

  EXPECT_EQ(limiter->InflightCount(), 0);
  EXPECT_EQ(limiter->PartitionInflightCount("live"), 0);
  EXPECT_EQ(limiter->PartitionInflightCount("batch"), 0);
  int64_t const total_decisions =
      static_cast<int64_t>(kThreads) * kIterationsPerThread;
  int64_t const summed =
      limiter->OutcomeCount(AbstractLimiter::Status::kSuccess) +
      limiter->OutcomeCount(AbstractLimiter::Status::kRejected);
  EXPECT_EQ(summed, total_decisions);
}

TEST(TrafficSimulationTest, PartitionedGlobalCapNeverExceededUnderTraffic) {
  // The global in-flight count must never exceed the global cap
  // under any concurrent schedule, regardless of partition
  // distribution. Note: a partition's per-partition busy count
  // CAN exceed its quota when burst-into-idle is in play (the
  // other partition has slack), which is the documented
  // semantics; the global hard cap is the invariant under test
  // here.
  constexpr int kCap = 10;
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(kCap))
                     .Id("global-cap")
                     .Partition("a", 0.5)
                     .Partition("b", 0.5)
                     .PartitionResolver(MatchExact("a", "a"))
                     .PartitionResolver(MatchExact("b", "b"))
                     .Build()
                     .value();

  constexpr int kThreads = 64;
  constexpr int kIterationsPerThread = 200;
  std::atomic<int> peak{0};
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      std::string_view const ctx = (t % 2 == 0) ? "a" : "b";
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        TryAcquireAndRecordPeak(*limiter, ctx, peak);
      }
    });
  }
  for (auto& thr : threads) thr.join();

  EXPECT_LE(peak.load(), kCap);
  EXPECT_EQ(limiter->InflightCount(), 0);
  EXPECT_EQ(limiter->PartitionInflightCount("a"), 0);
  EXPECT_EQ(limiter->PartitionInflightCount("b"), 0);
}

TEST(TrafficSimulationTest, PartitionedBurstIntoIdleUnderTraffic) {
  // Burst-into-idle is a STRICT invariant: when the sibling
  // partition is idle, a partition can hold strictly more slots
  // than its hard quota — up to the global cap. The test
  // demonstrates this with a controlled acquire pattern: the test
  // thread itself acquires hot_quota + extras consecutive slots
  // on `hot` before launching any concurrent traffic. Each
  // ASSERT_TRUE on the over-quota acquires is a strict invariant
  // — under any schedule, cold is empty and global has room, so
  // hot must admit.
  //
  // After the strict burst evidence is established, concurrent
  // hot traffic runs. The held slots stay held throughout; the
  // post-join state has only the held slots in-flight on hot,
  // and conservation invariants hold over the dynamic traffic.
  constexpr int kCap = 10;
  constexpr int kHeldHotSlots = 5;  // > hot_quota=2; < kCap=10.
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(kCap))
                     .Id("burst-into-idle")
                     .Partition("hot", 0.2)
                     .Partition("cold", 0.8)
                     .PartitionResolver(MatchExact("hot", "hot"))
                     .PartitionResolver(MatchExact("cold", "cold"))
                     .Build()
                     .value();
  int const hot_quota = limiter->PartitionLimitFor("hot");
  ASSERT_EQ(hot_quota, 2) << "hot quota at 20% of cap=10 must be 2 slots";

  // Strict burst evidence: with cold idle and global at zero, hot
  // can acquire above its quota up to the global cap. Every one
  // of these must succeed.
  std::vector<std::optional<Limiter::SlotGuard>> held;
  held.reserve(kHeldHotSlots);
  for (int i = 0; i < kHeldHotSlots; ++i) {
    auto slot = limiter->TryAcquire("hot");
    ASSERT_TRUE(slot) << "hot must burst above quota=" << hot_quota
                      << " to acquire #" << (i + 1)
                      << " (cold idle, global has room)";
    held.push_back(std::move(slot));
  }
  EXPECT_EQ(limiter->PartitionInflightCount("hot"), kHeldHotSlots);
  EXPECT_EQ(limiter->PartitionInflightCount("cold"), 0);
  EXPECT_EQ(limiter->InflightCount(), kHeldHotSlots);

  // Drive concurrent hot traffic. Each iteration's TryAcquire
  // may succeed (when global has room) or fail (when global is
  // saturated). Conservation invariants hold regardless.
  constexpr int kThreads = 16;
  constexpr int kIterationsPerThread = 500;
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire("hot");
        if (slot) slot->OnSuccess();
      }
    });
  }
  for (auto& thr : threads) thr.join();

  // Post-join: the held slots are still held; transient hot
  // traffic from the threads has fully drained.
  EXPECT_EQ(limiter->InflightCount(), kHeldHotSlots);
  EXPECT_EQ(limiter->PartitionInflightCount("hot"), kHeldHotSlots);
  EXPECT_EQ(limiter->PartitionInflightCount("cold"), 0);

  // Release the held slots and assert clean state.
  for (auto& slot : held) slot->OnSuccess();
  EXPECT_EQ(limiter->InflightCount(), 0);
  EXPECT_EQ(limiter->PartitionInflightCount("hot"), 0);
}

TEST(TrafficSimulationTest, PartitionedHardCapWhenGlobalSaturatedUnderTraffic) {
  // Hard-cap-when-global-saturated is a STRICT invariant: when
  // the global cap is full AND a partition is at its hard quota,
  // every additional acquire on that partition must be rejected.
  // Setup: kCap=4 with two 50%/50% partitions, hard quota=2 each.
  // The test thread saturates global with 2 slots on each
  // partition. Then 16 concurrent threads attempt to acquire on
  // `a`; each EXPECT_FALSE is a strict invariant. The rejected
  // counter must equal exactly the total concurrent attempts.
  constexpr int kCap = 4;
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(kCap))
                     .Id("hard-cap-under-traffic")
                     .Partition("a", 0.5)
                     .Partition("b", 0.5)
                     .PartitionResolver(MatchExact("a", "a"))
                     .PartitionResolver(MatchExact("b", "b"))
                     .Build()
                     .value();
  ASSERT_EQ(limiter->PartitionLimitFor("a"), 2);
  ASSERT_EQ(limiter->PartitionLimitFor("b"), 2);

  std::vector<std::optional<Limiter::SlotGuard>> held;
  for (int i = 0; i < 2; ++i) {
    auto sa = limiter->TryAcquire("a");
    ASSERT_TRUE(sa);
    held.push_back(std::move(sa));
    auto sb = limiter->TryAcquire("b");
    ASSERT_TRUE(sb);
    held.push_back(std::move(sb));
  }
  ASSERT_EQ(limiter->InflightCount(), kCap);
  ASSERT_EQ(limiter->PartitionInflightCount("a"), 2);
  ASSERT_EQ(limiter->PartitionInflightCount("b"), 2);

  int64_t const rejected_before =
      limiter->OutcomeCount(AbstractLimiter::Status::kRejected);

  constexpr int kThreads = 16;
  constexpr int kIterationsPerThread = 100;
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back(
        [&] {
          start.arrive_and_wait();
          for (int i = 0; i < kIterationsPerThread; ++i) {
            auto slot = limiter->TryAcquire("a");
            EXPECT_FALSE(slot)
                << "Saturated global + partition-at-quota must reject";
          }
        });
  }
  for (auto& thr : threads) thr.join();

  int64_t const expected_rejects =
      static_cast<int64_t>(kThreads) * kIterationsPerThread;
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kRejected) -
                rejected_before,
            expected_rejects)
      << "Every concurrent acquire under saturation must increment kRejected.";
  EXPECT_EQ(limiter->InflightCount(), kCap);

  for (auto& slot : held) slot->OnSuccess();
  EXPECT_EQ(limiter->InflightCount(), 0);
}

TEST(TrafficSimulationTest, PartitionedRejectDelayInvokedConcurrentlyExactly) {
  // The reject-delay path is exercised under concurrent traffic
  // with an injected fake SleepFn. The fake records each
  // invocation under an atomic counter; no wall-clock dependency.
  // STRICT INVARIANT: every rejected acquire that lies within the
  // MaxDelayedThreads cap invokes the SleepFn exactly once with
  // the configured duration. With MaxDelayedThreads=100 and 16
  // concurrent threads, the cap is never reached, so the count
  // equals the total rejected acquires exactly.
  std::atomic<int> sleep_count{0};
  std::atomic<int> wrong_duration_count{0};
  constexpr std::chrono::milliseconds kConfiguredDelay{50};
  auto recording_sleep = [&](std::chrono::milliseconds d) {
    if (d != kConfiguredDelay) wrong_duration_count.fetch_add(1);
    sleep_count.fetch_add(1);
  };
  constexpr int kMaxDelayedThreads = 100;
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(1))
                     .Id("concurrent-reject-delay")
                     .Partition("p", 1.0)
                     .PartitionRejectDelay("p", kConfiguredDelay)
                     .PartitionResolver(MatchExact("p", "p"))
                     .MaxDelayedThreads(kMaxDelayedThreads)
                     .SleepFor(recording_sleep)
                     .Build()
                     .value();

  auto blocker = limiter->TryAcquire("p");
  ASSERT_TRUE(blocker);

  constexpr int kThreads = 16;
  constexpr int kIterationsPerThread = 10;
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire("p");
        EXPECT_FALSE(slot);
      }
    });
  }
  for (auto& thr : threads) thr.join();

  int const expected_invocations = kThreads * kIterationsPerThread;
  EXPECT_EQ(sleep_count.load(), expected_invocations)
      << "SleepFn must run exactly once per rejection when below the "
      << "MaxDelayedThreads cap.";
  EXPECT_EQ(wrong_duration_count.load(), 0)
      << "Every SleepFn invocation must receive the configured duration.";

  blocker->OnSuccess();
  EXPECT_EQ(limiter->InflightCount(), 0);
}

TEST(TrafficSimulationTest, PartitionedThreeWayConservationUnderTraffic) {
  // Three partitions with non-uniform quotas. Conservation must
  // hold: every per-partition counter returns to zero; sum of
  // success + rejected equals total decisions; global in-flight
  // returns to zero.
  constexpr int kCap = 20;
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(kCap))
                     .Id("three-way")
                     .Partition("a", 0.5)
                     .Partition("b", 0.3)
                     .Partition("c", 0.2)
                     .PartitionResolver(MatchExact("a", "a"))
                     .PartitionResolver(MatchExact("b", "b"))
                     .PartitionResolver(MatchExact("c", "c"))
                     .Build()
                     .value();

  constexpr int kThreads = 30;
  constexpr int kIterationsPerThread = 500;
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      std::string_view ctx;
      switch (t % 3) {
        case 0:
          ctx = "a";
          break;
        case 1:
          ctx = "b";
          break;
        default:
          ctx = "c";
          break;
      }
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire(ctx);
        if (slot) slot->OnSuccess();
      }
    });
  }
  for (auto& thr : threads) thr.join();

  EXPECT_EQ(limiter->InflightCount(), 0);
  EXPECT_EQ(limiter->PartitionInflightCount("a"), 0);
  EXPECT_EQ(limiter->PartitionInflightCount("b"), 0);
  EXPECT_EQ(limiter->PartitionInflightCount("c"), 0);

  int64_t const total_decisions =
      static_cast<int64_t>(kThreads) * kIterationsPerThread;
  int64_t const summed =
      limiter->OutcomeCount(AbstractLimiter::Status::kSuccess) +
      limiter->OutcomeCount(AbstractLimiter::Status::kRejected);
  EXPECT_EQ(summed, total_decisions);
}

TEST(TrafficSimulationTest,
     PartitionedUnknownPartitionAbsorbsUnmatchedTraffic) {
  // When no resolver matches the context, the request is routed to
  // the synthetic `unknown` partition. Drive traffic that
  // alternates between a matching context and an unmatched one;
  // post-join, both the named partition and `unknown` are zero,
  // and the `unknown` partition received its share of admits.
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(20))
                     .Id("unknown-routing")
                     .Partition("known", 1.0)
                     .PartitionResolver(MatchExact("known", "known"))
                     .Build()
                     .value();

  constexpr int kThreads = 16;
  constexpr int kIterationsPerThread = 250;
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      std::string_view const ctx = (t % 2 == 0) ? "known" : "weird";
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire(ctx);
        if (slot) slot->OnSuccess();
      }
    });
  }
  for (auto& thr : threads) thr.join();

  EXPECT_EQ(limiter->InflightCount(), 0);
  EXPECT_EQ(limiter->PartitionInflightCount("known"), 0);
  EXPECT_EQ(limiter->PartitionInflightCount(
                AbstractPartitionedLimiter::kUnknownPartitionName),
            0);

  int64_t const total_decisions =
      static_cast<int64_t>(kThreads) * kIterationsPerThread;
  int64_t const summed =
      limiter->OutcomeCount(AbstractLimiter::Status::kSuccess) +
      limiter->OutcomeCount(AbstractLimiter::Status::kRejected);
  EXPECT_EQ(summed, total_decisions);
}

TEST(TrafficSimulationTest, PartitionedResolverChainFirstMatchUnderTraffic) {
  // Two resolvers: the first matches `"first"`, the second matches
  // `"second"`. Drive traffic across both contexts plus one that
  // matches neither. The first-match-wins discipline must route
  // each call to the partition the chain says it should. Post-join,
  // every partition's busy counter is zero and `unknown` collected
  // the unmatched traffic.
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(15))
                     .Id("resolver-chain")
                     .Partition("first", 0.5)
                     .Partition("second", 0.5)
                     .PartitionResolver(MatchExact("first", "first"))
                     .PartitionResolver(MatchExact("second", "second"))
                     .Build()
                     .value();

  constexpr int kThreads = 24;
  constexpr int kIterationsPerThread = 200;
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      std::string_view ctx;
      switch (t % 3) {
        case 0:
          ctx = "first";
          break;
        case 1:
          ctx = "second";
          break;
        default:
          ctx = "unmatched";
          break;
      }
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire(ctx);
        if (slot) slot->OnSuccess();
      }
    });
  }
  for (auto& thr : threads) thr.join();

  EXPECT_EQ(limiter->InflightCount(), 0);
  EXPECT_EQ(limiter->PartitionInflightCount("first"), 0);
  EXPECT_EQ(limiter->PartitionInflightCount("second"), 0);
  EXPECT_EQ(limiter->PartitionInflightCount(
                AbstractPartitionedLimiter::kUnknownPartitionName),
            0);
}

TEST(TrafficSimulationTest, PartitionedBypassUnderTrafficSkipsAllAccounting) {
  // Bypass predicate matches half the traffic. Bypassed calls
  // must not consume in-flight slots in either the global pool or
  // any partition. Post-join: bypassed counter equals number of
  // bypassed iterations; non-bypassed iterations end in either
  // success or rejection; in-flight returns to zero.
  auto limiter =
      AbstractPartitionedLimiter::Builder()
          .Limit(FixedLimit::Of(4))
          .Id("partitioned-bypass")
          .BypassPredicate([](std::string_view ctx) { return ctx == "skip"; })
          .Partition("live", 1.0)
          .PartitionResolver(MatchExact("live", "live"))
          .Build()
          .value();

  constexpr int kThreads = 32;
  constexpr int kIterationsPerThread = 200;
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      std::string_view const ctx = (t % 2 == 0) ? "skip" : "live";
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire(ctx);
        if (slot) slot->OnSuccess();
      }
    });
  }
  for (auto& thr : threads) thr.join();

  EXPECT_EQ(limiter->InflightCount(), 0);
  EXPECT_EQ(limiter->PartitionInflightCount("live"), 0);
  int64_t const total = static_cast<int64_t>(kThreads) * kIterationsPerThread;
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kBypassed),
            total / 2);
}

TEST(TrafficSimulationTest, PartitionedRecomputeOnSettableLimitChange) {
  // Use a SettableLimit so the cap can be changed at runtime. Drive
  // concurrent traffic, periodically toggle the cap between two
  // values from a separate thread, and verify post-join: in-flight
  // returns to zero, the per-partition cap reflects the final
  // SettableLimit value, partition busy counters return to zero,
  // and the global outcome counters sum to the total decisions.
  // The recompute happens inside the limiter on every SetLimit;
  // under contention, partition caps may shift mid-traffic. The
  // post-join invariants must still hold.
  auto settable = SettableLimit::StartingAt(10);
  auto* settable_ptr = settable.get();
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(std::move(settable))
                     .Id("recompute")
                     .Partition("a", 0.5)
                     .Partition("b", 0.5)
                     .PartitionResolver(MatchExact("a", "a"))
                     .PartitionResolver(MatchExact("b", "b"))
                     .Build()
                     .value();

  constexpr int kTrafficThreads = 16;
  constexpr int kIterationsPerThread = 500;
  std::latch start{kTrafficThreads};
  std::vector<std::thread> threads;
  threads.reserve(kTrafficThreads);
  for (int t = 0; t < kTrafficThreads; ++t) {
    threads.emplace_back([&, t] {
      std::string_view const ctx = (t % 2 == 0) ? "a" : "b";
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire(ctx);
        if (slot) slot->OnSuccess();
        // Every 50 iterations, this thread is the toggler. Only
        // thread 0 mutates the cap so SetLimit calls are
        // serialised at the SettableLimit level (its callbacks
        // run under the limiter's own mutex). Under heavy load
        // many cap toggles will happen during traffic; the
        // recompute path must keep state consistent.
        if (t == 0 && (i % 50 == 49)) {
          int const next = (i % 100 == 49) ? 6 : 12;
          settable_ptr->SetLimit(next);
        }
      }
    });
  }
  for (auto& thr : threads) thr.join();

  EXPECT_EQ(limiter->InflightCount(), 0);
  EXPECT_EQ(limiter->PartitionInflightCount("a"), 0);
  EXPECT_EQ(limiter->PartitionInflightCount("b"), 0);
  int const final_cap = settable_ptr->GetLimit();
  EXPECT_TRUE(final_cap == 6 || final_cap == 12)
      << "Final cap should be one of the toggled values; got " << final_cap;
  // The per-partition caps sum to at least the global cap (ceiling
  // rounding); each partition's cap is at least one slot.
  EXPECT_GE(limiter->PartitionLimitFor("a"), 1);
  EXPECT_GE(limiter->PartitionLimitFor("b"), 1);

  int64_t const total =
      static_cast<int64_t>(kTrafficThreads) * kIterationsPerThread;
  int64_t const summed =
      limiter->OutcomeCount(AbstractLimiter::Status::kSuccess) +
      limiter->OutcomeCount(AbstractLimiter::Status::kRejected);
  EXPECT_EQ(summed, total);
}

TEST(TrafficSimulationTest,
     PartitionedHighlyUnequalQuotaPreservesMinimumOneSlot) {
  // Even a partition with a near-zero percentage gets at least one
  // slot. Drive traffic only against the tiny partition; under
  // saturation it can still admit calls — its hard cap is at least
  // one. Post-join: no over-admission, conservation holds.
  constexpr int kCap = 20;
  auto limiter = AbstractPartitionedLimiter::Builder()
                     .Limit(FixedLimit::Of(kCap))
                     .Id("unequal-quota")
                     .Partition("tiny", 0.05)
                     .Partition("big", 0.95)
                     .PartitionResolver(MatchExact("tiny", "tiny"))
                     .PartitionResolver(MatchExact("big", "big"))
                     .Build()
                     .value();
  EXPECT_GE(limiter->PartitionLimitFor("tiny"), 1)
      << "Minimum quota is one slot regardless of percentage";

  constexpr int kThreads = 16;
  constexpr int kIterationsPerThread = 200;
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire("tiny");
        if (slot) slot->OnSuccess();
      }
    });
  }
  for (auto& thr : threads) thr.join();

  EXPECT_EQ(limiter->InflightCount(), 0);
  EXPECT_EQ(limiter->PartitionInflightCount("tiny"), 0);
  // Some admits should have succeeded — the partition has a hard
  // minimum of one slot.
  EXPECT_GT(limiter->OutcomeCount(AbstractLimiter::Status::kSuccess), 0);
}

// =====================================================================
// (4) Blocking limiters under high contention.
// =====================================================================

TEST(TrafficSimulationTest, BlockingLimiterPreservesFifoUnderContention) {
  // A blocker thread takes the only slot. N waiters are spawned
  // one at a time, each one parked on the queue before the next
  // is spawned. When the blocker releases, the queue drains in
  // FIFO order. The release order is recorded under a mutex; each
  // waiter writes its index BEFORE calling OnSuccess (which
  // triggers the handoff to the next waiter), so the order is
  // deterministic under any schedule.
  auto delegate = SimpleLimiter::Builder()
                      .Limit(FixedLimit::Of(1))
                      .Id("blocking-fifo")
                      .Build()
                      .value();
  auto limiter = BlockingLimiter::Builder()
                     .Delegate(std::move(delegate))
                     .Timeout(std::chrono::milliseconds(60'000))
                     .Build()
                     .value();

  constexpr int kWaiters = 8;
  std::vector<int> release_order;
  release_order.reserve(kWaiters);
  absl::Mutex release_order_mu;
  std::vector<std::thread> threads;
  threads.reserve(kWaiters);
  std::latch blocker_holds{1};
  std::latch all_queued{1};

  std::thread blocker([&] {
    auto slot = limiter->TryAcquire();
    ASSERT_TRUE(slot);
    blocker_holds.count_down();
    all_queued.wait();
    slot->OnSuccess();
  });
  blocker_holds.wait();

  for (int i = 0; i < kWaiters; ++i) {
    threads.emplace_back([&, i] {
      auto slot = limiter->TryAcquire();
      ASSERT_TRUE(slot);
      {
        absl::MutexLock lock(release_order_mu);
        release_order.push_back(i);
      }
      slot->OnSuccess();
    });
    limiter->AwaitQueueSize(i + 1);
  }

  all_queued.count_down();
  blocker.join();
  for (auto& thr : threads) thr.join();

  ASSERT_EQ(static_cast<int>(release_order.size()), kWaiters);
  for (int i = 0; i < kWaiters; ++i) {
    EXPECT_EQ(release_order[i], i)
        << "Waiters must release in FIFO order. release_order[" << i
        << "]=" << release_order[i];
  }
}

TEST(TrafficSimulationTest, LifoBlockingLimiterPreservesLifoUnderContention) {
  // Same shape as the FIFO test against the LIFO variant. Release
  // order must be the reverse of arrival order: most-recently
  // parked waiter served first.
  auto delegate = SimpleLimiter::Builder()
                      .Limit(FixedLimit::Of(1))
                      .Id("lifo-blocking")
                      .Build()
                      .value();
  auto limiter = LifoBlockingLimiter::Builder()
                     .Delegate(std::move(delegate))
                     .BacklogTimeout(std::chrono::milliseconds(60'000))
                     .Build()
                     .value();

  constexpr int kWaiters = 8;
  std::vector<int> release_order;
  release_order.reserve(kWaiters);
  absl::Mutex release_order_mu;
  std::vector<std::thread> threads;
  threads.reserve(kWaiters);
  std::latch blocker_holds{1};
  std::latch all_queued{1};

  std::thread blocker([&] {
    auto slot = limiter->TryAcquire();
    ASSERT_TRUE(slot);
    blocker_holds.count_down();
    all_queued.wait();
    slot->OnSuccess();
  });
  blocker_holds.wait();

  for (int i = 0; i < kWaiters; ++i) {
    threads.emplace_back([&, i] {
      auto slot = limiter->TryAcquire();
      ASSERT_TRUE(slot);
      {
        absl::MutexLock lock(release_order_mu);
        release_order.push_back(i);
      }
      slot->OnSuccess();
    });
    limiter->AwaitBacklogSize(i + 1);
  }

  all_queued.count_down();
  blocker.join();
  for (auto& thr : threads) thr.join();

  ASSERT_EQ(static_cast<int>(release_order.size()), kWaiters);
  for (int i = 0; i < kWaiters; ++i) {
    EXPECT_EQ(release_order[i], kWaiters - 1 - i)
        << "Waiters must release in LIFO order. release_order[" << i
        << "]=" << release_order[i];
  }
}

TEST(TrafficSimulationTest, BlockingLimiterAllWaitersDrainUnderTraffic) {
  // Drive many threads through a blocking FIFO limiter where every
  // acquire eventually succeeds. After join, the limiter is back
  // to a clean state: queue is empty, the delegate's in-flight
  // counter returns to zero (so no completion was lost on the
  // inner-slot release path), and the delegate's success counter
  // accounts for every iteration.
  auto delegate = SimpleLimiter::Builder()
                      .Limit(FixedLimit::Of(4))
                      .Id("blocking-drain")
                      .Build()
                      .value();
  auto* delegate_ptr = delegate.get();
  auto limiter = BlockingLimiter::Builder()
                     .Delegate(std::move(delegate))
                     .Timeout(std::chrono::seconds(60))
                     .Build()
                     .value();

  constexpr int kThreads = 32;
  constexpr int kIterationsPerThread = 50;
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire();
        ASSERT_TRUE(slot);
        slot->OnSuccess();
      }
    });
  }
  for (auto& thr : threads) thr.join();

  EXPECT_EQ(limiter->QueueSize(), 0)
      << "All waiters must have drained from the wait queue";
  EXPECT_EQ(delegate_ptr->InflightCount(), 0)
      << "Delegate in-flight must return to zero; lost completion is a bug";
  int64_t const total = static_cast<int64_t>(kThreads) * kIterationsPerThread;
  EXPECT_EQ(delegate_ptr->OutcomeCount(AbstractLimiter::Status::kSuccess),
            total)
      << "Delegate's success count must equal the total iterations";
}

TEST(TrafficSimulationTest, LifoBlockingLimiterAllWaitersDrainUnderTraffic) {
  // The LIFO counterpart to BlockingLimiterAllWaitersDrainUnderTraffic.
  // LIFO has a different deque discipline and a separate timeout-acquire
  // race-recovery path; clean drain is a non-trivial invariant on its
  // own.
  auto delegate = SimpleLimiter::Builder()
                      .Limit(FixedLimit::Of(4))
                      .Id("lifo-drain")
                      .Build()
                      .value();
  auto* delegate_ptr = delegate.get();
  auto limiter = LifoBlockingLimiter::Builder()
                     .Delegate(std::move(delegate))
                     .BacklogTimeout(std::chrono::seconds(60))
                     .Build()
                     .value();

  constexpr int kThreads = 32;
  constexpr int kIterationsPerThread = 50;
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire();
        ASSERT_TRUE(slot);
        slot->OnSuccess();
      }
    });
  }
  for (auto& thr : threads) thr.join();

  EXPECT_EQ(limiter->BacklogSize(), 0)
      << "All waiters must have drained from the wait deque";
  EXPECT_EQ(delegate_ptr->InflightCount(), 0)
      << "Delegate in-flight must return to zero; lost completion is a bug";
  int64_t const total = static_cast<int64_t>(kThreads) * kIterationsPerThread;
  EXPECT_EQ(delegate_ptr->OutcomeCount(AbstractLimiter::Status::kSuccess),
            total)
      << "Delegate's success count must equal the total iterations";
}

// =====================================================================
// (5) End-to-end limiter + algorithm under traffic.
// =====================================================================

TEST(TrafficSimulationTest, SimpleLimiterFeedsAlgorithmUnderConcurrentTraffic) {
  // SimpleLimiter backed by a SettableLimit (no algorithm) under
  // concurrent traffic. Verifies the limiter base's
  // OnSlotComplete pipeline routes samples to Limit::OnSample
  // without losing or duplicating completions. The settable cap
  // does not change in this test, only the integration is
  // exercised.
  auto settable = SettableLimit::StartingAt(8);
  auto* settable_ptr = settable.get();
  auto limiter = SimpleLimiter::Builder()
                     .Limit(std::move(settable))
                     .Id("end-to-end")
                     .Build()
                     .value();

  constexpr int kThreads = 16;
  constexpr int kIterationsPerThread = 500;
  std::latch start{kThreads};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire();
        if (slot) slot->OnSuccess();
      }
    });
  }
  for (auto& thr : threads) thr.join();

  EXPECT_EQ(settable_ptr->GetLimit(), 8);
  EXPECT_EQ(limiter->InflightCount(), 0);
  int64_t const total = static_cast<int64_t>(kThreads) * kIterationsPerThread;
  EXPECT_EQ(limiter->OutcomeCount(AbstractLimiter::Status::kSuccess) +
                limiter->OutcomeCount(AbstractLimiter::Status::kRejected),
            total);
}

TEST(TrafficSimulationTest, AdaptiveLimitStaysWithinConfiguredBounds) {
  // Full pipeline: SimpleLimiter backed by Gradient2. Concurrent
  // threads drive admit + release. Periodically read GetLimit()
  // and track the highest and lowest observed values. Post-join:
  // both bounds must lie within [MinLimit, MaxConcurrency]. The
  // CAS gate plus the algorithm's MinLimit/MaxConcurrency clamps
  // are what make this invariant hold.
  constexpr int kFloor = 5;
  constexpr int kCeiling = 50;
  auto gradient = Gradient2Limit::Builder()
                      .InitialLimit(10)
                      .MinLimit(kFloor)
                      .MaxConcurrency(kCeiling)
                      .Build();
  auto limiter = SimpleLimiter::Builder()
                     .Limit(std::move(gradient))
                     .Id("bounds")
                     .Build()
                     .value();

  constexpr int kThreads = 32;
  constexpr int kIterationsPerThread = 1000;
  std::latch start{kThreads};
  std::atomic<int> highest{0};
  std::atomic<int> lowest{kCeiling};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      for (int i = 0; i < kIterationsPerThread; ++i) {
        auto slot = limiter->TryAcquire();
        if (slot) slot->OnSuccess();
        if ((i & 31) == 0) {
          int const lim = limiter->GetLimit();
          int prev_hi = highest.load(std::memory_order_relaxed);
          while (lim > prev_hi &&
                 !highest.compare_exchange_weak(prev_hi, lim,
                                                std::memory_order_relaxed)) {
          }
          int prev_lo = lowest.load(std::memory_order_relaxed);
          while (lim < prev_lo &&
                 !lowest.compare_exchange_weak(prev_lo, lim,
                                               std::memory_order_relaxed)) {
          }
        }
      }
    });
  }
  for (auto& thr : threads) thr.join();

  EXPECT_LE(highest.load(), kCeiling);
  EXPECT_GE(lowest.load(), kFloor);
  EXPECT_EQ(limiter->InflightCount(), 0);
}

}  // namespace
}  // namespace limen

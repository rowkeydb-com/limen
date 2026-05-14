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

#include "limen/blocking_limiter.h"
#include "limen/fixed_limit.h"
#include "limen/settable_limit.h"
#include "limen/simple_limiter.h"
#include "absl/status/status.h"
#include "absl/synchronization/notification.h"
#include "gtest/gtest.h"
#include <array>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace limen {
namespace {

// Builds a BlockingLimiter wrapping a SimpleLimiter with the given
// cap. Convenience for the tests.
std::unique_ptr<BlockingLimiter> MakeBlockingLimiter(
    int cap, std::chrono::milliseconds timeout) {
  auto inner = SimpleLimiter::Builder()
                   .Limit(FixedLimit::Of(cap))
                   .Id("inner")
                   .Build()
                   .value();
  return BlockingLimiter::Builder()
      .Delegate(std::move(inner))
      .Timeout(timeout)
      .Build()
      .value();
}

TEST(BlockingLimiterTest, BuildRejectsMissingDelegate) {
  auto result =
      BlockingLimiter::Builder().Timeout(std::chrono::milliseconds(50)).Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("Delegate()"), std::string::npos);
}

TEST(BlockingLimiterTest, BuildRejectsZeroTimeout) {
  auto inner = SimpleLimiter::Builder()
                   .Limit(FixedLimit::Of(1))
                   .Id("inner")
                   .Build()
                   .value();
  auto result = BlockingLimiter::Builder()
                    .Delegate(std::move(inner))
                    .Timeout(std::chrono::milliseconds::zero())
                    .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("Timeout"), std::string::npos);
}

TEST(BlockingLimiterTest, BuildRejectsExcessiveTimeout) {
  auto inner = SimpleLimiter::Builder()
                   .Limit(FixedLimit::Of(1))
                   .Id("inner")
                   .Build()
                   .value();
  auto result =
      BlockingLimiter::Builder()
          .Delegate(std::move(inner))
          .Timeout(BlockingLimiter::kMaxTimeout + std::chrono::milliseconds(1))
          .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("must not exceed"),
            std::string::npos);
}

TEST(BlockingLimiterTest, AcquireImmediateWhenDelegateAdmits) {
  auto limiter = MakeBlockingLimiter(/*cap=*/2, std::chrono::seconds(1));
  auto a = limiter->TryAcquire();
  auto b = limiter->TryAcquire();
  EXPECT_TRUE(a);
  EXPECT_TRUE(b);
  EXPECT_EQ(limiter->QueueSize(), 0);
}

TEST(BlockingLimiterTest, AcquireBlocksUntilSlotReleased) {
  auto limiter = MakeBlockingLimiter(/*cap=*/1, std::chrono::seconds(1));
  auto held = limiter->TryAcquire();
  ASSERT_TRUE(held);

  // Spawn a waiter; it must park because the cap is saturated.
  std::optional<Limiter::SlotGuard> waiter_result;
  absl::Notification waiter_returned;
  std::thread waiter([&] {
    waiter_result = limiter->TryAcquire();
    waiter_returned.Notify();
  });

  // Wait deterministically until the waiter is parked.
  limiter->AwaitQueueSize(1);
  EXPECT_FALSE(waiter_returned.HasBeenNotified());

  // Release the held slot. The waiter must be served.
  held->OnSuccess();
  waiter_returned.WaitForNotification();
  waiter.join();

  EXPECT_TRUE(waiter_result.has_value());
  EXPECT_EQ(limiter->QueueSize(), 0);
}

TEST(BlockingLimiterTest, AcquireTimesOutWhenNoSlotComes) {
  // 50 ms timeout. The wait will complete in ~50 ms wall time but
  // the outcome (nullopt) is deterministic: no slot is ever
  // released while the wait is in flight.
  auto limiter = MakeBlockingLimiter(/*cap=*/1, std::chrono::milliseconds(50));
  auto held = limiter->TryAcquire();
  ASSERT_TRUE(held);

  std::optional<Limiter::SlotGuard> result = limiter->TryAcquire();
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(limiter->QueueSize(), 0);
}

TEST(BlockingLimiterTest, WaitersServedInFifoOrder) {
  // The Limen divergence from upstream: waiters are served in
  // strict first-in-first-out order. Spawn N waiters one at a
  // time, waiting for each to park on the queue before spawning
  // the next. Then release slots in sequence; each release wakes
  // the head of the queue. Assert the wake-up order matches the
  // spawn order.
  auto limiter = MakeBlockingLimiter(/*cap=*/1, std::chrono::seconds(2));
  auto held = limiter->TryAcquire();
  ASSERT_TRUE(held);

  constexpr int kWaiters = 4;
  std::vector<std::thread> threads;
  std::array<absl::Notification, kWaiters> woke;
  std::array<std::optional<Limiter::SlotGuard>, kWaiters> slots;

  for (int i = 0; i < kWaiters; ++i) {
    threads.emplace_back([&, i] {
      slots[i] = limiter->TryAcquire();
      woke[i].Notify();
    });
    // Ensure waiter i is parked before spawning waiter i+1.
    limiter->AwaitQueueSize(i + 1);
  }

  // All kWaiters are parked. Release the held slot to wake the
  // first one; each waiter, on completion, will release its
  // slot via OnSuccess and wake the next.
  held->OnSuccess();

  // Now serve them in order: each waiter is awoken in FIFO
  // order, completes its work, releases, the next is served.
  for (int i = 0; i < kWaiters; ++i) {
    woke[i].WaitForNotification();
    ASSERT_TRUE(slots[i].has_value()) << "waiter " << i;
    slots[i]->OnSuccess();
  }
  for (auto& t : threads) t.join();

  EXPECT_EQ(limiter->QueueSize(), 0);
}

TEST(BlockingLimiterTest, LimitReductionWhileSlotsHeld) {
  // Reduce the limit while slots are held: existing slots stay
  // valid; new acquires must wait until enough are released to
  // bring in-flight below the new cap. The limiter remains in a
  // consistent state — release of held slots eventually frees a
  // parked waiter.
  auto settable = SettableLimit::StartingAt(2);
  auto* settable_ptr = settable.get();
  auto inner = SimpleLimiter::Builder()
                   .Limit(std::move(settable))
                   .Id("inner")
                   .Build()
                   .value();
  auto limiter = BlockingLimiter::Builder()
                     .Delegate(std::move(inner))
                     .Timeout(std::chrono::seconds(2))
                     .Build()
                     .value();

  auto a = limiter->TryAcquire();
  auto b = limiter->TryAcquire();
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);

  // Reduce cap to 1. Both held slots stay; new acquires block.
  settable_ptr->SetLimit(1);

  std::optional<Limiter::SlotGuard> waiter_result;
  absl::Notification waiter_returned;
  std::thread waiter([&] {
    waiter_result = limiter->TryAcquire();
    waiter_returned.Notify();
  });
  limiter->AwaitQueueSize(1);

  // Release `a`. In-flight drops from 2 to 1, still at the new
  // cap, so the waiter cannot be served yet.
  a->OnSuccess();
  EXPECT_FALSE(waiter_returned.HasBeenNotified());

  // Release `b`. In-flight drops to 0, below the new cap. The
  // waiter is served.
  b->OnSuccess();
  waiter_returned.WaitForNotification();
  waiter.join();

  EXPECT_TRUE(waiter_result.has_value());
}

TEST(BlockingLimiterTest, CompletionStatusMirroredToDelegate) {
  // The outer SlotGuard's OnSuccess / OnIgnore / OnDropped must
  // route to the delegate's matching outcome counter so the
  // wrapped algorithm sees the right sample.
  auto inner_unique = SimpleLimiter::Builder()
                          .Limit(FixedLimit::Of(3))
                          .Id("inner")
                          .Build()
                          .value();
  auto* inner = inner_unique.get();
  auto limiter = BlockingLimiter::Builder()
                     .Delegate(std::move(inner_unique))
                     .Timeout(std::chrono::seconds(1))
                     .Build()
                     .value();

  {
    auto s = limiter->TryAcquire();
    ASSERT_TRUE(s);
    s->OnSuccess();
  }
  {
    auto s = limiter->TryAcquire();
    ASSERT_TRUE(s);
    s->OnIgnore();
  }
  {
    auto s = limiter->TryAcquire();
    ASSERT_TRUE(s);
    s->OnDropped();
  }
  EXPECT_EQ(inner->OutcomeCount(AbstractLimiter::Status::kSuccess), 1);
  EXPECT_EQ(inner->OutcomeCount(AbstractLimiter::Status::kIgnored), 1);
  EXPECT_EQ(inner->OutcomeCount(AbstractLimiter::Status::kDropped), 1);
  EXPECT_EQ(inner->InflightCount(), 0);
}

}  // namespace
}  // namespace limen

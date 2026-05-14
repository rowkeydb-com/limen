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

#include "limen/lifo_blocking_limiter.h"
#include "limen/fixed_limit.h"
#include "limen/settable_limit.h"
#include "limen/simple_limiter.h"
#include "absl/status/status.h"
#include "absl/synchronization/notification.h"
#include "gtest/gtest.h"
#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace limen {
namespace {

std::unique_ptr<LifoBlockingLimiter> MakeLifoLimiter(
    int cap, std::chrono::milliseconds timeout, int backlog_size = 100) {
  auto inner = SimpleLimiter::Builder()
                   .Limit(FixedLimit::Of(cap))
                   .Id("inner")
                   .Build()
                   .value();
  return LifoBlockingLimiter::Builder()
      .Delegate(std::move(inner))
      .BacklogTimeout(timeout)
      .BacklogSize(backlog_size)
      .Build()
      .value();
}

TEST(LifoBlockingLimiterTest, BuildRejectsMissingDelegate) {
  auto result = LifoBlockingLimiter::Builder()
                    .BacklogTimeout(std::chrono::milliseconds(50))
                    .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("Delegate()"), std::string::npos);
}

TEST(LifoBlockingLimiterTest, BuildRejectsNonPositiveBacklogSize) {
  auto inner = SimpleLimiter::Builder()
                   .Limit(FixedLimit::Of(1))
                   .Id("inner")
                   .Build()
                   .value();
  auto result = LifoBlockingLimiter::Builder()
                    .Delegate(std::move(inner))
                    .BacklogSize(0)
                    .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("BacklogSize"), std::string::npos);
}

TEST(LifoBlockingLimiterTest, BuildRejectsExcessiveBacklogTimeout) {
  auto inner = SimpleLimiter::Builder()
                   .Limit(FixedLimit::Of(1))
                   .Id("inner")
                   .Build()
                   .value();
  auto result = LifoBlockingLimiter::Builder()
                    .Delegate(std::move(inner))
                    .BacklogTimeout(LifoBlockingLimiter::kMaxTimeout +
                                    std::chrono::milliseconds(1))
                    .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("must not exceed"),
            std::string::npos);
}

TEST(LifoBlockingLimiterTest, BuildRejectsZeroFixedBacklogTimeout) {
  auto inner = SimpleLimiter::Builder()
                   .Limit(FixedLimit::Of(1))
                   .Id("inner")
                   .Build()
                   .value();
  auto result = LifoBlockingLimiter::Builder()
                    .Delegate(std::move(inner))
                    .BacklogTimeout(std::chrono::milliseconds::zero())
                    .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("must be positive"),
            std::string::npos);
}

TEST(LifoBlockingLimiterTest, BuilderExposesFixedTimeoutWhenConfigured) {
  auto limiter = MakeLifoLimiter(/*cap=*/1, std::chrono::milliseconds(250));
  ASSERT_TRUE(limiter->GetFixedBacklogTimeout().has_value());
  EXPECT_EQ(*limiter->GetFixedBacklogTimeout(), std::chrono::milliseconds(250));
}

TEST(LifoBlockingLimiterTest, BuilderReturnsEmptyFixedTimeoutWhenDynamic) {
  auto inner = SimpleLimiter::Builder()
                   .Limit(FixedLimit::Of(1))
                   .Id("inner")
                   .Build()
                   .value();
  auto limiter =
      LifoBlockingLimiter::Builder()
          .Delegate(std::move(inner))
          .BacklogTimeout([](std::string_view) -> std::chrono::milliseconds {
            return std::chrono::seconds(1);
          })
          .Build()
          .value();
  EXPECT_FALSE(limiter->GetFixedBacklogTimeout().has_value());
}

TEST(LifoBlockingLimiterTest, BuildRejectsNullTimeoutFn) {
  auto inner = SimpleLimiter::Builder()
                   .Limit(FixedLimit::Of(1))
                   .Id("inner")
                   .Build()
                   .value();
  auto result = LifoBlockingLimiter::Builder()
                    .Delegate(std::move(inner))
                    .BacklogTimeout(LifoBlockingLimiter::TimeoutFn{})
                    .Build();
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_NE(result.status().message().find("must not be null"),
            std::string::npos);
}

TEST(LifoBlockingLimiterTest, AcquireImmediateWhenDelegateAdmits) {
  auto limiter = MakeLifoLimiter(/*cap=*/2, std::chrono::seconds(1));
  auto a = limiter->TryAcquire();
  auto b = limiter->TryAcquire();
  EXPECT_TRUE(a);
  EXPECT_TRUE(b);
  EXPECT_EQ(limiter->BacklogSize(), 0);
}

TEST(LifoBlockingLimiterTest, AcquireBlocksUntilSlotReleased) {
  auto limiter = MakeLifoLimiter(/*cap=*/1, std::chrono::seconds(1));
  auto held = limiter->TryAcquire();
  ASSERT_TRUE(held);

  std::optional<Limiter::SlotGuard> waiter_result;
  absl::Notification waiter_returned;
  std::thread waiter([&] {
    waiter_result = limiter->TryAcquire();
    waiter_returned.Notify();
  });
  limiter->AwaitBacklogSize(1);

  held->OnSuccess();
  waiter_returned.WaitForNotification();
  waiter.join();

  EXPECT_TRUE(waiter_result.has_value());
}

TEST(LifoBlockingLimiterTest, WaitersServedInLifoOrder) {
  // Last-in-first-out: the most recently parked waiter is the
  // first to be served. Spawn kWaiters waiters in known order,
  // then release slots one at a time. The wake-up order must
  // be the reverse of the spawn order.
  auto limiter = MakeLifoLimiter(/*cap=*/1, std::chrono::seconds(2));
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
    limiter->AwaitBacklogSize(i + 1);
  }

  // Release the held slot. Waiter kWaiters-1 (last in) wakes
  // first; on completion of each, the next waiter (in reverse
  // order of arrival) is served.
  held->OnSuccess();
  for (int i = kWaiters - 1; i >= 0; --i) {
    woke[i].WaitForNotification();
    ASSERT_TRUE(slots[i].has_value()) << "waiter " << i;
    slots[i]->OnSuccess();
  }
  for (auto& t : threads) t.join();

  EXPECT_EQ(limiter->BacklogSize(), 0);
}

TEST(LifoBlockingLimiterTest, BacklogCapEnforced) {
  // BacklogSize=2: with the delegate saturated and 2 waiters
  // already parked, a third arrival is rejected immediately
  // rather than queued. Uses a long enough timeout that the
  // already-parked waiters never themselves time out.
  auto limiter = MakeLifoLimiter(/*cap=*/1, std::chrono::seconds(2),
                                 /*backlog_size=*/2);
  auto held = limiter->TryAcquire();
  ASSERT_TRUE(held);

  std::vector<std::thread> parked_threads;
  std::array<absl::Notification, 2> parked_returned;
  std::array<std::optional<Limiter::SlotGuard>, 2> parked_slots;
  for (int i = 0; i < 2; ++i) {
    parked_threads.emplace_back([&, i] {
      parked_slots[i] = limiter->TryAcquire();
      parked_returned[i].Notify();
    });
    limiter->AwaitBacklogSize(i + 1);
  }

  // Backlog is now full. A third TryAcquire from the main
  // thread must return nullopt without queuing.
  EXPECT_FALSE(limiter->TryAcquire().has_value());
  EXPECT_EQ(limiter->BacklogSize(), 2);

  // Release everything cleanly.
  held->OnSuccess();
  for (int i = 1; i >= 0; --i) {
    parked_returned[i].WaitForNotification();
    if (parked_slots[i]) parked_slots[i]->OnSuccess();
  }
  for (auto& t : parked_threads) t.join();
}

TEST(LifoBlockingLimiterTest, PerRequestTimeoutFunctionHonored) {
  // The timeout function is invoked per acquire and gets the
  // context. A waiter with a 50 ms timeout returns nullopt; a
  // sibling with a 2 second timeout admits as soon as a slot
  // is released. The function maps "short" to 50 ms and "long"
  // to 2 s.
  auto timeout_fn = [](std::string_view ctx) -> std::chrono::milliseconds {
    if (ctx == "short") return std::chrono::milliseconds(50);
    return std::chrono::seconds(2);
  };
  auto inner = SimpleLimiter::Builder()
                   .Limit(FixedLimit::Of(1))
                   .Id("inner")
                   .Build()
                   .value();
  auto limiter = LifoBlockingLimiter::Builder()
                     .Delegate(std::move(inner))
                     .BacklogTimeout(timeout_fn)
                     .BacklogSize(10)
                     .Build()
                     .value();
  auto held = limiter->TryAcquire();
  ASSERT_TRUE(held);

  // Short-timeout waiter parks; with no slot released within
  // 50 ms, it returns nullopt.
  std::optional<Limiter::SlotGuard> short_result = limiter->TryAcquire("short");
  EXPECT_FALSE(short_result.has_value());

  // Long-timeout waiter parks; main thread releases the held
  // slot before the 2 s deadline. The waiter is served.
  std::optional<Limiter::SlotGuard> long_result;
  absl::Notification long_done;
  std::thread long_waiter([&] {
    long_result = limiter->TryAcquire("long");
    long_done.Notify();
  });
  limiter->AwaitBacklogSize(1);
  held->OnSuccess();
  long_done.WaitForNotification();
  long_waiter.join();
  EXPECT_TRUE(long_result.has_value());
}

TEST(LifoBlockingLimiterTest, LimitIncreaseServesWaitersAfterRelease) {
  // Cap starts at 1 with both slots and a held slot. Two
  // waiters park. Raise the cap to 3 (delegate now admits 2
  // more). Release the held slot to fire Unblock; both waiters
  // are served, in LIFO order.
  auto settable = SettableLimit::StartingAt(1);
  auto* settable_ptr = settable.get();
  auto inner = SimpleLimiter::Builder()
                   .Limit(std::move(settable))
                   .Id("inner")
                   .Build()
                   .value();
  auto limiter = LifoBlockingLimiter::Builder()
                     .Delegate(std::move(inner))
                     .BacklogTimeout(std::chrono::seconds(2))
                     .BacklogSize(10)
                     .Build()
                     .value();

  auto held = limiter->TryAcquire();
  ASSERT_TRUE(held);

  std::array<absl::Notification, 2> woke;
  std::array<std::optional<Limiter::SlotGuard>, 2> slots;
  std::vector<std::thread> threads;
  for (int i = 0; i < 2; ++i) {
    threads.emplace_back([&, i] {
      slots[i] = limiter->TryAcquire();
      woke[i].Notify();
    });
    limiter->AwaitBacklogSize(i + 1);
  }

  settable_ptr->SetLimit(3);
  held->OnSuccess();

  // After OnSuccess fires Unblock, the delegate admits both
  // parked waiters (in-flight will be 2, cap 3). LIFO order:
  // waiter 1 first, waiter 0 second.
  for (int i = 1; i >= 0; --i) {
    woke[i].WaitForNotification();
    ASSERT_TRUE(slots[i].has_value()) << "waiter " << i;
    slots[i]->OnSuccess();
  }
  for (auto& t : threads) t.join();
}

// Delegate that lets the test deterministically engineer the
// timeout-acquire race: the first N TryAcquire calls pass
// through to the inner limiter; from call N+1 on, TryAcquire
// blocks until `Signal()` is called by the test thread.
// Holding the limiter inside `Unblock` while the waiter's
// deadline passes is exactly the window the race fix
// addresses.
class BlockableDelegate : public Limiter {
 public:
  explicit BlockableDelegate(std::unique_ptr<Limiter> inner)
      : inner_(std::move(inner)) {}

  std::optional<SlotGuard> TryAcquire(std::string_view context) override {
    int const n = call_count_.fetch_add(1, std::memory_order_relaxed);
    if (n >= block_after_call_.load(std::memory_order_relaxed)) {
      proceed_.WaitForNotification();
    }
    return inner_->TryAcquire(context);
  }

  void SetBlockAfterCall(int n) {
    block_after_call_.store(n, std::memory_order_relaxed);
  }

  void Signal() { proceed_.Notify(); }

 protected:
  // BlockableDelegate returns the inner's SlotGuards directly,
  // so its own OnSlotComplete is never invoked. Provide a no-op
  // override to satisfy the pure-virtual.
  void OnSlotComplete(CompletionStatus /*status*/, int64_t /*start_time_ns*/,
                      int /*inflight_at_acquire*/, bool /*bypassed*/,
                      int /*partition_index*/) override {}

 private:
  std::unique_ptr<Limiter> inner_;
  std::atomic<int> call_count_{0};
  std::atomic<int> block_after_call_{0};
  absl::Notification proceed_;
};

TEST(LifoBlockingLimiterTest, TimeoutAcquireRaceReturnsDeliveredSlot) {
  // Upstream-fixed race regression. The fix line: after the
  // wait returns (with notified=false on timeout), the waiter
  // acquires mu_ and re-reads waiter.slot before deciding to
  // return nullopt. If the unblocker set the slot under mu_
  // while the waiter was timing out, the slot survives.
  //
  // The test engineers the race deterministically. A
  // BlockableDelegate gates the unblocker's call to the inner
  // limiter: call 0 (the main thread's held acquire) and call
  // 1 (the waiter's fast-path attempt) pass through; call 2
  // (from Unblock after held release) blocks until the test
  // signals. The test releases the held slot on a separate
  // thread, sleeps long enough for the waiter's 50 ms deadline
  // to elapse (the waiter then parks on mu_, which the
  // unblocker still holds), and finally signals the delegate.
  // The unblocker assigns waiter.slot, notifies, and releases
  // mu_; the waiter resumes, re-reads waiter.slot under mu_,
  // and returns the delivered slot.
  auto inner = SimpleLimiter::Builder()
                   .Limit(FixedLimit::Of(1))
                   .Id("inner")
                   .Build()
                   .value();
  auto blockable = std::make_unique<BlockableDelegate>(std::move(inner));
  auto* blockable_ptr = blockable.get();
  blockable_ptr->SetBlockAfterCall(2);
  auto limiter = LifoBlockingLimiter::Builder()
                     .Delegate(std::move(blockable))
                     .BacklogTimeout(std::chrono::milliseconds(50))
                     .Build()
                     .value();

  auto held = limiter->TryAcquire();  // call 0: passes through.
  ASSERT_TRUE(held);

  std::optional<Limiter::SlotGuard> waiter_result;
  absl::Notification waiter_returned;
  std::thread waiter([&] {
    waiter_result = limiter->TryAcquire();  // call 1 fast path; returns
                                            // nullopt; waiter parks.
    waiter_returned.Notify();
  });
  limiter->AwaitBacklogSize(1);

  // Release the held slot on a separate thread. The release
  // path runs OnSlotComplete → Unblock, which calls
  // delegate.TryAcquire (call 2, blocks on `proceed_`). mu_
  // stays held by Unblock for the duration of that block.
  std::thread releaser([&] { held->OnSuccess(); });

  // Wait long enough for the waiter's deadline (50 ms) to
  // elapse with the unblocker still holding mu_. After the
  // deadline, the waiter parks on mu_ (held by the unblocker).
  std::this_thread::sleep_for(std::chrono::milliseconds(150));

  // Now let the unblocker complete: it admits via the inner,
  // assigns waiter.slot, notifies, releases mu_.
  blockable_ptr->Signal();

  waiter_returned.WaitForNotification();
  waiter.join();
  releaser.join();

  // The race fix delivers the slot rather than losing it.
  EXPECT_TRUE(waiter_result.has_value());
}

TEST(LifoBlockingLimiterTest, LimitDecreaseRejectsExcessWaiters) {
  // A cap reduction while waiters are parked must not break
  // the limiter — the waiters who can't be served (because
  // in-flight is at or above the reduced cap after each
  // release) eventually time out and return nullopt.
  auto settable = SettableLimit::StartingAt(2);
  auto* settable_ptr = settable.get();
  auto inner = SimpleLimiter::Builder()
                   .Limit(std::move(settable))
                   .Id("inner")
                   .Build()
                   .value();
  auto limiter = LifoBlockingLimiter::Builder()
                     .Delegate(std::move(inner))
                     .BacklogTimeout(std::chrono::milliseconds(60))
                     .BacklogSize(10)
                     .Build()
                     .value();

  auto a = limiter->TryAcquire();
  auto b = limiter->TryAcquire();
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);

  // Reduce cap to 0 — no admission will be possible regardless
  // of releases.
  settable_ptr->SetLimit(0);

  // Spawn two waiters with the 60 ms timeout configured above.
  std::array<absl::Notification, 2> woke;
  std::array<std::optional<Limiter::SlotGuard>, 2> slots;
  std::vector<std::thread> threads;
  for (int i = 0; i < 2; ++i) {
    threads.emplace_back([&, i] {
      slots[i] = limiter->TryAcquire();
      woke[i].Notify();
    });
    limiter->AwaitBacklogSize(i + 1);
  }

  // Release both held slots. Each release triggers Unblock,
  // which calls delegate.TryAcquire — the delegate denies
  // because cap=0. Waiters stay parked until their timeouts
  // elapse, then return nullopt.
  a->OnSuccess();
  b->OnSuccess();

  for (int i = 0; i < 2; ++i) {
    woke[i].WaitForNotification();
    EXPECT_FALSE(slots[i].has_value()) << "waiter " << i;
  }
  for (auto& t : threads) t.join();
  EXPECT_EQ(limiter->BacklogSize(), 0);
}

TEST(LifoBlockingLimiterTest, CompletionStatusMirroredToDelegate) {
  auto inner_unique = SimpleLimiter::Builder()
                          .Limit(FixedLimit::Of(3))
                          .Id("inner")
                          .Build()
                          .value();
  auto* inner = inner_unique.get();
  auto limiter = LifoBlockingLimiter::Builder()
                     .Delegate(std::move(inner_unique))
                     .BacklogTimeout(std::chrono::seconds(1))
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
}

}  // namespace
}  // namespace limen

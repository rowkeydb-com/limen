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

#ifndef LIMEN_BLOCKING_LIMITER_H_
#define LIMEN_BLOCKING_LIMITER_H_

#include "limen/limiter.h"
#include "absl/base/thread_annotations.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/synchronization/notification.h"
#include <atomic>
#include <chrono>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace limen {

// Wraps a non-blocking Limiter and blocks the caller when the cap
// is full instead of rejecting immediately. Waiting callers are
// served in first-in-first-out order — a deliberate Limen
// divergence from upstream Netflix's BlockingLimiter, which uses
// `Object.wait` / `notifyAll` and makes no ordering guarantee.
// The FIFO discipline is what most operators expect from a
// "blocking" limiter; the test plan includes an explicit
// ordering assertion that upstream's test suite does not have.
//
// Each waiting caller is a stack-allocated `WaiterSlot`
// registered on an `absl::Mutex`-guarded queue. When a release
// fires (the user completes a SlotGuard returned by an earlier
// admission), the limiter walks the queue head-first, tries the
// delegate, and hands the resulting SlotGuard to the next
// waiter. The waiter wakes via `absl::Notification`.
//
// The per-acquire SlotGuard returned to the caller is a thin
// outer wrapper around the delegate's SlotGuard: the outer
// SlotGuard's `partition_index` field carries a unique slot id;
// the delegate's SlotGuard lives in an internal map keyed by
// that id; the outer SlotGuard's OnSlotComplete looks the inner
// up, mirrors the caller's completion status (OnSuccess /
// OnIgnore / OnDropped) onto the inner, then releases the inner
// and calls Unblock to wake the next waiter.
//
// Use this limiter for batch clients that can afford a small
// wait. Not appropriate for callers running on a fixed event
// loop or in a direct-executor model — blocking a request thread
// of those will starve the rest of the work on that thread.
//
// Ported from Netflix's `BlockingLimiter.java`.
class BlockingLimiter final : public Limiter {
 public:
  // Maximum wait duration permitted by the Builder. Mirrors
  // upstream Java's `MAX_TIMEOUT = Duration.ofHours(1)` —
  // longer waits indicate a misconfiguration rather than a
  // legitimate use of blocking.
  static constexpr std::chrono::milliseconds kMaxTimeout =
      std::chrono::hours(1);

  class Builder {
   public:
    Builder() = default;

    // Wrapped non-blocking limiter. Required.
    Builder& Delegate(std::unique_ptr<Limiter> delegate);

    // Maximum time a caller will block waiting for a slot.
    // Must not exceed `kMaxTimeout`. Defaults to one hour.
    Builder& Timeout(std::chrono::milliseconds timeout);

    // Validates the configuration and constructs the limiter.
    // Returns `InvalidArgumentError` if `Delegate()` was not
    // supplied or if the timeout exceeds `kMaxTimeout`.
    absl::StatusOr<std::unique_ptr<BlockingLimiter>> Build();

   private:
    std::unique_ptr<Limiter> delegate_;
    std::chrono::milliseconds timeout_ = kMaxTimeout;
  };

  std::optional<SlotGuard> TryAcquire(std::string_view context = {}) override;

  // Current number of callers parked on the wait queue. Wait-free
  // approximate read; mostly useful for tests and for operator
  // observability (a sustained non-zero value signals back-pressure
  // the application should track).
  int QueueSize() const ABSL_LOCKS_EXCLUDED(mu_);

  // Block until the queue has at least `n` parked waiters. Used by
  // tests to coordinate "the waiter has reached TryAcquire and is
  // parked on the queue" without sleep-based timing. Uses
  // `absl::Mutex::Await` so it is futex-backed and TSan-friendly.
  void AwaitQueueSize(int n) const ABSL_LOCKS_EXCLUDED(mu_);

 protected:
  void OnSlotComplete(CompletionStatus status, int64_t start_time_ns,
                      int inflight_at_acquire, bool bypassed,
                      int partition_index) override;

 private:
  // Construction goes through the inner Builder. The PrivateTag
  // makes the public constructor unusable except through
  // make_unique called from Build().
  struct PrivateTag {
   private:
    PrivateTag() = default;
    friend class Builder;
  };

 public:
  BlockingLimiter(PrivateTag, std::unique_ptr<Limiter> delegate,
                  std::chrono::milliseconds timeout);

 private:
  // One waiter parked on the queue. Lives on the calling
  // thread's stack while that thread is blocked in TryAcquire,
  // so a raw pointer in the queue is safe — the caller cannot
  // return from TryAcquire until either the Notification fires
  // or the waiter is removed from the queue under mu_. The
  // `slot` field is mutated only under `BlockingLimiter::mu_`
  // (by Unblock on handoff, by the caller on timeout-race
  // recovery). The Clang thread-safety annotation cannot reach
  // mu_ from this nested type so the discipline is enforced by
  // code review.
  struct WaiterSlot {
    std::string_view context;
    std::optional<SlotGuard> slot;
    absl::Notification ready;
  };

  // Walks the waiter queue head-first and hands the next
  // available delegate SlotGuard to each waiter in turn. Returns
  // when the queue is empty or the delegate cannot admit another
  // request; the next call to Unblock (triggered by another
  // release) will resume from the new head.
  void Unblock() ABSL_LOCKS_EXCLUDED(mu_);

  std::unique_ptr<Limiter> const delegate_;
  std::chrono::milliseconds const timeout_;

  mutable absl::Mutex mu_;
  // FIFO queue of currently-parked waiters. Pointer values are
  // owned by the caller's stack; entries are removed either by
  // Unblock on successful handoff or by the waiter itself on
  // timeout / cancellation.
  std::list<WaiterSlot*> queue_ ABSL_GUARDED_BY(mu_);
  // In-flight delegate SlotGuards belonging to this limiter.
  // Keyed by a per-acquire id (also stored in the outer
  // SlotGuard's `partition_index` field). The map lookup gives
  // OnSlotComplete the inner SlotGuard so it can mirror the
  // caller's completion status onto the delegate.
  std::map<int, SlotGuard> active_slots_ ABSL_GUARDED_BY(mu_);
  std::atomic<int> next_slot_id_{0};
};

}  // namespace limen

#endif  // LIMEN_BLOCKING_LIMITER_H_

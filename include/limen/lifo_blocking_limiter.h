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

#ifndef LIMEN_LIFO_BLOCKING_LIMITER_H_
#define LIMEN_LIFO_BLOCKING_LIMITER_H_

#include "limen/limiter.h"
#include "absl/base/thread_annotations.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/synchronization/notification.h"
#include <atomic>
#include <chrono>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace limen {

// Wraps a non-blocking Limiter and blocks the caller when the cap
// is full, serving waiting callers in last-in-first-out order.
// Under overload, older queued requests have probably already
// timed out on the client side, so processing them is wasted
// work. LIFO ordering routes the freshest requests through
// first and lets stale ones either time out or be cleared by a
// backlog cap.
//
// Two knobs in addition to those `BlockingLimiter` carries:
//
// - `BacklogSize`: a cap on the number of concurrently queued
//   waiters. When the cap is reached, further blocked requests
//   are rejected immediately rather than queued. Default 100,
//   matching upstream Netflix.
//
// - `BacklogTimeout`: per-request timeout function. The
//   application supplies a callable that returns the maximum
//   wait duration as a function of the request context. This
//   lets request deadlines (e.g. a gRPC method's remaining
//   deadline) shape the wait. A fixed-duration overload sets
//   the same timeout for every request. Default 1 second
//   (matches upstream).
//
// Includes the upstream-fixed timeout-acquire race regression:
// a slot delivered to a waiter just as the waiter is timing
// out used to be lost; the Limen port preserves the fix (the
// waiter checks its slot under mu_ after the wait returns and
// returns the slot if it was set by the unblocker concurrently)
// and the regression test for it.
//
// Ported from Netflix's `LifoBlockingLimiter.java`.
class LifoBlockingLimiter final : public Limiter {
 public:
  // Computes the maximum wait duration for one request from
  // that request's context. The fixed-timeout overload of
  // `BacklogTimeout` returns a closure that ignores the
  // context.
  using TimeoutFn =
      std::function<std::chrono::milliseconds(std::string_view context)>;

  // Upper bound on the per-request wait duration enforced by
  // the fixed-timeout overload of `BacklogTimeout`. Mirrors
  // `BlockingLimiter::kMaxTimeout` — longer waits indicate a
  // misconfiguration. The dynamic-timeout overload cannot be
  // bounded at Build() time (timeouts are computed per request)
  // so the cap there is the application's own responsibility.
  static constexpr std::chrono::milliseconds kMaxTimeout =
      std::chrono::hours(1);

  class Builder {
   public:
    Builder() = default;

    // Wrapped non-blocking limiter. Required.
    Builder& Delegate(std::unique_ptr<Limiter> delegate);

    // Cap on concurrently queued waiters. Default 100. Must be
    // positive.
    Builder& BacklogSize(int size);

    // Fixed per-request timeout. Equivalent to passing a
    // `TimeoutFn` that ignores the context and returns this
    // duration. Default 1 second.
    Builder& BacklogTimeout(std::chrono::milliseconds timeout);

    // Per-request timeout function. The supplied callable is
    // invoked once per blocked acquire to compute the wait
    // deadline. The callable must be safe to invoke from any
    // thread.
    Builder& BacklogTimeout(TimeoutFn fn);

    absl::StatusOr<std::unique_ptr<LifoBlockingLimiter>> Build();

   private:
    std::unique_ptr<Limiter> delegate_;
    int backlog_size_ = 100;
    TimeoutFn timeout_fn_ = [](std::string_view) {
      return std::chrono::seconds(1);
    };
    // Populated when the fixed-duration `BacklogTimeout(ms)`
    // overload was used; empty when the per-context overload
    // was used (the per-context timeout cannot be expressed as
    // a single fixed value). Used by Build() to enforce the
    // `kMaxTimeout` cap on the fixed form, and exposed on the
    // built limiter via `GetFixedBacklogTimeout()`.
    std::optional<std::chrono::milliseconds> fixed_timeout_ =
        std::chrono::seconds(1);
  };

  std::optional<SlotGuard> TryAcquire(std::string_view context = {}) override;

  // Current number of callers parked on the wait deque.
  int BacklogSize() const ABSL_LOCKS_EXCLUDED(mu_);

  // Block until at least `n` waiters are parked. Test helper;
  // uses `absl::Mutex::Await` (futex-backed).
  void AwaitBacklogSize(int n) const ABSL_LOCKS_EXCLUDED(mu_);

  // The fixed timeout the Builder was configured with, or an
  // empty optional if the per-context timeout function was used
  // instead. Mirrors upstream's `getFixedBacklogTimeoutMillis()`
  // which returns `null` in the dynamic case; lets applications
  // introspect the configuration after Build().
  std::optional<std::chrono::milliseconds> GetFixedBacklogTimeout() const {
    return fixed_timeout_;
  }

 protected:
  void OnSlotComplete(CompletionStatus status, int64_t start_time_ns,
                      int inflight_at_acquire, bool bypassed,
                      int partition_index) override;

 private:
  struct PrivateTag {
   private:
    PrivateTag() = default;
    friend class Builder;
  };

 public:
  LifoBlockingLimiter(PrivateTag, std::unique_ptr<Limiter> delegate,
                      int backlog_size, TimeoutFn timeout_fn,
                      std::optional<std::chrono::milliseconds> fixed_timeout);

 private:
  // One waiter parked on the deque. Lives on the calling
  // thread's stack while that thread is blocked. The `slot`
  // field is mutated only under `LifoBlockingLimiter::mu_`.
  struct WaiterSlot {
    std::string_view context;
    std::optional<SlotGuard> slot;
    absl::Notification ready;
  };

  void Unblock() ABSL_LOCKS_EXCLUDED(mu_);

  std::unique_ptr<Limiter> const delegate_;
  int const backlog_size_;
  TimeoutFn const timeout_fn_;
  std::optional<std::chrono::milliseconds> const fixed_timeout_;

  mutable absl::Mutex mu_;
  // LIFO deque of currently-parked waiters: new arrivals push
  // to the front, releases pop from the front. A waiter that
  // times out removes itself from somewhere inside the deque
  // (typically near the back, where older arrivals have been
  // pushed by fresher ones).
  std::list<WaiterSlot*> deque_ ABSL_GUARDED_BY(mu_);
  std::map<int, SlotGuard> active_slots_ ABSL_GUARDED_BY(mu_);
  std::atomic<int> next_slot_id_{0};
};

}  // namespace limen

#endif  // LIMEN_LIFO_BLOCKING_LIMITER_H_

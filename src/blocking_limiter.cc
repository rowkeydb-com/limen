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
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "absl/synchronization/notification.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>

namespace limen {

BlockingLimiter::Builder& BlockingLimiter::Builder::Delegate(
    std::unique_ptr<Limiter> delegate) {
  delegate_ = std::move(delegate);
  return *this;
}

BlockingLimiter::Builder& BlockingLimiter::Builder::Timeout(
    std::chrono::milliseconds timeout) {
  timeout_ = timeout;
  return *this;
}

absl::StatusOr<std::unique_ptr<BlockingLimiter>>
BlockingLimiter::Builder::Build() {
  if (delegate_ == nullptr) {
    return absl::InvalidArgumentError(
        "BlockingLimiter::Builder::Delegate() is required");
  }
  if (timeout_ <= std::chrono::milliseconds::zero()) {
    return absl::InvalidArgumentError(
        "BlockingLimiter::Builder::Timeout() must be positive");
  }
  if (timeout_ > kMaxTimeout) {
    return absl::InvalidArgumentError(
        absl::StrCat("BlockingLimiter::Builder::Timeout() must not exceed ",
                     kMaxTimeout.count(), " ms (one hour)"));
  }
  return std::make_unique<BlockingLimiter>(PrivateTag{}, std::move(delegate_),
                                           timeout_);
}

BlockingLimiter::BlockingLimiter(PrivateTag, std::unique_ptr<Limiter> delegate,
                                 std::chrono::milliseconds timeout)
    : delegate_(std::move(delegate)), timeout_(timeout) {}

int BlockingLimiter::QueueSize() const {
  absl::MutexLock lock(mu_);
  return static_cast<int>(queue_.size());
}

void BlockingLimiter::AwaitQueueSize(int n) const {
  absl::MutexLock lock(mu_);
  auto const condition = [this, n]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    return static_cast<int>(queue_.size()) >= n;
  };
  mu_.Await(absl::Condition(&condition));
}

std::optional<Limiter::SlotGuard> BlockingLimiter::TryAcquire(
    std::string_view context) {
  // Fast path: try the delegate immediately. If it admits, wrap
  // the resulting SlotGuard in an outer SlotGuard that carries
  // our per-acquire slot id in its `partition_index` field.
  if (auto inner = delegate_->TryAcquire(context); inner) {
    int const id = next_slot_id_.fetch_add(1, std::memory_order_relaxed);
    {
      absl::MutexLock lock(mu_);
      active_slots_.emplace(id, std::move(*inner));
    }
    return MakeSlot(this, /*start_time_ns=*/0, /*inflight_at_acquire=*/0,
                    /*bypassed=*/false, /*partition_index=*/id);
  }

  // Slow path: park the caller on the queue under mu_. The
  // WaiterSlot lives on this stack frame for the lifetime of
  // the wait; the queue holds a raw pointer to it. The waiter
  // cannot return from this function (and so cannot let its
  // stack go) until either the Notification fires or it removes
  // itself from the queue post-timeout.
  WaiterSlot waiter;
  waiter.context = context;
  {
    absl::MutexLock lock(mu_);
    queue_.push_back(&waiter);
  }

  absl::Time const deadline = absl::Now() + absl::FromChrono(timeout_);
  bool const notified = waiter.ready.WaitForNotificationWithDeadline(deadline);

  // Whether the notification fired or the wait timed out, the
  // unblock-side may have set `waiter.slot` under mu_. Re-acquire
  // mu_ to read it safely and to remove the waiter from the
  // queue if it is still there (it has been removed by Unblock
  // when the notification fired; on timeout it usually has not).
  std::optional<SlotGuard> delivered;
  {
    absl::MutexLock lock(mu_);
    if (waiter.slot.has_value()) {
      delivered = std::move(waiter.slot);
    } else {
      // Timed out before a slot was delivered. Remove ourselves
      // from the queue. The queue is a doubly-linked list so
      // erase-by-value (linear search) is O(n) in the queue
      // size, which is acceptable on a timeout-path.
      for (auto it = queue_.begin(); it != queue_.end(); ++it) {
        if (*it == &waiter) {
          queue_.erase(it);
          break;
        }
      }
    }
  }

  if (!delivered) {
    (void)notified;  // notified=false on timeout; notified=true would imply
                     // `waiter.slot` was set under mu_ above.
    return std::nullopt;
  }

  // We received a slot from the delegate (either through normal
  // handoff or the timeout-race window). Wrap it in our outer
  // SlotGuard.
  int const id = next_slot_id_.fetch_add(1, std::memory_order_relaxed);
  {
    absl::MutexLock lock(mu_);
    active_slots_.emplace(id, std::move(*delivered));
  }
  return MakeSlot(this, /*start_time_ns=*/0, /*inflight_at_acquire=*/0,
                  /*bypassed=*/false, /*partition_index=*/id);
}

void BlockingLimiter::OnSlotComplete(CompletionStatus status,
                                     int64_t /*start_time_ns*/,
                                     int /*inflight_at_acquire*/,
                                     bool /*bypassed*/, int partition_index) {
  // Extract the inner SlotGuard under mu_, then release the
  // mutex before mirroring the completion status onto it.
  // Mirroring runs the delegate's OnSlotComplete which may
  // touch its own locks; we must not hold ours during that.
  std::optional<SlotGuard> inner;
  {
    absl::MutexLock lock(mu_);
    auto it = active_slots_.find(partition_index);
    if (it != active_slots_.end()) {
      inner = std::move(it->second);
      active_slots_.erase(it);
    }
  }

  if (inner.has_value()) {
    switch (status) {
      case CompletionStatus::kSuccess:
        inner->OnSuccess();
        break;
      case CompletionStatus::kIgnored:
        inner->OnIgnore();
        break;
      case CompletionStatus::kDropped:
        inner->OnDropped();
        break;
    }
    // inner falls out of scope here; its destructor is a no-op
    // because Complete was called above.
  }

  // Wake the next waiter (if any) by walking the queue and
  // handing off whatever the delegate now admits.
  Unblock();
}

void BlockingLimiter::Unblock() {
  // Precondition: the wrapped delegate's `TryAcquire` must not
  // re-enter this limiter, directly or transitively. Unblock
  // holds `mu_` across the delegate call so a callback that
  // tried to acquire `mu_` would deadlock.
  absl::MutexLock lock(mu_);
  while (!queue_.empty()) {
    WaiterSlot* const w = queue_.front();
    auto inner = delegate_->TryAcquire(w->context);
    if (!inner) {
      // The delegate cannot admit another request right now. The
      // next release will retry; until then, leave the waiter on
      // the queue and stop walking.
      return;
    }
    w->slot = std::move(*inner);
    queue_.pop_front();
    // Notify after pop so the waker observes a consistent state
    // when it re-acquires mu_.
    w->ready.Notify();
  }
}

}  // namespace limen

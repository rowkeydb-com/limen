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

LifoBlockingLimiter::Builder& LifoBlockingLimiter::Builder::Delegate(
    std::unique_ptr<Limiter> delegate) {
  delegate_ = std::move(delegate);
  return *this;
}

LifoBlockingLimiter::Builder& LifoBlockingLimiter::Builder::BacklogSize(
    int size) {
  backlog_size_ = size;
  return *this;
}

LifoBlockingLimiter::Builder& LifoBlockingLimiter::Builder::BacklogTimeout(
    std::chrono::milliseconds timeout) {
  timeout_fn_ = [timeout](std::string_view) { return timeout; };
  fixed_timeout_ = timeout;
  return *this;
}

LifoBlockingLimiter::Builder& LifoBlockingLimiter::Builder::BacklogTimeout(
    TimeoutFn fn) {
  timeout_fn_ = std::move(fn);
  fixed_timeout_.reset();
  return *this;
}

absl::StatusOr<std::unique_ptr<LifoBlockingLimiter>>
LifoBlockingLimiter::Builder::Build() {
  if (delegate_ == nullptr) {
    return absl::InvalidArgumentError(
        "LifoBlockingLimiter::Builder::Delegate() is required");
  }
  if (backlog_size_ < 1) {
    return absl::InvalidArgumentError(absl::StrCat(
        "LifoBlockingLimiter::Builder::BacklogSize() must be positive; got ",
        backlog_size_));
  }
  if (!timeout_fn_) {
    return absl::InvalidArgumentError(
        "LifoBlockingLimiter::Builder::BacklogTimeout() must not be null");
  }
  if (fixed_timeout_.has_value()) {
    if (*fixed_timeout_ <= std::chrono::milliseconds::zero()) {
      return absl::InvalidArgumentError(
          "LifoBlockingLimiter::Builder::BacklogTimeout() must be positive");
    }
    if (*fixed_timeout_ > kMaxTimeout) {
      return absl::InvalidArgumentError(absl::StrCat(
          "LifoBlockingLimiter::Builder::BacklogTimeout() must not exceed ",
          kMaxTimeout.count(), " ms (one hour)"));
    }
  }
  return std::make_unique<LifoBlockingLimiter>(
      PrivateTag{}, std::move(delegate_), backlog_size_, std::move(timeout_fn_),
      fixed_timeout_);
}

LifoBlockingLimiter::LifoBlockingLimiter(
    PrivateTag, std::unique_ptr<Limiter> delegate, int backlog_size,
    TimeoutFn timeout_fn,
    std::optional<std::chrono::milliseconds> fixed_timeout)
    : delegate_(std::move(delegate)),
      backlog_size_(backlog_size),
      timeout_fn_(std::move(timeout_fn)),
      fixed_timeout_(fixed_timeout) {}

int LifoBlockingLimiter::BacklogSize() const {
  absl::MutexLock lock(mu_);
  return static_cast<int>(deque_.size());
}

void LifoBlockingLimiter::AwaitBacklogSize(int n) const {
  absl::MutexLock lock(mu_);
  auto const condition = [this, n]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    return static_cast<int>(deque_.size()) >= n;
  };
  mu_.Await(absl::Condition(&condition));
}

std::optional<Limiter::SlotGuard> LifoBlockingLimiter::TryAcquire(
    std::string_view context) {
  // Fast path: try the delegate immediately.
  if (auto inner = delegate_->TryAcquire(context); inner) {
    int const id = next_slot_id_.fetch_add(1, std::memory_order_relaxed);
    {
      absl::MutexLock lock(mu_);
      active_slots_.emplace(id, std::move(*inner));
    }
    return MakeSlot(this, /*start_time_ns=*/0, /*inflight_at_acquire=*/0,
                    /*bypassed=*/false, /*partition_index=*/id);
  }

  // Slow path: park on the front of the deque (LIFO). If the
  // backlog is full, reject immediately.
  WaiterSlot waiter;
  waiter.context = context;
  {
    absl::MutexLock lock(mu_);
    if (static_cast<int>(deque_.size()) >= backlog_size_) {
      return std::nullopt;
    }
    deque_.push_front(&waiter);
  }

  std::chrono::milliseconds const timeout = timeout_fn_(context);
  absl::Time const deadline = timeout <= std::chrono::milliseconds::zero()
                                  ? absl::Now()
                                  : absl::Now() + absl::FromChrono(timeout);
  bool const notified = waiter.ready.WaitForNotificationWithDeadline(deadline);

  // Whether the notification fired or the wait timed out, the
  // unblocker may have set `waiter.slot` under mu_. Re-acquire
  // mu_ to inspect it and to remove ourselves from the deque if
  // we are still parked.
  std::optional<SlotGuard> delivered;
  {
    absl::MutexLock lock(mu_);
    if (waiter.slot.has_value()) {
      delivered = std::move(waiter.slot);
    } else {
      // Timed out before a slot was delivered. Remove ourselves
      // from the deque. The dequeue is LIFO so a timed-out
      // waiter is most likely near the back (older arrivals
      // were pushed there by fresher ones). Scan from back to
      // front to keep the typical-case cost minimal.
      for (auto it = deque_.rbegin(); it != deque_.rend(); ++it) {
        if (*it == &waiter) {
          // `erase` requires a forward iterator; for a
          // reverse_iterator `r`, `std::next(r).base()` is the
          // forward iterator pointing at the same element.
          deque_.erase(std::next(it).base());
          break;
        }
      }
    }
  }

  if (!delivered) {
    (void)notified;
    return std::nullopt;
  }

  int const id = next_slot_id_.fetch_add(1, std::memory_order_relaxed);
  {
    absl::MutexLock lock(mu_);
    active_slots_.emplace(id, std::move(*delivered));
  }
  return MakeSlot(this, /*start_time_ns=*/0, /*inflight_at_acquire=*/0,
                  /*bypassed=*/false, /*partition_index=*/id);
}

void LifoBlockingLimiter::OnSlotComplete(CompletionStatus status,
                                         int64_t /*start_time_ns*/,
                                         int /*inflight_at_acquire*/,
                                         bool /*bypassed*/,
                                         int partition_index) {
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
  }

  Unblock();
}

void LifoBlockingLimiter::Unblock() {
  // Precondition: the wrapped delegate's `TryAcquire` must not
  // re-enter this limiter, directly or transitively. Unblock
  // holds `mu_` across the delegate call so a callback that
  // tried to acquire `mu_` would deadlock.
  absl::MutexLock lock(mu_);
  while (!deque_.empty()) {
    WaiterSlot* const w = deque_.front();
    auto inner = delegate_->TryAcquire(w->context);
    if (!inner) {
      return;
    }
    w->slot = std::move(*inner);
    deque_.pop_front();
    w->ready.Notify();
  }
}

}  // namespace limen

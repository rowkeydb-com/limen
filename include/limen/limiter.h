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

#ifndef LIMEN_LIMITER_H_
#define LIMEN_LIMITER_H_

#include <cstdint>
#include <optional>
#include <string_view>

namespace limen {

// Contract for an admission-control limiter.
//
// A caller asks the limiter for permission to start a unit of work
// by calling `TryAcquire`. The return value is `std::nullopt` on
// rejection and a `SlotGuard` on admission. The caller invokes one
// of `OnSuccess`, `OnIgnore`, or `OnDropped` on the SlotGuard when
// the work completes; the SlotGuard's destructor defaults to
// `OnIgnore` if no completion call was made by the time the
// SlotGuard falls out of scope.
//
// `SlotGuard` is move-only and stores its per-call state inline:
// a back-pointer to the limiter, the start timestamp, the in-flight
// count at admission, a bypassed flag, a completion-flag, and an
// optional partition index (-1 for non-partitioned limiters). The
// per-request path performs no heap allocation: TryAcquire returns
// the SlotGuard by value through `std::optional`, and the SlotGuard
// lives on the caller's stack until the work completes.
//
// The `context` argument is an opaque string view applications use
// to scope the admission decision: a route name, a tenant id, an
// RPC method, anything. The default context is empty.
//
// Ported from Netflix's `Limiter.java`. The polymorphic-listener
// shape upstream's Java relies on for garbage-collected lifetime
// becomes a value-type RAII guard in C++ so the per-request hot
// path stays allocation-free.
class Limiter {
 public:
  // Completion outcomes used by SlotGuard to notify the limiter.
  enum class CompletionStatus { kSuccess, kIgnored, kDropped };

  class SlotGuard;

  Limiter() = default;
  virtual ~Limiter() = default;

  Limiter(Limiter const&) = delete;
  Limiter& operator=(Limiter const&) = delete;
  Limiter(Limiter&&) = delete;
  Limiter& operator=(Limiter&&) = delete;

  // Attempts to acquire admission for one unit of work. Returns a
  // SlotGuard on admission or std::nullopt on rejection. The caller
  // must invoke exactly one completion method on the returned
  // SlotGuard (or rely on the destructor default of OnIgnore).
  virtual std::optional<SlotGuard> TryAcquire(
      std::string_view context = {}) = 0;

 protected:
  // Called by SlotGuard on completion (either an explicit
  // On{Success,Ignore,Dropped} method invocation or the
  // destructor default). Implementations decrement the in-flight
  // counter, increment the appropriate outcome counter, and feed
  // a sample to the wrapped algorithm where appropriate.
  // `partition_index` is -1 for non-partitioned limiters and the
  // resolved partition slot for AbstractPartitionedLimiter.
  virtual void OnSlotComplete(CompletionStatus status, int64_t start_time_ns,
                              int inflight_at_acquire, bool bypassed,
                              int partition_index) = 0;

  // Factory used by Limiter implementations to construct a
  // SlotGuard. The SlotGuard's constructor is private and only
  // Limiter is a friend; routing all construction through this
  // static factory keeps the access-control surface small (one
  // friend declaration on SlotGuard, no per-subclass friending)
  // and gives every derived class a single, named entry point.
  // `partition_index` defaults to -1; AbstractPartitionedLimiter
  // passes the resolved partition slot so SlotGuard's destructor
  // can route the release to the right partition counter.
  static SlotGuard MakeSlot(Limiter* limiter, int64_t start_time_ns,
                            int inflight_at_acquire, bool bypassed,
                            int partition_index = -1);
};

// Move-only RAII guard returned by TryAcquire on admission.
class Limiter::SlotGuard {
 public:
  ~SlotGuard() {
    if (limiter_ != nullptr && !completed_) {
      limiter_->OnSlotComplete(CompletionStatus::kIgnored, start_time_ns_,
                               inflight_at_acquire_, bypassed_,
                               partition_index_);
    }
  }

  SlotGuard(SlotGuard const&) = delete;
  SlotGuard& operator=(SlotGuard const&) = delete;

  SlotGuard(SlotGuard&& other) noexcept
      : limiter_(other.limiter_),
        start_time_ns_(other.start_time_ns_),
        inflight_at_acquire_(other.inflight_at_acquire_),
        partition_index_(other.partition_index_),
        bypassed_(other.bypassed_),
        completed_(other.completed_) {
    other.limiter_ = nullptr;
  }

  SlotGuard& operator=(SlotGuard&& other) noexcept {
    if (this != &other) {
      if (limiter_ != nullptr && !completed_) {
        limiter_->OnSlotComplete(CompletionStatus::kIgnored, start_time_ns_,
                                 inflight_at_acquire_, bypassed_,
                                 partition_index_);
      }
      limiter_ = other.limiter_;
      start_time_ns_ = other.start_time_ns_;
      inflight_at_acquire_ = other.inflight_at_acquire_;
      partition_index_ = other.partition_index_;
      bypassed_ = other.bypassed_;
      completed_ = other.completed_;
      other.limiter_ = nullptr;
    }
    return *this;
  }

  // The wrapped unit of work completed normally. Releases the
  // in-flight slot and feeds a successful sample to the wrapped
  // algorithm. Idempotent: subsequent calls are no-ops.
  void OnSuccess() { Complete(CompletionStatus::kSuccess); }

  // The wrapped unit of work completed but should not be treated
  // as a real signal of system health (a duplicate retry, a
  // cancellation, an early bail-out). Releases the in-flight slot
  // without feeding a sample.
  void OnIgnore() { Complete(CompletionStatus::kIgnored); }

  // The wrapped unit of work was dropped by the downstream system
  // (timed out, rejected by a load-shedder lower in the stack).
  // Releases the in-flight slot and feeds a `did_drop=true` sample
  // to the wrapped algorithm.
  void OnDropped() { Complete(CompletionStatus::kDropped); }

 private:
  friend class Limiter;

  SlotGuard(Limiter* limiter, int64_t start_time_ns, int inflight_at_acquire,
            bool bypassed, int partition_index)
      : limiter_(limiter),
        start_time_ns_(start_time_ns),
        inflight_at_acquire_(inflight_at_acquire),
        partition_index_(partition_index),
        bypassed_(bypassed),
        completed_(false) {}

  void Complete(CompletionStatus status) {
    if (limiter_ == nullptr || completed_) {
      return;
    }
    limiter_->OnSlotComplete(status, start_time_ns_, inflight_at_acquire_,
                             bypassed_, partition_index_);
    completed_ = true;
  }

  Limiter* limiter_;
  int64_t start_time_ns_;
  int inflight_at_acquire_;
  int partition_index_;
  bool bypassed_;
  bool completed_;
};

inline Limiter::SlotGuard Limiter::MakeSlot(Limiter* limiter,
                                            int64_t start_time_ns,
                                            int inflight_at_acquire,
                                            bool bypassed,
                                            int partition_index) {
  return SlotGuard{limiter, start_time_ns, inflight_at_acquire, bypassed,
                   partition_index};
}

}  // namespace limen

#endif  // LIMEN_LIMITER_H_

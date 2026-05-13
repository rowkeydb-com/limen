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

#ifndef LIMEN_ABSTRACT_LIMIT_H_
#define LIMEN_ABSTRACT_LIMIT_H_

#include "limen/limit.h"
#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include <atomic>
#include <cstdint>
#include <vector>

namespace limen {

// Concrete base class for `Limit` algorithms. Owns the current limit
// value, the change-callback list, and the locking that serialises
// `Update` invocations against each other.
//
// Subclasses implement `Update`, the per-sample math that decides
// what the new limit should be.
//
// `GetLimit` reads the limit value with a relaxed atomic load and is
// safe to call from any thread without coordination. `OnSample` and
// `NotifyOnChange` take an internal mutex.
//
// Ported from Netflix's `AbstractLimit.java`.
class AbstractLimit : public Limit {
 public:
  explicit AbstractLimit(int initial_limit);

  int GetLimit() const final { return limit_.load(std::memory_order_relaxed); }

  void OnSample(int64_t start_time_ns, int64_t rtt_ns, int inflight,
                bool did_drop) final ABSL_LOCKS_EXCLUDED(mu_);

  void NotifyOnChange(ChangeCallback callback) final ABSL_LOCKS_EXCLUDED(mu_);

 protected:
  // Subclasses implement the per-sample math here. Invoked under the
  // internal mutex; subclasses must not call other public methods of
  // the same instance from within `Update`.
  virtual int Update(int64_t start_time_ns, int64_t rtt_ns, int inflight,
                     bool did_drop) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) = 0;

  // Publishes a new limit value. Notifies change callbacks if the
  // new value differs from the current one. Takes the internal
  // mutex; callable from subclasses that need to programmatically
  // change the limit outside the `Update` path (for example,
  // `SettableLimit`).
  void SetLimit(int new_limit) ABSL_LOCKS_EXCLUDED(mu_);

 private:
  // Internal `SetLimit` variant that runs with the mutex already
  // held. Used both by the public `SetLimit` and by `OnSample`'s
  // post-`Update` publication path, which already holds the mutex.
  void SetLimitLocked(int new_limit) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  std::atomic<int> limit_;
  absl::Mutex mu_;
  std::vector<ChangeCallback> listeners_ ABSL_GUARDED_BY(mu_);
};

}  // namespace limen

#endif  // LIMEN_ABSTRACT_LIMIT_H_

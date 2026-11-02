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

#include "limen/abstract_limit.h"
#include "absl/synchronization/mutex.h"
#include <cstdint>
#include <utility>

namespace limen {

AbstractLimit::AbstractLimit(int initial_limit) : limit_(initial_limit) {}

void AbstractLimit::OnSample(int64_t start_time_ns, int64_t rtt_ns,
                             int inflight, bool did_drop) {
  absl::MutexLock lock(mu_);
  int const new_limit = Update(start_time_ns, rtt_ns, inflight, did_drop);
  SetLimitLocked(new_limit);
}

void AbstractLimit::NotifyOnChange(ChangeCallback callback) {
  absl::MutexLock lock(mu_);
  listeners_.push_back(std::move(callback));
}

void AbstractLimit::SetLimit(int new_limit) {
  absl::MutexLock lock(mu_);
  SetLimitLocked(new_limit);
}

void AbstractLimit::SetLimitLocked(int new_limit) {
  int const old_limit = limit_.load(std::memory_order_relaxed);
  if (new_limit == old_limit) return;
  limit_.store(new_limit, std::memory_order_relaxed);
  for (auto const& listener : listeners_) {
    listener(new_limit);
  }
}

}  // namespace limen

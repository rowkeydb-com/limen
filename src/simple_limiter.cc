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

#include "limen/simple_limiter.h"
#include <atomic>
#include <cassert>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>

namespace limen {

std::unique_ptr<SimpleLimiter> SimpleLimiter::Builder::Build() {
  assert(limit_ != nullptr && "SimpleLimiter::Builder::Limit() is required");
  AbstractLimiter::Params params{std::move(limit_), std::move(id_),
                                 std::move(bypass_predicate_),
                                 std::move(meter_provider_)};
  return std::make_unique<SimpleLimiter>(PrivateTag{}, std::move(params));
}

SimpleLimiter::SimpleLimiter(PrivateTag, AbstractLimiter::Params params)
    : AbstractLimiter(std::move(params)) {}

std::optional<int> SimpleLimiter::DoAcquire(std::string_view /*context*/) {
  // Compare-and-swap loop on the in-flight atomic against the cap.
  // The cap is re-read on every iteration because it can change
  // mid-acquire (a cap reduction from `SettableLimit::SetLimit` or
  // a Gradient2 adaptive update). The CAS commits the increment
  // only when the slot was genuinely available, so two concurrent
  // racing acquires at cap-minus-one cannot both succeed.
  int current = inflight_.load(std::memory_order_relaxed);
  while (true) {
    int const cap = GetLimit();
    if (current >= cap) {
      return std::nullopt;
    }
    if (inflight_.compare_exchange_weak(current, current + 1,
                                        std::memory_order_relaxed,
                                        std::memory_order_relaxed)) {
      return current + 1;
    }
    // The CAS updated `current` to the live value; the loop
    // re-checks against the (possibly also updated) cap.
  }
}

}  // namespace limen

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

#ifndef LIMEN_FIXED_LIMIT_H_
#define LIMEN_FIXED_LIMIT_H_

#include "limen/abstract_limit.h"
#include <cstdint>
#include <memory>

namespace limen {

// A `Limit` whose value never changes. Useful for testing and for
// "freeze the cap here" debugging in production.
//
// Ported from Netflix's `FixedLimit.java`.
class FixedLimit final : public AbstractLimit {
 public:
  explicit FixedLimit(int limit) : AbstractLimit(limit) {}

  // Convenience factory matching upstream's static `of(int)` method.
  static std::unique_ptr<FixedLimit> Of(int limit) {
    return std::make_unique<FixedLimit>(limit);
  }

 protected:
  int Update(int64_t /*start_time_ns*/, int64_t /*rtt_ns*/, int /*inflight*/,
             bool /*did_drop*/) override {
    return GetLimit();
  }
};

}  // namespace limen

#endif  // LIMEN_FIXED_LIMIT_H_

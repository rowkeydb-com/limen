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

#ifndef LIMEN_SETTABLE_LIMIT_H_
#define LIMEN_SETTABLE_LIMIT_H_

#include "limen/abstract_limit.h"
#include <cstdint>
#include <memory>

namespace limen {

// A `Limit` whose value is set programmatically by the application,
// not derived from samples. Useful for tests and for externally
// driven cap policies. Calling `SetLimit` triggers the change
// callbacks registered through `NotifyOnChange`.
//
// Ported from Netflix's `SettableLimit.java`.
class SettableLimit final : public AbstractLimit {
 public:
  explicit SettableLimit(int initial_limit) : AbstractLimit(initial_limit) {}

  // Convenience factory matching upstream's static `startingAt(int)`.
  static std::unique_ptr<SettableLimit> StartingAt(int limit) {
    return std::make_unique<SettableLimit>(limit);
  }

  // Expose `AbstractLimit::SetLimit` to callers. The protected base
  // method is the same machinery that subclasses use to publish a
  // new value; here we surface it to applications.
  using AbstractLimit::SetLimit;

 protected:
  int Update(int64_t /*start_time_ns*/, int64_t /*rtt_ns*/, int /*inflight*/,
             bool /*did_drop*/) override {
    return GetLimit();
  }
};

}  // namespace limen

#endif  // LIMEN_SETTABLE_LIMIT_H_

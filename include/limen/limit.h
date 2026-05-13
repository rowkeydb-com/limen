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

#ifndef LIMEN_LIMIT_H_
#define LIMEN_LIMIT_H_

#include <cstdint>
#include <functional>

namespace limen {

// Contract for an algorithm that computes a concurrency limit from
// per-request round-trip-time samples.
//
// Implementations are thread-safe. `GetLimit` is called frequently on
// the request hot path and is expected to be wait-free; `OnSample`
// and `NotifyOnChange` are called less often.
//
// Ported from Netflix's `Limit.java`.
class Limit {
 public:
  // Type of callback invoked when the limit changes. Receives the
  // new limit value as its only argument.
  using ChangeCallback = std::function<void(int)>;

  Limit() = default;
  virtual ~Limit() = default;

  Limit(Limit const&) = delete;
  Limit& operator=(Limit const&) = delete;
  Limit(Limit&&) = delete;
  Limit& operator=(Limit&&) = delete;

  // Returns the algorithm's current concurrency limit.
  virtual int GetLimit() const = 0;

  // Registers a callback invoked whenever the algorithm publishes a
  // new limit. The callback may run on a request thread (whichever
  // thread crossed a window boundary or completed a request that
  // caused the limit to change), so it must be cheap and must not
  // call back into the same `Limit`'s mutating methods.
  virtual void NotifyOnChange(ChangeCallback callback) = 0;

  // Updates the limiter with one sample. `start_time_ns` and `rtt_ns`
  // are nanosecond timestamps. `inflight` is the number of requests
  // in flight when this sample was recorded. `did_drop` indicates
  // whether the underlying call was dropped (timed out or rejected
  // by an external bound).
  virtual void OnSample(int64_t start_time_ns, int64_t rtt_ns, int inflight,
                        bool did_drop) = 0;
};

}  // namespace limen

#endif  // LIMEN_LIMIT_H_

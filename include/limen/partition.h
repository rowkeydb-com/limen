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

#ifndef LIMEN_PARTITION_H_
#define LIMEN_PARTITION_H_

#include <atomic>
#include <chrono>
#include <string>
#include <utility>

namespace limen {

// One slot in an AbstractPartitionedLimiter's quota table.
//
// A partition is a named percentage share of the global cap. The
// `limit` field is recomputed from `percent * global_cap` whenever
// the wrapped Limit's value changes; until then it is read on the
// admission hot path with a relaxed atomic load. The `busy` field
// is the per-partition in-flight counter — incremented on
// admission, decremented on completion, observed by the
// partition-labelled `limen.inflight` callback.
//
// `reject_delay` is the optional per-partition back-pressure
// sleep duration applied when the partition rejects a request.
// Zero (the default) means rejection returns immediately. The
// reject delay is a deliberate server-side throttle that mirrors
// upstream's design; the AbstractPartitionedLimiter caps the
// total number of concurrent sleepers via `max_delayed_threads`.
//
// `std::atomic` members make Partition neither copyable nor
// movable. Partitions live behind a `std::unique_ptr` in the
// limiter's partition vector so the vector can grow at Builder
// time without relocating an atomic.
//
// Ported from Netflix's `AbstractPartitionedLimiter.Partition`
// inner class.
struct Partition {
  std::string name;
  double percent;
  std::chrono::milliseconds reject_delay;
  std::atomic<int> busy{0};
  std::atomic<int> limit{0};

  Partition(std::string n, double p, std::chrono::milliseconds delay)
      : name(std::move(n)), percent(p), reject_delay(delay) {}

  Partition(Partition const&) = delete;
  Partition& operator=(Partition const&) = delete;
  Partition(Partition&&) = delete;
  Partition& operator=(Partition&&) = delete;
};

}  // namespace limen

#endif  // LIMEN_PARTITION_H_

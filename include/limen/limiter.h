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

#include <memory>
#include <string_view>

namespace limen {

// Contract for an admission-control limiter.
//
// A caller asks the limiter for permission to start a unit of work
// by calling `TryAcquire`. If the limiter admits the call, it
// returns a Listener that the caller is expected to notify when the
// work completes; the Listener carries the bookkeeping that updates
// the in-flight counter and feeds a sample to the wrapped algorithm.
// If the limiter rejects the call, `TryAcquire` returns null and the
// caller is expected to fail-fast or shed elsewhere.
//
// The `context` argument is an opaque string view applications use
// to scope the admission decision: a route name, a tenant id, an
// RPC method, anything. The default context is empty. Implementations
// inspect it through configured bypass predicates and partitioning
// hooks; the base `Limiter` itself imposes no structure on it.
//
// Ported from Netflix's `Limiter.java`.
class Limiter {
 public:
  // The Listener captures the per-call state the limiter needs to
  // record an outcome (the in-flight count at admission, a
  // monotonic timestamp at admission). The caller invokes exactly
  // one of `OnSuccess`, `OnIgnore`, or `OnDropped` when the work
  // completes.
  //
  // Ported from Netflix's `Limiter.Listener` inner interface.
  class Listener {
   public:
    virtual ~Listener() = default;

    Listener() = default;
    Listener(Listener const&) = delete;
    Listener& operator=(Listener const&) = delete;
    Listener(Listener&&) = delete;
    Listener& operator=(Listener&&) = delete;

    // The wrapped unit of work completed normally. Releases the
    // in-flight slot and feeds a successful sample to the wrapped
    // algorithm.
    virtual void OnSuccess() = 0;

    // The wrapped unit of work completed but should not be treated
    // as a real signal of system health (a duplicate retry, a
    // cancellation, an early bail-out). Releases the in-flight slot
    // without feeding a sample.
    virtual void OnIgnore() = 0;

    // The wrapped unit of work was dropped by the downstream system
    // (timed out, rejected by a load-shedder lower in the stack).
    // Releases the in-flight slot and feeds a `did_drop=true` sample
    // to the wrapped algorithm.
    virtual void OnDropped() = 0;
  };

  Limiter() = default;
  virtual ~Limiter() = default;

  Limiter(Limiter const&) = delete;
  Limiter& operator=(Limiter const&) = delete;
  Limiter(Limiter&&) = delete;
  Limiter& operator=(Limiter&&) = delete;

  // Attempts to acquire admission for one unit of work. Returns a
  // non-null Listener on admission or null on rejection. The caller
  // must invoke exactly one completion method on the returned
  // Listener.
  virtual std::unique_ptr<Listener> TryAcquire(
      std::string_view context = {}) = 0;
};

}  // namespace limen

#endif  // LIMEN_LIMITER_H_

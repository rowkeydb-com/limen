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

// Minimal Limen example. Builds an adaptive concurrency limiter and a
// CoDel admission filter, runs a small synthetic workload through them,
// and prints the outcome.
//
// The wiring matches the README quickstart: a Gradient2 algorithm
// wrapped in a `WindowedLimit`, fronted by a `SimpleLimiter`, with a
// `CodelFilter` covering the dequeue point. There is no application-
// level RPC library in this example — the "request" is a `sleep_for`
// that simulates handler work. See README.md for the matching
// integration shape against a real RPC library.
//
// The algorithm dynamics (cap adapting under load, CoDel entering
// drop mode under sustained sojourn) are exercised by
// test/traffic_simulation_test.cc and test/codel_filter_test.cc. This
// program demonstrates the API surface and runs end-to-end.

#include "limen/codel_filter.h"
#include "limen/gradient2_limit.h"
#include "limen/simple_limiter.h"
#include "limen/windowed_limit.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <utility>

int main() {
  // Build the limiter chain. `Gradient2Limit::Builder::Build` returns
  // a `unique_ptr` directly; the other three builders return
  // `absl::StatusOr<unique_ptr<T>>` because their configuration can
  // fail validation, and `.value()` unwraps the success case.
  auto gradient2 = limen::Gradient2Limit::Builder()
                       .InitialLimit(20)
                       .MinLimit(10)
                       .MaxConcurrency(200)
                       .Id("demo")
                       .Build();

  auto windowed =
      limen::WindowedLimit::Builder().Build(std::move(gradient2)).value();

  auto limiter = limen::SimpleLimiter::Builder()
                     .Limit(std::move(windowed))
                     .Id("demo")
                     .Build()
                     .value();

  auto codel = limen::CodelFilter::Builder()
                   .Target(absl::Milliseconds(5))
                   .Interval(absl::Milliseconds(100))
                   .Id("demo")
                   .Build()
                   .value();

  // Run 1000 sequential "requests". Each one takes one millisecond of
  // simulated handler work. The workload is deliberately undemanding;
  // a serious traffic test (concurrent threads, real RTT spikes,
  // sustained overload) lives under test/.
  int admitted = 0;
  int rejected = 0;
  int dropped = 0;
  for (int i = 0; i < 1000; ++i) {
    auto slot = limiter->TryAcquire("demo");
    if (!slot) {
      ++rejected;
      continue;
    }
    // In a real application the dequeue point is wherever the worker
    // thread pulls an item off its in-process queue. Here it is
    // immediate, so sojourn is zero and CoDel never drops.
    absl::Time const enqueue = absl::Now();
    if (codel->ShouldDrop(enqueue)) {
      slot->OnDropped();
      ++dropped;
      continue;
    }
    // Simulated handler work.
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    slot->OnSuccess();
    ++admitted;
  }

  std::cout << "admitted=" << admitted << " rejected=" << rejected
            << " dropped=" << dropped << " final_cap=" << limiter->GetLimit()
            << "\n";
  return 0;
}

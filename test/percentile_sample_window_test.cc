// Copyright 2019 Netflix, Inc.
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

#include "limen/percentile_sample_window.h"
#include "gtest/gtest.h"
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <latch>
#include <limits>
#include <random>
#include <thread>
#include <vector>

namespace limen {
namespace {

TEST(PercentileSampleWindowTest, P50OfFiveSamples) {
  PercentileSampleWindow window(/*percentile=*/0.5,
                                /*max_samples_per_window=*/16);
  std::vector<int64_t> rtts = {5, 1, 3, 4, 2};
  for (auto rtt : rtts) {
    window.AddSample(rtt, 1, false);
  }
  // Upstream selects `round(sample_count * percentile)` as a
  // one-based index. round(5 * 0.5) = 3 (one-based) → 3 (zero-based
  // index 2) of [1, 2, 3, 4, 5] = 3.
  EXPECT_EQ(window.TrackedRttNs(), 3);
}

TEST(PercentileSampleWindowTest, P999PicksSlowest) {
  // Upstream's percentile formula is `round(sample_count *
  // percentile)` as a one-based index. With p999 and 100 samples
  // that resolves to round(99.9) = 100, i.e., the slowest sample
  // — which is what an operator expects from a "p999" probe.
  PercentileSampleWindow window(/*percentile=*/0.999,
                                /*max_samples_per_window=*/256);
  for (int i = 0; i < 99; ++i) {
    window.AddSample(/*rtt_ns=*/1'000'000, 1, false);
  }
  window.AddSample(/*rtt_ns=*/999'000'000, 1, false);
  EXPECT_EQ(window.TrackedRttNs(), 999'000'000);
}

TEST(PercentileSampleWindowTest, OrderIndependence) {
  // The percentile is computed by sorting the captured array, so
  // any permutation of the same input produces the same output.
  std::vector<int64_t> input;
  input.reserve(200);
  for (int i = 1; i <= 200; ++i) {
    input.push_back(static_cast<int64_t>(i));
  }
  int64_t reference = 0;
  {
    PercentileSampleWindow window(0.9, 256);
    for (auto rtt : input) {
      window.AddSample(rtt, 1, false);
    }
    reference = window.TrackedRttNs();
  }
  std::mt19937 rng(42);
  for (int trial = 0; trial < 8; ++trial) {
    std::shuffle(input.begin(), input.end(), rng);
    PercentileSampleWindow window(0.9, 256);
    for (auto rtt : input) {
      window.AddSample(rtt, 1, false);
    }
    EXPECT_EQ(window.TrackedRttNs(), reference);
  }
}

TEST(PercentileSampleWindowTest, OverflowKeepsFirstSamples) {
  // With a bound of 4, only the first 4 RTTs participate in the
  // percentile, but the sample count keeps incrementing for the
  // benefit of the surrounding algorithm.
  PercentileSampleWindow window(0.5, /*max_samples_per_window=*/4);
  window.AddSample(10, 1, false);
  window.AddSample(20, 1, false);
  window.AddSample(30, 1, false);
  window.AddSample(40, 1, false);
  window.AddSample(50, 1, false);
  window.AddSample(60, 1, false);
  EXPECT_EQ(window.SampleCount(), 6);
  // round(4 * 0.5) = 2 (one-based) → zero-based index 1 of sorted
  // [10, 20, 30, 40] = 20.
  EXPECT_EQ(window.TrackedRttNs(), 20);
}

TEST(PercentileSampleWindowTest, DroppedSampleCountsInPercentileAndSetsFlag) {
  // Ports upstream `droppedSampleShouldChangeTrackedRtt`. The
  // sample that reported a drop is still recorded into the
  // percentile array; only the `DidDrop` flag is reported
  // separately.
  PercentileSampleWindow window(0.5, 16);
  window.AddSample(100, 1, false);
  window.AddSample(200, 1, false);
  window.AddSample(300, 1, false);
  window.AddSample(800, 1, /*did_drop=*/true);
  EXPECT_TRUE(window.DidDrop());
  EXPECT_EQ(window.SampleCount(), 4);
  // round(4 * 0.5) = 2 (one-based) → zero-based index 1 of
  // sorted [100, 200, 300, 800] = 200.
  EXPECT_EQ(window.TrackedRttNs(), 200);
}

TEST(PercentileSampleWindowTest, MinRttAndMaxInflightTrack) {
  PercentileSampleWindow window(0.5, 16);
  window.AddSample(500, 3, false);
  window.AddSample(200, 9, false);
  window.AddSample(700, 5, false);
  EXPECT_EQ(window.CandidateRttNs(), 200);
  EXPECT_EQ(window.MaxInflight(), 9);
}

TEST(PercentileSampleWindowTest, ResetClearsAllFields) {
  PercentileSampleWindow window(0.5, 16);
  window.AddSample(100, 5, true);
  window.AddSample(200, 7, false);
  ASSERT_EQ(window.SampleCount(), 2);
  ASSERT_TRUE(window.DidDrop());

  window.Reset();
  EXPECT_EQ(window.SampleCount(), 0);
  EXPECT_EQ(window.CandidateRttNs(), std::numeric_limits<int64_t>::max());
  EXPECT_EQ(window.MaxInflight(), 0);
  EXPECT_FALSE(window.DidDrop());
  EXPECT_EQ(window.TrackedRttNs(), 0);
}

TEST(PercentileSampleWindowTest, ConcurrentAddsAreLossless) {
  PercentileSampleWindow window(0.5, /*max_samples_per_window=*/100'000);
  constexpr int kThreads = 8;
  constexpr int kSamplesPerThread = 5000;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  std::latch start{kThreads};
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      for (int i = 0; i < kSamplesPerThread; ++i) {
        window.AddSample(1000, 1, false);
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  EXPECT_EQ(window.SampleCount(), kThreads * kSamplesPerThread);
}

}  // namespace
}  // namespace limen

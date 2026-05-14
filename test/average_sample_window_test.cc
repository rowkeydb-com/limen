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

#include "limen/average_sample_window.h"
#include "gtest/gtest.h"
#include <atomic>
#include <limits>
#include <thread>
#include <vector>

namespace limen {
namespace {

TEST(AverageSampleWindowTest, MeanOverThreeSamples) {
  AverageSampleWindow window;
  window.AddSample(100, /*inflight=*/1, /*did_drop=*/false);
  window.AddSample(200, 1, false);
  window.AddSample(300, 1, false);
  EXPECT_EQ(window.SampleCount(), 3);
  EXPECT_EQ(window.TrackedRttNs(), 200);
}

TEST(AverageSampleWindowTest, DroppedSampleCountsInMeanAndSetsFlag) {
  // Ports upstream `droppedSampleShouldChangeTrackedAverage`. The
  // sample that reported a drop still contributes to the running
  // mean; only the `DidDrop` flag is affected separately.
  AverageSampleWindow window;
  window.AddSample(100, 1, false);
  window.AddSample(200, 1, false);
  window.AddSample(300, 1, false);
  window.AddSample(800, 1, /*did_drop=*/true);
  EXPECT_TRUE(window.DidDrop());
  EXPECT_EQ(window.SampleCount(), 4);
  EXPECT_EQ(window.TrackedRttNs(), (100 + 200 + 300 + 800) / 4);
}

TEST(AverageSampleWindowTest, MinRttTracks) {
  AverageSampleWindow window;
  window.AddSample(500, 1, false);
  window.AddSample(300, 1, false);
  window.AddSample(100, 1, false);
  EXPECT_EQ(window.CandidateRttNs(), 100);
}

TEST(AverageSampleWindowTest, MaxInflightTracks) {
  AverageSampleWindow window;
  window.AddSample(100, /*inflight=*/5, false);
  window.AddSample(100, 12, false);
  window.AddSample(100, 7, false);
  EXPECT_EQ(window.MaxInflight(), 12);
}

TEST(AverageSampleWindowTest, ResetClearsAllFields) {
  AverageSampleWindow window;
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

TEST(AverageSampleWindowTest, ConcurrentAddsAreLossless) {
  // The window aggregates via independent relaxed atomic operations
  // — fetch_add on the count and the sum, CAS loops on the min and
  // the max. None of those can drop an update. Verify the count
  // matches the total writes across many threads.
  AverageSampleWindow window;
  constexpr int kThreads = 8;
  constexpr int kSamplesPerThread = 5000;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  std::atomic<int> ready_threads{0};
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      ready_threads.fetch_add(1);
      while (ready_threads.load() < kThreads) {
      }
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

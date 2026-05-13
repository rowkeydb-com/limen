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

#include "limen/exp_avg_measurement.h"
#include "gtest/gtest.h"

namespace limen {
namespace {

constexpr double kEpsilon = 1e-9;

// During the warmup window the measurement reports the simple
// running mean of the samples added so far.
TEST(ExpAvgMeasurementTest, WarmupIsRunningAverage) {
  ExpAvgMeasurement measurement(/*window=*/100, /*warmup_window=*/5);

  EXPECT_NEAR(measurement.Add(10.0), 10.0, kEpsilon);
  EXPECT_NEAR(measurement.Add(20.0), 15.0, kEpsilon);
  EXPECT_NEAR(measurement.Add(30.0), 20.0, kEpsilon);
  EXPECT_NEAR(measurement.Add(40.0), 25.0, kEpsilon);
  EXPECT_NEAR(measurement.Add(50.0), 30.0, kEpsilon);  // last warmup
}

// After the warmup is filled the measurement switches to an
// exponentially-weighted moving average with factor 2/(window+1).
TEST(ExpAvgMeasurementTest, SteadyStateIsExponentialAverage) {
  int const window = 9;
  int const warmup = 1;
  ExpAvgMeasurement measurement(window, warmup);

  // First sample populates the running average at exactly the
  // sample value because the warmup window is one.
  EXPECT_NEAR(measurement.Add(100.0), 100.0, kEpsilon);

  // Second sample is the first in steady state. Smoothing factor:
  //   factor = 2 / (window + 1) = 2 / 10 = 0.2
  //   new_value = 100 * 0.8 + 50 * 0.2 = 90.0
  double const factor = 2.0 / (window + 1);
  EXPECT_NEAR(factor, 0.2, kEpsilon);
  EXPECT_NEAR(measurement.Add(50.0), 100.0 * 0.8 + 50.0 * 0.2, kEpsilon);

  // Third sample:
  //   new_value = 90 * 0.8 + 70 * 0.2 = 86.0
  EXPECT_NEAR(measurement.Add(70.0), 90.0 * 0.8 + 70.0 * 0.2, kEpsilon);
}

// Reset clears the value, sum, and the warmup counter so the next
// sample restarts the running-average phase.
TEST(ExpAvgMeasurementTest, ResetReturnsToWarmupPhase) {
  ExpAvgMeasurement measurement(/*window=*/100, /*warmup_window=*/3);
  measurement.Add(100.0);
  measurement.Add(200.0);
  EXPECT_NEAR(measurement.Get(), 150.0, kEpsilon);

  measurement.Reset();
  EXPECT_NEAR(measurement.Get(), 0.0, kEpsilon);

  // Post-reset, the next sample is once again in warmup.
  EXPECT_NEAR(measurement.Add(50.0), 50.0, kEpsilon);
  EXPECT_NEAR(measurement.Add(150.0), 100.0, kEpsilon);
}

// `Update` applies the given transformation to the stored value
// in-place. Gradient2 uses this to compress the long-term measurement
// during its recovery branch.
TEST(ExpAvgMeasurementTest, UpdateTransformsValueInPlace) {
  ExpAvgMeasurement measurement(/*window=*/100, /*warmup_window=*/1);
  measurement.Add(200.0);
  EXPECT_NEAR(measurement.Get(), 200.0, kEpsilon);

  measurement.Update([](double v) { return v * 0.5; });
  EXPECT_NEAR(measurement.Get(), 100.0, kEpsilon);

  measurement.Update([](double v) { return v + 7.0; });
  EXPECT_NEAR(measurement.Get(), 107.0, kEpsilon);
}

}  // namespace
}  // namespace limen

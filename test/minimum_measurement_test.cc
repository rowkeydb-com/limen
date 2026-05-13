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

#include "limen/minimum_measurement.h"
#include "gtest/gtest.h"

namespace limen {
namespace {

constexpr double kEpsilon = 1e-9;

TEST(MinimumMeasurementTest, TracksRunningMinimum) {
  MinimumMeasurement measurement;
  EXPECT_NEAR(measurement.Get(), 0.0, kEpsilon);

  EXPECT_NEAR(measurement.Add(100.0), 100.0, kEpsilon);
  EXPECT_NEAR(measurement.Add(150.0), 100.0, kEpsilon);  // larger, no change
  EXPECT_NEAR(measurement.Add(50.0), 50.0, kEpsilon);    // new minimum
  EXPECT_NEAR(measurement.Add(75.0), 50.0, kEpsilon);    // larger, no change
  EXPECT_NEAR(measurement.Add(25.0), 25.0, kEpsilon);    // new minimum

  EXPECT_NEAR(measurement.Get(), 25.0, kEpsilon);
}

TEST(MinimumMeasurementTest, ResetReturnsToInitialState) {
  MinimumMeasurement measurement;
  measurement.Add(10.0);
  measurement.Add(5.0);
  EXPECT_NEAR(measurement.Get(), 5.0, kEpsilon);

  measurement.Reset();
  EXPECT_NEAR(measurement.Get(), 0.0, kEpsilon);

  // After reset, the first new sample becomes the minimum.
  EXPECT_NEAR(measurement.Add(100.0), 100.0, kEpsilon);
}

TEST(MinimumMeasurementTest, ZeroIsATrackedValueNotASentinel) {
  // Upstream Java treats `0.0` as a sentinel for "no sample yet",
  // which produces the wrong minimum the moment a real sample of
  // `0.0` is seen: any subsequent larger sample replaces it.
  // Limen distinguishes "no sample" from "the minimum so far is
  // zero", so the minimum stays correct.
  MinimumMeasurement measurement;
  EXPECT_NEAR(measurement.Add(0.0), 0.0, kEpsilon);
  EXPECT_NEAR(measurement.Add(5.0), 0.0, kEpsilon);
  EXPECT_NEAR(measurement.Add(3.0), 0.0, kEpsilon);
  EXPECT_NEAR(measurement.Get(), 0.0, kEpsilon);
}

TEST(MinimumMeasurementTest, TracksNegativeMinimumBelowZero) {
  // Negative samples are valid input for any double-typed
  // measurement. A negative minimum must be reported even when a
  // later non-negative sample arrives.
  MinimumMeasurement measurement;
  EXPECT_NEAR(measurement.Add(-3.0), -3.0, kEpsilon);
  EXPECT_NEAR(measurement.Add(0.0), -3.0, kEpsilon);
  EXPECT_NEAR(measurement.Add(-1.0), -3.0, kEpsilon);
  EXPECT_NEAR(measurement.Add(-7.0), -7.0, kEpsilon);
}

TEST(MinimumMeasurementTest, UpdateIsNoOp) {
  MinimumMeasurement measurement;
  measurement.Add(50.0);
  EXPECT_NEAR(measurement.Get(), 50.0, kEpsilon);
  measurement.Update([](double v) { return v * 10.0; });
  // Upstream's `MinimumMeasurement.update(...)` is intentionally a
  // no-op. Limen matches that.
  EXPECT_NEAR(measurement.Get(), 50.0, kEpsilon);
}

}  // namespace
}  // namespace limen

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

#include "limen/single_measurement.h"
#include "gtest/gtest.h"

namespace limen {
namespace {

constexpr double kEpsilon = 1e-9;

TEST(SingleMeasurementTest, PassesLastValueThrough) {
  SingleMeasurement measurement;
  EXPECT_NEAR(measurement.Get(), 0.0, kEpsilon);

  EXPECT_NEAR(measurement.Add(10.0), 10.0, kEpsilon);
  EXPECT_NEAR(measurement.Get(), 10.0, kEpsilon);

  EXPECT_NEAR(measurement.Add(5.0), 5.0, kEpsilon);
  EXPECT_NEAR(measurement.Get(), 5.0, kEpsilon);

  EXPECT_NEAR(measurement.Add(100.0), 100.0, kEpsilon);
  EXPECT_NEAR(measurement.Get(), 100.0, kEpsilon);
}

TEST(SingleMeasurementTest, ResetReturnsToInitialState) {
  SingleMeasurement measurement;
  measurement.Add(42.0);
  EXPECT_NEAR(measurement.Get(), 42.0, kEpsilon);
  measurement.Reset();
  EXPECT_NEAR(measurement.Get(), 0.0, kEpsilon);
}

TEST(SingleMeasurementTest, UpdateTransformsStoredValue) {
  SingleMeasurement measurement;
  measurement.Add(5.0);
  measurement.Update([](double v) { return v * 3.0; });
  EXPECT_NEAR(measurement.Get(), 15.0, kEpsilon);
}

}  // namespace
}  // namespace limen

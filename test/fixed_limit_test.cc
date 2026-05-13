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

#include "limen/fixed_limit.h"
#include "gtest/gtest.h"

namespace limen {
namespace {

TEST(FixedLimitTest, ReturnsConstant) {
  FixedLimit limit(42);
  EXPECT_EQ(limit.GetLimit(), 42);
  // Samples have no effect on the value.
  limit.OnSample(/*start_time_ns=*/0, /*rtt_ns=*/1000, /*inflight=*/5,
                 /*did_drop=*/false);
  EXPECT_EQ(limit.GetLimit(), 42);
  limit.OnSample(/*start_time_ns=*/0, /*rtt_ns=*/9'999'999,
                 /*inflight=*/100, /*did_drop=*/true);
  EXPECT_EQ(limit.GetLimit(), 42);
}

TEST(FixedLimitTest, FactoryReturnsUniquePtr) {
  auto limit = FixedLimit::Of(7);
  ASSERT_NE(limit, nullptr);
  EXPECT_EQ(limit->GetLimit(), 7);
}

}  // namespace
}  // namespace limen

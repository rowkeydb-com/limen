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

#include "limen/settable_limit.h"
#include "gtest/gtest.h"
#include <vector>

namespace limen {
namespace {

TEST(SettableLimitTest, SetUpdatesLimit) {
  SettableLimit limit(10);
  EXPECT_EQ(limit.GetLimit(), 10);
  limit.SetLimit(20);
  EXPECT_EQ(limit.GetLimit(), 20);
  limit.SetLimit(5);
  EXPECT_EQ(limit.GetLimit(), 5);
}

TEST(SettableLimitTest, ChangeCallbackFires) {
  SettableLimit limit(10);
  std::vector<int> received;
  limit.NotifyOnChange([&](int new_limit) { received.push_back(new_limit); });
  limit.SetLimit(20);
  limit.SetLimit(30);
  EXPECT_EQ(received, std::vector<int>({20, 30}));
}

TEST(SettableLimitTest, ChangeCallbackDoesNotFireOnSameValue) {
  SettableLimit limit(10);
  int call_count = 0;
  limit.NotifyOnChange([&](int) { ++call_count; });
  limit.SetLimit(10);  // same as current
  EXPECT_EQ(call_count, 0);
  limit.SetLimit(20);  // change
  EXPECT_EQ(call_count, 1);
  limit.SetLimit(20);  // same as current
  EXPECT_EQ(call_count, 1);
}

TEST(SettableLimitTest, FactoryReturnsUniquePtr) {
  auto limit = SettableLimit::StartingAt(15);
  ASSERT_NE(limit, nullptr);
  EXPECT_EQ(limit->GetLimit(), 15);
}

TEST(SettableLimitTest, MultipleCallbacksAllFire) {
  SettableLimit limit(10);
  int first = 0;
  int second = 0;
  limit.NotifyOnChange([&](int v) { first = v; });
  limit.NotifyOnChange([&](int v) { second = v; });
  limit.SetLimit(42);
  EXPECT_EQ(first, 42);
  EXPECT_EQ(second, 42);
}

}  // namespace
}  // namespace limen

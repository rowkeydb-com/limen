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

// The tests that assert no allocation pass trivially if the replaced
// allocation functions are not the ones called, since the count then
// stays at zero. These tests show that each call is counted.
//
// They call the allocation functions directly. A compiler may omit the
// allocation of a new-expression, but not a direct call, so each call
// reaches the replacement.

#include "test/allocation_counter.h"
#include "gtest/gtest.h"
#include <new>

namespace limen {
namespace {

TEST(AllocationCounterTest, CountsEachOperatorNewCall) {
  AllocationCounter allocations;
  void* first = ::operator new(16);
  void* second = ::operator new(32);
  void* third = ::operator new(64);
  int const counted = allocations.Stop();
  ::operator delete(third);
  ::operator delete(second);
  ::operator delete(first);
  EXPECT_EQ(counted, 3);
}

TEST(AllocationCounterTest, CountsEachOperatorNewArrayCall) {
  AllocationCounter allocations;
  void* first = ::operator new[](16);
  void* second = ::operator new[](32);
  int const counted = allocations.Stop();
  ::operator delete[](second);
  ::operator delete[](first);
  EXPECT_EQ(counted, 2);
}

TEST(AllocationCounterTest, DoesNotCountAfterStop) {
  AllocationCounter allocations;
  void* counted = ::operator new(16);
  EXPECT_EQ(allocations.Stop(), 1);
  void* uncounted = ::operator new(16);
  EXPECT_EQ(allocations.Stop(), 1);
  ::operator delete(uncounted);
  ::operator delete(counted);
}

TEST(AllocationCounterTest, StartsFromZero) {
  {
    AllocationCounter earlier;
    ::operator delete(::operator new(16));
  }
  AllocationCounter allocations;
  EXPECT_EQ(allocations.Stop(), 0);
}

}  // namespace
}  // namespace limen

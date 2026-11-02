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

#ifndef LIMEN_TEST_ALLOCATION_COUNTER_H_
#define LIMEN_TEST_ALLOCATION_COUNTER_H_

namespace limen {

// Counts the calls to the global operator new and operator new[] that
// any thread of the test makes while counting is on. A test that links
// //:allocation_counter gets replacements of both operators, and of the
// matching operator delete, from test/allocation_counter.cc; they are
// compiled there, apart from the tests, so that no caller inlines them.
//
// Construction resets the count to zero and starts counting. Stop()
// stops it and returns the count; destruction stops it if Stop() was
// not called, so an assertion that returns early does not leave
// counting on for the next test. Only one counter may be live at a
// time.
//
// The replacements cover the unaligned forms only. An allocation of a
// type aligned beyond __STDCPP_DEFAULT_NEW_ALIGNMENT__ goes through
// operator new(std::size_t, std::align_val_t), which is not counted.
class AllocationCounter {
 public:
  AllocationCounter();
  ~AllocationCounter();

  AllocationCounter(AllocationCounter const&) = delete;
  AllocationCounter& operator=(AllocationCounter const&) = delete;

  // Stops counting and returns the number of allocations counted
  // since construction.
  int Stop();

 private:
  bool counting_ = true;
};

}  // namespace limen

#endif  // LIMEN_TEST_ALLOCATION_COUNTER_H_

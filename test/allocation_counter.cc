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

#include "test/allocation_counter.h"
#include <atomic>
#include <cstddef>
#include <cstdlib>

namespace {

std::atomic<int> allocation_count{0};
std::atomic<bool> counting{false};

void* CountedAllocate(std::size_t size) {
  if (counting.load(std::memory_order_relaxed)) {
    allocation_count.fetch_add(1, std::memory_order_relaxed);
  }
  void* p = std::malloc(size);
  if (p == nullptr) {
    std::abort();
  }
  return p;
}

}  // namespace

// The replacements of the global allocation functions. Defined in this
// translation unit only: where GCC can inline a replacement into its
// caller at -O2, it sees malloc or free there and reports the pair as
// mismatched (-Wmismatched-new-delete).
void* operator new(std::size_t size) { return CountedAllocate(size); }
void* operator new[](std::size_t size) { return CountedAllocate(size); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t /*size*/) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t /*size*/) noexcept { std::free(p); }

namespace limen {

AllocationCounter::AllocationCounter() {
  allocation_count.store(0, std::memory_order_relaxed);
  counting.store(true, std::memory_order_relaxed);
}

AllocationCounter::~AllocationCounter() {
  if (counting_) {
    Stop();
  }
}

int AllocationCounter::Stop() {
  counting_ = false;
  counting.store(false, std::memory_order_relaxed);
  return allocation_count.load(std::memory_order_relaxed);
}

}  // namespace limen

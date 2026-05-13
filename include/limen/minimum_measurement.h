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

#ifndef LIMEN_MINIMUM_MEASUREMENT_H_
#define LIMEN_MINIMUM_MEASUREMENT_H_

#include "limen/measurement.h"
#include <functional>

namespace limen {

// Tracks the minimum of all samples added since the last `Reset`.
//
// Ported from Netflix's `MinimumMeasurement.java` with one
// deliberate semantic fix: upstream uses `0.0` as a sentinel for
// "no sample yet", which silently produces an incorrect minimum
// whenever a sample equals zero (a subsequent larger sample replaces
// the legitimate zero). Limen tracks the "has any sample arrived"
// bit explicitly, so the minimum is correct over every double-valued
// input, including zero and negative samples.
//
// `Get()` returns `0.0` before the first sample, matching upstream's
// initial-value contract.
//
// Not thread-safe.
class MinimumMeasurement final : public Measurement {
 public:
  double Add(double sample) override {
    if (!has_sample_ || sample < value_) {
      value_ = sample;
      has_sample_ = true;
    }
    return value_;
  }

  double Get() const override { return value_; }

  void Reset() override {
    value_ = 0.0;
    has_sample_ = false;
  }

  // Upstream's `MinimumMeasurement.update(...)` is a no-op. Limen
  // matches that: there is no sensible way to transform a "minimum"
  // observation without changing its meaning.
  void Update(std::function<double(double)> const& /*operation*/) override {}

 private:
  double value_ = 0.0;
  bool has_sample_ = false;
};

}  // namespace limen

#endif  // LIMEN_MINIMUM_MEASUREMENT_H_

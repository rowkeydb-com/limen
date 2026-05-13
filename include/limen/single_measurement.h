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

#ifndef LIMEN_SINGLE_MEASUREMENT_H_
#define LIMEN_SINGLE_MEASUREMENT_H_

#include "limen/measurement.h"
#include <functional>

namespace limen {

// Passthrough measurement that simply reports the most recently
// added sample. No smoothing, no aggregation. Used by limits that
// want to consume per-sample values directly.
//
// Not thread-safe. Ported from Netflix's `SingleMeasurement.java`.
// Upstream uses `null` as the unset state; Limen reports `0.0` until
// the first sample arrives, matching the rest of the measurement
// hierarchy.
class SingleMeasurement final : public Measurement {
 public:
  double Add(double sample) override {
    value_ = sample;
    return value_;
  }

  double Get() const override { return value_; }

  void Reset() override { value_ = 0.0; }

  void Update(std::function<double(double)> const& operation) override {
    value_ = operation(value_);
  }

 private:
  double value_ = 0.0;
};

}  // namespace limen

#endif  // LIMEN_SINGLE_MEASUREMENT_H_

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

#ifndef LIMEN_EXP_AVG_MEASUREMENT_H_
#define LIMEN_EXP_AVG_MEASUREMENT_H_

#include "limen/measurement.h"
#include <functional>

namespace limen {

// Exponentially smoothed moving average with a fixed warmup phase.
//
// For the first `warmup_window` samples, the measurement reports the
// simple running average — this keeps very small sample counts from
// being dominated by whichever value happened to arrive first. Once
// the warmup window has been filled, the measurement switches to a
// standard exponentially-weighted moving average with smoothing
// factor `2 / (window + 1)`.
//
// Used by `Gradient2Limit` to track the long-term baseline
// round-trip time.
//
// Not thread-safe. Callers serialise access (the algorithms that
// consume measurements call them from under the `AbstractLimit`
// mutex).
//
// Ported from Netflix's `ExpAvgMeasurement.java`.
class ExpAvgMeasurement final : public Measurement {
 public:
  // `window` controls the steady-state smoothing factor. A larger
  // window means more smoothing. `warmup_window` is the number of
  // initial samples that contribute to a simple running average
  // before the exponential blend kicks in.
  ExpAvgMeasurement(int window, int warmup_window);

  double Add(double sample) override;
  double Get() const override { return value_; }
  void Reset() override;
  void Update(std::function<double(double)> const& operation) override;

 private:
  double value_ = 0.0;
  double sum_ = 0.0;
  int count_ = 0;
  int const window_;
  int const warmup_window_;
};

}  // namespace limen

#endif  // LIMEN_EXP_AVG_MEASUREMENT_H_

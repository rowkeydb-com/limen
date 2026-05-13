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

#include "limen/exp_avg_measurement.h"
#include <functional>

namespace limen {
namespace {

// Steady-state smoothing factor for an exponential moving average
// over `n` samples. Matches the formula upstream uses.
double SteadyStateFactor(int n) { return 2.0 / (n + 1); }

}  // namespace

ExpAvgMeasurement::ExpAvgMeasurement(int window, int warmup_window)
    : window_(window), warmup_window_(warmup_window) {}

double ExpAvgMeasurement::Add(double sample) {
  if (count_ < warmup_window_) {
    ++count_;
    sum_ += sample;
    value_ = sum_ / count_;
  } else {
    double const factor = SteadyStateFactor(window_);
    value_ = value_ * (1 - factor) + sample * factor;
  }
  return value_;
}

void ExpAvgMeasurement::Reset() {
  value_ = 0.0;
  sum_ = 0.0;
  count_ = 0;
}

void ExpAvgMeasurement::Update(std::function<double(double)> const& operation) {
  value_ = operation(value_);
}

}  // namespace limen

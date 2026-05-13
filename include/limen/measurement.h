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

#ifndef LIMEN_MEASUREMENT_H_
#define LIMEN_MEASUREMENT_H_

#include <functional>

namespace limen {

// Contract for tracking an aggregate measurement over a sample set —
// for example, the long-term smoothed average RTT used by the
// Gradient2 algorithm or the minimum RTT used by Vegas.
//
// Implementations are not required to be thread-safe; the caller is
// expected to serialise access. The `Limit` algorithms that consume
// measurements call into them from `Update`, which runs under the
// `AbstractLimit` mutex.
//
// Ported from Netflix's `Measurement.java`. Upstream parameterises
// over `java.lang.Number`; Limen uses `double` directly because
// every measurement value in the library is floating-point.
class Measurement {
 public:
  Measurement() = default;
  virtual ~Measurement() = default;

  Measurement(Measurement const&) = delete;
  Measurement& operator=(Measurement const&) = delete;
  Measurement(Measurement&&) = delete;
  Measurement& operator=(Measurement&&) = delete;

  // Adds a sample to the measurement. Returns the value reported by
  // the measurement after the sample is incorporated.
  virtual double Add(double sample) = 0;

  // Returns the current value reported by the measurement, without
  // adding a sample.
  virtual double Get() const = 0;

  // Resets the measurement to its initial state.
  virtual void Reset() = 0;

  // Applies the given function to the current value, replacing the
  // stored value with the function's result. Used by algorithms that
  // need to compress or shift the long-term measurement based on
  // external conditions (for example, Gradient2's drift correction).
  virtual void Update(std::function<double(double)> const& operation) = 0;
};

}  // namespace limen

#endif  // LIMEN_MEASUREMENT_H_

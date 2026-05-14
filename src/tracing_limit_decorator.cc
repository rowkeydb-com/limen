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

#include "limen/tracing_limit_decorator.h"
#include <cstdint>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

namespace limen {
namespace {

void WriteToClog(std::string line) { std::clog << line << '\n'; }

}  // namespace

std::unique_ptr<TracingLimitDecorator> TracingLimitDecorator::Wrap(
    std::unique_ptr<Limit> delegate, LogSink sink) {
  if (!sink) {
    sink = &WriteToClog;
  }
  return std::make_unique<TracingLimitDecorator>(
      PrivateTag{}, std::move(delegate), std::move(sink));
}

TracingLimitDecorator::TracingLimitDecorator(PrivateTag,
                                             std::unique_ptr<Limit> delegate,
                                             LogSink sink)
    : delegate_(std::move(delegate)), sink_(std::move(sink)) {}

void TracingLimitDecorator::OnSample(int64_t start_time_ns, int64_t rtt_ns,
                                     int inflight, bool did_drop) {
  // Match upstream's logfmt-style line: in-flight count followed by
  // RTT in milliseconds. Keep the line format stable so an operator
  // grep is easy.
  std::ostringstream out;
  out << "limen.trace maxInFlight=" << inflight
      << " minRtt_ms=" << (static_cast<double>(rtt_ns) / 1'000'000.0)
      << " didDrop=" << (did_drop ? "true" : "false");
  sink_(out.str());

  delegate_->OnSample(start_time_ns, rtt_ns, inflight, did_drop);
}

}  // namespace limen

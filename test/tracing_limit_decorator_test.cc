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
#include "limen/limit.h"
#include "gtest/gtest.h"
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace limen {
namespace {

class CountingLimit final : public Limit {
 public:
  int GetLimit() const override { return 42; }

  void OnSample(int64_t /*start_time_ns*/, int64_t rtt_ns, int inflight,
                bool did_drop) override {
    ++count;
    last_rtt_ns = rtt_ns;
    last_inflight = inflight;
    last_did_drop = did_drop;
  }

  void NotifyOnChange(ChangeCallback /*callback*/) override {}

  int count = 0;
  int64_t last_rtt_ns = 0;
  int last_inflight = 0;
  bool last_did_drop = false;
};

TEST(TracingLimitDecoratorTest, LogsOncePerSample) {
  auto delegate = std::make_unique<CountingLimit>();
  std::vector<std::string> lines;
  auto traced = TracingLimitDecorator::Wrap(
      std::move(delegate), [&](std::string line) { lines.push_back(line); });

  traced->OnSample(0, 1'000'000, /*inflight=*/5, /*did_drop=*/false);
  traced->OnSample(0, 2'000'000, 7, true);
  traced->OnSample(0, 500'000, 3, false);

  ASSERT_EQ(lines.size(), 3u);
  // Each line carries the inflight count, the RTT in milliseconds,
  // and the did-drop flag.
  EXPECT_NE(lines[0].find("maxInFlight=5"), std::string::npos);
  EXPECT_NE(lines[0].find("minRtt_ms=1"), std::string::npos);
  EXPECT_NE(lines[0].find("didDrop=false"), std::string::npos);
  EXPECT_NE(lines[1].find("maxInFlight=7"), std::string::npos);
  EXPECT_NE(lines[1].find("didDrop=true"), std::string::npos);
  EXPECT_NE(lines[2].find("maxInFlight=3"), std::string::npos);
}

TEST(TracingLimitDecoratorTest, PassesSamplesThrough) {
  auto delegate = std::make_unique<CountingLimit>();
  auto* counting = delegate.get();
  auto traced =
      TracingLimitDecorator::Wrap(std::move(delegate), [](std::string) {});

  traced->OnSample(0, 1'000'000, 5, false);
  traced->OnSample(0, 2'000'000, 7, true);

  EXPECT_EQ(counting->count, 2);
  EXPECT_EQ(counting->last_rtt_ns, 2'000'000);
  EXPECT_EQ(counting->last_inflight, 7);
  EXPECT_TRUE(counting->last_did_drop);
}

TEST(TracingLimitDecoratorTest, GetLimitDelegates) {
  auto delegate = std::make_unique<CountingLimit>();
  auto traced =
      TracingLimitDecorator::Wrap(std::move(delegate), [](std::string) {});
  EXPECT_EQ(traced->GetLimit(), 42);
}

}  // namespace
}  // namespace limen

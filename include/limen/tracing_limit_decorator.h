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

#ifndef LIMEN_TRACING_LIMIT_DECORATOR_H_
#define LIMEN_TRACING_LIMIT_DECORATOR_H_

#include "limen/limit.h"
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace limen {

// Wraps any `Limit` and emits one diagnostic log line per sample
// before forwarding the sample to the wrapped algorithm.
//
// When layered over a `WindowedLimit` (the usual configuration),
// the wrapped algorithm only sees one sample per window boundary,
// so this decorator emits roughly one log line per window —
// typically once a second.
//
// The log sink is injected through a callback so the decorator
// stays neutral on logging infrastructure. The default sink writes
// a single line to `std::clog`. Applications wanting structured
// output or routing through a particular logger pass their own
// callback at construction.
//
// Ported from Netflix's `TracingLimitDecorator.java`.
class TracingLimitDecorator final : public Limit {
 public:
  // Sink that receives one fully-formatted log line per sample.
  using LogSink = std::function<void(std::string)>;

  // Wraps `delegate` with a decorator that uses the supplied sink.
  // Passing an empty sink installs the default (writes to
  // `std::clog`).
  static std::unique_ptr<TracingLimitDecorator> Wrap(
      std::unique_ptr<Limit> delegate, LogSink sink = LogSink{});

  int GetLimit() const final { return delegate_->GetLimit(); }

  void OnSample(int64_t start_time_ns, int64_t rtt_ns, int inflight,
                bool did_drop) final;

  void NotifyOnChange(ChangeCallback callback) final {
    delegate_->NotifyOnChange(std::move(callback));
  }

 private:
  // PrivateTag lets `std::make_unique<TracingLimitDecorator>(...)`
  // succeed from `Wrap` (a friend's factory) while preventing free
  // construction by external callers. Outside callers cannot
  // construct a PrivateTag.
  struct PrivateTag {
   private:
    PrivateTag() = default;
    friend class TracingLimitDecorator;
  };

 public:
  TracingLimitDecorator(PrivateTag, std::unique_ptr<Limit> delegate,
                        LogSink sink);

 private:
  std::unique_ptr<Limit> delegate_;
  LogSink sink_;
};

}  // namespace limen

#endif  // LIMEN_TRACING_LIMIT_DECORATOR_H_

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

#ifndef LIMEN_SIMPLE_LIMITER_H_
#define LIMEN_SIMPLE_LIMITER_H_

#include "limen/abstract_limiter.h"
#include "limen/limit.h"
#include "absl/status/statusor.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace limen {

// Basic admit-or-reject limiter. On every TryAcquire, the limiter
// compares the current in-flight count against the wrapped Limit's
// cap and admits if there is headroom. The compare-and-check is a
// compare-and-swap loop on the in-flight atomic so the increment
// is committed only when the slot was genuinely available — no
// transient overshoot under contention.
//
// Construction goes through the inner Builder. SimpleLimiter is
// the concrete limiter type the gRPC adapter and the blocking
// decorators wrap by default.
//
// Ported from Netflix's `SimpleLimiter.java`.
class SimpleLimiter final : public AbstractLimiter {
 public:
  class Builder {
   public:
    Builder() = default;

    // Wrapped concurrency-cap algorithm. Required.
    Builder& Limit(std::unique_ptr<limen::Limit> limit) {
      limit_ = std::move(limit);
      return *this;
    }

    // Label value emitted on every metric the limiter records.
    Builder& Id(std::string id) {
      id_ = std::move(id);
      return *this;
    }

    // Optional bypass predicate. If supplied and the predicate
    // returns true for the call's context, the limiter admits the
    // call without touching the in-flight counter and counts the
    // outcome as `bypassed`.
    Builder& BypassPredicate(AbstractLimiter::BypassPredicate predicate) {
      bypass_predicate_ = std::move(predicate);
      return *this;
    }

    // Optional MeterProvider. When supplied, the limiter registers
    // observable instruments for `limen.limit`, `limen.inflight`,
    // and `limen.call`.
    Builder& MeterProvider(
        std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> provider) {
      meter_provider_ = std::move(provider);
      return *this;
    }

    // Validates the accumulated configuration and constructs the
    // limiter. Returns `InvalidArgumentError` when the caller did
    // not supply a `Limit()`. A library never crashes the host
    // process on bad input; the application picks the response
    // (log and abort, fall back to a default, fail-the-startup
    // health check, etc.).
    absl::StatusOr<std::unique_ptr<SimpleLimiter>> Build();

   private:
    std::unique_ptr<limen::Limit> limit_;
    std::string id_;
    AbstractLimiter::BypassPredicate bypass_predicate_;
    std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> meter_provider_;
  };

 protected:
  std::optional<AcquireResult> DoAcquire(std::string_view context) override;

 private:
  // Construction goes through the inner Builder. The PrivateTag is
  // only constructible by Builder, so the public ctor cannot be
  // called from elsewhere — but `std::make_unique` can call it,
  // which keeps Build() free of raw `new`.
  struct PrivateTag {
   private:
    PrivateTag() = default;
    friend class Builder;
  };

 public:
  SimpleLimiter(PrivateTag, AbstractLimiter::Params params);
};

}  // namespace limen

#endif  // LIMEN_SIMPLE_LIMITER_H_

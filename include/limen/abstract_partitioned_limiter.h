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

#ifndef LIMEN_ABSTRACT_PARTITIONED_LIMITER_H_
#define LIMEN_ABSTRACT_PARTITIONED_LIMITER_H_

#include "limen/abstract_limiter.h"
#include "limen/limit.h"
#include "limen/partition.h"
#include "absl/status/statusor.h"
#include "opentelemetry/metrics/async_instruments.h"
#include "opentelemetry/metrics/observer_result.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace limen {

// Limiter with named percentage-quota partitions. The mechanism by
// which an application says "reserve 80 % of the cap for live
// traffic, 20 % for batch."
//
// Admission semantics — burst-into-idle:
//
// - When the global cap has unused capacity, any partition may
//   exceed its quota. A request from a busy partition still
//   admits because spare global capacity is fair game.
// - When the global cap is exhausted, partition quotas become
//   hard. A request from a partition at quota is rejected even if
//   the global cap could in principle still accept it elsewhere.
//
// This lets quiet partitions lend their capacity to busy ones
// under normal load, while still protecting each partition's
// minimum share when the server is saturated.
//
// Per-partition quotas are computed by rounding up: a partition's
// slice is at least one slot, and the slot count is the ceiling
// of `global_cap * percent`. When the global cap changes (a
// `SettableLimit::SetLimit` override or a `Gradient2` adaptive
// update), the limiter recomputes every partition's slot count.
//
// Partition resolution: the application supplies one or more
// `PartitionResolver` callbacks. Each resolver maps a request
// context (an opaque string_view) to an optional partition name.
// Resolvers run in registration order; the first non-empty
// optional whose name matches a configured partition wins. If
// every resolver returns nullopt, or returns a name that does
// not match, the request falls through to a synthetic "unknown"
// partition.
//
// Reject-delay back-pressure: each partition may declare a sleep
// duration applied when rejection happens (default zero — the
// rejection returns immediately). When a partition's reject
// delay is non-zero, the rejection path sleeps for the
// configured duration before returning. `MaxDelayedThreads` caps
// the total number of threads that may be sleeping concurrently
// in this path so the mechanism cannot itself become a
// back-pressure failure mode.
//
// Naming note: the class keeps upstream's `Abstract*` prefix for
// recognisability against the Java port, but Limen's version is
// `final` and the Builder builds it directly. Upstream Java
// declares the class abstract and lets each application supply a
// concrete subclass; in C++ that pattern produces nothing useful
// (no per-instance virtual overrides are needed since all
// partition behaviour is data-driven through the Builder), so the
// concrete class is the leaf.
//
// Ported from Netflix's `AbstractPartitionedLimiter.java`.
class AbstractPartitionedLimiter final : public AbstractLimiter {
 public:
  // Maps a request context to an optional partition name. Empty
  // optional means "this resolver has no opinion" and the next
  // resolver in the chain is consulted.
  using PartitionResolver =
      std::function<std::optional<std::string>(std::string_view context)>;

  // Function used to sleep in the reject-delay path. Default is
  // `std::this_thread::sleep_for`. Tests inject a recording fake
  // so assertions on reject-delay behaviour do not depend on
  // wall-clock elapsed time.
  using SleepFn = std::function<void(std::chrono::milliseconds)>;

  // Synthetic partition name used when no resolver matches.
  static constexpr char kUnknownPartitionName[] = "unknown";

  class Builder {
   public:
    Builder();

    // Wrapped concurrency-cap algorithm. Required.
    Builder& Limit(std::unique_ptr<limen::Limit> limit);

    // Label value emitted on every metric the limiter records.
    Builder& Id(std::string id);

    // Optional bypass predicate. If supplied and the predicate
    // returns true for the call's context, the limiter admits
    // the call without touching any counter and records the
    // outcome as `bypassed`.
    Builder& BypassPredicate(AbstractLimiter::BypassPredicate predicate);

    // Optional MeterProvider. When supplied, the limiter
    // registers observable instruments for `limen.limit`,
    // `limen.inflight` (with global and per-partition rows),
    // `limen.limit.partition`, and `limen.call`.
    Builder& MeterProvider(
        std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> provider);

    // Declare a named partition with a percentage share in
    // [0, 1]. Calling Partition twice with the same name updates
    // the existing entry's percentage. The sum of all
    // partition percentages must be <= 1.0, validated at Build()
    // time. At least one Partition declaration is required.
    Builder& Partition(std::string name, double percent);

    // Set the reject-delay duration for a previously-declared
    // partition. The named partition must exist (declare it via
    // Partition first). Zero (the default) means rejections
    // return immediately.
    Builder& PartitionRejectDelay(std::string const& name,
                                  std::chrono::milliseconds delay);

    // Add a context->partition-name resolver. Multiple resolvers
    // run in registration order; the first non-empty optional
    // whose name matches a configured partition wins. At least
    // one PartitionResolver is required at Build() time.
    Builder& PartitionResolver(
        AbstractPartitionedLimiter::PartitionResolver resolver);

    // Maximum number of threads that may be sleeping in the
    // reject-delay path concurrently. Further rejected requests
    // return immediately when this cap is reached. Default 100.
    Builder& MaxDelayedThreads(int max);

    // Override the sleep function used by the reject-delay path.
    // Defaults to `std::this_thread::sleep_for`. Tests inject a
    // recording fake.
    Builder& SleepFor(SleepFn sleep_fn);

    // Validates the accumulated configuration and constructs the
    // limiter. Returns an `InvalidArgumentError` Status carrying a
    // plain-English description of the problem when the
    // configuration is broken (missing required setter, partition
    // name empty or reserved, percentage out of range, percent sum
    // > 1.0, no resolvers, negative MaxDelayedThreads, a
    // PartitionRejectDelay naming an undeclared partition). A
    // library does not crash the host process on bad input; the
    // application picks the response (log and abort, fall back to a
    // default, fail the startup health check, etc.).
    absl::StatusOr<std::unique_ptr<AbstractPartitionedLimiter>> Build();

   private:
    struct PartitionSpec {
      std::string name;
      double percent = 0.0;
      std::chrono::milliseconds reject_delay{0};
    };

    // Pending reject-delay setting: PartitionRejectDelay is
    // append-only and may name a partition that has not yet been
    // declared via Partition() (the chain order is not constrained).
    // Build() resolves each pending entry against partition_specs_
    // and reports `InvalidArgumentError` when a name does not match.
    struct PendingRejectDelay {
      std::string name;
      std::chrono::milliseconds delay;
    };

    PartitionSpec& FindOrAddPartitionSpec(std::string const& name);

    std::unique_ptr<limen::Limit> limit_;
    std::string id_;
    AbstractLimiter::BypassPredicate bypass_predicate_;
    std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> meter_provider_;
    std::vector<PartitionSpec> partition_specs_;
    std::vector<PendingRejectDelay> pending_reject_delays_;
    std::vector<AbstractPartitionedLimiter::PartitionResolver>
        partition_resolvers_;
    int max_delayed_threads_ = 100;
    SleepFn sleep_fn_;
  };

  // Current in-flight count for the named partition. Returns -1
  // if no partition by that name is configured (the synthetic
  // "unknown" partition counts as configured under
  // `kUnknownPartitionName`).
  int PartitionInflightCount(std::string_view name) const;

  // Current per-partition cap for the named partition. Returns 0
  // if no partition by that name is configured. A configured
  // partition always has a cap of at least one slot.
  int PartitionLimitFor(std::string_view name) const;

  // Number of threads currently parked in the reject-delay sleep
  // path. Test accessor.
  int DelayedThreadCount() const;

  // Override Limiter behaviour to consult per-partition state.
  std::optional<AcquireResult> DoAcquire(std::string_view context) override;
  void OnReleasePartition(int partition_index) override;
  void OnObserveInflight(
      opentelemetry::metrics::ObserverResult const& result) const override;

 private:
  // Construction goes through the inner Builder. The PrivateTag
  // makes the public ctor unusable except through `make_unique`
  // from `Build()`.
  struct PrivateTag {
   private:
    PrivateTag() = default;
    friend class Builder;
  };

 public:
  AbstractPartitionedLimiter(
      PrivateTag, AbstractLimiter::Params params,
      std::vector<std::unique_ptr<limen::Partition>> partitions,
      std::map<std::string, int> partition_index_by_name,
      std::vector<PartitionResolver> resolvers, int unknown_partition_index,
      int max_delayed_threads, SleepFn sleep_fn);

 private:
  int ResolvePartition(std::string_view context) const;
  void RecomputePartitionLimits(int new_global_limit);
  void RegisterPartitionInstruments();
  void MaybeBackoffDelay(limen::Partition const& part);

  static void ObservePartitionLimit(
      opentelemetry::metrics::ObserverResult result, void* state) noexcept;

  std::vector<std::unique_ptr<limen::Partition>> partitions_;
  std::map<std::string, int> partition_index_by_name_;
  std::vector<PartitionResolver> partition_resolvers_;
  int const unknown_partition_index_;
  int const max_delayed_threads_;
  std::atomic<int> delayed_threads_{0};
  SleepFn sleep_fn_;

  std::shared_ptr<opentelemetry::metrics::ObservableInstrument>
      partition_limit_gauge_;
};

}  // namespace limen

#endif  // LIMEN_ABSTRACT_PARTITIONED_LIMITER_H_

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

#include "limen/abstract_partitioned_limiter.h"
#include "limen/partition.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "opentelemetry/metrics/meter.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace limen {

namespace {

// Default sleep function used when the Builder does not inject
// one. Sleeps the calling thread for the requested duration.
void DefaultSleepFn(std::chrono::milliseconds duration) {
  std::this_thread::sleep_for(duration);
}

// Per-partition cap arithmetic: at least one slot, ceiling of
// `global_cap * percent`. Matches upstream's `updateLimit`.
int ComputePartitionLimit(int global_limit, double percent) {
  int const computed = static_cast<int>(std::ceil(global_limit * percent));
  return std::max(1, computed);
}

}  // namespace

AbstractPartitionedLimiter::Builder::Builder() = default;

AbstractPartitionedLimiter::Builder& AbstractPartitionedLimiter::Builder::Limit(
    std::unique_ptr<limen::Limit> limit) {
  limit_ = std::move(limit);
  return *this;
}

AbstractPartitionedLimiter::Builder& AbstractPartitionedLimiter::Builder::Id(
    std::string id) {
  id_ = std::move(id);
  return *this;
}

AbstractPartitionedLimiter::Builder&
AbstractPartitionedLimiter::Builder::BypassPredicate(
    AbstractLimiter::BypassPredicate predicate) {
  bypass_predicate_ = std::move(predicate);
  return *this;
}

AbstractPartitionedLimiter::Builder&
AbstractPartitionedLimiter::Builder::MeterProvider(
    std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> provider) {
  meter_provider_ = std::move(provider);
  return *this;
}

AbstractPartitionedLimiter::Builder&
AbstractPartitionedLimiter::Builder::Partition(std::string name,
                                               double percent) {
  // Chainable setters do not validate; Build() reports the full
  // set of configuration errors via Status. Repeated calls with
  // the same name overwrite the previous percentage (matching
  // upstream's LinkedHashMap.computeIfAbsent shape).
  PartitionSpec& spec = FindOrAddPartitionSpec(name);
  spec.percent = percent;
  return *this;
}

AbstractPartitionedLimiter::Builder&
AbstractPartitionedLimiter::Builder::PartitionRejectDelay(
    std::string const& name, std::chrono::milliseconds delay) {
  // Append-only; Build() resolves each entry against the
  // partition list and reports an error if the name does not
  // match a declared partition.
  pending_reject_delays_.push_back(PendingRejectDelay{name, delay});
  return *this;
}

AbstractPartitionedLimiter::Builder&
AbstractPartitionedLimiter::Builder::PartitionResolver(
    AbstractPartitionedLimiter::PartitionResolver resolver) {
  partition_resolvers_.push_back(std::move(resolver));
  return *this;
}

AbstractPartitionedLimiter::Builder&
AbstractPartitionedLimiter::Builder::MaxDelayedThreads(int max) {
  max_delayed_threads_ = max;
  return *this;
}

AbstractPartitionedLimiter::Builder&
AbstractPartitionedLimiter::Builder::SleepFor(SleepFn sleep_fn) {
  sleep_fn_ = std::move(sleep_fn);
  return *this;
}

AbstractPartitionedLimiter::Builder::PartitionSpec&
AbstractPartitionedLimiter::Builder::FindOrAddPartitionSpec(
    std::string const& name) {
  for (auto& spec : partition_specs_) {
    if (spec.name == name) return spec;
  }
  partition_specs_.push_back(
      PartitionSpec{name, 0.0, std::chrono::milliseconds{0}});
  return partition_specs_.back();
}

absl::StatusOr<std::unique_ptr<AbstractPartitionedLimiter>>
AbstractPartitionedLimiter::Builder::Build() {
  if (limit_ == nullptr) {
    return absl::InvalidArgumentError(
        "AbstractPartitionedLimiter requires Limit()");
  }
  if (partition_specs_.empty()) {
    return absl::InvalidArgumentError(
        "AbstractPartitionedLimiter requires at least one Partition()");
  }
  if (partition_resolvers_.empty()) {
    return absl::InvalidArgumentError(
        "AbstractPartitionedLimiter requires at least one "
        "PartitionResolver()");
  }
  if (max_delayed_threads_ < 0) {
    return absl::InvalidArgumentError(
        "AbstractPartitionedLimiter MaxDelayedThreads must be non-negative");
  }

  // Per-partition validation.
  for (auto const& spec : partition_specs_) {
    if (spec.name.empty()) {
      return absl::InvalidArgumentError("Partition name must not be empty");
    }
    if (spec.name == kUnknownPartitionName) {
      return absl::InvalidArgumentError(
          "Partition name \"unknown\" is reserved for the synthetic "
          "fallback used when no resolver matches");
    }
    if (spec.percent < 0.0 || spec.percent > 1.0) {
      return absl::InvalidArgumentError(
          absl::StrCat("Partition \"", spec.name,
                       "\" percentage must be in [0, 1]; got ", spec.percent));
    }
  }

  // Sum-of-percentages must not exceed 1.0 (within float epsilon).
  double sum = 0.0;
  for (auto const& spec : partition_specs_) sum += spec.percent;
  if (sum > 1.0 + 1e-9) {
    return absl::InvalidArgumentError(
        absl::StrCat("Sum of partition percentages must be <= 1.0; got ", sum));
  }

  // Resolve pending reject-delay settings against the declared
  // partition list. PartitionRejectDelay is append-only at call
  // time so we validate every entry here.
  for (auto const& pending : pending_reject_delays_) {
    PartitionSpec* spec = nullptr;
    for (auto& s : partition_specs_) {
      if (s.name == pending.name) {
        spec = &s;
        break;
      }
    }
    if (spec == nullptr) {
      return absl::InvalidArgumentError(
          absl::StrCat("PartitionRejectDelay names undeclared partition \"",
                       pending.name, "\""));
    }
    spec->reject_delay = pending.delay;
  }

  // Every resolver must be non-null.
  for (size_t i = 0; i < partition_resolvers_.size(); ++i) {
    if (!partition_resolvers_[i]) {
      return absl::InvalidArgumentError(
          absl::StrCat("PartitionResolver at index ", i, " is null"));
    }
  }

  // Build the runtime partition vector. Declaration order maps
  // directly to `partition_index`; the synthetic "unknown"
  // partition is appended at the end.
  std::vector<std::unique_ptr<limen::Partition>> partitions;
  std::map<std::string, int> partition_index_by_name;
  partitions.reserve(partition_specs_.size() + 1);
  for (auto const& spec : partition_specs_) {
    partition_index_by_name[spec.name] = static_cast<int>(partitions.size());
    partitions.push_back(std::make_unique<limen::Partition>(
        spec.name, spec.percent, spec.reject_delay));
  }
  int const unknown_index = static_cast<int>(partitions.size());
  partition_index_by_name[std::string(kUnknownPartitionName)] = unknown_index;
  partitions.push_back(std::make_unique<limen::Partition>(
      std::string(kUnknownPartitionName), 0.0, std::chrono::milliseconds{0}));

  if (!sleep_fn_) sleep_fn_ = &DefaultSleepFn;

  AbstractLimiter::Params params{std::move(limit_), std::move(id_),
                                 std::move(bypass_predicate_),
                                 std::move(meter_provider_)};
  return std::make_unique<AbstractPartitionedLimiter>(
      PrivateTag{}, std::move(params), std::move(partitions),
      std::move(partition_index_by_name), std::move(partition_resolvers_),
      unknown_index, max_delayed_threads_, std::move(sleep_fn_));
}

AbstractPartitionedLimiter::AbstractPartitionedLimiter(
    PrivateTag, AbstractLimiter::Params params,
    std::vector<std::unique_ptr<limen::Partition>> partitions,
    std::map<std::string, int> partition_index_by_name,
    std::vector<PartitionResolver> resolvers, int unknown_partition_index,
    int max_delayed_threads, SleepFn sleep_fn)
    : AbstractLimiter(std::move(params)),
      partitions_(std::move(partitions)),
      partition_index_by_name_(std::move(partition_index_by_name)),
      partition_resolvers_(std::move(resolvers)),
      unknown_partition_index_(unknown_partition_index),
      max_delayed_threads_(max_delayed_threads),
      sleep_fn_(std::move(sleep_fn)) {
  RecomputePartitionLimits(GetLimit());
  RegisterLimitChangeCallback(
      [this](int new_limit) { RecomputePartitionLimits(new_limit); });
  RegisterPartitionInstruments();
}

int AbstractPartitionedLimiter::PartitionInflightCount(
    std::string_view name) const {
  auto it = partition_index_by_name_.find(std::string(name));
  if (it == partition_index_by_name_.end()) return -1;
  return partitions_[it->second]->busy.load(std::memory_order_relaxed);
}

int AbstractPartitionedLimiter::PartitionLimitFor(std::string_view name) const {
  auto it = partition_index_by_name_.find(std::string(name));
  if (it == partition_index_by_name_.end()) return 0;
  return partitions_[it->second]->limit.load(std::memory_order_relaxed);
}

int AbstractPartitionedLimiter::DelayedThreadCount() const {
  return delayed_threads_.load(std::memory_order_relaxed);
}

std::optional<AbstractLimiter::AcquireResult>
AbstractPartitionedLimiter::DoAcquire(std::string_view context) {
  int const partition_index = ResolvePartition(context);
  limen::Partition& part = *partitions_[partition_index];

  // Burst-into-idle: if the global cap has unused capacity, admit
  // unconditionally and let the partition counter run above its
  // own quota. The partition cap is enforced only once the global
  // cap is saturated. The global-cap check is a relaxed read; we
  // accept a transient over-cap under concurrent burst-admissions
  // because the design explicitly trades strict global enforcement
  // for the lend-capacity-between-partitions behaviour. (Strict
  // global enforcement is what SimpleLimiter is for.)
  bool partition_admitted;
  if (inflight_.load(std::memory_order_relaxed) >= GetLimit()) {
    // Global is full: partition CAS gate.
    int current = part.busy.load(std::memory_order_relaxed);
    while (true) {
      int const part_cap = part.limit.load(std::memory_order_relaxed);
      if (current >= part_cap) {
        partition_admitted = false;
        break;
      }
      if (part.busy.compare_exchange_weak(current, current + 1,
                                          std::memory_order_relaxed,
                                          std::memory_order_relaxed)) {
        partition_admitted = true;
        break;
      }
    }
  } else {
    part.busy.fetch_add(1, std::memory_order_relaxed);
    partition_admitted = true;
  }

  if (!partition_admitted) {
    MaybeBackoffDelay(part);
    return std::nullopt;
  }

  int const post_inflight =
      inflight_.fetch_add(1, std::memory_order_relaxed) + 1;
  return AcquireResult{post_inflight, partition_index};
}

void AbstractPartitionedLimiter::OnReleasePartition(int partition_index) {
  partitions_[partition_index]->busy.fetch_sub(1, std::memory_order_relaxed);
}

void AbstractPartitionedLimiter::OnObserveInflight(
    opentelemetry::metrics::ObserverResult const& result) const {
  // Global row matches AbstractLimiter's default emission.
  AbstractLimiter::OnObserveInflight(result);
  // Per-partition rows.
  for (auto const& part : partitions_) {
    EmitObservation(
        result,
        static_cast<int64_t>(part->busy.load(std::memory_order_relaxed)),
        {{"id", GetId()}, {"partition", part->name}});
  }
}

int AbstractPartitionedLimiter::ResolvePartition(
    std::string_view context) const {
  for (auto const& resolver : partition_resolvers_) {
    std::optional<std::string> const name = resolver(context);
    if (!name) continue;
    auto it = partition_index_by_name_.find(*name);
    if (it != partition_index_by_name_.end()) return it->second;
  }
  return unknown_partition_index_;
}

void AbstractPartitionedLimiter::RecomputePartitionLimits(
    int new_global_limit) {
  for (auto& part : partitions_) {
    part->limit.store(ComputePartitionLimit(new_global_limit, part->percent),
                      std::memory_order_relaxed);
  }
}

void AbstractPartitionedLimiter::RegisterPartitionInstruments() {
  auto const& provider = GetMeterProvider();
  if (provider == nullptr) return;
  auto meter = provider->GetMeter(AbstractLimiter::kMeterName, /*version=*/"",
                                  /*schema=*/"");
  partition_limit_gauge_ = meter->CreateInt64ObservableGauge(
      "limen.limit.partition", "Per-partition concurrency cap.", /*unit=*/"1");
  partition_limit_gauge_->AddCallback(
      &AbstractPartitionedLimiter::ObservePartitionLimit, this);
}

void AbstractPartitionedLimiter::ObservePartitionLimit(
    opentelemetry::metrics::ObserverResult result, void* state) noexcept {
  auto* self = static_cast<AbstractPartitionedLimiter*>(state);
  for (auto const& part : self->partitions_) {
    EmitObservation(
        result,
        static_cast<int64_t>(part->limit.load(std::memory_order_relaxed)),
        {{"id", self->GetId()}, {"partition", part->name}});
  }
}

void AbstractPartitionedLimiter::MaybeBackoffDelay(
    limen::Partition const& part) {
  if (part.reject_delay.count() == 0) return;
  // CAS-loop reserve a slot in the delayed-threads budget. The
  // reservation is strict: at most `max_delayed_threads_` threads
  // may be sleeping in this path at any one moment.
  int current = delayed_threads_.load(std::memory_order_relaxed);
  while (current < max_delayed_threads_) {
    if (delayed_threads_.compare_exchange_weak(current, current + 1,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
      sleep_fn_(part.reject_delay);
      delayed_threads_.fetch_sub(1, std::memory_order_relaxed);
      return;
    }
  }
  // Budget exhausted; rejection returns without sleeping.
}

}  // namespace limen

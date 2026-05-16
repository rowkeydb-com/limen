# Limen

[![Static Analysis](https://github.com/rowkeydb-com/limen/actions/workflows/static-analysis.yml/badge.svg)](https://github.com/rowkeydb-com/limen/actions/workflows/static-analysis.yml)
[![Build and Test](https://github.com/rowkeydb-com/limen/actions/workflows/build-test.yml/badge.svg)](https://github.com/rowkeydb-com/limen/actions/workflows/build-test.yml)
[![Sanitizers](https://github.com/rowkeydb-com/limen/actions/workflows/sanitizers.yml/badge.svg)](https://github.com/rowkeydb-com/limen/actions/workflows/sanitizers.yml)
[![Code Coverage](https://github.com/rowkeydb-com/limen/actions/workflows/coverage.yml/badge.svg)](https://github.com/rowkeydb-com/limen/actions/workflows/coverage.yml)
[![codecov](https://codecov.io/gh/rowkeydb-com/limen/branch/main/graph/badge.svg)](https://app.codecov.io/gh/rowkeydb-com/limen)

**Limen is an idiomatic C++20 port of
[Netflix's `concurrency-limits`](https://github.com/Netflix/concurrency-limits),
implementing Netflix's Gradient2 algorithm in full upstream-compatible
form**. Limen also ships a transport-agnostic Controlled Delay (CoDel)
admission filter that implements RFC 8289 in full. Limen is built for
production C++ servers that need to defend themselves against overload.

Limen is:

- **In-process.** No sidecar, no separate service, no extra network
  hop. The library runs inside your server's address space, reads its
  own atomics, and is consulted on whatever code path you choose.
- **General.** Limen has no dependency on any specific RPC library.
  It works with any application-level RPC library that lets you (1)
  reject a request before its body is parsed and (2) attach a context
  handle that flows from that pre-body hook through to your handler
  callback. The two properties above are common: gRPC, Thrift, and
  Cap'n Proto all provide them. They are everything Limen needs.
- **Rejecting a request is cheap.** When the adaptive limiter says
  no, the cost is a handful of relaxed atomic operations and a
  return. When the CoDel filter drops, the cost is a short
  mutex-guarded state-machine step and an atomic increment.
  Neither path allocates on the heap, performs I/O, or makes a
  synchronous OpenTelemetry call. The CoDel mutex is held only
  for the brief state-machine transition, so contention from
  concurrent worker threads is bounded. Admitting a request pays
  the same as rejection in the limiter, plus one `steady_clock`
  timestamp read.
  The two blocking limiters (`BlockingLimiter`,
  `LifoBlockingLimiter`) block by design. The partitioned limiter
  sleeps on rejection only when an operator configures a non-zero
  `reject_delay`. Every other rejection path stays non-blocking.
- **Fully observable.** Every internal signal Limen tracks is
  collected asynchronously. Gauges, up-down counters, and counters
  are exposed as OpenTelemetry observable instruments that the SDK
  collector polls on its own schedule. The three Gradient2
  histograms are recorded once per window (about once per second
  by default), on the request thread that crosses the boundary,
  never on the per-request path. The full catalogue lives in
  [`Observability.md`](Observability.md).

Netflix released the upstream Java library in 2018 under the Apache
2.0 License. Limen is also Apache 2.0. Every file ported from
upstream preserves Netflix's copyright notice. The repository's
[`NOTICE`](NOTICE) file credits the upstream source.

## What Limen does

Limen sits in front of a request handler and cheaply answers the
question "should this request be admitted right now?". The answer
adapts automatically to the server's measured execution latency.

The heart of the library is Netflix's Gradient2 algorithm. Each time
window (one second by default), the algorithm compares the current
short-term round-trip time against a long-term smoothed average and
adjusts a concurrency cap up or down. Requests that exceed the
current cap are rejected immediately, before any meaningful work has
been done on them.

Optional partition support lets the application reserve named slices
of the cap ("eighty percent for live traffic, twenty percent for
batch jobs"). Each partition's slice acts as a soft suggestion
until the global cap saturates, at which point it becomes a hard
ceiling.

Limen also ships a CoDel admission filter for queue management. The
filter watches how long items sit in your queue and starts dropping
items when the minimum sojourn time exceeds a configurable target for
at least a sliding interval. CoDel is independent of Gradient2.
Gradient2 keeps the server from accepting more concurrent work than
it can handle. CoDel keeps work that has already been accepted from
sitting in the queue too long. The two compose naturally and a server
that runs both gets both protections. The full algorithm is in
[`design/architecture.md`](design/architecture.md).

## The additive-increase default

Each time Gradient2 raises the concurrency cap, it adds a small
"queue size" amount. Upstream Netflix's library uses a constant
default of 4, regardless of cap.

Limen's default is `ceil(sqrt(current_limit))`, floored at 1. So at
a cap of 100 the algorithm probes by 10 per window (10% of cap),
and at 10000 it probes by 100 per window (1% of cap). Recovery from
a clamp-down is proportionally faster at higher caps. That is the
case that matters most operationally: a server that has just been
throttled and is opening capacity back up should walk back up
quickly. The upstream constant-form default is still available
through `Gradient2Limit::Builder().QueueSize(4)`.

## Quickstart

At minimum, an application needs two pieces: a `SimpleLimiter` (or
`AbstractPartitionedLimiter`), and an interception point in the
application-level RPC library where the application calls
`TryAcquire` early and uses the returned `SlotGuard` to feed the
handler's completion outcome back. A `CodelFilter` is optional and
orthogonal. Wire it where the application decides whether to dequeue
a queued item.

The example below uses a hypothetical application-level RPC library
`MyRpcLib` that exposes the two integration hooks Limen needs:

- A pre-body interception hook (`OnRequest`) that runs before the
  request body is parsed.
- A way to attach an arbitrary value to the per-call context that the
  later handler-side callbacks can read back.

```cpp
#include "limen/codel_filter.h"
#include "limen/gradient2_limit.h"
#include "limen/simple_limiter.h"
#include "limen/windowed_limit.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"

// Limen objects live for the lifetime of the server.
std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> otel = ...;

auto limiter = limen::SimpleLimiter::Builder()
    .Limit(limen::WindowedLimit::Builder()
               .Build(limen::Gradient2Limit::Builder()
                          .InitialLimit(50)
                          .MaxConcurrency(500)
                          .MeterProvider(otel)
                          .Id("my-service")
                          .Build())
               .value())
    .MeterProvider(otel)
    .Id("my-service")
    .Build()
    .value();

auto codel = limen::CodelFilter::Builder()
    .Target(absl::Milliseconds(5))
    .Interval(absl::Milliseconds(100))
    .Id("my-service")
    .MeterProvider(otel)
    .Build()
    .value();

// Pre-body interception. Runs before the request body is parsed.
// If Limen rejects, we tell the library to reject. On admission we
// stash the SlotGuard and an enqueue timestamp in the per-call
// context for later steps to read back.
my_rpc_server.OnRequest([&](MyRpcLib::Request& req) {
  auto slot = limiter->TryAcquire(req.method_name());
  if (!slot) {
    return req.Reject(MyRpcLib::Status::Overloaded);
  }
  req.context().Set("limen.slot", std::move(*slot));
  req.context().Set("limen.enqueue_time", absl::Now());
});

// Dequeue point. Wherever a worker thread pulls an item off the
// in-process queue and decides whether to run it, ask CoDel. If
// CoDel drops, complete the slot with OnDropped so the algorithm
// hears about the drop, and tell the library to reject. A
// second completion call from OnHandlerDone after a CoDel drop
// is a no-op: SlotGuard's completion path is idempotent.
my_rpc_server.OnDequeue([&](MyRpcLib::Context& ctx) {
  auto enqueue = ctx.Get<absl::Time>("limen.enqueue_time");
  if (codel->ShouldDrop(enqueue)) {
    auto& slot = ctx.Get<limen::Limiter::SlotGuard>("limen.slot");
    slot.OnDropped();
    return ctx.Reject(MyRpcLib::Status::Overloaded);
  }
});

// Handler completion. Whichever way the handler finishes, mirror
// the outcome onto the SlotGuard. If neither path runs before the
// SlotGuard falls out of scope, its destructor reports OnIgnore.
my_rpc_server.OnHandlerDone([&](MyRpcLib::Context& ctx, bool ok) {
  auto& slot = ctx.Get<limen::Limiter::SlotGuard>("limen.slot");
  if (ok) slot.OnSuccess();
  else slot.OnDropped();
});
```

The same shape works on top of gRPC's `AuthMetadataProcessor` (the
pre-body hook) plus a `POST_RECV_CLOSE` interceptor (the completion
hook), and on top of any other library with similar pre-body /
per-call-context entry points. The design document's section on
[wiring Limen onto your application-level RPC library](design/architecture.md#wiring-limen-onto-your-application-level-rpc-library)
walks through the integration in more depth.

A compilable version of the example above (without the `MyRpcLib`
placeholder; the request is a `sleep_for` standing in for real
handler work) lives at
[`examples/quickstart.cc`](examples/quickstart.cc). Build and run
it with `bazel build //examples:quickstart &&
bazel-bin/examples/quickstart`.

## Building

Limen builds with [Bazel](https://bazel.build/). Every CI test runs
inside an Ubuntu 24.04 Docker image.

To run the test suite locally:

```
# Release-mode test suite in Docker:
./.github/scripts/run-bazel-in-docker.sh release test

# AddressSanitizer + UndefinedBehaviorSanitizer:
./.github/scripts/run-bazel-in-docker.sh asan-ubsan test

# ThreadSanitizer:
./.github/scripts/run-bazel-in-docker.sh tsan test

# clang-tidy:
./.github/scripts/run-bazel-in-docker.sh clang-tidy build
```

On a developer's machine the script builds the `limen-ci` image on
first invocation and reuses it from the local Docker cache on
subsequent runs. In CI the same image is built per job by the
`docker/build-push-action` step with GitHub Actions layer caching.

### Using Limen in a downstream Bazel project

Until Limen is published to the
[Bazel Central Registry](https://registry.bazel.build/) (planned for
v0.1.0), depend on it directly from source with a `git_override`:

```python
# MODULE.bazel of your project
bazel_dep(name = "limen", version = "0.1.0")
git_override(
    module_name = "limen",
    remote = "https://github.com/rowkeydb-com/limen.git",
    commit = "<sha you want to pin>",
)
```

```python
# BUILD of the target that uses Limen
cc_library(
    name = "my_server",
    srcs = ["my_server.cc"],
    deps = [
        "@limen//:limen",
        # ... your other deps ...
    ],
)
```

Running `bazel build //...` after the `MODULE.bazel` update fetches
Limen and its transitive dependencies. Once Limen is in the Bazel
Central Registry, the `git_override` block goes away and
`bazel_dep(name = "limen", version = "0.1.0")` is everything you need.

## Documentation

- [`design/architecture.md`](design/architecture.md) — the full design
  document, with the algorithms in depth, the integration contract,
  the cost analysis, and the test plan.
- [`Observability.md`](Observability.md) — full catalogue of
  OpenTelemetry signals, with units, label sets, semantics, and the
  asynchronous-collection model.
- [`examples/quickstart.cc`](examples/quickstart.cc) — the runnable
  version of the README quickstart, built as `//examples:quickstart`.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — pull-request mechanics,
  commit-message conventions, code style.
- [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md) — Contributor Covenant
  2.1.
- [`SECURITY.md`](SECURITY.md) — security-disclosure process.

## Wiring Limen onto a different application-level RPC library

The integration contract is small: a pre-body interception hook, and
a per-call context channel that propagates a value from that hook to
the handler-completion callback. See the design document's
integration section for the shape of a minimal adapter. The total
adapter code typically lands on the order of one hundred lines per
library.

## License

Apache 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).

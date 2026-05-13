# Limen — Architecture and Design

## What this library is, and where it came from

Limen is a C++20 port of Netflix's
[`concurrency-limits`](https://github.com/Netflix/concurrency-limits/tree/78a74b9878d38c4c048b0304ce12a162ab7b7222)
Java library. Netflix's library introduced a set of production-tested
algorithms that let a server automatically decide how many requests it can
safely handle at the same time — and reject the rest before they cause harm.
This is called adaptive concurrency limiting.

Netflix released the original work in 2018 under the
[Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0). Limen is
also Apache 2.0. Limen's copyright holder is RowKeyDB, the organisation
that hosts and maintains the project; every file ported from the upstream
library keeps Netflix's 2018 copyright notice alongside the
`Copyright 2026 RowKeyDB` notice, the repository carries a `NOTICE` file
crediting the upstream source, and the README opens with the same credit
prominently. We are porting their work, not claiming to have invented it.

The reason this port exists is simple: today there is no high-quality,
standalone, plug-and-play C++ library that does what Netflix's Java library
does. The two existing C++ implementations of these algorithms — one inside
[Envoy](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter)
and one inside Apache brpc — are deeply tangled with the frameworks that
host them, and cannot easily be extracted. Limen fills that gap.

## Why a server needs adaptive concurrency control

The most common way to protect a server from overload is to pick a number —
"this server can handle two hundred requests at the same time" — and reject
everything above that number. The number is usually a guess. The number is
often wrong. Server capacity is not a constant: it shifts with hardware
changes, with the mix of work the server is doing, with caching, with garbage
collection in managed languages, with cache misses in unmanaged ones, and
with time of day. A number chosen conservatively wastes capacity when
conditions are good; a number chosen aggressively causes outages when
conditions are bad.

Adaptive concurrency control replaces the guess with a measurement loop. The
server continuously watches how long its work is taking. When work is taking
roughly as long as it usually does, the server slowly increases how many
concurrent requests it accepts. When work starts taking noticeably longer
than usual — a sign that something is overloaded — the server shrinks its
accepted concurrency until latency comes back to normal. Requests that exceed
the current concurrency cap are rejected immediately, with a status that
tells the client to back off and try later.

The intellectual ancestry of this approach is TCP congestion control, in
particular [TCP Vegas](https://en.wikipedia.org/wiki/TCP_Vegas), an
algorithm from the mid-1990s that uses latency rather than packet loss as
its congestion signal. Netflix adapted Vegas for RPC services and improved
on it in later iterations; their current recommended algorithm, Gradient2,
is what Limen ports first.

## What Limen does, in one sentence

Limen sits in front of an RPC handler and answers, very cheaply, the
question "should this request be admitted right now?" — and the answer
adapts automatically to the server's measured latency.

## What is in scope for the first release

Limen v0.1.0 ports the parts of Netflix's library that a production C++
server actually needs:

- The Gradient2 algorithm, which is Netflix's current recommended
  algorithm for RPC services.
- All four limiter wrappers from the upstream library: a simple admit-or-
  reject limiter; a partitioned limiter that lets the application reserve
  named slices of capacity ("eighty percent for live traffic, twenty
  percent for batch jobs"); a FIFO blocking limiter for callers that can
  afford to wait briefly; and a LIFO blocking limiter for the same need
  but with the property that the freshest request is processed first when
  capacity opens.
- All four algorithm-supporting helpers: a fixed-value algorithm; a
  manually-settable algorithm; a windowing wrapper that batches samples
  into fixed time intervals; and a tracing decorator useful for diagnostics.
- Two implementations of the per-window aggregation: one that averages
  the round-trip times within a window, and one that takes a configurable
  percentile.
- Three measurement helpers used by the algorithms: an exponentially
  smoothed moving average, a minimum tracker, and a passthrough.
- A gRPC server-side adapter that wires the limiter onto a real gRPC
  server using gRPC's interceptor mechanism. This adapter is in a
  separate Bazel target so applications that do not use gRPC can take a
  dependency on the core without pulling gRPC in.
- Native [OpenTelemetry](https://opentelemetry.io/docs/languages/cpp/)
  metrics for every signal Netflix's library exposes, so an operator can
  watch the algorithm's behaviour on a dashboard.
- A test suite organised in four layers: deterministic
  algorithm tests, mock-clock traffic simulations, real gRPC end-to-end
  tests, and observability tests that drive the in-memory OpenTelemetry
  exporter.

What is explicitly out of scope for v0.1.0 is named in the "Upcoming work"
section near the end of this document, so it is not lost.

## How the algorithm works, in plain English

Every time the server handles a request, it learns one number: how long
that request took to handle. The algorithm collects these numbers, in time
windows of about one second each. At the end of each window, it asks two
questions: how long is the request taking now, and how long has the
request usually taken over a much longer period? It compares the two.

If the current window is roughly as fast as the long-term average, the
server is not overloaded, and the algorithm grows the concurrency cap by a
small fixed amount — the same way TCP slowly grows the size of its window
when packets are flowing fine. This is the additive-increase half of
TCP-style congestion control.

If the current window is noticeably slower than the long-term average,
the server is in trouble. The algorithm shrinks the concurrency cap by a
factor proportional to how much slower things have become. The factor is
clamped between two limits: at one end, the cap is never shrunk by more
than half in a single window (no matter how bad latency gets); at the
other end, small increases in latency are forgiven by a tolerance factor
(by default, latency has to rise roughly 50 percent above the long-term
baseline before any shrinking begins at all). Inside that window, the
cap shrinks in proportion to the latency increase.

The result is smoothed: the algorithm does not flip the cap up and down
violently between windows. Instead it blends the old cap and the new cap
with a configurable weighting. There are also hard floors and ceilings on
the cap so that, even if the math goes wrong, the cap cannot collapse to
zero (which would prevent the server from ever recovering) or grow without
bound (which would cause memory exhaustion).

Three further refinements matter in practice:

- The long-term latency is itself a moving average, not a fixed baseline.
  This lets the algorithm adapt as the server's normal latency drifts
  (hardware changes, workload shifts, etc.) without operator intervention.
- If the server is recovering from a long load spike, the smoothed
  long-term average can stay high for a while, which would make the
  algorithm pessimistic and shrink the cap when it shouldn't. To handle
  this, the algorithm explicitly compresses the long-term average when it
  is more than double the short-term measurement, as a deliberate
  "I think we're recovering, let me forgive the recent past" step.
- If the server is using less than half of its current cap when the
  window closes, the algorithm leaves the cap alone. A lightly used
  server tells us nothing useful about its real capacity: the measured
  latency in that window is dominated by whatever the few in-flight
  requests happened to be doing, not by load. The algorithm skips the
  update rather than chase a misleading signal.

These details are visible in Netflix's
[`Gradient2Limit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/Gradient2Limit.java).
The math is small — roughly thirty lines of arithmetic. Limen ports it
faithfully.

The algorithm exposes a handful of knobs through a builder, with
defaults taken straight from upstream:

| Knob | Default | What it controls |
|---|---|---|
| `initial_limit` | 20 | Starting cap before any samples have arrived. |
| `min_limit` | 20 | Floor on the cap; the algorithm will never drive it lower. |
| `max_concurrency` | 200 | Ceiling on the cap; the algorithm will never drive it higher. |
| `queue_size` | 4 | Additive-increase amount when the gradient says "we have headroom." |
| `rtt_tolerance` | 1.5 | How much higher the short-term RTT must climb above the long-term average before the algorithm starts shrinking. A value of 1.5 means a 50 percent latency increase is forgiven. |
| `smoothing` | 0.2 | Weight given to the newly computed cap when blending against the old cap. A lower value damps the algorithm; a higher value lets it react faster. |
| `long_window` | 600 | Sample count over which the long-term RTT is smoothed (a 10-minute moving window at one-sample-per-second). |

The `WindowedLimit` decorator adds its own knobs: `min_window_time` and
`max_window_time` (defaults: both one second, so by default the window
is a flat one-second interval; if the operator sets the two to
different values, the window resizes adaptively between those bounds
based on observed RTT, but the default does not exercise that
mechanism), `window_size` (default: 10; the minimum number of samples
required before the algorithm runs against a window), and
`min_rtt_threshold` (default: 100 microseconds; samples below this
are discarded as noise). When the percentile sample window is in use,
a fourth knob applies: `max_samples_per_window` (the size of the
pre-allocated RTT array in each percentile-window slot; default 1024,
which exceeds the expected per-window sample count by a comfortable
margin for most workloads).

## The pieces of the library

There are three groups of types.

### Algorithms

An algorithm, in Limen's vocabulary, is a piece of code that takes a
round-trip time sample and decides what the concurrency cap should be
next. The upstream interface is
[`Limit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/Limit.java).
Limen ports it as a small C++ class hierarchy:

- **`Gradient2Limit`** — the primary algorithm described above. Ports
  [`Gradient2Limit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/Gradient2Limit.java).
- **`FixedLimit`** — returns a constant cap. Ports
  [`FixedLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/FixedLimit.java).
  Used for tests and for "freeze the cap here" debugging.
- **`SettableLimit`** — the cap is set programmatically by the
  application, useful for tests and for external override.
  Ports [`SettableLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/SettableLimit.java).

And two algorithm decorators that wrap any of the above:

- **`WindowedLimit`** — collects per-request samples and only passes
  aggregated, per-window summaries down to the wrapped algorithm. Without
  this, the algorithm's math would run on every single request, which
  would defeat the cheap-admission guarantee. Ports
  [`WindowedLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/WindowedLimit.java),
  with a deliberate divergence from upstream's storage strategy
  described in the next paragraph.

  Upstream's `SampleWindow` is immutable. Each new sample causes a
  fresh `SampleWindow` to be allocated, populated with the new
  aggregate, and atomically swapped in via a compare-and-swap loop on
  the pointer. That gives Java's garbage collector something to do on
  every single sample, which Java accepts as the price of admission;
  it would not survive review in a hot-path C++ design. Limen instead
  uses a fixed pair of pre-allocated, mutable `SampleWindow` slots and
  an atomic active-buffer index. The trade-off this introduces is
  explained below.

  Each `SampleWindow` slot holds its aggregate fields — sample count,
  RTT sum, RTT minimum, maximum-inflight, drop-occurred flag — as
  independent atomic variables. A request thread recording a sample
  loads the active-buffer index (relaxed memory ordering) and then
  applies a small number of independent relaxed-atomic updates to the
  buffer's fields: a `fetch_add` on the count and the sum, a
  compare-and-swap loop on the minimum and the maximum, an
  unconditional store on the drop-occurred flag. There is no lock and
  no allocation.

  At the window boundary, the thread that won the try-lock atomically
  flips the active-buffer index from one slot to the other. New
  samples now land in the freshly-active buffer. The boundary thread
  then reads the now-inactive buffer's fields, runs the algorithm's
  math against them, and finally resets the inactive buffer's atomic
  fields back to their initial values before the next boundary will
  make it active again.

  There is one race the design accepts deliberately. A request thread
  that captures the active-buffer index just before the boundary swap
  will land its field updates in what is, by the time the updates
  complete, the now-inactive buffer. Two outcomes are possible. If
  the updates land before the boundary thread reads the buffer, the
  sample is included in the aggregate the algorithm sees; the writer
  is attributed to the window that just closed. If the updates land
  after the boundary thread has reset the buffer, the sample is
  attributed to whichever future window picks up that buffer when it
  next becomes active. A narrow third interleaving exists where the
  boundary thread reads the field, the writer's update lands, and the
  boundary thread then resets the field — in that one case the
  writer's sample is overwritten by the reset and is effectively
  lost. The width of this interleaving is on the order of a few
  microseconds; the proportion of samples that hit it at realistic
  request-rate-to-window-size ratios is small enough that the
  algorithm's aggregates are not measurably affected.

  Two `SampleWindow` slots are pre-allocated at the time the
  `WindowedLimit` is constructed; the active-buffer pointer never
  points anywhere else. No allocation occurs on any path after
  construction.

  The description above covers the *average* sample window, which
  needs only a few aggregate fields per slot. The *percentile* sample
  window (`ImmutablePercentileSampleWindow` upstream) is a different
  shape: it stores every individual RTT in the window in an array,
  because computing a percentile at boundary time requires sorting
  the observed values. Alongside the array, the percentile slot
  carries the same three auxiliary atomic fields the average slot
  carries — `min_rtt` (the minimum observed RTT over the window),
  `max_inflight` (the running maximum of the in-flight count
  reported with each sample), and the drop-occurred flag — and updates
  them on each sample with the same patterns the average slot uses (a
  compare-and-swap loop on `min_rtt` and `max_inflight`, an
  unconditional store on the drop-occurred flag). The boundary
  thread reads all of them, hands them to the algorithm, and resets
  them when it resets `sample_count_`.

  The RTT array itself is the new piece. Each percentile slot
  pre-allocates a fixed-size
  `std::array<std::atomic<int64_t>, kMaxSamplesPerWindow>` alongside
  an `std::atomic<int> sample_count_`. A request thread recording a
  sample does a `fetch_add` on `sample_count_` to claim an index,
  and (if the returned index is within bounds) writes the RTT into
  the array at that index with a relaxed store. If the array fills
  (the returned index is at or beyond `kMaxSamplesPerWindow`), the
  sample is dropped from the array — the boundary thread sees only
  the first `kMaxSamplesPerWindow` samples and computes the
  percentile from those. The `sample_count_` value still increments
  past the bound and is exposed to the algorithm as the true count;
  the array bound only affects which samples can participate in the
  percentile calculation. `kMaxSamplesPerWindow` is a configurable
  default on the `WindowedLimit` builder; see the defaults section
  above.

  At the boundary, the percentile-slot's `sample_count_` is read, the
  array is read up to `min(sample_count_, kMaxSamplesPerWindow)`,
  the values are sorted, and the requested percentile is selected.
  The slot is then reset by storing zero into `sample_count_` and
  resetting the auxiliary fields to their initial values; the
  array's stale contents are left in place because the next window
  will overwrite them as it fills.

  The race window described above applies in the same shape: a late
  writer that captured the active-buffer index before the swap may
  claim an array index in the now-inactive slot. If that claim
  happens after the boundary thread has read `sample_count_`, the
  late sample is invisible to that boundary's percentile computation
  but is included in the next boundary's count (the reset zeroed
  `sample_count_`, so the late `fetch_add` from a now-stale writer
  produces an index of zero or higher in the next window's
  accounting). The same statistical argument applies: a small
  proportion of samples slip across the boundary; for percentile
  measurement, where many samples per window are aggregated, this
  introduces no operationally meaningful difference.
- **`TracingLimitDecorator`** — logs a one-line diagnostic for each
  sample the wrapped algorithm sees, including the in-flight count
  and the round-trip-time values. The log frequency therefore matches
  whatever frequency the wrapped algorithm is being driven at; when
  the decorator is layered on top of `WindowedLimit` (the usual
  case), that comes out to roughly one log line per window boundary,
  which is operationally tolerable. Ports
  [`TracingLimitDecorator.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/TracingLimitDecorator.java).

The windowing wrapper needs two more pieces: a sample aggregator (one for
averages, one for percentiles) and a long-term measurement primitive (an
exponentially smoothed moving average, a minimum, or a passthrough). These
all port from upstream files of the same name and live in the
`measurement/` and `window/` sub-directories of the source tree.

### Limiters

A limiter is the actual gate that decides "admit" or "reject" on a
specific request. It owns the counter that tracks how many requests are
currently in progress, it consults the algorithm to know what the cap is,
and it tells the algorithm about each completed request. The upstream
interface is
[`Limiter.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/Limiter.java).

Limen ports four limiters:

- **`SimpleLimiter`** — basic admit or reject. Ports
  [`SimpleLimiter.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limiter/SimpleLimiter.java).
- **`AbstractPartitionedLimiter`** — supports named partitions with
  percentage quotas, the mechanism by which an application says "reserve
  eighty percent of capacity for live traffic, twenty percent for batch."
  When the global cap has unused capacity, any partition may exceed its
  quota; only when the global cap is exhausted are partitions enforced.
  This lets quiet partitions lend their capacity to busy ones. Per-partition
  quotas are computed by rounding up: a partition's slice is at least one
  slot, and the slot count is the ceiling of (global cap × partition
  share). When the global cap changes (via, for example, a `SettableLimit`
  override or normal algorithm adaptation), the per-partition slot
  counts are recomputed accordingly. Ports
  [`AbstractPartitionedLimiter.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limiter/AbstractPartitionedLimiter.java).

  One optional feature of upstream is preserved verbatim: a partition
  can be configured with a small reject-delay (default zero). When that
  delay is non-zero, a rejected request from that partition sleeps for
  the configured duration before the rejection is returned. This is a
  deliberate server-side back-pressure mechanism. Limen carries it over
  because removing a configurable feature is a divergence; but when the
  reject-delay is set, the rejection path is no longer non-blocking —
  it is the one place where rejection can sleep. A second knob
  (`max_delayed_threads`, default 100) caps how many requests can be
  sleeping concurrently in this way, so the mechanism cannot itself
  become a back-pressure failure mode.
- **`BlockingLimiter`** — if the cap is full, the caller waits briefly
  rather than being rejected immediately. Useful for callers that can
  afford a small delay. Ports
  [`BlockingLimiter.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limiter/BlockingLimiter.java),
  with one deliberate divergence: Limen's port queues waiting
  callers in first-in-first-out order, whereas the upstream Java
  implementation uses `Object.wait` / `notifyAll` and therefore makes
  no ordering guarantee at all (whichever thread the JVM happens to
  schedule on `notifyAll` wakes up). Limen uses `absl::Mutex` and an
  explicit waiter queue to provide the FIFO discipline most operators
  would expect from a "blocking" variant, and the test plan includes
  an explicit ordering assertion that upstream does not have.
- **`LifoBlockingLimiter`** — same idea as the FIFO version, but the
  caller served first is the one that arrived most recently. Under
  overload, older requests in the queue have probably already timed out
  on the client side, so processing them is wasted work. LIFO ordering
  cuts that waste. Also supports a per-request timeout, so a request
  whose deadline has passed never gets woken up. Ports
  [`LifoBlockingLimiter.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limiter/LifoBlockingLimiter.java).

The shared machinery for all of them lives in
[`AbstractLimiter.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limiter/AbstractLimiter.java).

Upstream also exposes a small helper interface,
[`LimiterRegistry`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/LimiterRegistry.java),
which lets an application register one limiter per named key — for
example, one limiter per RPC method, so that read traffic and write
traffic each have their own independent cap. Limen ports this. Most
applications will use a single global limiter and never touch the
registry; the registry is the right tool for "different RPCs need
genuinely different capacity caps, not just different partitions
within one cap."

### The gRPC adapter

To use any of the above with a real gRPC server, the application needs an
adapter that connects the limiter to the network layer. Netflix's adapter
is
[`ConcurrencyLimitServerInterceptor.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-grpc/src/main/java/com/netflix/concurrency/limits/grpc/server/ConcurrencyLimitServerInterceptor.java).
Limen ports the same idea using gRPC C++'s server interceptor mechanism,
defined in the header
[`grpcpp/support/server_interceptor.h`](https://github.com/grpc/grpc/blob/master/include/grpcpp/support/server_interceptor.h)
and documented in the
[gRPC C++ API reference](https://grpc.github.io/grpc/cpp/classgrpc_1_1experimental_1_1_interceptor.html).
The API has lived in the `grpc::experimental` namespace since 2019; it is
the only practical way to intercept a request before the protocol buffer
parser sees it, which is why we use it here.

The adapter sits at the earliest moment of a request's lifecycle — before
the protocol buffer message has been parsed, before any handler thread has
been allocated. That position is important. Rejecting a request that has
not yet been parsed costs almost nothing; rejecting one that has been
parsed wastes the parsing work and the memory it allocated. Limen's
adapter does the former.

The adapter only admits unary RPCs into the limiter. Any non-unary RPC
— client-streaming, server-streaming, or bidirectional — is allowed
through without consulting the limiter at all, matching the upstream
default. The reasoning is that round-trip time is not a well-defined
concept for a streaming RPC (the call can live for minutes or hours
of legitimate work), so the algorithm has nothing useful to learn from
those calls. Applications that need admission control on streaming
RPCs can configure that explicitly, but in v0.1.0 the default is the
upstream default.

This lives in a separate Bazel target, `//:limen_grpc`, that depends on the
core library plus gRPC. Applications that do not use gRPC can depend on
`//:limen` alone.

## How an application uses the library

The application creates an algorithm, wraps it in a windowing decorator,
hands the result to a limiter, optionally configures partitions, and
registers a gRPC interceptor that consults the limiter. From there the
library does its work without further involvement.

The acquire-and-release dance is wrapped in an
[RAII](https://en.cppreference.com/w/cpp/language/raii)-style guard
object that the application receives from `try_acquire`. The guard
stores its state — a back-pointer to the limiter (non-owning), a start
timestamp, an inflight-at-acquire count, a completion-mode flag —
entirely inline as plain members. The application's stack frame owns
it. The guard knows three completion modes — "succeeded", "ignore me,
I do not have a real round-trip-time signal", and "this request was
dropped or cancelled" — and the application calls whichever applies.
If the application forgets to call any of them, the guard's destructor
defaults to "ignore me", which is safe.

For the gRPC interceptor path, the application does not write any of
this manually. The slot state lives as plain members of the
per-call `grpc::experimental::Interceptor` object that gRPC itself
allocates and manages as part of its existing per-call bookkeeping;
when gRPC destroys the interceptor at request completion, the
interceptor's destructor runs the release-and-sample logic. Limen
does not heap-allocate anything along this path.

A minimal application setup looks roughly like this:

```cpp
auto limit = limen::Gradient2Limit::Builder().InitialLimit(100).Build();
auto windowed = limen::WindowedLimit::Builder().Build(std::move(limit));
auto limiter = limen::SimpleLimiter::Builder()
    .Named("my-service")
    .Limit(std::move(windowed))
    .MeterProvider(otel_meter_provider)
    .Build();

grpc::ServerBuilder builder;
builder.experimental().SetInterceptorCreators(
    limen::grpc::MakeInterceptorFactories(limiter));
builder.RegisterService(&my_service);
auto server = builder.BuildAndStart();
```

For a partitioned setup, the application names the partitions and
provides a function that maps each incoming request to a partition name.

## How much it costs to use Limen

The admission decision is on the critical path of every request, so it
must be cheap — particularly when the answer is "reject", because that is
the case where the server is under attack and CPU cycles are most scarce.
If saying "no" to a request costs nearly as much as actually handling it,
the server denial-of-service-es itself before the limiter has done any
good. This concern shapes the implementation throughout.

The mechanism is a compare-and-swap loop on the in-flight atomic
counter, matching the gating semantics of Java's `Semaphore.tryAcquire`
that upstream relies on. A request thread arriving at the limiter
loads the current in-flight value and the current cap. If the
in-flight value is below the cap, the thread attempts a
compare-and-swap that bumps the counter by one only if it has not
changed since the load. If the compare-and-swap succeeds, the request
is admitted. If it fails because another thread changed the counter
first, the thread loops with the new value and tries again. If at any
iteration the loaded in-flight value is at or above the cap, the loop
exits and the request is rejected. The counter never transiently
exceeds the cap: an increment only happens through a successful
compare-and-swap, and the precondition `current < cap` is part of
that swap's success condition.

The outcome counter (`limen.call`) — separately, Limen-owned, one atomic
slot per `(id, status)` label combination — is then incremented to
record the outcome of the decision. This is one additional atomic
operation, on a different storage location from the in-flight gate,
and it is the same cost whether the outcome is admit or reject.

Concretely:

- **Rejecting a request** costs at minimum one atomic load of the
  in-flight counter, one atomic load of the cap, one comparison that
  drops out of the loop, and one atomic increment on Limen's own
  outcome-counter storage. Under contention, the compare-and-swap may
  fail one or more times before the loop converges, but each
  iteration is a small constant. No memory allocation. No locks. No
  synchronous OpenTelemetry calls. No string formatting. The
  rejected request returns a `Status::UNAVAILABLE` with a static
  error message, matching the upstream Java library's default.
  (Applications can override the rejection status — for example, to
  attach a `google.rpc.RetryInfo` payload — through the interceptor
  builder; we deliberately keep the upstream default so that operators
  familiar with the Java library see the same rejection code by
  default.) The rejected request never reaches the protocol buffer
  parser.
- **Admitting a request** costs one atomic load of the in-flight
  counter, one atomic load of the cap, one comparison, one successful
  compare-and-swap on the in-flight counter, one timestamp read, and
  one atomic increment on Limen's own outcome-counter storage. Under
  contention, the compare-and-swap may fail and retry; under no
  contention it succeeds on the first try. No memory allocation in
  Limen's code. No locks. No synchronous OpenTelemetry calls.
- **Releasing a slot** at request completion costs one atomic
  decrement on the in-flight counter, one atomic increment on Limen's
  own outcome-counter storage (whichever of `success` /
  `dropped` / `ignored` applies — see the listener outcomes in the
  observability section), one timestamp subtraction, and a small
  number of relaxed-atomic updates to the active sample-window
  buffer's fields. No allocation. No locks. No synchronous
  OpenTelemetry calls. The sample-window pointer itself is not
  swapped on release — the swap only happens at the window boundary,
  on the one thread that wins the try-lock there.
- **The math** — the actual algorithm — runs at most once per windowing
  interval, which defaults to one second. It runs on whichever request
  thread happens to cross the window boundary, under a try-lock so only
  one thread does it. Other threads on the boundary skip the work. The
  window-boundary swap exchanges two pre-allocated `SampleWindow`
  buffers (see the `WindowedLimit` description in the component
  catalogue), so the boundary work itself is also allocation-free.
  There is no background timer thread; this matches Netflix's design
  in
  [`WindowedLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/WindowedLimit.java).

Putting these together: **no per-request path in Limen's code performs
a heap allocation, takes a lock, or makes a synchronous OpenTelemetry
SDK call.** Every byte of state Limen needs is either pre-allocated at
limiter construction or carried inline in objects (the RAII slot guard
for direct use, or the per-call gRPC `Interceptor` object for the
gRPC adapter) that exist for other reasons. Note that gRPC will
allocate its own per-RPC state — `ServerContext`, the call object,
request and response buffers — regardless of whether Limen is present.
That is gRPC's own bookkeeping, paid before Limen sees the request,
and outside what this paragraph claims.

The atomic operations on the in-flight counter, the cap, and the
outcome-counter storage use the
[relaxed memory ordering](https://en.cppreference.com/w/cpp/atomic/memory_order#Relaxed_ordering)
mode wherever they are not part of a synchronization handshake,
because those counters do not need to synchronize other memory around
them. This is more aggressive than the Java upstream can express, and
is one of the small performance wins of porting to C++. The
sample-window index swap uses release-acquire ordering because the
boundary thread's reset writes to the buffer it just flipped away
from must be visible to any subsequent reader of that buffer before
the buffer is flipped back to active. The test suite verifies all of
this is correct under ThreadSanitizer.

How all the observability signals — including the outcome counter —
are kept off the synchronous OpenTelemetry path is in the
[How the signals are emitted without slowing the hot path](#how-the-signals-are-emitted-without-slowing-the-hot-path)
section below.

## What gets observed

Every signal Netflix's library exposes is emitted by Limen as an
OpenTelemetry metric, with the prefix `limen.` so an operator can see all
of Limen's metrics together on a dashboard. The full list:

| Metric | Type | Labels | What it means |
|---|---|---|---|
| `limen.limit` | Observable gauge | `id` | Current concurrency cap. |
| `limen.inflight` | Observable up-down counter | `id`, optional `partition` | How many requests are currently in progress. When the limiter is partitioned, the `partition` label distinguishes per-partition in-flight counts. |
| `limen.min_rtt` | Histogram (recorded per window) | `id` | Samples of the long-term baseline round-trip time. The name is inherited from Netflix's `MetricIds.MIN_RTT_NAME`, which dates back to the original Vegas algorithm; for the Gradient2 algorithm the value carried here is the exponentially smoothed long-term average RTT, not a minimum. |
| `limen.min_window_rtt` | Histogram (recorded per window) | `id` | The short-window RTT measurement that the algorithm compares against the long-term average. The name follows `MetricIds.WINDOW_MIN_RTT_NAME` upstream. |
| `limen.queue_size` | Histogram (recorded per window) | `id` | Per-window value of the algorithm's queue-size constant (the additive-increase amount). The name follows `MetricIds.WINDOW_QUEUE_SIZE_NAME` upstream, which is literally `"queue_size"` (despite the constant's name). |
| `limen.limit.partition` | Observable gauge | `id`, `partition` | Per-partition cap (partitioned limiter only). The dotted name follows `MetricIds.PARTITION_LIMIT_NAME` upstream and matches OpenTelemetry's recommended attribute-style naming convention. |
| `limen.call` | Observable counter | `id`, `status` ∈ {`success`, `rejected`, `dropped`, `ignored`, `bypassed`} | The outcome of every admission decision. |

The metric names follow Netflix's `MetricIds` catalogue verbatim with
the `limen.` prefix added; nothing has been renamed or reordered. The
`id` label is the name the application gave the limiter when it was
built.

The status values come from a combination of upstream's listener
interface and its two non-listener completion paths. The
`Limiter.Listener` interface has three completion methods —
[`onSuccess`, `onIgnore`, `onDropped`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/Limiter.java)
— which the limiter invokes when an admitted request completes. A
fourth outcome, `rejected`, is signalled by `acquire` returning an
empty optional (no listener at all). The fifth, `bypassed`, is for
requests that opted out of admission control entirely (the bypass
predicate matched). Limen counts all five.

For latency histograms, Limen sets explicit bucket boundaries that work
well for typical RPC latency ranges: one microsecond, ten microseconds,
one hundred microseconds, one millisecond, ten milliseconds, one hundred
milliseconds, one second, ten seconds. Applications can override these
through the OpenTelemetry MeterProvider's view configuration.

The library does not log on every admission or rejection. Under heavy
load that would produce a log flood and make operating the server
harder, not easier. The optional `TracingLimitDecorator` emits one
diagnostic log line per sample it sees from the wrapped algorithm;
when the decorator is layered on top of `WindowedLimit` (the usual
configuration), the wrapped algorithm only sees one sample per window
boundary, so the log frequency is approximately one line per window —
typically once a second.

### How the signals are emitted without slowing the hot path

A naive implementation would record every observation by calling the
OpenTelemetry SDK directly from the admission code path. That looks
correct, but it would bake an unbounded cost into every request: the
OpenTelemetry C++ SDK is a real piece of infrastructure code with its
own attribute lookups, view-registry consultation, and aggregator
mutation, and the per-call cost of those is not visible to Limen and
not stable across SDK releases. We expect that cost to be small in
absolute terms, and we will validate that expectation by measurement
during the test phase; but for a load shedder the principled answer
is to not depend on the SDK's hot-path performance at all. If the SDK
is on the per-request path, any future SDK regression that inflates
the per-call cost becomes a regression in Limen's own measured
round-trip time, and the algorithm responds by shedding traffic to
compensate for its own observability code. This is the kind of
self-inflicted denial-of-service the design is supposed to prevent.

Upstream's Java library does call its metric registry synchronously
on every admit and every release (see
[`SimpleLimiter.acquire`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limiter/SimpleLimiter.java)
line 62 and
[`AbstractLimiter`'s listener completion methods](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limiter/AbstractLimiter.java)
lines 162–185). This is a deliberate divergence in Limen, traded
for a hot path with no synchronous telemetry calls.

Limen avoids this by harvesting the bulk of its telemetry through
OpenTelemetry's
[asynchronous instrument mechanism](https://opentelemetry.io/docs/specs/otel/metrics/api/#asynchronous-instrument-api).
Asynchronous instruments work the other way around from the usual
"caller pushes a value" model: the library registers a callback with
the OpenTelemetry meter; OpenTelemetry's collector thread invokes the
callback on its own collection interval (commonly every few seconds)
and pulls the current value at that moment. The hot path is not
involved.

Concretely:

- The **gauges and up-down counters** (`limen.limit`, `limen.inflight`,
  `limen.limit.partition`) are registered as observable instruments.
  The callback reads Limen's internal `std::atomic<int>` state with
  relaxed memory ordering — the same state the algorithm itself
  reads. The hot path never calls into OpenTelemetry for these.
- The **outcome counter** (`limen.call`) is also exposed as an
  observable instrument. Limen maintains its own `std::atomic<int64_t>`
  per `(id, status)` label combination — a small, statically-known
  set — and increments the relevant atomic with relaxed memory
  ordering on each admission decision. The OpenTelemetry collector's
  callback enumerates these atomics and returns the current
  cumulative value for each combination. The hot path makes no call
  into OpenTelemetry; it performs one atomic increment on Limen's
  own storage, which is the same cost we would pay for the same
  accounting whether or not OpenTelemetry was involved at all.
- The **histograms** (`limen.min_rtt`, `limen.min_window_rtt`,
  `limen.queue_size`) are recorded **once per window** — not
  once per request — because the `WindowedLimit` decorator aggregates
  samples within the window and only emits at the window boundary.
  With a default one-second window, histogram recording into the
  OpenTelemetry SDK happens at roughly one Hertz regardless of request
  volume. OpenTelemetry does not provide an observable-histogram
  instrument variant, so this remains a synchronous `Histogram::Record`
  call — but it is far off the per-request path.

The result: **the per-request path makes no synchronous OpenTelemetry
calls at all.** Every increment, every state update, happens on
Limen's own atomics; OpenTelemetry's collector thread reads them at
its own poll schedule. The only synchronous SDK call anywhere in the
library is the histogram record at the window boundary, once per
second, on a single request thread.

One trade-off worth noting. Observable counters cannot carry
[exemplars](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exemplars)
— there is no moment-of-measurement for OpenTelemetry to attach a
representative trace ID to. We accept this. For an admission counter
the click-through value of an exemplar is low: a representative
"rejected" event leads to a trace of a request that was rejected
before the protocol buffer was parsed, with no handler activity to
inspect. The operationally useful information about rejections is the
aggregate pattern — rate, distribution across partitions, correlation
with CPU and memory — and that is the same whether the counter is
synchronous or observable. If a future need ever does require
exemplars on this metric, switching that one instrument to a
synchronous Counter is a localized change.

A note on cross-signal consistency. Because each observable callback
fires at a slightly different moment within a collection cycle, two
gauges read in the same export batch can reflect state from a few
microseconds apart. For an admission controller's observability this
is acceptable: operators look at trends over seconds, not at
cycle-accurate snapshots. If a future requirement ever demanded
perfectly correlated multi-variable snapshots, the standard remedy
is a double-buffered snapshot: the hot path writes to one half of a
struct under a flag, a background thread atomically swaps the flag,
and the export reads the now-frozen other half. The current design
does not need this and does not implement it. The hook to add it
later is a single observable instrument that returns the snapshot
struct rather than the current atomics.

## How testing is organised

Testing is structured into four layers, each proving a different
property. Every test that exists in Netflix's upstream library against
a class Limen is porting has a counterpart in the layers below; an
explicit mapping table follows the four layers.

**Algorithm correctness.** Deterministic tests that drive the algorithm
and its helpers with hand-crafted round-trip-time sequences and assert
that the output matches the math. No threads, no clocks, no randomness.

For the Gradient2 algorithm itself, the cases are: steady state with
the cap growing by the queue-size constant per window; latency-spike
clamp-down; recovery case where the drift-correction branch shrinks
the long-term average; cold-start with no prior samples; the
"underutilised" rule (in-flight below half the cap; the algorithm
returns the current cap unchanged); upper and lower boundary clamps;
and the smoothing factor.

For the supporting types: `ExpAvgMeasurement` is tested for the
transition from its warmup window (simple running average over the
first ten samples) to steady state (exponential moving average); the
average `SampleWindow` aggregator is tested for correct mean
computation and correct propagation of the drop-occurred flag; the
percentile `SampleWindow` aggregator is tested for correct percentile
placement (specifically p50 and p999), correct drop-flag propagation,
and sample-order independence (an upstream property that exists only
on the percentile window); `AbstractPartitionedLimiter`'s
partition-quota arithmetic is tested for ceiling-based rounding (the
sum of partition quotas may exceed the global cap by one slot per
partition under integer-rounding); `SimpleLimiter`'s basic
admit-or-reject and release semantics are tested; the
`LifoBlockingLimiter` builder is tested for input validation (rejects
timeouts above the documented maximum, exposes the fixed timeout when
configured, returns null for the fixed-timeout accessor when a
dynamic context-derived timeout is configured); the `BlockingLimiter`
builder is tested for the same input-validation rejection of
timeouts above its maximum.

**Behaviour under traffic.** Mock-clock tests that drive the limiter
with controlled concurrent traffic patterns and verify the
algorithm's adaptation visibly happens. The key claim — that under
load the cap actually changes — is proved here by reading the
OpenTelemetry gauge values over the course of the simulation. This
layer also includes the bimodal-latency test, which proves the
partitioned limiter does its job: two synthetic request types with
different latency profiles share a limiter; when the slow type
spikes, the partitioned limiter clamps it specifically without
strangling the fast type. Without partitioning, the same test shows
the global cap dropping — which is the failure mode the partitioning
mechanism exists to prevent.

Additional invariants asserted under traffic:

- **Counter conservation.** Every admission is followed by exactly one
  release, so the in-flight count returns to zero between bursts.
- **No over-admission under contention.** A `SimpleLimiter` driven
  by many threads (matching upstream's hundred-threads-by-thousand-
  iterations stress) never exceeds its current cap at any observed
  moment.
- **Burst-into-idle.** A partitioned limiter lets one partition use
  more than its quota while another partition is below its quota,
  confined by the global cap.
- **Capacity lending and reclamation.** When global capacity is
  exhausted, partition quotas become hard; when global capacity
  frees, the lending behaviour resumes.
- **Bypass predicate.** Both simple and partitioned limiters respect
  a configured bypass predicate (the bypass case counts as
  `bypassed`, in-flight is not affected, the algorithm sees no
  sample).
- **Partition quota recomputation on global-cap change.** When the
  global cap moves (either through algorithm adaptation or a
  `SettableLimit` override), partition quotas are recomputed; the
  test exercises a `SettableLimit` driving the global cap up and
  then down while partitions are busy.
- **`BlockingLimiter` behaviour.** First-in-first-out ordering of
  waiters (a Limen divergence from upstream — see the limiter's
  description); release wakes a waiting thread; timeout fires
  correctly; limit reduction while listeners are held leaves the
  semaphore in a valid state. The test of the FIFO ordering
  property is new in Limen and has no upstream counterpart.
- **`LifoBlockingLimiter` behaviour.** Last-in-first-out ordering of
  waiters; limit increase wakes the most recent waiter; limit
  decrease causes excess waiters to be rejected when they reach the
  front; the backlog cap is enforced; a per-request timeout function
  (where the timeout depends on the request's context — for example,
  on a deadline propagated in the gRPC metadata) is honoured. There
  is also a regression test for an upstream-fixed race where a token
  delivered to a waiter just as the waiter is timing out used to be
  lost; the Limen port preserves the fix and the regression test.

**Real gRPC integration.** End-to-end tests that stand up a real gRPC
server, register Limen's interceptor on it, send real RPCs from a
real gRPC client, and assert the outcomes. The most important test
in this layer answers the question "does the interceptor actually
intercept?": the test registers a handler that blocks on a mutex
held by the test thread, fires one thousand concurrent RPCs at a
server whose cap is ten, and asserts that exactly ten RPCs reached
the handler (and are now blocked) and the other nine hundred ninety
were rejected — without the handler being entered. This is the
proof that admission happens at the right layer.

Adjacent tests in this layer cover:

- **Rejection latency.** On localhost, the rejection round-trip
  should be sub-millisecond.
- **Streaming bypass.** A non-unary RPC (server-streaming,
  client-streaming, and bidirectional, tested independently) is
  admitted through the interceptor without consulting the limiter.
- **Listener mapping on successful RPC.** When the handler returns
  normally with `Status::OK`, the listener's `OnSuccess` is called
  and the in-flight slot is released.
- **Listener mapping on handler-returned non-OK status.** When the
  handler returns a non-OK status (for example `INVALID_ARGUMENT`),
  the listener still sees `OnSuccess`, matching upstream's behaviour
  in
  [`ConcurrencyLimitServerInterceptor`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-grpc/src/main/java/com/netflix/concurrency/limits/grpc/server/ConcurrencyLimitServerInterceptor.java)
  (lines 162–169). The reasoning upstream is that a handler-returned
  error is information about the request, not about whether the
  server was able to handle it; treating it as a successful
  measurement keeps the algorithm's RTT samples honest.
- **Listener mapping on handler exception.** When the handler exits
  abnormally (the C++ analogue of an upstream uncaught exception:
  for example, a `[[noreturn]]` path or an `abort`-on-bug condition
  guarded by tests), the listener sees `OnIgnore` rather than
  `OnSuccess` or `OnDropped`. The reasoning is symmetric: we have
  no useful RTT signal in this case.
- **Cancellation.** When the client cancels mid-flight and the
  server's close handler is reached with `Status::CANCELLED`, the
  listener sees `OnDropped`.
- **Deadline-exceeded.** Two distinct sub-cases. If the server's
  close handler is reached with `Status::DEADLINE_EXCEEDED`, the
  listener sees `OnDropped`. If the client's deadline fires but the
  server runs the handler to completion and closes with `OK` (the
  upstream
  [test for this](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-grpc/src/test/java/com/netflix/concurrency/limits/grpc/server/ConcurrencyLimitServerInterceptorTest.java)
  exercises this path), the listener sees `OnSuccess`. Both
  behaviours are tested.
- **Bypass predicate.** A configured bypass predicate (by method, by
  header, or by an arbitrary closure on the request context) routes
  the request around the limiter; the listener is not invoked; the
  counter increments at `status=bypassed`.
- **Partition routing.** Different gRPC methods route to different
  limiter instances via the `LimiterRegistry`-equivalent mapping in
  the interceptor builder.
- **ThreadSanitizer cleanliness.** All of the above pass under TSan
  with concurrent admission and observability-callback reads.

**Observability end-to-end.** Tests that drive the in-memory
OpenTelemetry exporter, exercise known traffic patterns, and assert
the exported metric streams have the right names, the right labels,
and sensible values. This is the test that catches naming mistakes,
missing metric attributes, and broken observable-callback wiring.

In addition, every test runs under three sanitizers (AddressSanitizer,
ThreadSanitizer, UndefinedBehaviorSanitizer) on every commit. There
is no separate "we'll sanitize it later" gate. If a commit introduces
a race or a memory error, CI catches it before the commit is merged.

### Mapping to upstream tests

Every upstream Java test file that covers a class Limen is porting is
covered by tests in the layers above. The mapping:

| Upstream test file | What it asserts | Covered by |
|---|---|---|
| `ExpAvgMeasurementTest` | Warmup-to-steady-state transition of the moving average. | Algorithm correctness. |
| `ImmutableAverageSampleWindowTest` | Per-window mean RTT, drop-flag propagation. | Algorithm correctness. |
| `ImmutablePercentileSampleWindowTest` | Per-window p50 and p999, drop-flag propagation, sample-order independence. | Algorithm correctness. |
| `SimpleLimiterTest` | Admit-or-reject up to the cap, release returns slot, bypass-when-configured, no-bypass-by-default, contention with hundreds of threads. | Algorithm correctness (the deterministic methods) plus Behaviour under traffic (contention) plus the TSan sweep. |
| `AbstractPartitionedLimiterTest` | Quota arithmetic, burst-into-idle, partition lends to global, hard partition cap when global is exhausted, release, quota recompute on global-cap change, bypass-with-partitions, bypass-with-simple, concurrent partitioned access. | Algorithm correctness (quota arithmetic) plus Behaviour under traffic (the dynamic cases). |
| `BlockingLimiterTest` | Basic acquire and release, multi-thread contention, timeout, no-timeout, builder rejects timeouts above the documented maximum. | Behaviour under traffic. The FIFO-ordering test is additional and Limen-specific. |
| `LifoBlockingLimiterTest` | LIFO ordering of waiters, blocking with timeout, wake-up before timeout, backlog cap, adaptation on limit increase, adaptation on limit decrease, the timeout-acquire race regression, the builder's fixed-vs-dynamic-timeout exposure. | Behaviour under traffic. |
| `ConcurrencyLimitServerInterceptorTest` | Listener mapping on success, on non-OK status, on handler exception, on cancellation, on deadline-exceeded; bypass-predicate behaviour in all three configurations. | Real gRPC integration. |

Upstream tests not in the table are for classes deliberately out of
scope for v0.1.0 (VegasLimit, AIMDLimit, GradientLimit, the Vegas
helper functions, the blocking-adaptive-executor simulation, the
client-side interceptor); they are deferred along with their
classes.

## How it builds

[Bazel](https://bazel.build/) with `bzlmod`. Two C++ library targets:

- **`//:limen`** — the core algorithms, limiters, and observability
  wiring. No gRPC dependency.
- **`//:limen_grpc`** — the gRPC server interceptor adapter. Depends on
  `//:limen` and gRPC.

The C++ standard is [C++20](https://en.cppreference.com/w/cpp/20).
Compilation follows the
[Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html),
which most relevantly means: no exceptions (the `-fno-exceptions` flag is
set), no run-time type information (`-fno-rtti`), ownership is expressed
through `std::unique_ptr` and `std::shared_ptr` rather than raw owning
pointers, and naming follows the style guide's conventions.

The non-trivial dependencies, in full:

- [Abseil](https://abseil.io/) — Limen uses `absl::Mutex` and its
  associated thread-safety annotations for the locking primitives. The
  reason it is `absl::Mutex` and not the standard library's
  `std::shared_mutex` is that Abseil's mutex is documented as a
  [starvation-free reader-writer lock](https://github.com/abseil/abseil-cpp/blob/master/absl/synchronization/mutex.h),
  whereas the standard library's shared mutex on glibc is reader-preferring,
  which in practice can starve writers under sustained read load.
- [OpenTelemetry C++](https://opentelemetry.io/docs/languages/cpp/) —
  Limen uses the OpenTelemetry C++ API directly to emit its observability
  signals. Applications choose their own OpenTelemetry SDK and exporter;
  Limen does not lock them into one.
- gRPC C++ — only the `//:limen_grpc` target depends on this. The core
  library does not.

We do not depend on Folly, on Boost, or on any other large infrastructure
library. The library is intended to be inexpensive to adopt into an
existing C++ build.

Once the library has been integrated and stabilised in at least one
production consumer, the plan is to publish it to the
[Bazel Central Registry](https://registry.bazel.build/) so that other
projects can depend on it through a one-line `bazel_dep`.

## How the work is ordered into commits

The work lands as a sequence of small, individually reviewable commits.
Each commit ships with the tests that prove the code in that commit
works.

1. **Repository scaffolding.** License file, NOTICE file, README skeleton
   with prominent Netflix credit, code of conduct (Contributor Covenant),
   contributing guide, security policy, gitignore, Bazel module file,
   clang-format and clang-tidy configurations, top-level Bazel build
   file, GitHub Actions workflows for build, test, and coverage. No
   library code in this commit.
2. **Algorithm core.** The `Limit` interface, the `Gradient2Limit`
   implementation, the measurement helpers (exponential average, minimum,
   passthrough), and the sample windows (average, percentile). Unit tests
   for the algorithm math.
3. **Algorithm decorators.** `FixedLimit`, `SettableLimit`,
   `WindowedLimit`, and `TracingLimitDecorator`. Unit tests for each.
4. **Limiter base.** The shared machinery: the in-flight counter, the
   listener callback interface, the OpenTelemetry wiring. Unit tests for
   the atomic counter under multi-threaded contention.
5. **Simple limiter.** The first concrete limiter, with admit-or-reject
   semantics and the RAII slot guard that applications use. Unit tests.
6. **Partitioned limiter.** The mechanism for named-percentage-quota
   partitions, with burst-into-idle behaviour. Unit tests for quota
   enforcement, burst, and partition resolution.
7. **FIFO blocking limiter.** Unit tests for FIFO order, timeout, and
   fair queueing.
8. **LIFO blocking limiter.** Unit tests for LIFO order, deadline-aware
   timeout, and the backlog cap.
9. **gRPC adapter.** The server interceptor, the builder used to wire
   methods to limiters and partitions, and the request context.
   Interceptor-level unit tests with mock gRPC contexts.
10. **gRPC end-to-end tests.** The "does the interceptor actually
    intercept" test and the rest of the integration matrix.
11. **Traffic simulation tests.** The mock-clock concurrency tests that
    prove the algorithm adapts under load.
12. **Observability end-to-end tests.** The in-memory exporter tests
    that verify every metric stream is emitted correctly.
13. **README quickstart and minimal example.** A short C++ file that
    shows how to wire the interceptor onto a `ServerBuilder`.
14. **Tag `v0.1.0`.** The first usable release.

After v0.1.0 has been integrated by at least one production consumer
(RowKeyDB) and has run stably for a few weeks, we submit to the Bazel
Central Registry.

## Upcoming work, not in v0.1.0

These items are deliberately deferred. They are named here so that
nothing is lost.

- **gRPC asynchronous and callback APIs.** Limen v0.1.0 supports gRPC's
  synchronous server API only, which is what RowKeyDB and many other
  production C++ services use today. In the synchronous API a handler
  thread runs the request to completion, and that thread is allowed to
  block. The two blocking limiters in this library (`BlockingLimiter`,
  `LifoBlockingLimiter`) rely on that property. The
  [gRPC callback API](https://grpc.io/docs/languages/cpp/callback/),
  introduced in gRPC 1.30 and stable from 1.42, is cooperative — handlers
  cannot block, because blocking would deadlock the I/O reactor. A future
  Limen release will add callback-API-safe variants of the two blocking
  limiters that use a queue-and-notify pattern rather than a
  condition-variable wait. The non-blocking limiters (`SimpleLimiter`,
  `AbstractPartitionedLimiter`) work correctly under both APIs already.
  This is the most likely first major addition after v0.1.0.
- **Coroutine-friendly variants.** As the C++ ecosystem moves toward
  `co_await`-based async, the blocking limiters will eventually need
  awaitable interfaces. This follows the callback API work above.
- **Other algorithms.** Netflix's library has several other algorithm
  implementations:
  [`VegasLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/VegasLimit.java)
  (the older TCP-Vegas-derived algorithm),
  [`GradientLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/GradientLimit.java)
  (the predecessor of Gradient2), and
  [`AIMDLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/AIMDLimit.java)
  (the classic TCP-style additive-increase-multiplicative-decrease).
  Gradient2 is Netflix's current recommendation, so Limen ports it first.
  The others port cleanly into the same `Limit` interface and will be
  added as the need arises.
- **CoDel queue management.** CoDel (Controlled Delay) is a different
  algorithm that solves a related but distinct problem: it watches how
  long requests are sitting in queues, rather than how long they take
  to execute. Most production load-shedding deployments use adaptive
  concurrency control (what Limen does) in front of CoDel (which protects
  the queue behind the admission gate). CoDel will be a separate library,
  not part of Limen.
- **Bigger application toolkit.** Higher-level features that an
  application typically builds on top of a concurrency limiter — for
  example, per-tenant quotas, memory-pressure-aware shedding, or
  attaching a [`google.rpc.RetryInfo`](https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto)
  message to rejection responses — are deliberately not part of Limen.
  Limen exposes the building blocks and the application combines them.

## References

This document refers to the following live sources.

Netflix's `concurrency-limits` library, at the commit Limen was ported
from:

- [Repository root](https://github.com/Netflix/concurrency-limits/tree/78a74b9878d38c4c048b0304ce12a162ab7b7222)
- [`Limit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/Limit.java)
- [`Limiter.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/Limiter.java)
- [`Gradient2Limit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/Gradient2Limit.java)
- [`WindowedLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/WindowedLimit.java)
- [`FixedLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/FixedLimit.java)
- [`SettableLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/SettableLimit.java)
- [`TracingLimitDecorator.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/TracingLimitDecorator.java)
- [`AbstractLimiter.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limiter/AbstractLimiter.java)
- [`LimiterRegistry.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/LimiterRegistry.java)
- [`SimpleLimiter.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limiter/SimpleLimiter.java)
- [`AbstractPartitionedLimiter.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limiter/AbstractPartitionedLimiter.java)
- [`BlockingLimiter.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limiter/BlockingLimiter.java)
- [`LifoBlockingLimiter.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limiter/LifoBlockingLimiter.java)
- [`ConcurrencyLimitServerInterceptor.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-grpc/src/main/java/com/netflix/concurrency/limits/grpc/server/ConcurrencyLimitServerInterceptor.java)
- [`VegasLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/VegasLimit.java)
- [`GradientLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/GradientLimit.java)
- [`AIMDLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/AIMDLimit.java)

Other live references:

- [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0)
- [Contributor Covenant v2.1](https://www.contributor-covenant.org/version/2/1/code_of_conduct/)
- [TCP Vegas (Wikipedia)](https://en.wikipedia.org/wiki/TCP_Vegas)
- [Envoy's adaptive concurrency filter](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter)
- [gRPC C++ server interceptor header](https://github.com/grpc/grpc/blob/master/include/grpcpp/support/server_interceptor.h)
- [gRPC C++ server interceptor API reference](https://grpc.github.io/grpc/cpp/classgrpc_1_1experimental_1_1_interceptor.html)
- [gRPC C++ callback API](https://grpc.io/docs/languages/cpp/callback/)
- [OpenTelemetry C++](https://opentelemetry.io/docs/languages/cpp/)
- [Abseil](https://abseil.io/)
- [`absl/synchronization/mutex.h`](https://github.com/abseil/abseil-cpp/blob/master/absl/synchronization/mutex.h)
- [Bazel](https://bazel.build/) and the
  [Bazel Central Registry](https://registry.bazel.build/)
- [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)
- [C++20 (cppreference)](https://en.cppreference.com/w/cpp/20)
- [Relaxed memory ordering (cppreference)](https://en.cppreference.com/w/cpp/atomic/memory_order#Relaxed_ordering)
- [RAII (cppreference)](https://en.cppreference.com/w/cpp/language/raii)
- [`google.rpc.RetryInfo`](https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto)

# Limen — Architecture and Design

## What this library is, and where it came from

Limen is a C++20 port of Netflix's
[`concurrency-limits`](https://github.com/Netflix/concurrency-limits/tree/78a74b9878d38c4c048b0304ce12a162ab7b7222)
Java library. Netflix's library introduced a set of production-tested
algorithms that let a server automatically decide how many requests
it can safely handle at the same time, and reject the rest before
they cause harm. This is called adaptive concurrency limiting.

Netflix released the original work in 2018 under the
[Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0). Limen is
also Apache 2.0. Limen's copyright holder is RowKeyDB, the
organisation that hosts and maintains the project. Every file ported
from the upstream library keeps Netflix's 2018 copyright notice
alongside the
`Copyright 2026 RowKeyDB` notice, the repository carries a `NOTICE` file
crediting the upstream source, and the README opens with the same credit
prominently. We are porting their work, not claiming to have invented it.

Today there is no high-quality, standalone C++ library that does what
Netflix's Java library does and that an application can adopt without
first refactoring its build around someone else's framework. The two
existing C++ implementations of these algorithms — one inside
[Envoy](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter)
and one inside Apache brpc — are deeply tangled with the frameworks that
host them, and cannot easily be extracted. Limen fills that gap.

Limen also ships a Controlled Delay (CoDel) admission filter that
implements [RFC 8289](https://www.rfc-editor.org/rfc/rfc8289) in full.
CoDel watches how long items sit in a queue the application owns and
starts dropping items when the minimum sojourn time exceeds a target
for at least a sliding interval. The filter is transport-agnostic in
the same sense the rest of Limen is: it does not know how items got
into the queue, only how long they have been there.

## Why a server needs adaptive concurrency control

The most common way to protect a server from overload is to pick a number —
"this server can handle two hundred requests at the same time" — and reject
everything above that number. The number is usually a guess. The number is
often wrong. Server capacity is not a constant: it shifts with hardware
changes, with the mix of work the server is doing, with caching, with garbage
collection in managed languages, with cache misses in unmanaged ones, and
with time of day. A static number cannot track all of that and
ends up either lower than necessary on good days (wasted capacity)
or higher than safe on bad days (outages).

Adaptive concurrency control replaces the guess with a measurement loop. The
server continuously watches how long its work is taking. When work is taking
roughly as long as it usually does, the server slowly increases how many
concurrent requests it accepts. When work starts taking noticeably longer
than usual — a sign that something is overloaded — the server shrinks its
accepted concurrency until latency comes back to normal. Requests that exceed
the current concurrency cap are rejected immediately, with a status that
tells the client to back off and try later.

This approach comes from TCP congestion control, particularly
[TCP Vegas](https://en.wikipedia.org/wiki/TCP_Vegas), an algorithm
from the mid-1990s that uses latency rather than packet loss as its
congestion signal. Netflix adapted Vegas for RPC services and
improved on it in later iterations. Their current recommended
algorithm, Gradient2, is what Limen ports first.

## What Limen does, in one sentence

Limen sits in front of a request handler and cheaply answers the
question "should this request be admitted right now?". The answer
adapts automatically to the server's measured latency. Where a
queue sits between admission and the handler, a companion
`CodelFilter` answers the related question "has this queued item
already sat too long to be worth running?".

## What is in scope for the first release

Limen v0.1.0 ports the parts of Netflix's library that a production
C++ server needs, plus CoDel for queue management:

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
- A `CodelFilter` that implements the full RFC 8289 CoDel algorithm,
  including the recent-episode hysteresis from Linux's implementation
  notes.
- Native [OpenTelemetry](https://opentelemetry.io/docs/languages/cpp/)
  metrics for every internal signal Limen tracks, kept off the
  per-request hot path. The full catalogue lives in the repository's
  [`Observability.md`](../Observability.md).
- A test suite organised in three layers: deterministic algorithm
  tests, mock-clock traffic simulations, and observability tests that
  drive the in-memory OpenTelemetry exporter.

What is explicitly out of scope for v0.1.0 is named in the "Upcoming work"
section near the end of this document, so it is not lost.

## How the Gradient2 algorithm works, in plain English

Every time the server handles a request, it learns one number: how long
that request took to handle. The algorithm collects these numbers, in time
windows of about one second each. At the end of each window, it asks two
questions: how long is the request taking now, and how long has the
request usually taken over a much longer period? It compares the two.

If the current window is roughly as fast as the long-term average, the
server is not overloaded, and the algorithm grows the concurrency cap by a
small amount — the same way TCP slowly grows the size of its window
when packets are flowing fine. This is the additive-increase half of
TCP-style congestion control.

If the current window is noticeably slower than the long-term average,
the server is in trouble. The algorithm shrinks the concurrency cap by a
factor proportional to how much slower things have become. The
factor is clamped: no single window can shrink the cap by more than
half (no matter how bad latency gets), and small latency rises are
forgiven by a tolerance factor (by default, latency has to rise
roughly 50 percent above the long-term baseline before any
shrinking begins). Between those clamps, the cap shrinks in
proportion to the latency increase.

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
The math is small, roughly thirty lines of arithmetic. Limen ports it
faithfully.

The algorithm exposes a handful of knobs through
`Gradient2Limit::Builder`. Defaults match upstream except for the
additive-increase term (`QueueSize`), which is the one place Limen
deliberately diverges.

| Builder method | Default | What it controls |
|---|---|---|
| `InitialLimit` | 20 | Starting cap before any samples have arrived. |
| `MinLimit` | 20 | Floor on the cap. The algorithm will never drive it lower. |
| `MaxConcurrency` | 200 | Ceiling on the cap. The algorithm will never drive it higher. |
| `QueueSize` | `ceil(sqrt(current_limit))` (floored at 1) | Additive-increase amount per window. Upstream Netflix's Java library defaults to a constant `4`. |
| `RttTolerance` | 1.5 | How much higher the short-term RTT must climb above the long-term average before the algorithm starts shrinking. A value of 1.5 means a 50 percent latency increase is forgiven. |
| `Smoothing` | 0.2 | Weight given to the newly computed cap when blending against the old cap. Higher values speed adaptation, lower values damp it. |
| `LongWindow` | 600 | Sample count over which the long-term RTT is exponentially smoothed. At the default `WindowedLimit` window of one second this is a ten-minute moving baseline. |

The `QueueSize` divergence is worth a paragraph. Upstream's
constant default of 4 means the algorithm's recovery walk after a
clamp-down is the same slow probe regardless of cap. That probe is
about 20% of cap when the cap is 20, but only 0.4% of cap when the
cap is 1000, so a server with a high cap walks back up unacceptably
slowly. Recovery time matters
most operationally: a server that has just been throttled is the
one we most want to walk its cap back up promptly. Limen's default of
`ceil(sqrt(current_limit))` keeps the probe rate sublinear (at
cap=100 the algorithm probes by 10 per window, about 10%, and at
cap=10000 by 100 per window, about 1%) while scaling naturally with
cap. Constant-form behaviour remains available through
`Gradient2Limit::Builder().QueueSize(4)` for applications that want
strict upstream behaviour.

The `WindowedLimit::Builder` adds its own knobs. `MinWindowTimeNs`
and `MaxWindowTimeNs` (defaults: both one second, in nanoseconds, so
by default the window is a flat one-second interval). If the
operator sets the two to different values, the window resizes
adaptively between those bounds based on observed RTT. The default
leaves the window pinned at one second. `WindowSize` (default: 10,
the minimum number of samples required before the algorithm runs
against a window). `MinRttThresholdNs` (default: 100 microseconds in
nanoseconds, samples below this are discarded as noise). When the
application installs a percentile sample window, the
percentile-window's pre-allocated RTT array is sized through
`PercentileSampleWindow`'s constructor (`max_samples_per_window`, no
default). The application's `SampleWindowFactory` supplies the
value.

## How CoDel works, in plain English

CoDel (Controlled Delay) is a different mechanism from Gradient2.
Where the adaptive limiter watches concurrent requests (how many
are in flight at once), CoDel watches queue residency (how long an
item has been sitting waiting). The two are complementary. A
server can have plenty of concurrent capacity available and still
be in trouble if its work queue is so deep that items time out
before the handler ever reaches them.

The algorithm is small. The original Nichols-Jacobson paper from
2012 is six pages.
[RFC 8289](https://www.rfc-editor.org/rfc/rfc8289) from 2018
specifies the state machine in about a hundred lines of pseudocode.

At enqueue time, the application stamps a wall-clock time on each
item. At the place in the code where the application would
normally pull an item off the queue and run it, it asks
`CodelFilter::ShouldDrop(enqueue_time)`. The filter computes the
sojourn (now minus enqueue) and runs a state machine that tracks:

- The earliest moment the minimum sojourn first rose above a
  configured **target** (default 5 ms).
- Whether the algorithm is currently in drop mode.
- A drops-in-burst counter and the time of the next scheduled drop.

If sojourn has stayed above target for at least the configured
**interval** (default 100 ms), the algorithm enters drop mode and
schedules each next drop `interval / sqrt(count)` after the
previous one — the inverse-square-root law from the RFC. As `count`
grows the inter-drop interval shrinks, so sustained overload yields
a progressively higher drop rate. Transient bursts that resolve
within the interval are not dropped at all.

The filter does not own a queue, does not enforce a queue-length
cap, and does not know how many items are pending. Those are the
application's responsibility. The filter only answers, given the
sojourn of one item, whether to drop it.

## The pieces of the library

There are three groups of types: algorithms, limiters, and the
standalone CoDel filter.

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
  every single sample, which Java accepts as the price of admission.
  It would not survive review in a hot-path C++ design. Limen instead
  uses a fixed pair of pre-allocated, mutable `SampleWindow` slots and
  an atomic active-buffer index. The trade-off this introduces is
  explained below.

  Each `SampleWindow` slot holds its aggregate fields — sample count,
  RTT sum, RTT minimum, maximum-inflight, drop-occurred flag — as
  independent atomic variables. A request thread recording a sample
  loads the active-buffer index (acquire memory ordering) and then
  applies a small number of independent relaxed-atomic updates to the
  buffer's fields: a `fetch_add` on the count and the sum, a
  compare-and-swap loop on the minimum and the maximum, and a store
  to the drop-occurred flag when the sample reports a drop. The
  drop-occurred flag is a sticky OR-aggregate: once set within a
  window it stays set until the boundary thread resets it. There is
  no lock and no allocation.

  At the window boundary, the thread that won the try-lock first
  reads the active buffer's aggregate fields, then atomically flips
  the active-buffer index from one slot to the other (new samples
  land in the freshly-active buffer from that point on), runs the
  algorithm's math against the aggregate it read, and finally resets
  the now-inactive buffer's atomic fields back to their initial
  values before the next boundary will make it active again. Reading
  the aggregate before the flip gives a late writer that captured
  the pre-flip index a chance to land its update before the slot is
  reset.

  There is one race the design accepts deliberately. A request thread
  that captures the active-buffer index just before the boundary swap
  will land its field updates in what is, by the time the updates
  complete, the now-inactive buffer. Two outcomes are possible. If
  the updates land before the boundary thread reads the buffer, the
  sample is included in the aggregate the algorithm sees, and the
  writer is attributed to the window that just closed. If the
  updates land after the boundary thread has reset the buffer, the
  sample is attributed to whichever future window picks up that
  buffer when it next becomes active. A narrow third interleaving
  exists where the boundary thread reads the field, the writer's
  update lands, and the boundary thread then resets the field. In
  that one case the writer's sample is overwritten by the reset and
  lost. The width of this interleaving is on the
  order of a few microseconds, and the proportion of samples that
  hit it at realistic request-rate-to-window-size ratios is small
  enough that the algorithm's aggregates are not measurably
  affected.

  Two `SampleWindow` slots are pre-allocated at the time the
  `WindowedLimit` is constructed. The active-buffer index only ever
  takes the values 0 and 1, and no allocation occurs on any path
  after construction.

  The description above covers the *average* sample window, which
  needs only a few aggregate fields per slot. The *percentile* sample
  window (`ImmutablePercentileSampleWindow` upstream) is a different
  shape: it stores every individual RTT in the window in an array,
  because computing a percentile at boundary time requires sorting
  the observed values. Alongside the array, the percentile slot
  carries the same three auxiliary atomic fields the average slot
  carries — min-RTT (the minimum observed RTT over the window),
  max-inflight (the running maximum of the in-flight count reported
  with each sample), and the drop-occurred flag — and updates
  them on each sample with the same patterns the average slot uses (a
  compare-and-swap loop on the min-RTT and max-inflight fields, and a
  sticky OR-aggregate store on the drop-occurred flag when the sample
  reports a drop). The boundary thread reads all of them, hands them
  to the algorithm, and resets them when it resets `sample_count_`.

  The RTT array itself is the new piece. Each percentile slot
  pre-allocates the array at construction —
  `std::unique_ptr<std::atomic<int64_t>[]>` sized to the
  `max_samples_per_window` value passed to
  `PercentileSampleWindow`'s constructor — alongside an
  `std::atomic<int> sample_count_`. A request thread recording a
  sample does a `fetch_add` on `sample_count_` to claim an index,
  and (if the returned index is within bounds) writes the RTT into
  the array at that index with a relaxed store. If the array fills
  (the returned index is at or beyond `max_samples_`), the sample
  is dropped from the array. The boundary thread sees only the
  first `max_samples_` samples and computes the percentile from
  those. The `sample_count_` value still increments past the bound
  and is exposed to the algorithm as the true count. The array
  bound only affects which samples can participate in the
  percentile calculation. `max_samples_per_window` is a constructor
  argument on `PercentileSampleWindow`. An application supplies it
  through the `SampleWindowFactory` installed on
  `WindowedLimit::Builder`.

  At the boundary, the percentile-slot's `sample_count_` is read,
  the array is read up to `min(sample_count_, max_samples_)`, the
  values are sorted, and the requested percentile is selected. The
  slot is then reset by storing zero into `sample_count_` and
  resetting the auxiliary fields to their initial values. The
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
  proportion of samples slip across the boundary. For percentile
  measurement, where many samples per window are aggregated, this
  introduces no operationally meaningful difference.
- **`TracingLimitDecorator`** — logs a one-line diagnostic for each
  sample the wrapped algorithm sees, including the in-flight count
  and the round-trip-time values. The log frequency therefore matches
  whatever frequency the wrapped algorithm is being driven at. When
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
  When the global cap has unused capacity, any partition may exceed
  its quota. Only when the global cap is exhausted are partitions
  enforced.
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
  because removing a configurable feature is a divergence. When the
  reject-delay is set, the rejection path is no longer non-blocking.
  It is the one place where rejection can sleep. A second knob
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
The two blocking limiters are appropriate for batch clients that can
tolerate a small wait. They are not appropriate for callers running
on a fixed event loop or in a cooperative event-driven model.
Blocking a request thread of those will starve the rest of the work
on that thread.

### The CoDel filter

The `CodelFilter` lives separately from the limiter machinery. It
implements RFC 8289 in full — both the original specification (the
`OkToDrop` predicate based on the minimum sojourn over a sliding
interval) and the recent-episode hysteresis from the Linux
implementation (the `delta` adjustment in §5.5). It takes no
dependency on `Limit`, `Limiter`, or anything else in the algorithm
hierarchy.

The hot path is one method, `ShouldDrop(absl::Time enqueue_time)`. The
filter takes a brief `absl::Mutex` to mutate its five state variables,
runs the state-machine transition, and increments an atomic drop
counter if it decided to drop. There is no allocation, no synchronous
OpenTelemetry call, no blocking I/O. The mutex is held only for the
brief state-machine transition, so contention from concurrent worker
threads is bounded, and the critical section is a handful of
arithmetic operations.

The filter exposes its current state through diagnostic accessors —
`IsDropping`, `DropCount`, `DropEpisodeCount`, `DropNext` — and
through two OpenTelemetry observable instruments
(`limen.codel.drops`, `limen.codel.dropping`). The same
asynchronous-collection pattern applies as for the limiter signals.
The SDK reads the state at its own poll schedule, and the hot path
is not involved.

CoDel knobs:

| Knob | Default | What it controls |
|---|---|---|
| `Target` | 5 ms | Sojourn threshold. The filter enters drop mode only when the minimum sojourn over `Interval` exceeds this value. |
| `Interval` | 100 ms | Sliding window. The filter waits at least this long after sojourn first rises above target before dropping anything. |
| `Clock` | `&absl::Now` | Time source. Tests inject a mock clock. |
| `Id` | (empty) | Label value emitted on every metric. |
| `MeterProvider` | (none) | OpenTelemetry SDK MeterProvider for the two observable instruments. |

## Wiring Limen onto your application-level RPC library

Limen is in-process and transport-agnostic. To use it with any
application-level RPC library, the application needs two integration
hooks:

1. **A pre-body interception hook.** Somewhere in the library's
   request-handling path, before the request body has been parsed and
   before the handler thread has started running, the library must
   give the application a chance to reject the request. The rejection
   must be cheap — no protocol-buffer parsing, no handler dispatch,
   no thread allocation. Limen's `TryAcquire` runs here.

2. **A per-call context propagation channel.** The library must let
   the application stash a small piece of state (an `absl::Any`, a
   typed slot in a per-call context, a void-pointer table) at the
   interception hook and read it back when the handler completes (and,
   if a CoDel filter is in use, at the dequeue point). Limen stores
   its `SlotGuard` and the CoDel enqueue timestamp here.

Most production C++ RPC libraries provide both. gRPC has the
`grpc::AuthMetadataProcessor` (pre-body) and the `ServerContext`'s
per-call state for context propagation, plus a `POST_RECV_CLOSE`
interceptor that fires on completion. Thrift exposes pre-handler
filters and per-call context maps. Cap'n Proto has analogues in its
server infrastructure.

A complete copy-pasteable example using a hypothetical `MyRpcLib`
lives in the [README quickstart](../README.md#quickstart). The
integration semantics the example relies on:

- **The pre-body hook must run before any per-request work the
  application would not also do for a rejected request.** If the
  library parses the request body before the hook fires, the
  parsing cost is paid for rejected requests — exactly what Limen
  avoids. The cost of rejection includes whatever the library does
  before the hook.
- **The per-call context must outlive the handler.** Limen stashes
  a `SlotGuard` and an `absl::Time` at the pre-body hook and reads
  them back at the dequeue point and at handler completion. If the
  context is destroyed before completion, the `SlotGuard`'s
  destructor runs and reports `OnIgnore`. The in-flight slot is
  released, but Gradient2 sees no sample for that request.
- **The completion hook must fire exactly once per admitted
  request.** Multiple calls to `OnSuccess` / `OnDropped` /
  `OnIgnore` on the same `SlotGuard` are idempotent (the second
  call is a no-op). The architecture still relies on the library
  not racing two unrelated codepaths against each other on the
  same per-call object.

For an application-level RPC library that does not already expose a
pre-body hook, the integration cost is the cost of finding the
earliest stable point where the application can run code before the
body is parsed. In gRPC this is `AuthMetadataProcessor` plus a
`POST_RECV_CLOSE` interceptor for completion. In libraries that let
the application wire arbitrary filters, the first filter in the
chain is the place. Total adapter code typically lands on the order
of one hundred lines.

## How much it costs to use Limen

The admission decision is on the critical path of every request, so
it must be cheap, particularly when the answer is "reject", because
that is the case where the server is under attack and CPU cycles are
most scarce. If saying "no" to a request costs nearly as much as
handling it, the server denial-of-service-es itself before the
limiter has done any good. This concern shapes the implementation
throughout.

`SimpleLimiter`'s gate is a compare-and-swap loop on the in-flight
atomic counter, matching the gating semantics of Java's
`Semaphore.tryAcquire` that upstream relies on. A request thread
arriving at the limiter loads the current in-flight value and the
current cap. If the in-flight value is below the cap, the thread
attempts a compare-and-swap that bumps the counter by one only if
it has not changed since the load. If the compare-and-swap succeeds,
the request is admitted. If it fails because another thread changed
the counter first, the thread loops with the new value and tries
again. If at any iteration the loaded in-flight value is at or above
the cap, the loop exits and the request is rejected. Under
`SimpleLimiter` the global counter never transiently exceeds the
cap: an increment only happens through a successful compare-and-swap,
and the precondition `current < cap` is part of that swap's success
condition.

`AbstractPartitionedLimiter`'s gate is different. When the global
in-flight count has headroom (`inflight < cap`), the limiter admits
unconditionally with a `fetch_add` on the global counter and a
`fetch_add` on the partition's busy counter — no CAS, no retry. When
the global cap is saturated, the limiter falls back to a CAS loop on
the partition's busy counter against the partition's quota, and
rejects when that partition is at its share. Because the headroom
branch uses a relaxed `fetch_add` on the global counter, the global
counter can transiently exceed the cap under concurrent burst-into-
idle admissions. The design accepts that trade-off: strict global
enforcement would defeat the burst-into-idle behaviour, and strict
global enforcement is what `SimpleLimiter` is for.

The outcome counter (`limen.call`) — separately, Limen-owned, one
atomic slot per `(id, status)` label combination — is then
incremented to record the outcome of the decision. This is one
additional atomic operation, on a different storage location from the
in-flight gate, and it is the same cost whether the outcome is admit
or reject.

Concretely:

- **Rejecting a request** in the non-blocking limiters
  (`SimpleLimiter` and `AbstractPartitionedLimiter` with the default
  reject-delay of zero) costs at minimum one atomic load of the
  in-flight counter, one atomic load of the cap, one comparison that
  drops out of the gate, and one atomic increment on Limen's own
  outcome-counter storage. The partitioned limiter under a saturated
  global cap also pays a partition CAS gate against the partition's
  quota; under contention the CAS may fail and retry, but each
  iteration is a small constant. No memory allocation. No locks. No
  synchronous OpenTelemetry call. No string formatting. The rejected
  request never reaches the protocol-message parser of whichever RPC
  library hosts Limen, provided the application wires Limen onto the
  library's pre-body interception hook.
- **Admitting a request** costs one atomic load of the in-flight
  counter, one atomic load of the cap, one comparison, one atomic
  increment on the in-flight counter (a compare-and-swap on
  `SimpleLimiter`'s global gate; a relaxed `fetch_add` on the global
  counter plus a `fetch_add` on the partition's busy counter for
  `AbstractPartitionedLimiter`'s headroom path), one timestamp read,
  and one atomic increment on Limen's own outcome-counter storage.
  No memory allocation in Limen's code. No locks. No synchronous
  OpenTelemetry call.
- **Releasing a slot** at request completion costs one atomic
  decrement on the in-flight counter, one atomic increment on Limen's
  own outcome-counter storage (whichever of `success` /
  `dropped` / `ignored` applies), one timestamp read and one
  subtraction to compute the elapsed RTT, and a small number of
  relaxed-atomic updates to the active sample-window buffer's fields. No allocation. No locks. No synchronous
  OpenTelemetry call. The active-buffer index itself is not swapped
  on release. The swap only happens at the window boundary, on the
  one thread that wins the try-lock there.
- **CoDel's `ShouldDrop`** acquires a short `absl::Mutex`, reads the
  clock once, computes one subtraction, runs three to four
  comparisons against the state-machine variables, and (if dropping)
  performs one relaxed atomic increment on the drop counter. No
  allocation. No synchronous OpenTelemetry call. The mutex is held
  only for the brief state-machine transition, so contention from
  concurrent worker threads is bounded.
- **The Gradient2 math** — the actual algorithm — runs at most once
  per windowing interval, which defaults to one second. It runs on
  whichever request thread happens to cross the window boundary,
  under a try-lock so only one thread does it. Other threads on the
  boundary skip the work. The window-boundary swap is a one-bit
  toggle on `active_index_` between two pre-allocated `SampleWindow`
  buffers, so the boundary work itself is also allocation-free.
  There is no background timer thread. This matches Netflix's design in
  [`WindowedLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/WindowedLimit.java).

Putting these together: **no per-request path in Limen's code performs
a heap allocation, takes a blocking call, or makes a synchronous
OpenTelemetry SDK call.** Every byte of state Limen needs is either
pre-allocated at limiter construction or carried inline in objects
(the RAII `SlotGuard`) that the application owns through its existing
per-call bookkeeping. The two blocking limiters (`BlockingLimiter`,
`LifoBlockingLimiter`) are the deliberate exception, by design — they
block by definition — and the partitioned limiter sleeps on rejection
only when an operator explicitly configures a non-zero per-partition
`reject_delay`. The non-blocking, non-allocating guarantee covers
every other code path.

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

Every signal Limen tracks is emitted as an OpenTelemetry metric, with
the prefix `limen.` so an operator can see all of Limen's metrics
together on a dashboard. The full catalogue with semantics, units,
and label sets lives in [`Observability.md`](../Observability.md).
This section summarises it and explains the design choices behind
the asynchronous-collection model.

The signals fall into two groups:

- **Adaptive concurrency limiter (ACL) signals** — `limen.limit`,
  `limen.inflight`, `limen.call`, `limen.limit.partition`,
  `limen.min_rtt`, `limen.min_window_rtt`, `limen.queue_size`.
  Emitted by the limiter and the wrapped Gradient2 algorithm.
- **CoDel signals** — `limen.codel.drops`, `limen.codel.dropping`.
  Emitted by `CodelFilter`.

All metric names follow Netflix's `MetricIds` catalogue verbatim
where there is an upstream analogue, with the `limen.` prefix
added. Nothing has been renamed or reordered. The `id` label is the
name the application gave the relevant limiter or filter when it
was built. The `status` label on `limen.call` covers a five-valued
enumeration (`success`, `rejected`, `dropped`, `ignored`,
`bypassed`). See `Observability.md` for the meaning of each.

For latency histograms (`limen.min_rtt`, `limen.min_window_rtt`),
Limen sets explicit bucket boundaries that work well for typical
RPC latency ranges: one microsecond, ten microseconds, one hundred
microseconds, one millisecond, ten milliseconds, one hundred
milliseconds, one second, ten seconds. Applications can override
these through the OpenTelemetry MeterProvider's view configuration.

The library does not log on every admission or rejection. Under
heavy load that would produce a log flood and make operating the
server harder, not easier. The optional `TracingLimitDecorator`
emits one diagnostic log line per sample it sees from the wrapped
algorithm. When the decorator is layered on top of `WindowedLimit`
(the usual configuration), the wrapped algorithm only sees one
sample per window boundary, so the log frequency is approximately
one line per window — typically once a second.

### How the signals are emitted without slowing the hot path

A naive implementation would record every observation by calling
the OpenTelemetry SDK directly from the admission code path. That
looks correct, but it would bake an unbounded cost into every
request: the OpenTelemetry C++ SDK is a real piece of infrastructure
code with its own attribute lookups, view-registry consultation, and
aggregator mutation, and the per-call cost of those is not visible to
Limen and not stable across SDK releases. We expect that cost to be
small in absolute terms, and we will validate that expectation by
measurement during the test phase. For a load shedder, the
principled answer is to not depend on the SDK's hot-path performance
at all. If the SDK is on the per-request path, any future SDK
regression that inflates the per-call cost becomes a regression in
Limen's own measured round-trip time, and the algorithm responds by
shedding traffic to compensate for its own observability code. This
is the kind of self-inflicted denial-of-service the design is
supposed to prevent.

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
"caller pushes a value" model: the library registers a callback
with the OpenTelemetry meter. OpenTelemetry's collector thread
invokes the callback on its own collection interval (commonly every
few seconds) and pulls the current value at that moment. The hot path
is not involved.

Concretely:

- The **gauges and up-down counters** (`limen.limit`,
  `limen.inflight`, `limen.limit.partition`, `limen.codel.dropping`)
  are registered as observable instruments. The callback reads
  Limen's internal `std::atomic<int>` state with relaxed memory
  ordering — the same state the algorithm itself reads. The hot
  path never calls into OpenTelemetry for these.
- The **outcome counters** (`limen.call`, `limen.codel.drops`) are
  also exposed as observable instruments. Limen maintains its own
  `std::atomic<int64_t>` per `(id, status)` label combination — a
  small, statically-known set — and increments the relevant atomic
  with relaxed memory ordering on each admission decision. The
  OpenTelemetry collector's callback enumerates these atomics and
  returns the current cumulative value for each combination. The
  hot path makes no call into OpenTelemetry. It performs one
  atomic increment on Limen's own storage, which is the same cost
  we would pay for the same accounting whether or not
  OpenTelemetry was involved at all.
- The **histograms** (`limen.min_rtt`, `limen.min_window_rtt`,
  `limen.queue_size`) are recorded **once per window** — not
  once per request — because the `WindowedLimit` decorator
  aggregates samples within the window and only emits at the
  window boundary. With a default one-second window, histogram
  recording into the OpenTelemetry SDK happens at roughly one
  Hertz regardless of request volume. OpenTelemetry does not
  provide an observable-histogram instrument variant, so this
  remains a synchronous `Histogram::Record` call. The call still
  runs far off the per-request path.

The result: **the per-request path makes no synchronous
OpenTelemetry calls at all.** Every increment, every state update,
happens on Limen's own atomics. OpenTelemetry's collector thread
reads them at its own poll schedule. The only synchronous SDK call
anywhere in the library is the histogram record at the window
boundary, once per second, on a single request thread.

Observable counters cannot carry
[exemplars](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exemplars).
There is no moment-of-measurement for OpenTelemetry to attach a
representative trace ID to. We accept this. For an admission counter
the click-through value of an exemplar is low: a representative
"rejected" event leads to a trace of a request that was rejected
before the protocol message was parsed, with no handler activity to
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

Testing is structured into three layers, each proving a different
property. Every test that exists in Netflix's upstream library against
a class Limen is porting has a counterpart in the layers below. An
explicit mapping table follows the three layers.

**Algorithm correctness.** Deterministic tests that drive the algorithm
and its helpers with hand-crafted round-trip-time sequences and assert
that the output matches the math. No threads, no clocks, no randomness.

For the Gradient2 algorithm itself, the cases are: steady state
with the cap growing by the queue-size term per window,
latency-spike clamp-down, recovery case where the drift-correction
branch shrinks the long-term average, cold-start with no prior
samples, the "underutilised" rule (in-flight below half the cap, so
the algorithm returns the current cap unchanged), upper and lower
boundary clamps, and the smoothing factor.

For the supporting types:

- `ExpAvgMeasurement` is tested for the transition from its warmup
  window (simple running average over the first ten samples) to
  steady state (exponential moving average).
- The average `SampleWindow` aggregator is tested for correct mean
  computation and correct propagation of the drop-occurred flag.
- The percentile `SampleWindow` aggregator is tested for correct
  percentile placement (specifically p50 and p999), correct
  drop-flag propagation, and sample-order independence (an upstream
  property that exists only on the percentile window).
- `AbstractPartitionedLimiter`'s partition-quota arithmetic is
  tested for ceiling-based rounding (the sum of partition quotas
  may exceed the global cap by one slot per partition under
  integer-rounding).
- `SimpleLimiter`'s basic admit-or-reject and release semantics are
  tested.
- The `LifoBlockingLimiter` builder is tested for input validation
  (rejects timeouts above the documented maximum, exposes the fixed
  timeout when configured, returns null for the fixed-timeout
  accessor when a dynamic context-derived timeout is configured).
- The `BlockingLimiter` builder is tested for the same
  input-validation rejection of timeouts above its maximum.
- `CodelFilter` is tested against the RFC 8289 state machine:
  admission below target, sojourn-crossing followed by drop after
  the interval has elapsed, recent-episode hysteresis on the
  drops-in-burst counter, the inverse-square-root control-law
  schedule, the clock-skew clamp, and exit from drop mode when
  sojourn returns below target.

**Behaviour under traffic.** Mock-clock tests that drive the limiter
with controlled concurrent traffic patterns and verify the
algorithm's adaptation visibly happens. The key claim — that under
load the cap changes — is proved here by reading the
limiter's `GetLimit()` and `InflightCount()` accessors directly at
points in the simulation where adaptation should be visible. Cases
covered for the unpartitioned Gradient2 stack: steady-state ramp by
the queue-size term, shrink under sustained latency increase,
recovery after a latency spike, and the alternating-latency case
where Gradient2 alone has no defence against a bimodal workload
(the partitioned limiter is the mitigation for that case).
Partition-specific cases cover counter conservation under traffic,
the global cap never being exceeded under concurrent admission,
burst-into-idle, the hard partition cap under global saturation,
the reject-delay budget, three-way conservation across non-uniform
quotas, and the synthetic `unknown`-partition routing path.

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
  exhausted, partition quotas become hard. When global capacity
  frees, the lending behaviour resumes.
- **Bypass predicate.** Both simple and partitioned limiters respect
  a configured bypass predicate (the bypass case counts as
  `bypassed`, in-flight is not affected, the algorithm sees no
  sample).
- **Partition quota recomputation on global-cap change.** When the
  global cap moves (either through algorithm adaptation or a
  `SettableLimit` override), partition quotas are recomputed. The
  test exercises a `SettableLimit` driving the global cap up and
  then down while partitions are busy.
- **`BlockingLimiter` behaviour.** First-in-first-out ordering of
  waiters (a Limen divergence from upstream — see the limiter's
  description). Release wakes a waiting thread. Timeout fires
  correctly. Limit reduction while listeners are held leaves the
  semaphore in a valid state. The test of the FIFO ordering
  property is new in Limen and has no upstream counterpart.
- **`LifoBlockingLimiter` behaviour.** Last-in-first-out ordering of
  waiters. Limit increase wakes the most recent waiter. Limit
  decrease causes excess waiters to be rejected when they reach the
  front. The backlog cap is enforced. A per-request timeout function
  (where the timeout depends on the request's context) is honoured.
  There is also a regression test for an upstream-fixed race where a
  token delivered to a waiter just as the waiter is timing out used
  to be lost. The Limen port preserves the fix and the regression
  test.

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

Every upstream Java test file that covers a class Limen is porting
is covered by tests in the layers above. The mapping follows below.

| Upstream test file | What it asserts | Covered by |
|---|---|---|
| `ExpAvgMeasurementTest` | Warmup-to-steady-state transition of the moving average. | Algorithm correctness. |
| `ImmutableAverageSampleWindowTest` | Per-window mean RTT, drop-flag propagation. | Algorithm correctness. |
| `ImmutablePercentileSampleWindowTest` | Per-window p50 and p999, drop-flag propagation, sample-order independence. | Algorithm correctness. |
| `SimpleLimiterTest` | Admit-or-reject up to the cap, release returns slot, bypass-when-configured, no-bypass-by-default, contention with hundreds of threads. | Algorithm correctness (the deterministic methods) plus Behaviour under traffic (contention) plus the TSan sweep. |
| `AbstractPartitionedLimiterTest` | Quota arithmetic, burst-into-idle, partition lends to global, hard partition cap when global is exhausted, release, quota recompute on global-cap change, bypass-with-partitions, bypass-with-simple, concurrent partitioned access. | Algorithm correctness (quota arithmetic) plus Behaviour under traffic (the dynamic cases). |
| `BlockingLimiterTest` | Basic acquire and release, multi-thread contention, timeout, no-timeout, builder rejects timeouts above the documented maximum. | Behaviour under traffic. The FIFO-ordering test is additional and Limen-specific. |
| `LifoBlockingLimiterTest` | LIFO ordering of waiters, blocking with timeout, wake-up before timeout, backlog cap, adaptation on limit increase, adaptation on limit decrease, the timeout-acquire race regression, the builder's fixed-vs-dynamic-timeout exposure. | Behaviour under traffic. |

Upstream tests not in the table are for classes deliberately out of
scope for v0.1.0 (VegasLimit, AIMDLimit, GradientLimit, the Vegas
helper functions, the blocking-adaptive-executor simulation, and
upstream's client-side and server-side interceptors). They are
deferred along with their classes.

## How it builds

[Bazel](https://bazel.build/) with `bzlmod`. One C++ library target,
`//:limen`, with the core algorithms, limiters, the CoDel filter, and
the observability wiring.

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
  Limen uses the OpenTelemetry C++ API directly to emit its
  observability signals. Applications choose their own OpenTelemetry
  SDK and exporter. Limen does not lock them into one.

We do not depend on Folly, on Boost, or on any other large infrastructure
library. The library is intended to be inexpensive to adopt into an
existing C++ build.

Once the library has been integrated and stabilised in at least one
production consumer, the plan is to publish it to the
[Bazel Central Registry](https://registry.bazel.build/) so that other
projects can depend on it through a one-line `bazel_dep`.

## How the work has been ordered

The work has landed on `main` as a sequence of small,
individually-reviewable commits. Each commit shipped with the tests
that prove the code in that commit works. The progression has been:
repository scaffolding (license, NOTICE, README skeleton, code of
conduct, contributing guide, Bazel module, CI workflows); the
algorithm core (`Limit`, `Gradient2Limit`, measurement helpers,
sample windows, `WindowedLimit`, `FixedLimit`, `SettableLimit`,
`TracingLimitDecorator`); the limiter base and concrete limiters
(`AbstractLimiter`, `SimpleLimiter`, `AbstractPartitionedLimiter`
with burst-into-idle, `BlockingLimiter`, `LifoBlockingLimiter`); a
mock-clock traffic-simulation suite covering every limiter type; the
Gradient2 sqrt-based default `QueueSize` divergence from upstream;
`CodelFilter` with the full RFC 8289 state machine; a cross-cutting
observability-end-to-end test suite that drives the in-memory
OpenTelemetry exporter; and the documentation cleanup that drops the
gRPC-specific framing and adds `Observability.md`. `git log
--oneline main` is the truth of record for the commits themselves.

The `v0.1.0` tag follows after one final README quickstart commit
that adds a runnable minimal example. After v0.1.0 has been
integrated by at least one production consumer (RowKeyDB) and has
run stably for a few weeks, we submit to the Bazel Central Registry.

## Upcoming work, not in v0.1.0

These items are deliberately deferred. They are named here so that
nothing is lost.

- **Bazel Central Registry submission.** Planned after v0.1.0 has
  been integrated by RowKeyDB and run stably for a few weeks.
- **A gRPC adapter library.** v0.1.0 keeps the integration story
  generic so the `//:limen` library has no gRPC dependency. A
  separate adapter target that wires Limen onto gRPC's
  `AuthMetadataProcessor` (the pre-body hook) plus a
  `POST_RECV_CLOSE` interceptor (the completion hook) is the
  natural next add-on. It belongs in this repository as a
  separate optional Bazel target alongside `//:limen`.
- **Coroutine-friendly variants.** As the C++ standard library and
  the dominant server frameworks move toward `co_await`-based
  async, the blocking limiters will eventually need awaitable
  interfaces.
- **Other algorithms.** Netflix's library has several other
  algorithm implementations:
  [`VegasLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/VegasLimit.java)
  (the older TCP-Vegas-derived algorithm),
  [`GradientLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/GradientLimit.java)
  (the predecessor of Gradient2), and
  [`AIMDLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/AIMDLimit.java)
  (the classic TCP-style additive-increase-multiplicative-decrease).
  Gradient2 is Netflix's current recommendation, so Limen ports it
  first. The others port cleanly into the same `Limit` interface
  and will be added as the need arises.
- **Bigger application toolkit.** Higher-level features that an
  application typically builds on top of a concurrency limiter — for
  example, per-tenant quotas, memory-pressure-aware shedding, or
  attaching a [`google.rpc.RetryInfo`](https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto)
  payload to rejection responses — are deliberately not part of
  Limen. Limen exposes the building blocks and the application
  combines them.

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
- [`SimpleLimiter.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limiter/SimpleLimiter.java)
- [`AbstractPartitionedLimiter.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limiter/AbstractPartitionedLimiter.java)
- [`BlockingLimiter.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limiter/BlockingLimiter.java)
- [`LifoBlockingLimiter.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limiter/LifoBlockingLimiter.java)
- [`VegasLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/VegasLimit.java)
- [`GradientLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/GradientLimit.java)
- [`AIMDLimit.java`](https://github.com/Netflix/concurrency-limits/blob/78a74b9878d38c4c048b0304ce12a162ab7b7222/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/AIMDLimit.java)

CoDel references:

- [RFC 8289 (Controlled Delay Active Queue Management)](https://www.rfc-editor.org/rfc/rfc8289)
- [Nichols & Jacobson 2012 — "Controlling Queue Delay"](https://dl.acm.org/doi/10.1145/2208917.2209336)

Other live references:

- [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0)
- [Contributor Covenant v2.1](https://www.contributor-covenant.org/version/2/1/code_of_conduct/)
- [TCP Vegas (Wikipedia)](https://en.wikipedia.org/wiki/TCP_Vegas)
- [Envoy's adaptive concurrency filter](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter)
- [OpenTelemetry C++](https://opentelemetry.io/docs/languages/cpp/)
- [Abseil](https://abseil.io/)
- [`absl/synchronization/mutex.h`](https://github.com/abseil/abseil-cpp/blob/master/absl/synchronization/mutex.h)
- [Bazel](https://bazel.build/) and the
  [Bazel Central Registry](https://registry.bazel.build/)
- [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)
- [C++20 (cppreference)](https://en.cppreference.com/w/cpp/20)
- [Relaxed memory ordering (cppreference)](https://en.cppreference.com/w/cpp/atomic/memory_order#Relaxed_ordering)
- [`google.rpc.RetryInfo`](https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto)

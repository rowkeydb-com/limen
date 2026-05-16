# Limen Observability

Limen emits its full internal state through OpenTelemetry metrics.
Every metric is registered on the meter named `limen` and prefixed
with `limen.`. Every metric carries a string label `id` set to
whatever the application passed to the relevant `Builder().Id(...)`.
For processes that run more than one limiter or filter, `id`
distinguishes them on a dashboard.

## Collection model

With one exception, every Limen signal is exposed through an
OpenTelemetry observable instrument. The library does not push
values. The SDK's collector calls Limen's registered callbacks on
its own collection interval (typically every few seconds) and reads
the current state. The per-request hot path makes no synchronous
OpenTelemetry call.

The exception is the three Gradient2 histograms (`limen.min_rtt`,
`limen.min_window_rtt`, `limen.queue_size`). OpenTelemetry has no
observable-histogram variant, so these are recorded synchronously.
The recording only happens at window boundaries (about once per
second by default), on whichever request thread crosses the
boundary. The histograms never run on the per-request hot path.

What this means operationally:

- Dashboards see Limen state at the SDK's own resolution. If your SDK
  collects every five seconds, you see Limen's state every five
  seconds. There is no per-request fan-out into the SDK.
- The collection cost is bounded and pre-allocated. Limen's callbacks
  read existing atomics, format a small fixed set of labels, and
  return.
- Application-side OpenTelemetry views (bucket boundary overrides,
  attribute filters) compose with Limen's defaults. Register the
  view on the MeterProvider before passing the MeterProvider into
  Limen's builder.

## Adaptive concurrency limiter (ACL) signals

`SimpleLimiter` and `AbstractPartitionedLimiter` register these
metrics when an OpenTelemetry SDK MeterProvider is passed to the
limiter's builder. `BlockingLimiter` and `LifoBlockingLimiter` wrap
one of those two and do not register instruments of their own.
Their observability comes from the wrapped limiter, so an
application that uses a blocking decorator must pass the
MeterProvider through the inner builder, not the outer.

### `limen.limit`

- **Type**: observable gauge, int64.
- **Unit**: `1` (dimensionless).
- **Labels**: `id`.
- **What it means**: the current concurrency cap. For
  `Gradient2Limit`, this is the value the algorithm has settled on
  for the current window; for `FixedLimit`, the constant supplied at
  build time; for `SettableLimit`, whatever the application most
  recently set.

### `limen.inflight`

- **Type**: observable up-down counter, int64.
- **Unit**: `1`.
- **Labels**: `id` (the global row), plus `partition` for each
  partition row when the limiter is an `AbstractPartitionedLimiter`.
- **What it means**: requests currently admitted but not yet
  completed. For a partitioned limiter, the global row covers all
  partitions and each partition emits its own row.

### `limen.call`

- **Type**: observable counter, int64 (monotonic).
- **Unit**: `1`.
- **Labels**: `id`, `status` ∈ {`success`, `rejected`, `dropped`,
  `ignored`, `bypassed`}.
- **What it means**: cumulative count of admission decisions by
  outcome.
  - `success`: an admitted request completed normally and the
    algorithm received the sample.
  - `rejected`: a request was turned away at the limiter gate (the
    in-flight count was at the cap).
  - `dropped`: an admitted request was reported by the application
    as dropped (a downstream timeout, a load-shedder lower in the
    stack). The algorithm received the sample with
    `did_drop = true`.
  - `ignored`: an admitted request was reported as not a real
    signal (a duplicate retry, a cancellation, a `SlotGuard`
    destructor that ran without an explicit completion call). No
    sample was fed to the algorithm.
  - `bypassed`: the bypass predicate matched and the limiter let
    the call through without consulting its gate.

### `limen.limit.partition`

- **Type**: observable gauge, int64.
- **Unit**: `1`.
- **Labels**: `id`, `partition`.
- **What it means**: per-partition concurrency cap (emitted only by
  `AbstractPartitionedLimiter`). The synthetic partition `unknown`
  (used when no resolver matches a request) emits a row with cap 1.
  The per-partition cap calculation floors at one slot, so the
  fallback bucket always has a minimum slice. To detect requests
  falling through into the fallback bucket, watch the corresponding
  row on `limen.inflight` filtered by `partition="unknown"`.

### Gradient2 histograms

These three are recorded by `Gradient2Limit` once per window boundary
(about once per second by default), not once per request.

- **Type**: histogram, double.
- **Labels**: `id`.

| Metric | Unit | Default bucket boundaries | What it means |
|---|---|---|---|
| `limen.min_rtt` | `ns` | `1µs, 10µs, 100µs, 1ms, 10ms, 100ms, 1s, 10s` | The long-term exponentially smoothed baseline RTT for the most recent window. Name inherited from Netflix's `MetricIds.MIN_RTT_NAME`, which dates back to the original Vegas algorithm. For Gradient2 the value carried here is the exponentially smoothed long-term average, not a minimum. |
| `limen.min_window_rtt` | `ns` | `1µs, 10µs, 100µs, 1ms, 10ms, 100ms, 1s, 10s` | The short-window RTT measurement the algorithm compares against the long-term baseline. |
| `limen.queue_size` | `1` | `1, 4, 10, 50, 100, 500, 1000` | Per-window additive-increase term — the amount Gradient2 adds to the cap when the latency gradient indicates spare capacity. Defaults to `ceil(sqrt(current_limit))`. |

To override the bucket boundaries, register an OpenTelemetry view
on the same MeterProvider before passing it into the
`Gradient2Limit::Builder`. Limen's defaults register first.
Application views layered on top take precedence.

## Controlled-delay (CoDel) signals

These metrics are emitted by `CodelFilter` when an OpenTelemetry SDK
MeterProvider is passed to its builder.

### `limen.codel.drops`

- **Type**: observable counter, int64 (monotonic).
- **Unit**: `1` (dimensionless).
- **Labels**: `id`.
- **What it means**: cumulative count of items the filter has
  dropped. An operator watching the rate of this counter can see
  when CoDel starts shedding.

### `limen.codel.dropping`

- **Type**: observable up-down counter, int64.
- **Unit**: `1` (dimensionless).
- **Labels**: `id`.
- **What it means**: 1 when the filter is currently in drop mode
  (RFC 8289 §5.5 `dropping_`), 0 when not. A long-lived 1 means the
  queue has been sustainedly oversubscribed. A sequence of brief
  1-pulses means the algorithm is reacting to short overload bursts
  and recovering.

## A useful first dashboard

For any process running Limen, a useful starting dashboard shows, per
`id`:

- `limen.limit` over time, to see the cap moving.
- `limen.inflight` over time, with the cap overlaid.
- The rate of each `limen.call` status, to see admit-vs-reject and
  the drop / ignore mix.
- For partitioned limiters: per-partition `limen.inflight` against
  per-partition `limen.limit.partition`.
- For Gradient2: histograms of `limen.min_rtt` versus
  `limen.min_window_rtt` (the gap between them is the algorithm's
  congestion signal).
- For CoDel: the rate of `limen.codel.drops` and the
  `limen.codel.dropping` gauge.

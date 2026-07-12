# Gradient2 TLA+ Formal Verification Proposal

Gradient2 is an elegant load-shedding algorithm that relies on non-linear continuous control feedback loops (exponential smoothing, gradients, bounding clamps) to safely manage concurrency limits.

This document outlines a proposal for modeling Gradient2 as a deterministic mathematical block in TLA+ to formally verify its cybernetic control invariants. Since there is no distributed state or internal concurrency, TLA+ will be used to exhaustively prove that the feedback loop never runs away, starves, or deadlocks under any possible sequence of environmental `(short_rtt, inflight)` inputs.

## 1. Model State and Constants

The TLA+ model should abstract the math by scaling floats up to integers (e.g., using a `Multiplier = 100`) or by bounding the `Reals` state space tightly.

**Constants**:
- `MinLimit`, `MaxLimit`
- `Tolerance` (e.g., 150 representing 1.5x)
- `Smoothing` (e.g., 20 representing 0.2)
- `QueueSize` (additive-increase step)

**Variables**:
- `estimated_limit`
- `long_rtt`

**Environmental Inputs (Non-deterministic)**:
- `short_rtt \in {Normal, Elevated, Extreme}`
- `inflight \in {Low, High}`

## 2. Precise Safety Invariants

### Invariant 1: Type & Boundary Safety (`TypeOK`)
The most fundamental invariant. The concurrency cap must never mathematically breach its static boundaries, and RTTs must never drop to 0 or become negative (which would trigger a division-by-zero).
```tla
TypeOK == 
    /\ estimated_limit >= MinLimit
    /\ estimated_limit <= MaxLimit
    /\ long_rtt > 0
```

### Invariant 2: No Unjustified Shedding (`NoSheddingWithoutQueuing`)
The algorithm must *only* shrink the cap if the measured `short_rtt` breaches the `Tolerance` threshold relative to the historical `long_rtt`. If latency is within tolerance, the limit must strictly stay the same or increase.
```tla
NoSheddingWithoutQueuing ==
    (short_rtt <= Tolerance * long_rtt) => (estimated_limit' >= estimated_limit)
```

### Invariant 3: App-Limited Skip (`AppLimitedPreservation`)
If the service is not receiving enough traffic to fill the concurrency limit, the latency measurement is "hollow" (not reflective of actual saturation). The algorithm explicitly protects against this by skipping the update if `inflight < estimated_limit / 2`.
```tla
AppLimitedPreservation ==
    (inflight < estimated_limit / 2) => (estimated_limit' == estimated_limit)
```

### Invariant 4: Bounded Shedding Velocity (`MaxSheddingDrop`)
The gradient is explicitly clamped to a minimum of `0.5`. This prevents a single catastrophic latency spike from instantly crushing the limit down to `MinLimit`. The maximum possible drop in a single tick is strictly bounded by the gradient floor and the smoothing factor.
```tla
MaxSheddingDrop ==
    \* Even with gradient=0.5, the drop is dampened by the Smoothing weight
    estimated_limit' >= (estimated_limit * (1 - Smoothing)) + (estimated_limit * 0.5 * Smoothing)
```

## 3. Precise Liveness Invariants (Temporal Logic)

### Liveness 1: Recovery from Shedding (`RecoversAfterSpike`)
If the system experiences a latency spike, the limit correctly shrinks. However, if latency permanently stabilizes back to normal (`short_rtt == baseline_rtt`) and the system has enough demand to avoid the app-limited skip, the additive-increase `queue_size` term must continuously push the limit back up until it hits `MaxLimit`.
```tla
RecoversAfterSpike ==
    \* If latency is continuously good and demand is high...
    []<>(short_rtt <= long_rtt /\ inflight >= estimated_limit / 2) 
    \* ...then eventually the limit recovers to the ceiling.
    => <>(estimated_limit == MaxLimit)
```

### Liveness 2: Drift Correction Liveness (`RecoversFromAmnesia`)
Gradient2 has a known edge case: if a catastrophic spike pushes `long_rtt` incredibly high, the system might become "numb" to future spikes because `long_rtt` remains artificially large. Gradient2 handles this via Drift Correction (`long_rtt = long_rtt * 0.95`). We must prove this correction works mathematically.
```tla
RecoversFromAmnesia ==
    \* If short_rtt stays low for a prolonged period...
    []<>(short_rtt == Baseline)
    \* ...long_rtt must eventually compress back down to the baseline via drift correction.
    => <>(long_rtt <= Baseline * 1.05)
```

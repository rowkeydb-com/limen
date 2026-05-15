# Limen

[![Static Analysis](https://github.com/rowkeydb-com/limen/actions/workflows/static-analysis.yml/badge.svg)](https://github.com/rowkeydb-com/limen/actions/workflows/static-analysis.yml)
[![Build and Test](https://github.com/rowkeydb-com/limen/actions/workflows/build-test.yml/badge.svg)](https://github.com/rowkeydb-com/limen/actions/workflows/build-test.yml)
[![Sanitizers](https://github.com/rowkeydb-com/limen/actions/workflows/sanitizers.yml/badge.svg)](https://github.com/rowkeydb-com/limen/actions/workflows/sanitizers.yml)
[![Code Coverage](https://github.com/rowkeydb-com/limen/actions/workflows/coverage.yml/badge.svg)](https://github.com/rowkeydb-com/limen/actions/workflows/coverage.yml)
[![codecov](https://codecov.io/gh/rowkeydb-com/limen/branch/main/graph/badge.svg)](https://app.codecov.io/gh/rowkeydb-com/limen)

**Limen is an idiomatic C++20 port of
[Netflix's `concurrency-limits`](https://github.com/Netflix/concurrency-limits)**
— a production-tested adaptive concurrency limiter that protects a
backend service from overload by deciding, automatically and
continuously, how many requests it can safely handle in parallel.
Netflix released the original Java library in 2018 under the
[Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).
Limen is also Apache 2.0. Every file ported from the upstream library
preserves Netflix's copyright notice; the repository's
[`NOTICE`](NOTICE) file credits the upstream source.

## What this library does

Limen sits in front of a request handler and answers, very cheaply,
the question "should this request be admitted right now?" — and the
answer adapts automatically to the server's measured execution
latency.

The algorithm at the heart of Limen is Netflix's Gradient2: every
window, the algorithm compares the server's current round-trip time
against its long-term smoothed average, and adjusts a concurrency
cap up or down accordingly. Requests that exceed the current cap are
rejected immediately, before any meaningful work has been done on
the request.

The library is general-purpose. It is not tied to any specific
consumer.

## What this library is for

Any C++ server that needs to defend itself against overload —
typically by sitting downstream of a network gateway that does
admission control at the protocol level, but ahead of the
application's actual work — is a candidate. Limen is small, has no
runtime dependency beyond Abseil and OpenTelemetry's API, and is
intended to be inexpensive to drop into an existing build.

A first-class gRPC server-interceptor adapter is included in the
`//:limen_grpc` target.

## Status

Pre-release. The architecture is settled and documented at
[`design/architecture.md`](design/architecture.md). Implementation
is in progress; `v0.1.0` will be tagged once the commit plan in
that document completes.

## Building

Limen builds with [Bazel](https://bazel.build/). Every CI test
runs inside an Ubuntu 24.04 Docker image.

```
# Run the release-mode test suite in Docker:
./.github/scripts/run-bazel-in-docker.sh release test

# Run the same suite under AddressSanitizer + UndefinedBehaviorSanitizer:
./.github/scripts/run-bazel-in-docker.sh asan-ubsan test

# Run under ThreadSanitizer:
./.github/scripts/run-bazel-in-docker.sh tsan test

# Run clang-tidy:
./.github/scripts/run-bazel-in-docker.sh clang-tidy build
```

On a developer's machine the script builds the `limen-ci` image on
first invocation and reuses it from the local Docker cache on
subsequent runs. In CI the same image is built per job by the
`docker/build-push-action` step with GitHub Actions layer caching.

## Documentation

- [`design/architecture.md`](design/architecture.md) — the
  full design document.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — pull-request mechanics,
  commit-message conventions, code style.
- [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md) — Contributor
  Covenant 2.1.
- [`SECURITY.md`](SECURITY.md) — security-disclosure process.

## License

Apache 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).

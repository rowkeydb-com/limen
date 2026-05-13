#!/bin/bash
# Run a Bazel command inside the Limen CI Docker container.
#
# Usage: run-bazel-in-docker.sh <config> <command>
#   config:  release | asan-ubsan | tsan | clang-tidy | coverage
#   command: build | test | coverage
#
# The CI workflows build the Docker image (tag: limen-ci) before
# invoking this script. Local developers can either rely on the
# pre-built image or let this script build the image on first run.
#
# Adapted from rowkeydb/.github/scripts/run-bazel-in-docker.sh and
# simplified for the GitHub-hosted-runner case: no host-cache
# mounting, no low-priority knob, no concurrency lock.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
readonly SCRIPT_DIR
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
readonly WORKSPACE_DIR
readonly DOCKER_IMAGE="limen-ci"
CONTAINER_NAME="limen-ci-$$"
readonly CONTAINER_NAME

# Always remove the container on exit, including on Ctrl-C / SIGTERM.
# shellcheck disable=SC2317  # invoked indirectly via the EXIT trap below
cleanup() {
    if docker inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
        docker stop -t 5 "$CONTAINER_NAME" >/dev/null 2>&1 || true
        docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

if [ $# -ne 2 ]; then
    echo "Usage: $0 <config> <command>" >&2
    echo "  config:  release | asan-ubsan | tsan | clang-tidy | coverage" >&2
    echo "  command: build | test | coverage" >&2
    exit 1
fi

CONFIG="$1"
COMMAND="$2"

case "$CONFIG" in
    release|asan-ubsan|tsan|clang-tidy|coverage) ;;
    *)
        echo "Error: invalid config '$CONFIG'" >&2
        exit 1
        ;;
esac

case "$COMMAND" in
    build|test|coverage) ;;
    *)
        echo "Error: invalid command '$COMMAND'" >&2
        exit 1
        ;;
esac

# Build the image locally if it isn't present. In CI the
# docker/build-push-action step has already loaded it.
if ! docker image inspect "$DOCKER_IMAGE" >/dev/null 2>&1; then
    echo "==> Building $DOCKER_IMAGE image..."
    docker build -t "$DOCKER_IMAGE" \
        -f "$WORKSPACE_DIR/.github/Dockerfile.ci" "$WORKSPACE_DIR"
fi

DOCKER_OPTS=(--name "$CONTAINER_NAME")

# TSan needs the personality() syscall, which seccomp blocks by default.
if [ "$CONFIG" = "tsan" ]; then
    DOCKER_OPTS+=(--security-opt seccomp=unconfined)
fi

# Map the host user so output files are owned by the caller, not root.
DOCKER_OPTS+=(--user "$(id -u):$(id -g)")
DOCKER_OPTS+=(-e HOME=/workspace)
DOCKER_OPTS+=(-e USER=limen)
DOCKER_OPTS+=(-v "$WORKSPACE_DIR:/workspace")
DOCKER_OPTS+=(-w /workspace)

set +e
if [ "$COMMAND" = "coverage" ]; then
    # The `coverage` lines in .bazelrc already apply to this
    # subcommand, including `--config=clang` for thread-safety and
    # `--combined_report=lcov`. Don't pass `--config=$CONFIG` here:
    # CONFIG is the script's own "coverage" sentinel, not a Bazel
    # build config, and Bazel would reject it as undefined.
    # Run the build and copy the report file out to the workspace
    # root in a single container invocation, so paths inside the
    # container (where Bazel's symlinks resolve) are valid.
    docker run "${DOCKER_OPTS[@]}" "$DOCKER_IMAGE" bash -c '
        bazel coverage //...
        rc=$?
        if [ "$rc" -eq 0 ]; then
            cp "$(bazel info output_path)/_coverage/_coverage_report.dat" \
               /workspace/coverage.lcov
        elif [ "$rc" -eq 4 ]; then
            : > /workspace/coverage.lcov
            echo "No test targets in the repository yet; emitting empty coverage.lcov."
        fi
        exit $rc
    '
else
    docker run "${DOCKER_OPTS[@]}" "$DOCKER_IMAGE" \
        bazel "$COMMAND" --config="$CONFIG" //...
fi
rc=$?
set -e

# Bazel exits with code 4 when `bazel test` (or `bazel coverage`) is asked
# to run tests but none exist. Commit 1 ships no test targets; later
# commits populate them. Treat exit code 4 as success for the test and
# coverage subcommands. Once test targets exist, this branch becomes
# inert.
if [ "$rc" -eq 4 ] && { [ "$COMMAND" = "test" ] || [ "$COMMAND" = "coverage" ]; }; then
    echo "No test targets in the repository yet; treating as success."
    exit 0
fi
exit "$rc"

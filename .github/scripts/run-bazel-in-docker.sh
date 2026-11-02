#!/bin/bash
# Run a Bazel command inside the Limen CI Docker container.
#
# Usage: run-bazel-in-docker.sh <config> <command> [bazel_args...]
#   config:  release | asan-ubsan | tsan | msan | clang-tidy | coverage
#   command: build | test | coverage
#
# The image is the one rowkeydb-com/rowkeydb-build-infra publishes, named
# by its digest below and pulled on first use; CI_DOCKER_IMAGE names
# another. bazel/build_infra/image_references lists the files which name
# it, which the infrastructure's scripts/check.sh compares with its
# image.bzl.
#
# Adapted from zetasketch-cpp/.github/scripts/run-bazel-in-docker.sh. It runs
# on GitHub's machines in continuous integration and on a developer's
# desktop by hand. So Bazel always starts at idle priority, and a Bazel
# disk cache and repository cache are always mounted from the host, so
# that a run on the same machine rebuilds and fetches only what has
# changed.
#
# LIMEN_EXECUTION says where the actions run: "local", the default,
# in this container; or "buildbuddy", on BuildBuddy's machines with
# `--config=buildbuddy`, this container then running only the Bazel
# client. The second needs the BuildBuddy key and never prints it. On
# GitHub Actions the key comes only from BUILDBUDDY_API_KEY, which the
# workflows set from the repository's secret; elsewhere it may come from
# ~/.config/buildbuddy/api-key instead.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
readonly SCRIPT_DIR
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
readonly WORKSPACE_DIR
readonly DOCKER_IMAGE="${CI_DOCKER_IMAGE:-ghcr.io/rowkeydb-com/rowkeydb-build-infra@sha256:c811e74114ebedf787178468c9a796e4e37ae7e55f2d2fdc63501ea940c04515}"
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

if [ $# -lt 2 ]; then
    echo "Usage: $0 <config> <command> [bazel_args...]" >&2
    echo "  config:  release | asan-ubsan | tsan | msan | clang-tidy | coverage" >&2
    echo "  command: build | test | coverage" >&2
    exit 1
fi

CONFIG="$1"
COMMAND="$2"
shift 2

case "$CONFIG" in
    release|asan-ubsan|tsan|msan|clang-tidy|coverage) ;;
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

EXECUTION="${LIMEN_EXECUTION:-local}"
case "$EXECUTION" in
    local|buildbuddy) ;;
    *)
        echo "Error: LIMEN_EXECUTION is '$EXECUTION'; it must be local or buildbuddy" >&2
        exit 1
        ;;
esac
readonly EXECUTION

# Pull the image if it is not present. Named by digest, it is the same
# image on every machine and on BuildBuddy's executors.
if ! docker image inspect "$DOCKER_IMAGE" >/dev/null 2>&1; then
    echo "==> Pulling $DOCKER_IMAGE..."
    docker pull "$DOCKER_IMAGE"
fi

DOCKER_OPTS=(--name "$CONTAINER_NAME")

# TSan and MSan need the personality() syscall, which seccomp blocks by
# default. Only where their tests run here: on BuildBuddy they run in
# virtual machines which allow it.
if [ "$EXECUTION" = "local" ] && { [ "$CONFIG" = "tsan" ] || [ "$CONFIG" = "msan" ]; }; then
    DOCKER_OPTS+=(--security-opt seccomp=unconfined)
fi

# Map the host user so output files are owned by the caller, not root.
DOCKER_OPTS+=(--user "$(id -u):$(id -g)")
DOCKER_OPTS+=(-e HOME=/tmp)
DOCKER_OPTS+=(-e USER=limen)
DOCKER_OPTS+=(-v "$WORKSPACE_DIR:/workspace")
DOCKER_OPTS+=(-w /workspace)

# The configuration of the processor comes first on every command line,
# so that it stands at the same place in every repository which shares
# the build options, and their compilations stay equal. The shared
# options build for x86-64 alone.
case "$(uname -m)" in
    x86_64) BAZEL_OPTS=(--config=amd64) ;;
    *)
        echo "Error: the shared build options build for x86-64 only, not $(uname -m)" >&2
        exit 1
        ;;
esac

# Keep Bazel's disk cache on the host so that it survives the container,
# which is otherwise discarded along with everything built inside it. The
# cache is content addressed and the command line forms part of each key,
# so all the configurations may share one directory without colliding.
# LIMEN_DISK_CACHE puts it elsewhere. Nothing prunes it, so it will
# grow until it is deleted, and deleting it costs only the next build.
HOST_DISK_CACHE="${LIMEN_DISK_CACHE:-$HOME/.cache/limen-bazel-disk}"
mkdir -p "$HOST_DISK_CACHE"
DOCKER_OPTS+=(-v "$HOST_DISK_CACHE:/tmp/bazel-disk-cache")
BAZEL_OPTS+=(--disk_cache=/tmp/bazel-disk-cache)

# The same for what Bazel downloads, the dependencies, which would
# otherwise be fetched afresh by every container. The repository cache is
# content addressed as well, and made to be shared, the steps which run
# side by side included. LIMEN_REPOSITORY_CACHE puts it elsewhere.
HOST_REPOSITORY_CACHE="${LIMEN_REPOSITORY_CACHE:-$HOME/.cache/limen-bazel-repository}"
mkdir -p "$HOST_REPOSITORY_CACHE"
DOCKER_OPTS+=(-v "$HOST_REPOSITORY_CACHE:/tmp/bazel-repository-cache")
BAZEL_OPTS+=(--repository_cache=/tmp/bazel-repository-cache)

if [ -n "${BAZEL_REMOTE_CACHE:-}" ]; then
    BAZEL_OPTS+=(--remote_cache="$BAZEL_REMOTE_CACHE" --remote_upload_local_results=true)
    DOCKER_OPTS+=(--add-host=host.docker.internal:host-gateway)
fi

# On BuildBuddy every action runs on its machines, on the platforms the
# shared options name, and this container runs only the Bazel client. The
# key reaches Bazel through the credential helper .bazelrc names: from
# BUILDBUDDY_API_KEY where that is set, passed to the container by name so
# that the value is on no command line, and otherwise from the file,
# mounted read only where the helper looks for it. On GitHub Actions the
# file is never read: a run the repository's secret does not reach, such
# as a pull request from a fork, must not find a key on the machine.
if [ "$EXECUTION" = "buildbuddy" ]; then
    KEY_FILE="$HOME/.config/buildbuddy/api-key"
    if [ -n "${BUILDBUDDY_API_KEY:-}" ]; then
        DOCKER_OPTS+=(-e BUILDBUDDY_API_KEY)
    elif [ "${GITHUB_ACTIONS:-}" = "true" ]; then
        echo "Error: on GitHub Actions BuildBuddy needs its key in BUILDBUDDY_API_KEY." >&2
        exit 1
    elif [ -s "$KEY_FILE" ]; then
        DOCKER_OPTS+=(-v "$KEY_FILE:/tmp/.config/buildbuddy/api-key:ro")
    else
        echo "Error: BuildBuddy needs its key in BUILDBUDDY_API_KEY or in $KEY_FILE." >&2
        exit 1
    fi
    # The thread and memory sanitizers' tests run each in a virtual machine
    # of its own, which allows the personality() call they make.
    case "$CONFIG" in
        tsan) BAZEL_OPTS+=(--config=buildbuddy-tsan) ;;
        msan) BAZEL_OPTS+=(--config=buildbuddy-msan) ;;
        *) BAZEL_OPTS+=(--config=buildbuddy) ;;
    esac

    # Who ran the build and on which machine, which the container cannot
    # see, and on GitHub Actions what it built: the workspace status script
    # .bazelrc names reports them to BuildBuddy, and a run on GitHub is
    # marked as continuous integration. The tag names the configuration, so
    # that the runs of one step can be picked out of the history.
    if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
        DOCKER_OPTS+=(-e LIMEN_BUILD_USER="${GITHUB_ACTOR:-github-actions}")
        DOCKER_OPTS+=(-e GITHUB_REPOSITORY -e GITHUB_SHA -e GITHUB_REF_NAME -e GITHUB_HEAD_REF)
        BAZEL_OPTS+=(--build_metadata=ROLE=CI)
    else
        DOCKER_OPTS+=(-e LIMEN_BUILD_USER="${USER:-$(id -un)}")
    fi
    DOCKER_OPTS+=(-e LIMEN_BUILD_HOST="$(hostname)")
    BAZEL_OPTS+=(--build_metadata=TAGS="$CONFIG")

    # The container runs only the Bazel client, so it is given a fixed
    # limit in memory and in processors, so that a desktop which runs
    # several clients at once stays usable. A machine with fewer
    # processors, such as GitHub's, which has four, gives all of them.
    readonly CLIENT_GB=8
    CLIENT_CPUS="$(nproc)"
    if [ "$CLIENT_CPUS" -gt 8 ]; then
        CLIENT_CPUS=8
    fi
    readonly CLIENT_CPUS
    DOCKER_OPTS+=(--memory="${CLIENT_GB}g" --memory-swap="${CLIENT_GB}g")
    DOCKER_OPTS+=(--cpus="$CLIENT_CPUS")
    printf '==> BuildBuddy runs the actions; %sG and %s processors here for the Bazel client\n' "$CLIENT_GB" "$CLIENT_CPUS"
fi

# The machine may be someone's desktop, so Bazel starts at idle processor
# and disk priority, which its server inherits from the client. A run then
# gives way to whatever else the machine is doing; on a machine doing
# nothing else, as GitHub's, it costs nothing.
PRIORITY=(chrt -i 0 ionice -c3)

set +e
if [ "$COMMAND" = "coverage" ]; then
    # The `coverage` lines in .bazelrc already apply to this
    # subcommand, including `--combined_report=lcov`. Don't pass
    # `--config=$CONFIG` here: CONFIG is the script's own "coverage"
    # sentinel, not a Bazel build config, and Bazel would reject it as
    # undefined.
    # Run the build and copy the report file out to the workspace
    # root in a single container invocation, so paths inside the
    # container (where Bazel's symlinks resolve) are valid.
    # The arguments, the five words of the priority command first, are
    # passed positionally rather than interpolated into the command
    # string, so that a metadata value holding a bracket or a
    # parenthesis reaches Bazel intact instead of being read as shell
    # syntax.
    docker run "${DOCKER_OPTS[@]}" "$DOCKER_IMAGE" bash -c '
        "${@:1:5}" bazel coverage "${@:6}" //...
        rc=$?
        if [ "$rc" -eq 0 ]; then
            cp "$(bazel info output_path)/_coverage/_coverage_report.dat" \
               /workspace/coverage.lcov
        elif [ "$rc" -eq 4 ]; then
            : > /workspace/coverage.lcov
            echo "No test target matched; emitting empty coverage.lcov."
        fi
        exit $rc
    ' coverage-in-container "${PRIORITY[@]}" "${BAZEL_OPTS[@]}" "$@"
else
    # Docker runs the command as given, word by word, with no shell to
    # read it again.
    docker run "${DOCKER_OPTS[@]}" "$DOCKER_IMAGE" \
        "${PRIORITY[@]}" bazel "$COMMAND" "${BAZEL_OPTS[@]}" --config="$CONFIG" "$@" //...
fi
rc=$?
set -e

# Bazel exits with code 4 when `bazel test` (or `bazel coverage`) is asked
# to run tests but none match. The repository has tests, so this is inert
# unless a pattern matches none; it is kept so that such a run is not
# reported as a failure of the tests.
if [ "$rc" -eq 4 ] && { [ "$COMMAND" = "test" ] || [ "$COMMAND" = "coverage" ]; }; then
    echo "No test target matched; treating as success."
    exit 0
fi
exit "$rc"

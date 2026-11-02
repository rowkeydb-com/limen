#!/bin/bash
# Hands Bazel the BuildBuddy API key as a request header, so that the key
# appears in no command line, bazelrc or log.
#
# Bazel runs this as `buildbuddy-credential-helper.sh get`, with a request
# naming the URI on standard input, and sends the headers it prints with
# every request to that host: the cache, the executor and the build event
# service. The protocol is the credential helper specification,
# https://github.com/EngFlow/credential-helper-spec. `.bazelrc` names this
# script under `--config=buildbuddy`.
#
# The key comes from BUILDBUDDY_API_KEY where that is set, as it will be in
# continuous integration, and otherwise from the file
# $HOME/.config/buildbuddy/api-key, which the Docker runner mounts into the
# container at that path.

set -euo pipefail

if [ "${1:-}" != "get" ]; then
    echo "Usage: $0 get" >&2
    exit 2
fi

# The request names the URI; every BuildBuddy URI takes the same key.
cat >/dev/null

key="${BUILDBUDDY_API_KEY:-}"
if [ -z "$key" ]; then
    key_file="${HOME:-/tmp}/.config/buildbuddy/api-key"
    if [ ! -r "$key_file" ]; then
        echo "No BuildBuddy key: BUILDBUDDY_API_KEY is unset and $key_file cannot be read." >&2
        exit 1
    fi
    key="$(tr -d '[:space:]' <"$key_file")"
fi

# A key is letters and digits. Anything else is refused rather than
# written into the JSON below, where it could change what the header says.
if [[ ! "$key" =~ ^[A-Za-z0-9]+$ ]]; then
    echo "The BuildBuddy key holds something other than letters and digits." >&2
    exit 1
fi

printf '{"headers":{"x-buildbuddy-api-key":["%s"]}}\n' "$key"

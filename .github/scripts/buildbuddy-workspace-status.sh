#!/bin/bash
# The workspace status Bazel reports to BuildBuddy under
# `--config=buildbuddy`, so that each run's page and history name the
# repository, the commit, the branch, whether the tree had uncommitted
# changes, and who ran it on which machine.
#
# Bazel runs this from the workspace root at the start of every build and
# sends each "KEY value" line it prints with the build's results. Nothing
# is stamped into an output, the build not using --stamp, so no value here
# decides any action or costs a cache hit. It prints what it can and never
# fails, since a failure here would fail the build.
#
# Inside the Docker runner's container the user is the image's own and the
# host name is the container's, so the runner passes the real ones in
# LIMEN_BUILD_USER and LIMEN_BUILD_HOST. On GitHub Actions the
# runner passes GitHub's own variables, which name the commit and branch
# the workflow built.

set -uo pipefail

echo "USER ${LIMEN_BUILD_USER:-${USER:-unknown}}"
echo "HOST ${LIMEN_BUILD_HOST:-$(hostname 2>/dev/null || echo unknown)}"

if [ -n "${GITHUB_REPOSITORY:-}" ]; then
    echo "REPO_URL https://github.com/$GITHUB_REPOSITORY"
elif remote="$(git config --get remote.origin.url 2>/dev/null)"; then
    # One address for the repository however it was cloned, and never any
    # credentials written into it: git@github.com:owner/name.git and
    # https://user:token@github.com/owner/name.git both become
    # https://github.com/owner/name.
    remote="$(printf '%s' "$remote" | sed -E \
        -e 's#^git@([^:]+):#https://\1/#' \
        -e 's#^(https?://)[^/@]*@#\1#' \
        -e 's#\.git$##')"
    echo "REPO_URL $remote"
fi

if commit="${GITHUB_SHA:-$(git rev-parse HEAD 2>/dev/null)}" && [ -n "$commit" ]; then
    echo "COMMIT_SHA $commit"
fi

# A pull request's own branch, then the branch a push built, then the one
# checked out here; empty where the checkout is at no branch.
branch="${GITHUB_HEAD_REF:-${GITHUB_REF_NAME:-$(git branch --show-current 2>/dev/null)}}"
if [ -n "$branch" ]; then
    echo "GIT_BRANCH $branch"
fi

if git rev-parse --git-dir >/dev/null 2>&1; then
    if git diff-index --quiet HEAD -- 2>/dev/null; then
        echo "GIT_TREE_STATUS Clean"
    else
        echo "GIT_TREE_STATUS Modified"
    fi
fi

exit 0

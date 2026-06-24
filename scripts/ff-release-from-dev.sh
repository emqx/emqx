#!/usr/bin/env bash
# Fast-forward release-XX to dev-XX, refusing to overwrite if not a ff.
#
# Usage: scripts/ff-release-from-dev.sh <version>
#   <version>  release series (e.g. 58, 59, 510, 60, 61, 62, 63)
#
# Behaviour:
# - If origin/release-XX == origin/dev-XX: nothing to do, exit 0.
# - If origin/release-XX is an ancestor of origin/dev-XX: fast-forward push.
# - Otherwise: write NOT_FAST_FORWARD=1 to $GITHUB_ENV (when set), log a clear
#   error, exit 1. The caller's failure-notification step can read the env var
#   to phrase a more helpful Slack alert.
#
# Designed to run inside .github/workflows/sync-dev-to-release.yaml but does
# not strictly require GitHub Actions. Outside CI, $GITHUB_ENV is unset and
# the NOT_FAST_FORWARD signal is just skipped.

set -euxo pipefail

if [ $# -ne 1 ]; then
    echo "usage: $0 <version>" >&2
    exit 2
fi

VERSION="$1"
DEV_BRANCH="dev-${VERSION}"
RELEASE_BRANCH="release-${VERSION}"

git fetch origin "${DEV_BRANCH}" "${RELEASE_BRANCH}"

DEV_SHA=$(git rev-parse "origin/${DEV_BRANCH}")
REL_SHA=$(git rev-parse "origin/${RELEASE_BRANCH}")

if [ "${DEV_SHA}" = "${REL_SHA}" ]; then
    echo "::notice::${DEV_BRANCH} and ${RELEASE_BRANCH} already at ${DEV_SHA}; nothing to do."
    exit 0
fi

# Verify dev is a strict descendant of release (ff is possible).
if ! git merge-base --is-ancestor "${REL_SHA}" "${DEV_SHA}"; then
    if [ -n "${GITHUB_ENV:-}" ]; then
        echo "NOT_FAST_FORWARD=1" >> "$GITHUB_ENV"
    fi
    echo "::error::${RELEASE_BRANCH} (${REL_SHA}) is not an ancestor of ${DEV_BRANCH} (${DEV_SHA}); ff not possible."
    exit 1
fi

# Fast-forward the remote release branch.
git push origin "${DEV_SHA}:refs/heads/${RELEASE_BRANCH}"
echo "::notice::${RELEASE_BRANCH} advanced from ${REL_SHA} to ${DEV_SHA}."

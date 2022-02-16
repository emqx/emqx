#!/usr/bin/env bash
set -euo pipefail

# This script prints the release version for emqx

# ensure dir
cd -P -- "$(dirname -- "$0")"

if [ -f EMQX_ENTERPRISE ]; then
    EDITION='enterprise'
else
    EDITION='opensource'
fi

## emqx_release.hrl is the single source of truth for release version
RELEASE="$(grep -E "define.+EMQX_RELEASE.+${EDITION}" include/emqx_release.hrl | cut -d '"' -f2)"

git_exact_vsn() {
    local tag
    tag="$(git describe --tags --match "[e|v]*" --exact 2>/dev/null)"
    echo "${tag#[e|v]}"
}

GIT_EXACT_VSN="$(git_exact_vsn)"
if [ "$GIT_EXACT_VSN" != '' ]; then
    if [ "$GIT_EXACT_VSN" != "$RELEASE" ]; then
        echo "ERROR: Tagged $GIT_EXACT_VSN, but $RELEASE in include/emqx_release.hrl" 1>&2
        exit 1
    fi
    SUFFIX=''
else
    SUFFIX="-$(git rev-parse HEAD | cut -b1-8)"
fi

echo "${RELEASE}${SUFFIX}"

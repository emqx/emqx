#!/usr/bin/env bash
set -euo pipefail

# This script prints the release version for emqx

# ensure dir
cd -P -- "$(dirname -- "$0")"

case "${1:-}" in
    *enterprise*)
        RELEASE_EDITION="EMQX_RELEASE_EE"
        GIT_TAG_PREFIX="e"
        ;;
    *)
        RELEASE_EDITION="EMQX_RELEASE_CE"
        GIT_TAG_PREFIX="v"
        ;;
esac

## emqx_release.hrl is the single source of truth for release version
RELEASE="$(grep -E "define.+${RELEASE_EDITION}" apps/emqx/include/emqx_release.hrl | cut -d '"' -f2)"

git_exact_vsn() {
    local tag
    tag="$(git describe --tags --match "${GIT_TAG_PREFIX}*" --exact 2>/dev/null)"
    echo "${tag//^[v|e]/}"
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

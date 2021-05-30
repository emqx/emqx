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

## git commit hash is added as suffix in case the git tag and release version is not an exact match
if [ -d .git ] && ! git describe --tags --match "[e|v]${RELEASE}" --exact >/dev/null 2>&1; then
    SUFFIX="-$(git rev-parse HEAD | cut -b1-8)"
fi

echo "${RELEASE}${SUFFIX:-}"

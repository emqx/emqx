#!/usr/bin/env bash

## This script wrapps update_appup.escript,
## it provides a more commonly used set of default args.

## Arg1: EMQX PROFILE

set -euo pipefail

usage() {
    echo "$0 PROFILE PREV_VERSION"
}
# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

PROFILE="${1:-}"
case "$PROFILE" in
    emqx-ee)
        DIR='enterprise'
        TAG_PREFIX='e'
        ;;
    emqx)
        DIR='broker'
        TAG_PREFIX='v'
        ;;
    emqx-edge)
        DIR='edge'
        TAG_PREFIX='v'
        ;;
    *)
        echo "Unknown profile $PROFILE"
        usage
        exit 1
        ;;
esac

PREV_VERSION="$(git describe --tag --match "${TAG_PREFIX}*" | grep -oE "${TAG_PREFIX}4\.[0-9]+\.[0-9]+")"
PREV_VERSION="${PREV_VERSION#[e|v]}"

shift 1
ESCRIPT_ARGS="$*"

SYSTEM="${SYSTEM:-$(./scripts/get-distro.sh)}"
ARCH="${ARCH:-$(uname -m)}"
case "$ARCH" in
    x86_64)
        ARCH='amd64'
        ;;
    aarch64)
        ARCH='arm64'
        ;;
    arm*)
        ARCH=arm
        ;;
esac

PACKAGE_NAME="${PROFILE}-${SYSTEM}-${PREV_VERSION}-${ARCH}.zip"
DOWNLOAD_URL="https://www.emqx.com/downloads/${DIR}/v${PREV_VERSION}/${PACKAGE_NAME}"

# shellcheck disable=SC2086
./scripts/update_appup.escript --binary-rel-url "$DOWNLOAD_URL" $ESCRIPT_ARGS "$PREV_VERSION"

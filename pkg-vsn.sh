#!/usr/bin/env bash
set -euo pipefail

# This script prints the release version for emqx

# ensure dir
cd -P -- "$(dirname -- "$0")"

help() {
    echo
    echo "$0 PROFILE [options]"
    echo
    echo "-h|--help:       To display this usage information"
    echo "--long:          Print long vsn number. e.g. 5.0.0-ubuntu20.04-amd64"
    echo "                 Otherwise short e.g. 5.0.0"
    echo "--elixir:        Include elixir version in the long version string"
    echo "                 e.g. 5.0.0-elixir-ubuntu20.04-amd64"
    echo "--vsn_matcher:   For --long option, replace the EMQX version with '*'"
    echo "                 so it can be used in find commands"
}

PROFILE="${1:-}"
if [ -z "$PROFILE" ]; then
    echo "ERROR: missing profile"
    help
    exit 1
fi
shift

while [ "$#" -gt 0 ]; do
    case $1 in
    -h|--help)
        help
        exit 0
        ;;
    --long)
        LONG_VERSION='yes'
        shift 1
        ;;
    --elixir)
        shift 1
        case ${1:-novalue} in
            -*)
                # another option
                IS_ELIXIR='yes'
                ;;
            yes|no)
                IS_ELIXIR="${1}"
                shift 1
                ;;
            novalue)
                IS_ELIXIR='yes'
                ;;
            *)
                echo "ERROR: unknown option: --elixir $2"
                exit 1
                ;;
        esac
        ;;
    --vsn_matcher)
        IS_MATCHER='yes'
        shift 1
        ;;
    *)
      echo "WARN: Unknown arg (ignored): $1"
      exit 1
      ;;
  esac
done

# return immediately if version is already set
if [[ "${PKG_VSN:-novalue}" != novalue && "${LONG_VERSION:-novalue}" != 'yes' ]]; then
    echo "$PKG_VSN"
    exit 0
fi

case "${PROFILE}" in
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
    echo "${tag#[v|e]}"
}

GIT_EXACT_VSN="$(git_exact_vsn)"
if [ "$GIT_EXACT_VSN" != '' ]; then
    if [ "$GIT_EXACT_VSN" != "$RELEASE" ]; then
        echo "ERROR: Tagged $GIT_EXACT_VSN, but $RELEASE in include/emqx_release.hrl" 1>&2
        exit 1
    fi
    SUFFIX=''
else
    SUFFIX="-g$(git rev-parse HEAD | cut -b1-8)"
fi

PKG_VSN="${PKG_VSN:-${RELEASE}${SUFFIX}}"

if [ "${LONG_VERSION:-}" != 'yes' ]; then
    echo "$PKG_VSN"
    exit 0
fi

### --long LONG_VERSION handling start

if [ "${IS_MATCHER:-}" = 'yes' ]; then
    PKG_VSN='*'
fi

SYSTEM="$(./scripts/get-distro.sh)"

UNAME_M="$(uname -m)"
case "$UNAME_M" in
    x86_64)
        ARCH='amd64'
        ;;
    aarch64)
        ARCH='arm64'
        ;;
    arm64)
        ARCH='arm64'
        ;;
    arm*)
        ARCH='arm'
        ;;
esac

if [ "${IS_ELIXIR:-}" = "yes" ]; then
    infix='-elixir'
else
    infix=''
fi

echo "${PKG_VSN}${infix}-${SYSTEM}-${ARCH}"

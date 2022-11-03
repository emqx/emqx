#!/usr/bin/env bash

## This script wrapps update_appup.escript,
## it provides a more commonly used set of default args.

## Arg1: EMQX PROFILE

set -euo pipefail
set -x

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

usage() {
    echo "$0 PROFILE [options]"
    echo "options:"
    echo "--skip-build:      Skip building the profile only to re-generate the appup files."
    echo "--skip-build-base: This script by default forces a git clean before rebuilding on the base version "
    echo "                   this option is useful when you are sure the past builds can be trusted,"
    echo "                   that is, there were no re-tags or anything."
    echo "--check:           Exit with non-zero code if there is git diff after the execution."
    echo "                   Mostly used in CI."
}

PROFILE="${1:-}"
case "$PROFILE" in
    emqx-ee)
        GIT_REPO='emqx/emqx-enterprise.git'
        TAG_PREFIX='e'
        ;;
    emqx)
        GIT_REPO='emqx/emqx.git'
        TAG_PREFIX='v'
        ;;
    emqx-edge)
        GIT_REPO='emqx/emqx.git'
        TAG_PREFIX='v'
        ;;
    *)
        echo "Unknown profile $PROFILE"
        usage
        exit 1
        ;;
esac

## possible tags:
##  v4.3.11
##  e4.3.11
##  rel-v4.4.3
##  rel-e4.4.3
PREV_TAG="${PREV_TAG:-$(git describe --tag --abbrev=0 --match "[${TAG_PREFIX}|rel-]*" --exclude '*rc*' --exclude '*alpha*' --exclude '*beta*' --exclude '*gocsp*')}"

shift 1
# bash 3.2 treat empty array as unbound, so we can't use 'ESCRIPT_ARGS=()' here,
# but must add an empty-string element to the array
ESCRIPT_ARGS=( '' )
while [ "$#" -gt 0 ]; do
    case $1 in
    -h|--help)
        usage
        exit 0
        ;;
    --skip-build)
        SKIP_BUILD='yes'
        shift
        ;;
    --skip-build-base)
        SKIP_BUILD_BASE='yes'
        shift
        ;;
    --check)
        # hijack the --check option
        IS_CHECK='yes'
        shift
        ;;
    *)
        ESCRIPT_ARGS+=( "$1" )
        shift
        ;;
  esac
done

if [ "$TAG_PREFIX" = 'v' ]; then
    SRC_DIRS="{apps,src,lib-ce}"
else
    SRC_DIRS="{apps,src,lib-ee}"
fi

## make sure we build here in bash and always pass --skip-build to escript
if [ "${SKIP_BUILD:-}" != 'yes' ]; then
    make "${PROFILE}"
fi

PREV_DIR_BASE="/tmp/_w"
mkdir -p "${PREV_DIR_BASE}"
if [ ! -d "${PREV_DIR_BASE}/${PREV_TAG}" ]; then
    cp -R . "${PREV_DIR_BASE}/${PREV_TAG}"
    # always 'yes' in CI
    NEW_COPY='yes'
else
    NEW_COPY='no'
fi

TOOL_VERSIONS="$PWD/.tool-versions"

if [ "${SKIP_BUILD_BASE:-no}" = 'yes' ]; then
    echo "not building relup base ${PREV_DIR_BASE}/${PREV_TAG}"
else
    pushd "${PREV_DIR_BASE}/${PREV_TAG}"
    if [ "$NEW_COPY" = 'no' ]; then
        REMOTE="$(git remote -v | grep "${GIT_REPO}" | head -1 | awk '{print $1}')"
        git fetch "$REMOTE" --tags -f
    fi
    git reset --hard
    git clean -ffdx
    git checkout "${PREV_TAG}"
    # copy current .tool-versions to ensure same OTP version, even if
    # overridden.
    cp "$TOOL_VERSIONS" ./
    make "$PROFILE"
    popd
fi

PREV_REL_DIR="${PREV_DIR_BASE}/${PREV_TAG}/_build/${PROFILE}/lib"

# bash 3.2 does not allow empty array, so we had to add an empty string in the ESCRIPT_ARGS array,
# this in turn makes quoting "${ESCRIPT_ARGS[@]}" problematic, hence disable SC2068 check here
# shellcheck disable=SC2068
./scripts/update_appup.escript \
    --src-dirs "${SRC_DIRS}/**" \
    --release-dir "_build/${PROFILE}/lib" \
    --prev-release-dir "${PREV_REL_DIR}" \
    --skip-build \
    ${ESCRIPT_ARGS[@]} "$PREV_TAG"

if [ "${IS_CHECK:-}" = 'yes' ]; then
    diffs="$(git diff --name-only | grep -E '\.appup\.src' || true)"
    if [ "$diffs" != '' ]; then
        git --no-pager diff
        echo "$0 ---check produced git diff"
        exit 1
    fi
fi

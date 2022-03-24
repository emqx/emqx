#!/usr/bin/env bash

## This script wrapps update_appup.escript,
## it provides a more commonly used set of default args.

## Arg1: EMQX PROFILE

set -euo pipefail

usage() {
    echo "$0 PROFILE"
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

## possible tags:
##  v4.3.11
##  e4.3.11
##  rel-v4.4.3
##  rel-e4.4.3
PREV_TAG="$(git describe --tag --abbrev=0 --match "[${TAG_PREFIX}|rel-]*" --exclude '*rc*' --exclude '*alpha*' --exclude '*beta*')"

shift 1
# bash 3.2 treat empty array as unbound, so we can't use 'ESCRIPT_ARGS=()' here,
# but must add an empty-string element to the array
ESCRIPT_ARGS=( '' )
while [ "$#" -gt 0 ]; do
    case $1 in
    -h|--help)
        help
        exit 0
        ;;
    --skip-build)
        SKIP_BUILD='yes'
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

PREV_DIR_BASE="/tmp/emqx-appup-build"
mkdir -p "${PREV_DIR_BASE}"
if [ ! -d "${PREV_TAG}" ]; then
    cp -R . "${PREV_DIR_BASE}/${PREV_TAG}"
fi

pushd "${PREV_DIR_BASE}/${PREV_TAG}"
git reset --hard
git checkout ${PREV_TAG}
make clean-all
make $PROFILE
popd

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

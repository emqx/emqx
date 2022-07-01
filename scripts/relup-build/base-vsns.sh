#!/usr/bin/env bash
set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

## This script prints the relup upgrade base versions
## for the given EMQX edition (specified as first arg)
##
## The second argument is the current release version
## if not provided, it's taken from pkg-vsn.sh

usage() {
    echo "Usage: $0 <EMQX_PROFILE> [<CURRENT_VERSION>]"
    echo "e.g.   $0 enterprise 4.3.10"
    exit 1
}

parse_semver() {
    echo "$1" | tr '.|-' ' '
}

PROFILE="${1:-}"
[ -z "${PROFILE}" ] && usage

## Get the current release version
## e.g.
## 5.0.0                 when GA
## 5.0.0-beta.3          when pre-release
## 5.0.0-beta.3.abcdef00 when developing
CUR="${2:-}"
if [ -z "${CUR}" ]; then
    CUR="$(./pkg-vsn.sh "$PROFILE")"
fi

# shellcheck disable=SC2207
CUR_SEMVER=($(parse_semver "$CUR"))

if [ "${#CUR_SEMVER[@]}" -lt 3 ]; then
    echo "$CUR is not Major.Minor.Patch"
    usage
fi

## when the current version has no suffix such as -abcdef00
## it is a formal release
if [ "${#CUR_SEMVER[@]}" -eq 3 ]; then
    IS_RELEASE=true
else
    IS_RELEASE=false
fi

case "${PROFILE}" in
    *enterprise*)
        GIT_TAG_PREFIX="e"
        ;;
    *)
        GIT_TAG_PREFIX="v"
        ;;
esac

while read -r git_tag; do
    # shellcheck disable=SC2207
    semver=($(parse_semver "$git_tag"))
    if [ "${#semver[@]}" -eq 3 ] && [ "${semver[2]}" -le "${CUR_SEMVER[2]}" ]; then
        if [ ${IS_RELEASE} = true ] && [ "${semver[2]}" -eq "${CUR_SEMVER[2]}" ] ; then
            # do nothing
            # exact match, do not print current version
            # because current version is not an upgrade base
            true
        else
            echo "$git_tag"
        fi
    fi
done < <(git tag -l "${GIT_TAG_PREFIX}${CUR_SEMVER[0]}.${CUR_SEMVER[1]}.*")

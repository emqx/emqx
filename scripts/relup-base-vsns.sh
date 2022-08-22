#!/usr/bin/env bash
set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

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

EDITION="${1:-}"
[ -z "${EDITION}" ] && usage

## Get the current release version
## e.g.
## 5.0.0                 when GA
## 5.0.0-beta.3          when pre-release
## 5.0.0-beta.3.abcdef00 when developing
CUR="${2:-}"
if [ -z "${CUR}" ]; then
    CUR="$(./pkg-vsn.sh)"
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

case "${EDITION}" in
    *enterprise*)
        GIT_TAG_PREFIX="e"
        ;;
    *)
        GIT_TAG_PREFIX="v"
        ;;
esac

# must not be empty for MacOS (bash 3.x)
TAGS=( 'dummy' )
TAGS_EXCLUDE=( 'dummy' )

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
            TAGS+=( "$git_tag" )
        fi
    fi
done < <(git tag -l "${GIT_TAG_PREFIX}${CUR_SEMVER[0]}.${CUR_SEMVER[1]}.*")

# debian11 is introduced since v4.4.2 and e4.4.2
# exclude tags before them
SYSTEM="${SYSTEM:-$(./scripts/get-distro.sh)}"
if [ "$SYSTEM" = 'debian11' ]; then
    TAGS_EXCLUDE+=( 'v4.4.0' 'v4.4.1' )
    TAGS_EXCLUDE+=( 'e4.4.0' 'e4.4.1' )
fi

for tag_to_del in "${TAGS_EXCLUDE[@]}"; do
    TAGS=( "${TAGS[@]/$tag_to_del}" )
done

# 4.5.X versions uses the previous 4.4.Y as a base we emulate
# that we are the last 4.4.Y version that allows upgrading to 4.5.X
# We add that version, if available.
maybe_add_tag() {
  local tag="$1"
  if [[ $(git tag -l "$tag") ]]; then
    TAGS+=( "$tag" )
  fi
}

if [[ "${CUR_SEMVER[0]}" = 4 && "${CUR_SEMVER[1]}" = 5 ]]; then
  for tag in "v4.4.8" "e4.4.8"; do
    maybe_add_tag "$tag"
  done
fi

for tag in "${TAGS[@]}"; do
    if [ "$tag" != '' ]; then
        echo "$tag"
    fi
done

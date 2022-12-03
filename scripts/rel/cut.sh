#!/usr/bin/env bash

## cut a new 5.x release for EMQX (opensource or enterprise).

set -euo pipefail

if [ "${DEBUG:-}" = 1 ]; then
    set -x
fi

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

usage() {
    cat <<EOF
$0 RELEASE_GIT_TAG [option]
RELEASE_GIT_TAG is a 'v*' or 'e*' tag for example:
  v5.0.12
  e5.0.0-beta.6

options:
  -h|--help: Print this usage.
  -b|--base: Specify the current release base branch, can be one of
             release-50
             NOTE: this option should be used when --dryrun.
  --dryrun:  Do not actually create the git tag.
  --skip-appup: Skip checking appup
                Useful when you are sure that appup is already updated'

NOTE: For 5.0 series the current working branch must be 'release-50' for opensource edition
      and 'release-e50' for enterprise edition.
      --.--[  master  ]---------------------------.-----------.---
         \\                                      /
          \`---[release-50]----(v5.0.12 | e5.0.0)
EOF
}

logerr() {
    echo "$(tput setaf 1)ERROR: $1$(tput sgr0)"
}
logmsg() {
    echo "INFO: $1"
}

TAG="${1:-}"

case "$TAG" in
    v*)
        TAG_PREFIX='v'
        PROFILE='emqx'
        SKIP_APPUP='yes'
        ;;
    e*)
        TAG_PREFIX='e'
        PROFILE='emqx-enterprise'
        SKIP_APPUP='no'
        ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        logerr "Unknown version tag $TAG"
        usage
        exit 1
        ;;
esac

shift 1

DRYRUN='no'
while [ "$#" -gt 0 ]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        --skip-appup)
            shift
            SKIP_APPUP='yes'
            ;;
        --dryrun)
            shift
            DRYRUN='yes'
            ;;
        -b|--base)
            BASE_BR="${2:-}"
            if [ -z "${BASE_BR}" ]; then
                logerr "Must specify which base branch"
                exit 1
            fi
            shift 2
            ;;
        *)
            logerr "Unknown option $1"
            exit 1
            ;;
    esac
done

rel_branch() {
    local tag="$1"
    case "$tag" in
        v5.0.*)
            echo 'release-50'
            ;;
        e5.0.*)
            echo 'release-50'
            ;;
        *)
            logerr "Unsupported version tag $TAG"
            exit 1
            ;;
    esac
}

## Ensure the current work branch
assert_work_branch() {
    local tag="$1"
    local release_branch
    release_branch="$(rel_branch "$tag")"
    local base_branch
    base_branch="${BASE_BR:-$(git branch --show-current)}"
    if [ "$base_branch" != "$release_branch" ]; then
        logerr "Base branch: $base_branch"
        logerr "Relase tag must be on the release branch: $release_branch"
        logerr "or must use -b|--base option to specify which release branch is current branch based on"
        exit 1
    fi
}
assert_work_branch "$TAG"

## Ensure no dirty changes
assert_not_dirty() {
    local diff
    diff="$(git diff --name-only)"
    if [ -n "$diff" ]; then
        logerr "Git status is not clean? Changed files:"
        logerr "$diff"
        exit 1
    fi
}
assert_not_dirty

## Assert that the tag is not already created
assert_tag_absent() {
    local tag="$1"
    ## Fail if the tag already exists
    EXISTING="$(git tag --list "$tag")"
    if [ -n "$EXISTING" ]; then
        logerr "$tag already released?"
        logerr 'This script refuse to force re-tag.'
        logerr 'If re-tag is intended, you must first delete the tag from both local and remote'
        exit 1
    fi
}
assert_tag_absent "$TAG"

PKG_VSN=$(./pkg-vsn.sh "$PROFILE")

## Assert package version is updated to the tag which is being created
assert_release_version() {
    local tag="$1"
    # shellcheck disable=SC2001
    pkg_vsn="$(echo "$PKG_VSN" | sed 's/-g[0-9a-f]\{8\}$//g')"
    if [ "${TAG_PREFIX}${pkg_vsn}" != "${tag}" ]; then
        logerr "The release version ($pkg_vsn) is different from the desired git tag."
        logerr "Update the release version in emqx_release.hrl"
        exit 1
    fi
}
assert_release_version "$TAG"

## Check if all upstream branches are merged
if [ -z "${BASE_BR:-}" ]; then
    ./scripts/rel/sync-remotes.sh
else
    ./scripts/rel/sync-remotes.sh --base "$BASE_BR"
fi

## Check if the Chart versions are in sync
./scripts/rel/check-chart-vsn.sh "$PROFILE"

## Check if app versions are bumped
./scripts/apps-version-check.sh

## Ensure appup files are updated
if [ "$SKIP_APPUP" = 'no' ]; then
    logmsg "Checking appups"
    ./scripts/update-appup.sh "$PROFILE" --check
else
    logmsg "Skipped checking appup updates"
fi

## Ensure relup paths are updated
## TODO: add relup path db
#./scripts/relup-base-vsns.escript check-vsn-db "$PKG_VSN" "$RELUP_PATHS"

## Run some additional checks (e.g. some for enterprise edition only)
CHECKS_DIR="./scripts/rel/checks"
if [ -d "${CHECKS_DIR}" ]; then
    CHECKS="$(find "${CHECKS_DIR}" -name "*.sh" -print0 2>/dev/null | xargs -0)"
    for c in $CHECKS; do
        logmsg "Executing $c"
        $c
    done
fi

if [ "$DRYRUN" = 'yes' ]; then
    logmsg "Release tag is ready to be created with command: git tag $TAG"
else
    git tag "$TAG"
    logmsg "$TAG is created OK."
fi

#!/usr/bin/env bash

## cut a new 4.x release for EMQX (opensource or enterprise).

set -euo pipefail
# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

usage() {
    cat <<EOF
$0 RELEASE_GIT_TAG [option]
RELEASE_GIT_TAG is a 'v*' or 'e*' tag for example:
  v4.4.10
  e4.4.10-alpha.2

options:
  -h|--help: Print this usage.
  -b|--base: Specify the current release base branch, can be one of
             release-v44, release-e43, or release-e44.
             NOTE: this option should be used when --dryrun.
  --dryrun:  Do not actually create the git tag.
  --skip-appup: Skip checking appup
                Useful when you are sure that appup is already updated'

NOTE: When cutting a 'e*' tag release, both the opensource and enterprise
      repos should be found as a fetch-remote in the current git repo.

NOTE: For 4.4 series the current working branch must be 'release-v44' for opensource edition
      and 'release-e44' for enterprise edition.
      --.--[main-v4.4]------------------.-----------.---
         \\                             /             \\
          \`---[release-v44]----(v4.4.X)               \\
                                       \\               \\
          .---[release-e44]-------------'--(e4.4.Y)     \\
         /                                         \\     V
      --'------[main-v4.4-enterprise]---------------'----'---

NOTE: For 4.3 opensource edition, it reached its 18-month EOL in November 2022.
      only enterprise edition will be continue supported.
      For issues affecting both 4.3 and 4.4, two separate PRs should be created
      one for main-v4.3-enterprise and another for main-v4.4 or main-v4.4-enterprise
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
        if [ -f EMQX_ENTERPRISE ]; then
            logerr 'Cannot create v-tag on enterprise branch'
            exit 1
        fi
        TAG_PREFIX='v'
        APPUP_CHECK_PROFILE='emqx'
        ;;
    e*)
        if [ ! -f EMQX_ENTERPRISE ]; then
            logerr 'Cannot create e-tag on opensource branch'
            exit 1
        fi
        TAG_PREFIX='e'
        APPUP_CHECK_PROFILE='emqx-ee'
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

SKIP_APPUP='no'
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
        e4.3.*)
            echo 'release-e43'
            ;;
        v4.4.*)
            echo 'release-v44'
            ;;
        e4.4.*)
            echo 'release-e44'
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

PKG_VSN=$(./pkg-vsn.sh)

## Assert package version is updated to the tag which is being created
assert_release_version() {
    local tag="$1"
    # shellcheck disable=SC2001
    pkg_vsn="$(echo "$PKG_VSN" | sed 's/-[0-9a-f]\{8\}$//g')"
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
./scripts/check-chart-vsn.sh

## Check if app versions are bumped
./scripts/check-apps-vsn.sh

## Ensure appup files are updated
if [ "$SKIP_APPUP" = 'no' ]; then
    logmsg "Checking appups"
    ./scripts/update-appup.sh "$APPUP_CHECK_PROFILE" --check
else
    logmsg "Skipped checking appup updates"
fi

## Ensure relup paths are updated
case "${PKG_VSN}" in
    4.3.*)
        HAS_RELUP_DB='no'
        ;;
    *)
        HAS_RELUP_DB='yes'
        ;;
esac
if [ "$HAS_RELUP_DB" = 'yes' ]; then
    if [ -f EMQX_ENTERPRISE ]; then
        RELUP_PATHS='./data/relup-paths-ee.eterm'
    else
        RELUP_PATHS='./data/relup-paths.eterm'
    fi
    ./scripts/relup-base-vsns.escript check-vsn-db "$PKG_VSN" "$RELUP_PATHS"
fi

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

#!/usr/bin/env bash

## cut a new 5.x release for EMQX (opensource or enterprise).

set -euo pipefail

[ "${DEBUG:-}" = 1 ] && set -x

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

usage() {
    cat <<EOF
$0 RELEASE_GIT_TAG [option]
RELEASE_GIT_TAG is a 'Major.Minor.Patch' tag, for example: 6.0.0

options:
  -h|--help:         Print this usage.

  -b|--base:         Specify the current release base branch, can be one of
                     rel-M.N.P      (a release in code freeze, e.g. rel-6.0.4)
                     release-60
                     release-61
                     release-62
                     release-63
                     release-70
                     NOTE: not needed when the current branch is already the
                     freeze branch of the tag, or the release branch.

  --dryrun:          Do not actually create the git tag.

  --prev-tag <tag>:  Provide the prev tag to automatically generate changelogs
                     If this option is absent, the tag found by git describe will be used


The tag is signed, so a signing key must be configured. See
https://docs.github.com/en/authentication/managing-commit-signature-verification

A patch release is prepared on its own freeze branch, and the tag is cut there.
The release line keeps taking changes meanwhile, and those ship in the next
patch, so the line branch is not merged into the freeze branch:

      --.--[   dev-60   ]-----------------------------------------
         \\                        \\
          \\                        \`--[release-60]--------------
           \`---[rel-6.0.4]--(6.0.4-rc.1, 6.0.4-rc.2, 6.0.4)

Cutting straight from the release branch is still supported, for a line that
is not using a freeze branch.
EOF
}

logerr() {
    echo "$(tput setaf 1)ERROR: $1$(tput sgr0)"
}

logwarn() {
    echo "$(tput setaf 3)WARNING: $1$(tput sgr0)"
}

logmsg() {
    echo "INFO: $1"
}

TAG="${1:-}"

case "$TAG" in
    -h|--help)
        usage
        exit 0
        ;;
    *)
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
        --prev-tag)
            shift
            PREV_TAG="$1"
            shift
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
        6.*-patch.* | 7.*-patch.*)
            echo "patch-${tag%-patch.*}"
            ;;
        6.0.*)
            echo 'release-60'
            ;;
        6.1.*)
            echo 'release-61'
            ;;
        6.2.*)
            echo 'release-62'
            ;;
        6.3.*)
            echo 'release-63'
            ;;
        7.0.*)
            echo 'release-70'
            ;;
        *)
            logerr "Unsupported version tag $TAG"
            exit 1
            ;;
    esac
}

## The branch a patch release is prepared on while it is in code freeze.
## A pre-release tag is cut from the same branch as its final tag:
## 6.0.4-rc.2 comes from rel-6.0.4. Tags of the patch-* flow have no freeze
## branch of their own.
freeze_branch() {
    local tag="$1"
    case "$tag" in
        *-patch.*)
            echo ''
            ;;
        *)
            echo "rel-${tag%%-*}"
            ;;
    esac
}

## Ensure the current work branch, and remember which branch it is, so that
## the upstream check below looks at the same one.
WORK_BRANCH=''
assert_work_branch() {
    local tag="$1"
    local release_branch freeze_branch base_branch
    release_branch="$(rel_branch "$tag")"
    freeze_branch="$(freeze_branch "$tag")"
    base_branch="${BASE_BR:-$(git branch --show-current)}"
    if [ "$base_branch" != "$release_branch" ] && [ "$base_branch" != "$freeze_branch" ]; then
        logerr "Base branch: $base_branch"
        if [ -n "$freeze_branch" ]; then
            logerr "A release tag must be cut on the freeze branch $freeze_branch, or on $release_branch"
        else
            logerr "A release tag must be cut on the release branch: $release_branch"
        fi
        logerr "or must use -b|--base option to specify which release branch is current branch based on"
        exit 1
    fi
    WORK_BRANCH="$base_branch"
    logmsg "Cutting $tag on $WORK_BRANCH"
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

## Release tags are signed. Check for a usable key before the long checks
## below, so a missing key is reported in a second rather than in a minute.
assert_can_sign() {
    local format key
    format="$(git config --get gpg.format || echo 'openpgp')"
    key="$(git config --get user.signingkey || true)"
    case "$format" in
        openpgp)
            if [ -z "$key" ] && ! gpg --list-secret-keys --with-colons 2>/dev/null | grep -q '^sec'; then
                logerr "The release tag is signed, but no OpenPGP secret key was found."
                logerr "Configure one: git config --global user.signingkey <key-id>"
                return 1
            fi
            ;;
        *)
            if [ -z "$key" ]; then
                logerr "The release tag is signed, but user.signingkey is unset for gpg.format=$format."
                logerr "Configure one: git config --global user.signingkey <key-or-path>"
                return 1
            fi
            ;;
    esac
    logmsg "Signing with gpg.format=$format${key:+, user.signingkey=$key}"
}
if ! assert_can_sign; then
    if [ "$DRYRUN" = 'yes' ]; then
        logwarn 'Continuing because of --dryrun. The real cut will refuse to tag.'
    else
        exit 1
    fi
fi

bump_vsn() {
    local new_version="$1"
    local emqx_release_file_path="apps/emqx/include/emqx_release.hrl"
    local chart_file_path="deploy/charts/emqx-enterprise/Chart.yaml"

    # don't use -i since it has different syntax in GNU and BSD versions
    sed "s/-define(EMQX_RELEASE_EE, \"[^\"]*\")\./-define(EMQX_RELEASE_EE, \"$new_version\")./g" "$emqx_release_file_path" > "${emqx_release_file_path}.tmp"
    mv "${emqx_release_file_path}.tmp" "$emqx_release_file_path"

    sed "s/^version: [0-9][0-9.]*[a-zA-Z0-9.-]*$/version: $new_version/g; s/^appVersion: [0-9][0-9.]*[a-zA-Z0-9.-]*$/appVersion: $new_version/g" "$chart_file_path" > "${chart_file_path}.tmp"
    mv "${chart_file_path}.tmp" "$chart_file_path"

    git add "$emqx_release_file_path" "$chart_file_path"
    git diff --staged
    if [ "$DRYRUN" != 'yes' ]; then
        local commit_msg="chore: bump version to $new_version"
        # Ask for confirmation before committing
        read -r -p "git commit -m \"$commit_msg\" - Proceed? (y/n): " CONFIRM
        if [[ "$CONFIRM" =~ ^[Yy]$ ]]; then
            git commit -m "$commit_msg"
        fi
    fi
}

PROFILE='emqx-enterprise'
RELEASE_VSN=$(./pkg-vsn.sh "$PROFILE" --release)

## Assert package version is updated to the tag which is being created
assert_release_version() {
    local tag="$1"
    if [ "${RELEASE_VSN}" != "${tag}" ]; then
        logmsg "The release version ($RELEASE_VSN) is different from the desired git tag."
        logmsg "Updating the release version in emqx_release.hrl and Chart.yaml"
        bump_vsn "${tag#e}"
    fi
}
assert_release_version "$TAG"

## Check that the work branch has everything its remote has. For a freeze
## branch that is the only upstream to check: the release line carries changes
## that are deliberately not in this release.
SYNC_REMOTES_ARGS="--base $WORK_BRANCH"
[ "$DRYRUN" = 'yes' ] && SYNC_REMOTES_ARGS="--dryrun $SYNC_REMOTES_ARGS"
# shellcheck disable=SC2086
./scripts/rel/sync-remotes.sh $SYNC_REMOTES_ARGS

## Check if the Chart versions are in sync
./scripts/rel/check-chart-vsn.sh "$PROFILE"

## Check if app versions are bumped
./scripts/apps-version-check.exs

## Run some additional checks (e.g. some for enterprise edition only)
CHECKS_DIR="./scripts/rel/checks"
if [ -d "${CHECKS_DIR}" ]; then
    CHECKS="$(find "${CHECKS_DIR}" -name "*.sh" -print0 2>/dev/null | xargs -0)"
    for c in $CHECKS; do
        logmsg "Executing $c"
        $c
    done
fi

check_changelog() {
    local file="changes/${TAG}.en.md"
    if [ ! -f  "$file" ]; then
        logerr "Changelog file $file is missing."
        logerr "Generate it with command: ./scripts/rel/format-changelog.sh -b ${PREV_TAG} -v ${TAG} > ${file}"
        exit 1
    fi
}

check_bpapi() {
    local fname
    case "$TAG" in
        *.0)
            fname="$(echo "$TAG" | sed 's/^e//; s/\.0$//')"
            fpath="apps/emqx_bpapi/test/emqx_static_checks_data/${fname}.bpapi2"
            logmsg "Checking $fpath"
            if [ ! -f "$fpath" ]; then
                logerr "BPAPI file missing: $fpath"
                exit 1
            fi
            ;;
        *)
            true
            ;;
    esac
}

## Assert that EMQX_DASHBOARD_VERSION in Makefile is a final release,
## i.e. does not contain 'alpha' or 'beta'. Called only when cutting
## a final EMQX release; pre-release cuts (rc/alpha/beta) are allowed
## to ship a pre-release dashboard.
check_dashboard_version() {
    local dashboard_vsn
    dashboard_vsn="$(make -s print-dashboard-version)"
    if [ -z "$dashboard_vsn" ]; then
        logerr "Could not read EMQX_DASHBOARD_VERSION via 'make print-dashboard-version'"
        exit 1
    fi
    case "$dashboard_vsn" in
        *alpha*|*beta*)
            logerr "EMQX_DASHBOARD_VERSION is a pre-release ($dashboard_vsn)"
            logerr "A final EMQX release must not bundle an alpha or beta dashboard."
            logerr "Bump EMQX_DASHBOARD_VERSION in Makefile to a final release before cutting $TAG."
            exit 1
            ;;
        *)
            logmsg "EMQX_DASHBOARD_VERSION is $dashboard_vsn"
            ;;
    esac
}

case "$TAG" in
    *rc*)
        true
        ;;
    *alpha*)
        true
        ;;
    *beta*)
        true
        ;;
    *)
        check_bpapi
        check_changelog
        check_dashboard_version
        ;;
esac

if [ "$DRYRUN" = 'yes' ]; then
    logmsg "Release tag is ready to be created with command: git tag --sign -m $TAG $TAG"
else
    git tag --sign -m "$TAG" "$TAG"
    logmsg "$TAG is created and signed OK."
    logmsg "Verify it with: git tag --verify $TAG"
    logwarn "Don't forget to push the tag to emqx/emqx"
    echo "git push origin $TAG"
fi

#!/usr/bin/env bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

CE_BASES=( 'release-v43' 'release-v44' 'main-v4.3' 'main-v4.4' )
EE_BASES=( 'release-e43' 'release-e44' 'main-v4.3-enterprise' 'main-v4.4-enterprise' )

usage() {
    cat <<EOF
$0 [option]

options:

  -h|--help:
    This script works on one of the branches listed in the -b|--base option below.
    It tries to merge (by default with --ff-only option)
    upstreams branches for the current working branch.
    The uppstream branch of the current branch are as below:
    * v43: []    # no upstream for 4.3 opensource edition
    * e43: [v43] # 4.3 enterprise has 4.3 opensource as upstream
    * v44: []    # no upstream for 4.4 opensource edition (since November 2022)
    * e44: [v44] # 4.4 enterprise has all the above 3 as upstream

  -b|--base:
    The base branch of current working branch if currently is not
    on one of the following branches.
    ${CE_BASES[@]}
    ${EE_BASES[@]}

  -i|--interactive:
    With this option, the script will try to merge upstream
    branches to local working branch interactively.
    That is, there will be git prompts to edit commit messages etc.
    Without this option, the script executes 'git merge' command
    with '--ff-only' option which conveniently pulls remote
    updates if there is any, and fails when fast-forward is not possible
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

INTERACTIVE='no'
while [ "$#" -gt 0 ]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -i|--interactive)
            shift
            INTERACTIVE='yes'
            ;;
        -b|--base)
            shift
            BASE_BRANCH="$1"
            shift
            ;;
        *)
            logerr "Unknown option $1"
            exit 1
            ;;
    esac
done

if [ -f EMQX_ENTERPRISE ]; then
    IS_ENTERPRISE='yes'
    BASE_BRANCHES=( "${EE_BASES[@]}" )
else
    IS_ENTERPRISE='no'
    BASE_BRANCHES=( "${CE_BASES[@]}" )
fi

CURRENT_BRANCH="$(git branch --show-current)"
BASE_BRANCH="${BASE_BRANCH:-${CURRENT_BRANCH}}"

## check if arg1 is one of the elements in arg2-N
is_element() {
    local e match="$1"
    shift
    for e in "${@}"; do
        if [ "$e" = "$match" ]; then
            return 0
        fi
    done
    return 1
}

if ! is_element "$BASE_BRANCH" "${BASE_BRANCHES[@]}"; then
    logerr "Cannot work with branch $BASE_BRANCH"
    logerr "The base branch must be one of: ${BASE_BRANCHES[*]}"
    logerr "Change work branch to one of the above."
    logerr "OR: use -b|--base to specify from which base branch is current working branch created"
    exit 1
fi

## Find git remotes to fetch from.
##
## NOTE: For enterprise, the opensource repo must be added as a remote.
##       Because not all changes in opensource repo are synced to enterprise repo immediately.
##
## NOTE: grep -v enterprise here, but why not to match on full repo name 'emqx/emqx.git'?
##       It's because the git remote does not always end with .git
GIT_REMOTE_CE="$(git remote -v | grep 'emqx/emqx' | grep -v enterprise | grep fetch | head -1 | awk '{print $1}' || true)"
if [ -z "$GIT_REMOTE_CE" ]; then
	logerr "Cannot find git remote for emqx/emqx"
    exit 1
fi
REMOTES=( "${GIT_REMOTE_CE}" )
if [ "$IS_ENTERPRISE" = 'yes' ]; then
    GIT_REMOTE_EE="$(git remote -v | grep 'emqx/emqx-enterprise' | grep fetch | head -1 | awk '{print $1}' || true)"
    if [ -z "$GIT_REMOTE_EE" ]; then
        logerr "Cannot find git remote for emqx/emqx-enterprise"
        exit 1
    fi
    REMOTES+=( "$GIT_REMOTE_EE" )
fi

## Fetch the remotes
for remote in "${REMOTES[@]}"; do
    logwarn "Fetching from remote=${remote} (force tag sync)."
    git fetch "$remote" --tags --force
done

logmsg 'Fetched all remotes'

if [ "$INTERACTIVE" = 'yes' ]; then
    MERGE_OPTS=''
else
    ## Using --ff-only to *check* if the remote is already merged
    ## Also conveniently merged it in case it's *not* merged but can be fast-forwarded
    ## Alternative is to check with 'git merge-base'
    MERGE_OPTS='--ff-only'
fi

## Get the git remote reference of the given 'release-' or 'main-' branch
remote_ref() {
    local branch="$1"
    if is_element "$branch" "${EE_BASES[@]}"; then
        echo -n "${GIT_REMOTE_EE}/${branch} "
    else
        echo -n "${GIT_REMOTE_CE}/${branch} "
    fi
}

remote_refs() {
    local br
    for br in "${@}"; do
        remote_ref "$br"
    done
}

## Get upstream branches of the given branch
upstream_branches() {
    local base="$1"
    case "$base" in
        release-v43)
            remote_ref "$base"
            ;;
        release-v44)
            remote_refs "$base"
            ;;
        release-e43)
            remote_refs "$base" 'release-v43'
            ;;
        release-e44)
            remote_refs "$base" 'release-v44'
            ;;
        main-v4.3)
            remote_refs "$base" 'release-v43'
            ;;
        main-v4.4)
            remote_refs "$base" 'release-v44'
            ;;
        main-v4.3-enterprise)
            remote_refs "$base" 'release-e43' 'main-v4.3'
            ;;
        main-v4.4-enterprise)
            remote_refs "$base" 'release-e44' 'main-v4.4'
            ;;
    esac
}

for remote_ref in $(upstream_branches "$BASE_BRANCH"); do
    logmsg "Merging $remote_ref"
    git merge $MERGE_OPTS "$remote_ref"
done

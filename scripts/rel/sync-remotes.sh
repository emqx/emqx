#!/usr/bin/env bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

BASE_BRANCHES=( 'release-510' 'release-59' 'release-58' 'release-57' 'release-56' 'release-55' 'master' )

usage() {
    cat <<EOF
$0 [option]

options:

  -h|--help:
    This script works on one of the branches listed in the -b|--base option below.
    It tries to merge (by default with --ff-only option)
    upstreams branches for the current working branch.
    The uppstream branch of the current branch are as below:
    * release-55: []        # no upstream for 5.5 opensource edition
    * release-56: []        # no upstream for 5.6 opensource edition
    * release-57: []        # no upstream for 5.7 opensource edition
    * release-58: []        # no upstream for 5.8 opensource edition
    * release-59: []        # no upstream for 5.9
    * release-510: []       # no upstream for 5.10
    * master: [release-5x]  # sync release-5x to master

  -b|--base:
    The base branch of current working branch if currently is not
    on one of the following branches.
    ${BASE_BRANCHES[@]}

  -i|--interactive:
    With this option, the script will try to merge upstream
    branches to local working branch interactively.
    That is, there will be git prompts to edit commit messages etc.
    Without this option, the script executes 'git merge' command
    with '--ff-only' option which conveniently pulls remote
    updates if there is any, and fails when fast-forward is not possible

  --dryrun:
    Do not perform merge. Run the checks, fetch from remote,
    and show what's going to happen.
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
DRYRUN='no'
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
        --dryrun)
            shift
            DRYRUN='yes'
            ;;
        *)
            logerr "Unknown option $1"
            exit 1
            ;;
    esac
done

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
GIT_REMOTE="$(git remote -v | grep -E 'github\.com[:/]emqx/emqx(\.git)? \(fetch\)' | head -1 | awk '{print $1}' || true)"
if [ -z "$GIT_REMOTE" ]; then
	logerr "Cannot find git remote for emqx/emqx"
    exit 1
fi
REMOTES=( "${GIT_REMOTE}" )

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
    echo -n "${GIT_REMOTE}/${branch} "
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
        release-55)
            remote_ref "$base"
            ;;
        release-56)
            remote_ref "$base"
            ;;
        release-57)
            remote_ref "$base"
            ;;
        release-58)
            remote_ref "$base"
            ;;
        release-59)
            remote_ref "$base"
            ;;
        release-510)
            remote_ref "$base"
            ;;
        master)
            remote_refs "$base" 'release-55' 'release-56' 'release-57' 'release-58' 'release-59' 'release-510'
            ;;
    esac
}

for remote_ref in $(upstream_branches "$BASE_BRANCH"); do
    if [ "$DRYRUN" = 'yes' ]; then
        logmsg "Merge with this command: git merge $MERGE_OPTS $remote_ref"
    else
        logmsg "Merging $remote_ref"
        git merge $MERGE_OPTS "$remote_ref"
    fi
done

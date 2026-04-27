#!/usr/bin/env bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

usage() {
    cat <<EOF
$0 [option]

options:

  -h|--help:
    Fetch the official emqx/emqx remote, then merge the selected
    release base branch from that remote into the current branch.

  -b|--base:
    The base branch of current working branch if currently is not
    on one of the supported branches: patch-*, release-5[5-9],
    release-510, or release-6*.

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

is_supported_release_branch() {
    local branch="$1"
    [[ "$branch" == patch-* ]] || [[ "$branch" =~ ^release-(5[5-9]|510|6[0-9]+)$ ]]
}

if ! is_supported_release_branch "$BASE_BRANCH"; then
    logerr "Cannot work with branch $BASE_BRANCH"
    logerr "The release base branch must be patch-*, release-5[5-9], release-510, or release-6*"
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
logwarn "Fetching from remote=${GIT_REMOTE} (force tag sync)."
git fetch "$GIT_REMOTE" --tags --force

logmsg "Fetched ${GIT_REMOTE}"

if [ "$INTERACTIVE" = 'yes' ]; then
    MERGE_OPTS=''
else
    ## Using --ff-only to *check* if the remote is already merged
    ## Also conveniently merged it in case it's *not* merged but can be fast-forwarded
    ## Alternative is to check with 'git merge-base'
    MERGE_OPTS='--ff-only'
fi

## Get the git remote reference of the given release branch
remote_ref() {
    local branch="$1"
    echo "${GIT_REMOTE}/${branch}"
}

REMOTE_REF="$(remote_ref "$BASE_BRANCH")"
if [ "$DRYRUN" = 'yes' ]; then
    logmsg "Merge with this command: git merge $MERGE_OPTS $REMOTE_REF"
else
    logmsg "Merging $REMOTE_REF"
    git merge $MERGE_OPTS "$REMOTE_REF"
fi

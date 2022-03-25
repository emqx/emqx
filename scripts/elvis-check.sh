#!/usr/bin/env bash

## This script checks style of changed files.
## Expect argument 1 to be the git compare base.

set -euo pipefail

elvis_version='1.0.0-emqx-2'

base="${1:-}"
repo="${2:-emqx/emqx}"
REPO="${GITHUB_REPOSITORY:-${repo}}"
if [ "${base}" = "" ]; then
    echo "Usage $0 <git-compare-base-ref>"
    exit 1
fi

echo "elvis -v: $elvis_version"
echo "git diff base: $base"

if [ ! -f ./elvis ] || [ "$(./elvis -v | grep -oE '[1-9]+\.[0-9]+\.[0-9]+\-emqx-[0-9]+')" != "$elvis_version" ]; then
    curl  --silent --show-error -fLO "https://github.com/emqx/elvis/releases/download/$elvis_version/elvis"
    chmod +x ./elvis
fi

if [[ "$base" =~ [0-9a-f]{8,40} ]]; then
    # base is a commit sha1
    compare_base="$base"
else
    git remote -v
    remote="$(git remote -v | grep -E "github\.com(:|/)$REPO((\.git)|(\s))" | grep fetch | awk '{print $1}')"
    git fetch "$remote" "$base"
    compare_base="$remote/$base"
fi

git_diff() {
    git diff --name-only --diff-filter=ACMRTUXB "$compare_base"...HEAD
}

bad_file_count=0
for file in $(git_diff); do
    if [ ! -f "$file" ]; then
        # file is deleted, skip
        continue
    fi
    if [[ $file != *.erl ]]; then
        # not .erl file
        continue
    fi
    echo "$file ..."
    if ! ./elvis rock "$file" -c elvis.config; then
        bad_file_count=$(( bad_file_count + 1))
    fi
done
if [ $bad_file_count -gt 0 ]; then
    echo "elvis: $bad_file_count errors"
    exit 1
fi

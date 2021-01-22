#!/bin/bash

## This script checks style of changed files.
## Expect argument 1 to be the git compare base.

set -euo pipefail

ELVIS_VERSION='1.0.0-emqx-1'

base="${1:-}"
if [ "${base}" = "" ]; then
    echo "Usage $0 <git-compare-base-ref>"
    exit 1
fi

elvis_version="${2:-$ELVIS_VERSION}"

echo "elvis -v: $elvis_version"
echo "git diff base: $base"

if [ ! -f ./elvis ] || [ "$(./elvis -v | grep -oE '[1-9]+\.[0-9]+\.[0-9]+\-emqx-[0-9]+')" != "$elvis_version" ]; then
    curl  -fLO "https://github.com/emqx/elvis/releases/download/$elvis_version/elvis"
    chmod +x ./elvis
fi

git fetch origin "$base"

git_diff() {
    git diff --name-only origin/"$base"...HEAD
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
    if ! ./elvis rock "$file" -c elvis.config; then
        bad_file_count=$(( bad_file_count + 1))
    fi
done
if [ $bad_file_count -gt 0 ]; then
    echo "elvis: $bad_file_count errors"
    exit 1
fi

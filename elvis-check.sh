#!/bin/bash

set -euo pipefail

echo "GITHUB_BASE_REF  $1"
echo "GITHUB_HEAD_REF  $2"
echo "$GITHUB_SHA"

ELVIS_VERSION='1.0.0-emqx-1'

base=${GITHUB_BASE_REF:-$1}
elvis_version="${3:-$ELVIS_VERSION}"

echo "$elvis_version"
echo "$base"

if [ ! -f ./elvis ] || [ "$(./elvis -v | grep -oE '[1-9]+\.[0-9]+\.[0-9]+\-emqx-[0-9]+')" != "$ELVIS_VERSION" ]; then
    curl  -fLO "https://github.com/emqx/elvis/releases/download/$elvis_version/elvis"
    chmod +x ./elvis
fi

git fetch origin "$base"
git checkout -b refBranch HEAD
git diff --name-only origin/"$base" refBranch

bad_file_count=0
for n in $(git diff --name-only origin/"$base" refBranch); do

    if ! ./elvis rock "$n"; then
        bad_file_count=$(( bad_file_count + 1))
    fi
done
if [ $bad_file_count -gt 0 ]; then
    echo "elvis: $bad_file_count errors"
    exit 1
fi

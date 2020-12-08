#!/bin/bash

set -euo pipefail

echo "GITHUB_BASE_REF  $1"
echo "GITHUB_HEAD_REF  $2"

ELVIS_VERSION='1.0.0-emqx-1'

base=${GITHUB_BASE_REF:-$1}
elvis_version="${3:-$ELVIS_VERSION}"

echo "$elvis_version"
echo "$base"

if [ ! -f ./elvis ] || [ "$(./elvis -v | grep -oE '[1-9]+\.[0-9]+\.[0-9]+\-emqx-[0-9]+')" != "$ELVIS_VERSION" ]; then
    curl -LO "https://github.com/emqx/elvis/releases/download/$elvis_version/elvis"
fi

chmod +x ./elvis
/usr/bin/git fetch origin "$base"
/usr/bin/git checkout "$1"
/usr/bin/git checkout "$2"
/usr/bin/git diff --name-only "$1" "$2"

bad_file_count=0
for n in $(/usr/bin/git diff --name-only $1 $2); do
    if ! ./elvis rock $n; then
        bad_file_count=$(( bad_file_count + 1))
    fi
done
if [ $bad_file_count -gt 0 ]; then
    echo "elvis: $bad_file_count errors"
    exit 1
fi

#!/bin/bash

set -euo pipefail

base="${1:-}"
if [ "${base}" = "" ]; then
    echo "Usage $0 <git-compare-base-ref>"
    exit 1
fi

echo "git diff base: $base"

if [[ "$base" =~ [0-9a-f]{8,40} ]]; then
    # base is a commit sha1
    compare_base="$base"
else
    remote="$(git remote -v | grep -E 'github\.com(.|/)emqx' | grep fetch | awk '{print $1}')"
    git fetch "$remote" "$base"
    compare_base="$remote/$base"
fi

git_diff() {
    git diff --name-only --diff-filter=ACMRTUXB "$compare_base"...HEAD
}

#!/usr/bin/env bash

set -euo pipefail

files="$(git diff --cached --name-only | grep -E '.*\.erl' || true)"
if [[ "${files}" == '' ]]; then
    exit 0
fi
files="$(echo -e "$files" | xargs)"
# shellcheck disable=SC2086
./scripts/erlfmt -c $files

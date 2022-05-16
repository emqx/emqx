#!/usr/bin/env bash

set -euo pipefail

OPT="${1:--c}"

files_dirty="$(git diff --name-only | grep -E '.*\.erl' || true)"
files_cached="$(git diff --cached --name-only | grep -E '.*\.erl' || true)"
if [[ "${files_dirty}" == '' ]] && [[ "${files_cached}" == '' ]]; then
    exit 0
fi
files="$(echo -e "${files_dirty} \n ${files_cached}" | xargs)"
# shellcheck disable=SC2086
if ! (./scripts/erlfmt $OPT $files); then
    echo "EXECUTE 'make fmt' to fix" >&2
    exit 1
fi

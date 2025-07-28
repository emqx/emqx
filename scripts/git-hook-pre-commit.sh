#!/usr/bin/env bash

set -euo pipefail

if [ -n "${FORCE:-}" ]; then
    exit 0
fi

files_dirty="$(git diff --name-only | grep -E '.*\.erl' || true)"
files_cached="$(git diff --cached --name-only | grep -E '.*\.erl' || true)"
if [[ "${files_dirty}" == '' ]] && [[ "${files_cached}" == '' ]]; then
    exit 0
fi
files="$(echo -e "${files_dirty} \n ${files_cached}" | xargs)"

# mix format check is quite fast
which mix && mix format --check-formatted

if [ "${ERLFMT_WRITE:-false}" = 'true' ]; then
    # shellcheck disable=SC2086
    ./scripts/erlfmt -w $files
else
    # shellcheck disable=SC2086
    if ! (./scripts/erlfmt -c $files); then
        echo "EXECUTE 'make fmt-diff' to fix" >&2
        exit 1
    fi
    ./scripts/apps-version-check.exs
fi

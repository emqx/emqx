#!/usr/bin/env bash

## Used in CI. this scripts wraps format_app.py
## and check git diff

set -euo pipefail

cd -P -- "$(dirname -- "$0")/.."

APPS="$(./scripts/find-apps.sh | xargs)"
for app in ${APPS}; do
    echo "$app ..."
    ./scripts/format_app.py -a "$app" -f
done

DIFF_FILES="$(git diff --name-only)"
if [ "$DIFF_FILES" != '' ]; then
    echo "ERROR: Below files need reformat"
    echo "$DIFF_FILES"
    exit 1
fi

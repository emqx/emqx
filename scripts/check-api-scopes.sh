#!/usr/bin/env bash
set -euo pipefail

## Check that every Erlang module implementing minirest_api behaviour
## also exports a scopes/0 function (required for API key scope control).
##
## This is a source-level check (no compilation needed) suitable for
## CI sanity-checks that run before compilation.
##
## NOTE: This is an EXISTENCE check only. Per-path coverage of scopes/0
## (every path in paths/0 has an entry in the scopes/0 map, or is
## explicitly marked ?SCOPE_PUBLIC) is verified by
## scripts/check-api-scope-coverage.escript -- run after compile in the
## same CI job -- and by the CT case
## t_init_cache_no_missing_path_warnings in
## apps/emqx_management/test/emqx_mgmt_api_key_scopes_SUITE.erl.

cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

API_MODULES=$(grep -rl '\-behaviou\?r(minirest_api)' apps/*/src apps/*/src/**/ 2>/dev/null \
    | grep '\.erl$' \
    | sort -u)

if [ -z "$API_MODULES" ]; then
    echo "WARNING: no minirest_api modules found"
    exit 0
fi

TOTAL=0
MISSING=()

for f in $API_MODULES; do
    TOTAL=$((TOTAL + 1))
    # Check if the file exports scopes/0
    # Look for: -export([...scopes/0...]) or scopes() -> in function definition
    if ! grep -qE '^\s*-export\(\[.*scopes/0' "$f" && \
       ! grep -qE '^\s*scopes/0' "$f"; then
        # Double check: look for the function definition
        if ! grep -qE '^scopes\(\)' "$f"; then
            MISSING+=("$f")
        fi
    fi
done

if [ ${#MISSING[@]} -eq 0 ]; then
    echo "OK: all $TOTAL minirest_api modules export scopes/0"
    exit 0
else
    echo "ERROR: ${#MISSING[@]} of $TOTAL minirest_api module(s) missing scopes/0:"
    for f in "${MISSING[@]}"; do
        echo "  - $f"
    done
    exit 1
fi

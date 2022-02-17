#!/usr/bin/env bash

## this script prints out all test/*_SUITE.erl files of a given app,
## file names are separated by comma for rebar3 ct's `--suite` option

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "$0")/.."

TESTDIR="test"
if [ "$1" != "emqx" ]; then
    TESTDIR="$1/test"
fi
# shellcheck disable=SC2038
find "${TESTDIR}" -name "*_SUITE.erl" 2>/dev/null | xargs | tr ' ' ','

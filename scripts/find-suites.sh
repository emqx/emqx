#!/usr/bin/env bash

## this script prints out all test/*_SUITE.erl files of a given app,
## file names are separated by comma for rebar3 ct's `--suite` option

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "$0")/.."

TESTDIR="$1/test"
find "${TESTDIR}" -name "*_SUITE.erl" -print0 2>/dev/null | xargs -0 | tr ' ' ','

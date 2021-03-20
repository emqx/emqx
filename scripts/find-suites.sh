#!/usr/bin/env bash

## this script prints out all test/*_SUITE.erl files of a given app,
## file names are separated by comma for rebar3 ct's `--suite` option

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "$0")/.."

APPDIR="$1"

find "${APPDIR}/test" -name "*_SUITE.erl" | tr -d '\r' | tr '\n' ','

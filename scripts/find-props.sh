#!/usr/bin/env bash

## this script prints out all test/props/prop_*.erl files of a given app,
## file names are separated by comma for proper's `-m` option

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "$0")/.."

BASEDIR="."
if [ "$1" != "emqx" ]; then
    BASEDIR="$1"
fi

# shellcheck disable=SC2038
find "${BASEDIR}/test/props" -name "prop_*.erl" 2>/dev/null | xargs -I{} basename {} .erl | xargs | tr ' ' ','

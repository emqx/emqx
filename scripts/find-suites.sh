#!/usr/bin/env bash

## If EMQX_CT_SUITES is provided, it prints the variable.
## Otherwise this script tries to find all test/*_SUITE.erl files of then given app,
## file names are separated by comma for rebar3 ct's `--suite` option

## If SUITEGROUP is set as M_N, it prints the Nth chunk of all suites.
## SUITEGROUP default value is 1_1

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "$0")/.."

## EMQX_CT_SUITES is useful in ad-hoc runs
if [ -n "${EMQX_CT_SUITES:-}" ]; then
    echo "${EMQX_CT_SUITES}"
    exit 0
fi

TESTDIR="$1/test"
# Get the output of the find command
IFS=$'\n' read -r -d '' -a FILES < <(find "${TESTDIR}" -name "*_SUITE.erl" 2>/dev/null | sort && printf '\0')

SUITEGROUP_RAW="${SUITEGROUP:-1_1}"
SUITEGROUP="$(echo "$SUITEGROUP_RAW" | cut -d '_' -f1)"
SUITEGROUP_COUNT="$(echo "$SUITEGROUP_RAW" | cut -d '_' -f2)"

# Calculate the total number of files
FILE_COUNT=${#FILES[@]}

if (( SUITEGROUP > SUITEGROUP_COUNT )); then
    echo "Error: SUITEGROUP in the format of M_N, M must be not greater than M"
    exit 1
fi

# Calculate the number of files per group
FILES_PER_GROUP=$(( (FILE_COUNT + SUITEGROUP_COUNT - 1) / SUITEGROUP_COUNT ))
START_INDEX=$(( (SUITEGROUP - 1) * FILES_PER_GROUP ))
END_INDEX=$(( START_INDEX + FILES_PER_GROUP ))

# Print the desired suite group
sep=''
for (( i=START_INDEX; i<END_INDEX; i++ )); do
    if (( i < FILE_COUNT )); then
        echo -n "${sep}${FILES[$i]}"
        sep=','
    fi
done
echo

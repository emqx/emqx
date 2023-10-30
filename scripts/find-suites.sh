#!/usr/bin/env bash

## If EMQX_CT_SUITES or SUITES is provided, it prints the variable.
## Otherwise this script tries to find all test/*_SUITE.erl files of then given app,
## file names are separated by comma for rebar3 ct's `--suite` option

## If SUITEGROUP is set as M_N, it prints the Nth chunk of all suites.
## SUITEGROUP default value is 1_1

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "$0")/.."

DIR="$1"

complete_path() {
    local filename="$1"
    # Check if path prefix is present
    if [[ "$filename" != */* ]]; then
        filename="$DIR/test/$filename"
    fi

    # Check if suffix is present
    if [[ "$filename" != *.erl ]]; then
        filename="$filename.erl"
    fi

    echo "$filename"
}
## EMQX_CT_SUITES or SUITES is useful in ad-hoc runs
EMQX_CT_SUITES="${EMQX_CT_SUITES:-${SUITES:-}}"
if [ -n "${EMQX_CT_SUITES:-}" ]; then
    OUTPUT=""
    IFS=',' read -ra FILE_ARRAY <<< "$EMQX_CT_SUITES"
    for file in "${FILE_ARRAY[@]}"; do
        path=$(complete_path "$file")
        if [ ! -f "$path" ]; then
            echo ''
            echo "ERROR: '$path' is not a file. Ignored!" >&2
            exit 1
        fi
        if [ -z "$OUTPUT" ]; then
            OUTPUT="$path"
        else
            OUTPUT="$OUTPUT,$path"
        fi
    done
    echo "${OUTPUT}"
    exit 0
fi

TESTDIR="$DIR/test"
INTEGRATION_TESTDIR="$DIR/integration_test"
# Get the output of the find command
IFS=$'\n' read -r -d '' -a FILES < <(find "${TESTDIR}" -name "*_SUITE.erl" 2>/dev/null | sort && printf '\0')
if [[ -d "${INTEGRATION_TESTDIR}" ]]; then
  IFS=$'\n' read -r -d '' -a FILES_INTEGRATION < <(find "${INTEGRATION_TESTDIR}" -name "*_SUITE.erl" 2>/dev/null | sort && printf '\0')
fi
# shellcheck disable=SC2206
FILES+=(${FILES_INTEGRATION:-})

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

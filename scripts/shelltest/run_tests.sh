#!/usr/bin/env bash

# shellcheck disable=SC2164
cd -P -- "$(dirname -- "$0")/.."

exit_code=0

for test in shelltest/*.test; do
    echo "Running $test"
    [ -f "${test%.test}.setup" ] && /bin/sh "${test%.test}.setup"
    shelltest -c --diff --all --precise -- "$test"
    # shellcheck disable=SC2181
    if [ $? -ne 0 ]; then
        exit_code=1
    fi
    [ -f "${test%.test}.cleanup" ] && /bin/sh "${test%.test}.cleanup"
done

exit $exit_code

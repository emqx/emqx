#!/usr/bin/env bash

cd -P -- "$(dirname -- "$0")/.."

exit_code=0

for test in shelltest/*.test; do
    echo "Running $test"
    /bin/sh "${test%.test}.setup"
    shelltest -c --diff --all --precise -- "$test"
    if [ $? -ne 0 ]; then
        exit_code=1
    fi
    /bin/sh "${test%.test}.cleanup"
done

exit $exit_code

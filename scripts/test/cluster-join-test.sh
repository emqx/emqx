#!/usr/bin/env bash

## Run one mocked cluster-join scenario for test_cluster_join.py.
set -euo pipefail

# shellcheck source=scripts/test/cluster-join.sh disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/cluster-join.sh"

scenario="${1:?expected a test scenario}"
ready=false
boot_failure=false
response=joined
response_status=0
response_on_stderr=false
expected_error=""
max_retries=1

case "$scenario" in
    success)
        ;;
    expected-rejection)
        response=license-rejection
        response_status=1
        response_on_stderr=true
        expected_error=license-rejection
        ;;
    unexpected-success)
        expected_error=license-rejection
        ;;
    matching-output-success)
        response=license-rejection
        expected_error=license-rejection
        ;;
    unrelated-error)
        response=unrelated-error
        response_status=1
        expected_error=license-rejection
        max_retries=0
        ;;
    substring-error)
        response=license-rejection-extra
        response_status=1
        expected_error=license-rejection
        max_retries=0
        ;;
    regex-error)
        response=licenseXrejection
        response_status=1
        expected_error=license.rejection
        max_retries=0
        ;;
    boot-then-success)
        boot_failure=true
        ;;
    boot-then-rejection)
        boot_failure=true
        response=license-rejection
        response_status=1
        expected_error=license-rejection
        ;;
    *)
        echo "Unknown cluster-join test scenario: $scenario" >&2
        exit 2
        ;;
esac

docker() {
    if [ "$*" = "logs node2" ]; then
        echo container-logs
        return 0
    fi
    if [ "$*" != "exec node2 emqx ctl cluster join node1" ]; then
        echo "unexpected docker arguments: $*" >&2
        return 1
    fi
    if "$boot_failure" && ! "$ready"; then
        echo booting
        return 1
    fi
    if "$response_on_stderr"; then
        printf '%s\n' "$response" >&2
    else
        printf '%s\n' "$response"
    fi
    return "$response_status"
}

sleep() {
    ready=true
    echo retry
}

join_cluster node2 node1 "$max_retries" "$expected_error"

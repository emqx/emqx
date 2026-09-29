#!/usr/bin/env bash

## Retry joins while nodes start. Negative tests must match the requested rejection.
join_cluster() {
    local joiner="$1"
    local seed="$2"
    local max_retries="$3"
    local expected_error="${4:-}"
    local retries=0
    local output
    until output=$(docker exec "$joiner" emqx ctl cluster join "$seed" 2>&1); do
        if [ -n "$expected_error" ] && grep -Fxq -- "$expected_error" <<< "$output"; then
            echo "Expected cluster join rejection: $expected_error"
            return 0
        fi
        printf '%s\n' "$output"
        if [ "$retries" -ge "$max_retries" ]; then
            echo "timeout waiting for cluster join to be accepted"
            docker logs "$joiner"
            return 1
        fi
        retries=$(( retries + 1 ))
        sleep 1
    done
    printf '%s\n' "$output"
    if [ -n "$expected_error" ]; then
        echo "ERROR: cluster join succeeded, expected: $expected_error"
        return 1
    fi
}

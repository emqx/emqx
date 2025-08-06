#!/usr/bin/env bash

set -euo pipefail

[ $# -ne 2 ] && { echo "Usage: $0 host port"; exit 1; }

HOST=$1
PORT=$2
BASE_URL="http://$HOST:$PORT"

## Check if EMQX is responding
wait_for_emqx() {
    local attempts=10
    local url="$BASE_URL"/status
    while ! curl "$url" >/dev/null 2>&1; do
        if [ $attempts -eq 0 ]; then
            echo "emqx is not responding on $url"
            exit 1
        fi
        sleep 5
        attempts=$((attempts-1))
    done
}

## Get the JSON format status which is jq friendly and includes a version string
json_status() {
    local url="${BASE_URL}/status?format=json"
    local resp
    resp="$(curl -s "$url")"
    if (echo "$resp" | jq . >/dev/null 2>&1); then
        echo "$resp"
    else
        echo 'NOT_JSON'
    fi
}

## Check if the API docs are available
check_api_docs() {
    local attempts=5
    local url="$BASE_URL/api-docs/index.html"
    local status="undefined"
    while [ "$status" != "200" ]; do
        status="$(curl -s -o /dev/null -w "%{http_code}" "$url")"
        if [ "$status" != "200" ]; then
            if [ $attempts -eq 0 ]; then
                echo "emqx return non-200 responses($status) on $url"
                exit 1
            fi
            sleep 1
            attempts=$((attempts-1))
        fi
    done
}

## Check if the swagger.json contains hidden fields
## fail if it does
check_swagger_json() {
    local url="$BASE_URL/api-docs/swagger.json"
    ## assert swagger.json is valid json
    JSON="$(curl -s "$url")"
    echo "$JSON" | jq . >/dev/null
    ## assert swagger.json does not contain trie_compaction (which is a hidden field)
    if echo "$JSON" | grep -q trie_compaction; then
        echo "swagger.json contains hidden fields"
        exit 1
    fi
}

check_schema_json() {
    local name="$1"
    local expected_title="$2"
    local url="$BASE_URL/api/v5/schemas/$name"
    local json
    json="$(curl -s "$url" | jq .)"
    title="$(echo "$json" | jq -r '.info.title')"
    if [[ "$title" != "$expected_title" ]]; then
        echo "unexpected value from GET $url"
        echo "expected: $expected_title"
        echo "got     : $title"
        exit 1
    fi
}

main() {
    wait_for_emqx
    local JSON_STATUS
    JSON_STATUS="$(json_status)"
    check_api_docs
    ## The json status feature was added after hotconf API
    if [ "$JSON_STATUS" != 'NOT_JSON' ]; then
        check_swagger_json
        check_schema_json hotconf "Hot Conf Schema"
        check_schema_json actions "Actions and Sources Schema"
        check_schema_json connectors "Connectors Schema"
    fi
}

main

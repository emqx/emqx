#!/usr/bin/env bash

set -euo pipefail

[ $# -ne 2 ] && { echo "Usage: $0 host port"; exit 1; }

HOST=$1
PORT=$2
BASE_URL="http://$HOST:$PORT"
ADMIN_USER="${EMQX_SMOKE_USER:-admin}"
ADMIN_PASSWORD="${EMQX_SMOKE_PASSWORD:-public}"
ADMIN_TOKEN=""

## Login to the dashboard and cache a bearer token. The spec endpoints
## require authentication; this lets the rest of the script call them.
login() {
    local attempts=10
    local resp token
    while [ $attempts -gt 0 ]; do
        resp="$(curl -s -X POST "$BASE_URL/api/v5/login" \
            -H 'Content-Type: application/json' \
            -d "{\"username\":\"$ADMIN_USER\",\"password\":\"$ADMIN_PASSWORD\"}" || true)"
        token="$(echo "$resp" | jq -r '.token // empty' 2>/dev/null || true)"
        if [ -n "$token" ]; then
            ADMIN_TOKEN="$token"
            return 0
        fi
        sleep 1
        attempts=$((attempts-1))
    done
    echo "failed to login as $ADMIN_USER at $BASE_URL/api/v5/login: $resp"
    exit 1
}

auth_curl() {
    curl -s -H "Authorization: Bearer $ADMIN_TOKEN" "$@"
}

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

## Get the JSON format status (used as a feature probe -- the JSON format was
## introduced after the hotconf API).
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

## Check that /api-spec.html is gated by authentication and serves the
## explorer to authenticated callers.
check_api_spec() {
    local url="$BASE_URL/api-spec.html"
    local anon_status authed_status
    anon_status="$(curl -s -o /dev/null -w '%{http_code}' "$url")"
    if [ "$anon_status" != "401" ]; then
        echo "expected 401 from anonymous GET $url, got $anon_status"
        exit 1
    fi
    authed_status="$(auth_curl -o /dev/null -w '%{http_code}' "$url")"
    if [ "$authed_status" != "200" ]; then
        echo "expected 200 from authenticated GET $url, got $authed_status"
        exit 1
    fi
}

## Check that swagger.json requires authentication and that the
## authenticated response does not contain hidden fields.
check_swagger_json() {
    local url="$BASE_URL/api-docs/swagger.json"
    local anon_status
    anon_status="$(curl -s -o /dev/null -w '%{http_code}' "$url")"
    if [ "$anon_status" != "401" ]; then
        echo "expected 401 from anonymous GET $url, got $anon_status"
        exit 1
    fi
    ## assert authenticated swagger.json is valid json
    JSON="$(auth_curl "$url")"
    echo "$JSON" | jq . >/dev/null
    ## assert swagger.json does not contain trie_compaction (which is a hidden field)
    if echo "$JSON" | grep -q trie_compaction; then
        echo "swagger.json contains hidden fields"
        exit 1
    fi
}

## Check that /api-docs and /api-docs/index.html HTTP-redirect to the new
## in-tree spec explorer.
check_api_docs_redirect() {
    local path url status location
    for path in /api-docs /api-docs/index.html; do
        url="$BASE_URL$path"
        status="$(curl -s -o /dev/null -w '%{http_code}' "$url")"
        if [ "$status" != "308" ]; then
            echo "expected 308 from $url, got $status"
            exit 1
        fi
        location="$(curl -sI "$url" | awk -F': *' 'tolower($1)=="location" { sub(/\r$/,"",$2); print $2; exit }')"
        if [ "$location" != "/api-spec.html" ]; then
            echo "expected Location: /api-spec.html from $url, got '$location'"
            exit 1
        fi
    done
}

## Check that /api-docs/* subpaths other than swagger.json and index.html
## return 404 (the bundled Swagger UI assets were dropped from the release).
check_api_docs_subpaths_404() {
    local subpath status
    for subpath in /api-docs/swagger-ui.css /api-docs/swagger-ui-bundle.js /api-docs/some/junk; do
        status="$(curl -s -o /dev/null -w '%{http_code}' "$BASE_URL$subpath")"
        if [ "$status" != "404" ]; then
            echo "expected 404 from $BASE_URL$subpath, got $status"
            exit 1
        fi
    done
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
    login
    local JSON_STATUS
    JSON_STATUS="$(json_status)"
    check_api_spec
    check_api_docs_redirect
    check_api_docs_subpaths_404
    ## The json status feature was added after hotconf API
    if [ "$JSON_STATUS" != 'NOT_JSON' ]; then
        check_swagger_json
        check_schema_json hotconf "Hot Conf Schema"
        check_schema_json actions "Actions and Sources Schema"
        check_schema_json connectors "Connectors Schema"
    fi
}

main

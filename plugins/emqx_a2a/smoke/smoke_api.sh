#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
BASE_URL="${BASE_URL:-http://127.0.0.1:18083}"
LOGIN_USERNAME="${LOGIN_USERNAME:-admin}"
LOGIN_PASSWORD="${LOGIN_PASSWORD:-public}"
PLUGIN_BASE="$BASE_URL/api/v5/plugin_api/emqx_a2a"

login_token() {
    local res token
    res="$(curl -sS --fail -H 'content-type: application/json' \
        -X POST "$BASE_URL/api/v5/login" \
        -d "{\"username\":\"$LOGIN_USERNAME\",\"password\":\"$LOGIN_PASSWORD\"}")"
    token="$(printf '%s' "$res" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')"
    if [[ -z "$token" ]]; then
        echo "Failed to obtain login token from /api/v5/login" >&2
        echo "Response: $res" >&2
        return 1
    fi
    printf '%s' "$token"
}

http() {
    local method="$1"
    local path="$2"
    local body="${3:-}"
    local token="$4"
    if [[ -n "$body" ]]; then
        curl -sS --fail -H "Authorization: Bearer $token" -H 'content-type: application/json' -X "$method" "$PLUGIN_BASE$path" -d "$body"
    else
        curl -sS --fail -H "Authorization: Bearer $token" -X "$method" "$PLUGIN_BASE$path"
    fi
}

# Like http() but doesn't --fail so we can inspect 4xx/5xx responses
http_raw() {
    local method="$1"
    local path="$2"
    local body="${3:-}"
    local token="$4"
    if [[ -n "$body" ]]; then
        curl -sS -H "Authorization: Bearer $token" -H 'content-type: application/json' -X "$method" "$PLUGIN_BASE$path" -d "$body"
    else
        curl -sS -H "Authorization: Bearer $token" -X "$method" "$PLUGIN_BASE$path"
    fi
}

wait_api() {
    local _i
    for _i in $(seq 1 60); do
        if curl -sS "$BASE_URL/api/v5/status" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "EMQX API not ready at $BASE_URL" >&2
    return 1
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    if ! grep -Fq "$needle" <<<"$haystack"; then
        echo "  FAIL: expected output to contain: $needle" >&2
        echo "  actual: $haystack" >&2
        return 1
    fi
}

echo "[smoke][api] waiting EMQX API"
wait_api
TOKEN="$(login_token)"

# ---- Status ----
echo "[smoke][api] GET /status"
status_json="$(http GET /status "" "$TOKEN")"
assert_contains "$status_json" '"plugin"'
assert_contains "$status_json" '"provider"'
echo "  OK: $status_json"

# ---- Agent configs CRUD ----
echo "[smoke][api] POST /agent_configs (create hvac-advisor)"
create_json="$(http POST /agent_configs '{
    "id": "hvac-advisor",
    "system_prompt": "You are an HVAC advisor for smoke testing.",
    "model": "gpt-5-mini",
    "max_tokens": 512,
    "context_topics": ["devices/ahu-+/state"]
}' "$TOKEN")"
assert_contains "$create_json" '"hvac-advisor"'
echo "  OK"

echo "[smoke][api] GET /agent_configs (list)"
list_json="$(http GET /agent_configs "" "$TOKEN")"
assert_contains "$list_json" '"hvac-advisor"'
echo "  OK"

echo "[smoke][api] GET /agent_configs/hvac-advisor"
get_json="$(http GET /agent_configs/hvac-advisor "" "$TOKEN")"
assert_contains "$get_json" '"hvac-advisor"'
assert_contains "$get_json" '"gpt-5-mini"'
assert_contains "$get_json" '"context_topics"'
echo "  OK"

echo "[smoke][api] POST /agent_configs (create anomaly-detector)"
http POST /agent_configs '{
    "id": "anomaly-detector",
    "system_prompt": "You detect anomalies in sensor data.",
    "model": "gpt-5-mini",
    "max_tokens": 256
}' "$TOKEN" >/dev/null
echo "  OK"

echo "[smoke][api] GET /agent_configs (list both)"
list2_json="$(http GET /agent_configs "" "$TOKEN")"
assert_contains "$list2_json" '"hvac-advisor"'
assert_contains "$list2_json" '"anomaly-detector"'
echo "  OK"

echo "[smoke][api] DELETE /agent_configs/anomaly-detector"
http DELETE /agent_configs/anomaly-detector "" "$TOKEN" >/dev/null
echo "  OK"

echo "[smoke][api] GET /agent_configs/anomaly-detector (should be 404)"
not_found_json="$(http_raw GET /agent_configs/anomaly-detector "" "$TOKEN")"
assert_contains "$not_found_json" 'NOT_FOUND'
echo "  OK"

# ---- Agent config: missing id ----
echo "[smoke][api] POST /agent_configs (missing id -> 400)"
bad_json="$(http_raw POST /agent_configs '{"system_prompt": "no id"}' "$TOKEN")"
assert_contains "$bad_json" 'BAD_REQUEST'
echo "  OK"

# ---- Workflows CRUD ----
echo "[smoke][api] POST /workflows (create hvac-pipeline)"
http POST /workflows '{
    "id": "hvac-pipeline",
    "name": "HVAC Analysis Pipeline",
    "description": "Multi-step HVAC workflow",
    "tasks": [
        {"id": "analyze", "agent": "hvac-advisor", "description": "Analyze zone state", "needs": [], "next": "recommend"},
        {"id": "recommend", "agent": "hvac-advisor", "description": "Recommend actions", "needs": ["analyze"], "next": "output"}
    ]
}' "$TOKEN" >/dev/null
echo "  OK"

echo "[smoke][api] GET /workflows"
wf_list="$(http GET /workflows "" "$TOKEN")"
assert_contains "$wf_list" '"hvac-pipeline"'
echo "  OK"

echo "[smoke][api] GET /workflows/hvac-pipeline"
wf_get="$(http GET /workflows/hvac-pipeline "" "$TOKEN")"
assert_contains "$wf_get" '"hvac-pipeline"'
assert_contains "$wf_get" '"analyze"'
assert_contains "$wf_get" '"recommend"'
echo "  OK"

echo "[smoke][api] DELETE /workflows/hvac-pipeline"
http DELETE /workflows/hvac-pipeline "" "$TOKEN" >/dev/null
echo "  OK"

echo "[smoke][api] GET /workflows/hvac-pipeline (should be 404)"
wf_gone="$(http_raw GET /workflows/hvac-pipeline "" "$TOKEN")"
assert_contains "$wf_gone" 'NOT_FOUND'
echo "  OK"

# ---- Sessions (read-only, should be empty) ----
echo "[smoke][api] GET /sessions (empty)"
sessions_json="$(http GET /sessions "" "$TOKEN")"
assert_contains "$sessions_json" '"data"'
echo "  OK"

# ---- Cleanup ----
echo "[smoke][api] cleanup: DELETE /agent_configs/hvac-advisor"
http DELETE /agent_configs/hvac-advisor "" "$TOKEN" >/dev/null

echo "[smoke][api] PASS"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
BASE_URL="${BASE_URL:-http://127.0.0.1:18083}"
LOGIN_USERNAME="${LOGIN_USERNAME:-admin}"
LOGIN_PASSWORD="${LOGIN_PASSWORD:-public}"
PLUGIN_BASE="$BASE_URL/api/v5/plugin_api/emqx_unsgov"

MODEL_V1="$ROOT_DIR/plugins/emqx_unsgov/smoke/uns-model-v1.json"
MODEL_V2="$ROOT_DIR/plugins/emqx_unsgov/smoke/uns-model-v2.json"

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

wait_api() {
    local _i
    # shellcheck disable=SC2034
    for _i in $(seq 1 60); do
        if curl -sS "$BASE_URL/api/v5/status" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "EMQX API not ready at $BASE_URL" >&2
    return 1
}

echo "[smoke] waiting EMQX API"
wait_api
TOKEN="$(login_token)"

echo "[smoke] GET /status"
status_json="$(http GET /status "" "$TOKEN")"
echo "$status_json" | grep -q '"plugin"'

echo "[smoke] GET /stats"
stats_json="$(http GET /stats "" "$TOKEN")"
echo "$stats_json" | grep -q '"messages_total"'

echo "[smoke] POST /models (create+activate v1)"
post_v1_json="$(http POST /models "$(printf '{"activate":true,"model":%s}' "$(cat "$MODEL_V1")")" "$TOKEN")"
echo "$post_v1_json" | grep -q '"id":"smoke-model-v1"'
echo "$post_v1_json" | grep -q '"active":true'

echo "[smoke] GET /models (verify v1 active)"
models_v1_json="$(http GET /models "" "$TOKEN")"
echo "$models_v1_json" | grep -q '"id":"smoke-model-v1"'
echo "$models_v1_json" | grep -q '"active":true'

echo "[smoke] POST /validate/topic valid"
valid_json="$(http POST /validate/topic '{"topic":"default/Plant1/Area1/Furnaces/F1/Lines/Line1/LineControl"}' "$TOKEN")"
echo "$valid_json" | grep -q '"valid":true'

echo "[smoke] POST /validate/topic invalid"
invalid_json="$(http POST /validate/topic '{"topic":"default/Plant1/Area1/Furnaces/F1/Lines/Bad/LineControl"}' "$TOKEN")"
echo "$invalid_json" | grep -q '"valid":false'

echo "[smoke] POST /models (create v2 inactive)"
post_models_json="$(http POST /models "$(printf '{"activate":false,"model":%s}' "$(cat "$MODEL_V2")")" "$TOKEN")"
echo "$post_models_json" | grep -q '"id":"smoke-model-v2"'

echo "[smoke] GET /models"
models_json="$(http GET /models "" "$TOKEN")"
echo "$models_json" | grep -q 'smoke-model-v1'
echo "$models_json" | grep -q 'smoke-model-v2'

echo "[smoke] POST /models/:id/activate"
set +e
activate_body="$(curl -sS -H "Authorization: Bearer $TOKEN" -X POST "$PLUGIN_BASE/models/smoke-model-v2/activate")"
activate_rc=$?
set -e
if [[ "$activate_rc" -ne 0 ]]; then
    echo "Activation request failed unexpectedly: curl rc=$activate_rc" >&2
    echo "Body: $activate_body" >&2
    exit 1
fi
echo "$activate_body" | grep -q '"code":"CONFLICT"'
echo "$activate_body" | grep -q 'conflicting_topic_filter'

echo "[smoke] GET /models (v1 still active after failed v2 activation)"
models_final_json="$(http GET /models "" "$TOKEN")"
echo "$models_final_json" | grep -q '"id":"smoke-model-v1"'

echo "[smoke] PASS"

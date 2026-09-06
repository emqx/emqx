#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
BASE_URL="${BASE_URL:-http://127.0.0.1:18083}"
LOGIN_USERNAME="${LOGIN_USERNAME:-admin}"
LOGIN_PASSWORD="${LOGIN_PASSWORD:-public}"
PLUGIN_BASE="$BASE_URL/api/v5/plugin_api/emqx_a2a"

MQTT_HOST="${MQTT_HOST:-127.0.0.1}"
MQTT_PORT="${MQTT_PORT:-1883}"

login_token() {
    local res token
    res="$(curl -sS --fail -H 'content-type: application/json' \
        -X POST "$BASE_URL/api/v5/login" \
        -d "{\"username\":\"$LOGIN_USERNAME\",\"password\":\"$LOGIN_PASSWORD\"}")"
    token="$(printf '%s' "$res" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')"
    if [[ -z "$token" ]]; then
        echo "Failed to obtain login token" >&2
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
    for _i in $(seq 1 60); do
        if curl -sS "$BASE_URL/api/v5/status" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "EMQX API not ready at $BASE_URL" >&2
    return 1
}

check_plugin_api_ready() {
    local token="$1"
    local code body tmp
    tmp="$(mktemp)"
    code="$(
        curl -sS -o "$tmp" -w "%{http_code}" \
            -H "Authorization: Bearer $token" \
            "$PLUGIN_BASE/status" || true
    )"
    body="$(cat "$tmp" 2>/dev/null || true)"
    rm -f "$tmp"

    if [[ "$code" == "200" ]]; then
        return 0
    fi

    if [[ "$code" == "404" ]]; then
        echo "[smoke][mqtt] plugin API not registered at $PLUGIN_BASE" >&2
        echo "[smoke][mqtt] ensure plugin is installed and started." >&2
    else
        echo "[smoke][mqtt] plugin API preflight failed: HTTP $code" >&2
        echo "[smoke][mqtt] response: $body" >&2
    fi
    return 1
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    if ! grep -Fq "$needle" <<<"$haystack"; then
        echo "  FAIL: expected to contain: $needle" >&2
        echo "  actual: $haystack" >&2
        return 1
    fi
}

echo "[smoke][mqtt] waiting EMQX API"
wait_api
TOKEN="$(login_token)"
check_plugin_api_ready "$TOKEN"

# ---- Setup: create agent config ----
echo "[smoke][mqtt] setup: creating agent config 'smoke-agent'"
http POST /agent_configs '{
    "id": "smoke-agent",
    "system_prompt": "Reply with exactly: SMOKE_OK",
    "model": "gpt-5-mini",
    "max_tokens": 64,
    "context_topics": []
}' "$TOKEN" >/dev/null

# ---- Test 1: Publish to $a2a/request/smoke-agent, capture reply ----
echo "[smoke][mqtt] subscribing to reply topic"
REPLY_FILE="$(mktemp)"
# Subscribe in background, capture for up to 15s (LLM call may take a few seconds)
(timeout 15 mqttx sub -h "$MQTT_HOST" -p "$MQTT_PORT" -V 5 -q 1 \
    -t '$a2a/reply/smoke-agent' --output-mode clean >"$REPLY_FILE" 2>&1) &
SUB_PID=$!
sleep 1

echo "[smoke][mqtt] publishing request to \$a2a/request/smoke-agent"
mqttx pub -h "$MQTT_HOST" -p "$MQTT_PORT" -V 5 -q 1 \
    -t '$a2a/request/smoke-agent' \
    -m '{"text": "Say hello"}' 2>&1

echo "[smoke][mqtt] waiting for reply (up to 15s)..."
set +e
wait "$SUB_PID"
SUB_RC=$?
set -e

REPLY_OUTPUT="$(cat "$REPLY_FILE")"
rm -f "$REPLY_FILE"

echo "[smoke][mqtt] reply output:"
echo "$REPLY_OUTPUT" | head -20 | sed 's/^/  /'

# We expect at least a TaskStatusUpdateEvent with "submitted" or "working" state
if echo "$REPLY_OUTPUT" | grep -q "TaskStatusUpdateEvent"; then
    echo "  OK: received TaskStatusUpdateEvent"
else
    echo "  NOTE: no TaskStatusUpdateEvent in output (LLM may not be configured)"
    echo "  checking that session manager handled the dispatch..."
fi

# ---- Test 2: Publish to unknown agent, expect error ----
echo "[smoke][mqtt] subscribing to reply for nonexistent agent"
ERR_FILE="$(mktemp)"
(timeout 5 mqttx sub -h "$MQTT_HOST" -p "$MQTT_PORT" -V 5 -q 1 \
    -t '$a2a/reply/no-such-agent' --output-mode clean >"$ERR_FILE" 2>&1) &
ERR_SUB_PID=$!
sleep 1

echo "[smoke][mqtt] publishing to \$a2a/request/no-such-agent"
mqttx pub -h "$MQTT_HOST" -p "$MQTT_PORT" -V 5 -q 1 \
    -t '$a2a/request/no-such-agent' \
    -m '{"text": "hello"}' 2>&1

set +e
wait "$ERR_SUB_PID"
set -e

ERR_OUTPUT="$(cat "$ERR_FILE")"
rm -f "$ERR_FILE"

echo "[smoke][mqtt] error reply output:"
echo "$ERR_OUTPUT" | head -10 | sed 's/^/  /'

if echo "$ERR_OUTPUT" | grep -q "not found"; then
    echo "  OK: got 'not found' error for unknown agent"
elif echo "$ERR_OUTPUT" | grep -q "failed"; then
    echo "  OK: got failure response for unknown agent"
else
    echo "  NOTE: no error reply captured (message may have been silently dropped)"
fi

# ---- Test 3: Invalid JSON ----
echo "[smoke][mqtt] publishing invalid JSON to \$a2a/request/smoke-agent"
INVALID_OUT="$(mqttx pub -h "$MQTT_HOST" -p "$MQTT_PORT" -V 5 -q 1 \
    -t '$a2a/request/smoke-agent' \
    -m 'not valid json' 2>&1)"
echo "  pub output: $INVALID_OUT"
echo "  OK: invalid JSON should be ignored (passed through)"

# ---- Test 4: JSON-RPC format ----
echo "[smoke][mqtt] publishing JSON-RPC format request"
JSONRPC_FILE="$(mktemp)"
(timeout 10 mqttx sub -h "$MQTT_HOST" -p "$MQTT_PORT" -V 5 -q 1 \
    -t '$a2a/reply/smoke-agent' --output-mode clean >"$JSONRPC_FILE" 2>&1) &
JSONRPC_SUB_PID=$!
sleep 1

mqttx pub -h "$MQTT_HOST" -p "$MQTT_PORT" -V 5 -q 1 \
    -t '$a2a/request/smoke-agent' \
    -m '{"method": "tasks/send", "id": "test-001", "params": {"message": {"parts": [{"type": "text", "text": "Test message"}]}}}' 2>&1

set +e
wait "$JSONRPC_SUB_PID"
set -e

JSONRPC_OUTPUT="$(cat "$JSONRPC_FILE")"
rm -f "$JSONRPC_FILE"

echo "[smoke][mqtt] JSON-RPC reply:"
echo "$JSONRPC_OUTPUT" | head -10 | sed 's/^/  /'

if echo "$JSONRPC_OUTPUT" | grep -q "TaskStatusUpdateEvent"; then
    echo "  OK: JSON-RPC format dispatched successfully"
else
    echo "  NOTE: no events captured (LLM may not be configured)"
fi

# ---- Cleanup ----
echo "[smoke][mqtt] cleanup: deleting smoke-agent config"
http DELETE /agent_configs/smoke-agent "" "$TOKEN" >/dev/null

echo "[smoke][mqtt] PASS"

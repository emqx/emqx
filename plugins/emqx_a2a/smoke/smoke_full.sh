#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
PROFILE="${PROFILE:-emqx-enterprise}"
PLUGIN_APP="emqx_a2a"
PLUGIN_VSN="$(tr -d '[:space:]' < "$ROOT_DIR/plugins/$PLUGIN_APP/VERSION")"
PLUGIN="$PLUGIN_APP-$PLUGIN_VSN"
EMQX_BIN="$ROOT_DIR/_build/$PROFILE/rel/emqx/bin/emqx"

BASE_URL="${BASE_URL:-http://127.0.0.1:18083}"
MQTT_HOST="${MQTT_HOST:-127.0.0.1}"
MQTT_PORT="${MQTT_PORT:-1883}"

cd "$ROOT_DIR"

# ---- Detect LLM provider + API key from environment ----
A2A_PROVIDER=""
A2A_API_KEY=""

if [[ -n "${OPENAI_API_KEY:-}" ]]; then
    A2A_PROVIDER="openai"
    A2A_API_KEY="$OPENAI_API_KEY"
elif [[ -n "${ANTHROPIC_API_KEY:-}" ]]; then
    A2A_PROVIDER="anthropic"
    A2A_API_KEY="$ANTHROPIC_API_KEY"
else
    echo "Set OPENAI_API_KEY or ANTHROPIC_API_KEY to run E2E tests." >&2
    exit 1
fi

case "$A2A_PROVIDER" in
    openai)    A2A_MODEL="${A2A_MODEL:-gpt-5-mini}" ;;
    anthropic) A2A_MODEL="${A2A_MODEL:-claude-haiku-4-6}" ;;
esac

echo "[smoke] provider=$A2A_PROVIDER model=$A2A_MODEL"

# ---- Install plugin ----
echo "[smoke] installing and starting plugin via run-plugin-dev"
"$EMQX_BIN" ctl plugins stop "$PLUGIN" >/dev/null 2>&1 || true
"$EMQX_BIN" ctl plugins uninstall "$PLUGIN" >/dev/null 2>&1 || true
rm -rf "$ROOT_DIR/_build/$PROFILE/rel/emqx/plugins/$PLUGIN" || true
rm -f "$ROOT_DIR/_build/$PROFILE/rel/emqx/plugins/$PLUGIN.tar.gz" || true
PROFILE="$PROFILE" ./scripts/run-plugin-dev.sh "$PLUGIN_APP"

# ---- Push LLM config into running plugin ----
echo "[smoke] configuring plugin: provider=$A2A_PROVIDER model=$A2A_MODEL"
"$EMQX_BIN" eval "emqx_a2a_config:update(#{<<\"provider\">> => <<\"$A2A_PROVIDER\">>, <<\"api_key\">> => <<\"$A2A_API_KEY\">>, <<\"default_model\">> => <<\"$A2A_MODEL\">>, <<\"default_max_tokens\">> => 4096})." >/dev/null

echo "[smoke] ensuring dashboard user"
"$EMQX_BIN" ctl admins add smoke_admin smoke_pass "smoke user" administrator >/dev/null 2>&1 || \
    "$EMQX_BIN" ctl admins passwd smoke_admin smoke_pass >/dev/null

# ---- API smoke (no LLM needed) ----
echo "[smoke] running A2A API smoke checks"
LOGIN_USERNAME=smoke_admin LOGIN_PASSWORD=smoke_pass ./plugins/emqx_a2a/smoke/smoke_api.sh

# ---- E2E LLM test ----
echo ""
echo "=============================="
echo "[smoke] E2E LLM test"
echo "=============================="

login_token() {
    local res token
    res="$(curl -sS --fail -H 'content-type: application/json' \
        -X POST "$BASE_URL/api/v5/login" \
        -d '{"username":"smoke_admin","password":"smoke_pass"}')"
    token="$(printf '%s' "$res" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')"
    printf '%s' "$token"
}

PLUGIN_BASE="$BASE_URL/api/v5/plugin_api/emqx_a2a"

http() {
    local method="$1" path="$2" body="${3:-}" token="$4"
    if [[ -n "$body" ]]; then
        curl -sS --fail -H "Authorization: Bearer $token" -H 'content-type: application/json' \
            -X "$method" "$PLUGIN_BASE$path" -d "$body"
    else
        curl -sS --fail -H "Authorization: Bearer $token" -X "$method" "$PLUGIN_BASE$path"
    fi
}

TOKEN="$(login_token)"

# Create a simple test agent
echo "[smoke][e2e] creating test agent 'e2e-echo'"
http POST /agent_configs "{
    \"id\": \"e2e-echo\",
    \"system_prompt\": \"You are a test agent. Reply with exactly one short sentence. Do not use markdown.\",
    \"model\": \"$A2A_MODEL\",
    \"max_tokens\": 128,
    \"context_topics\": []
}" "$TOKEN" >/dev/null

# Subscribe to reply topic in background
REPLY_FILE="$(mktemp)"
echo "[smoke][e2e] subscribing to \$a2a/reply/e2e-echo"
(timeout 30 mqttx sub -h "$MQTT_HOST" -p "$MQTT_PORT" -V 5 -q 1 \
    -t '$a2a/reply/e2e-echo' --output-mode clean >"$REPLY_FILE" 2>&1) &
SUB_PID=$!
sleep 1

# Publish request
echo "[smoke][e2e] publishing request: 'Say hello world'"
mqttx pub -h "$MQTT_HOST" -p "$MQTT_PORT" -V 5 -q 1 \
    -t '$a2a/request/e2e-echo' \
    -m '{"text": "Say hello world"}' 2>&1
echo "[smoke][e2e] waiting for LLM response (up to 30s)..."

set +e
wait "$SUB_PID"
SUB_RC=$?
set -e

# Extract payloads from mqttx JSON output using jq
# mqttx outputs one JSON object per message; extract the .payload string and parse it
PAYLOADS="$(cat "$REPLY_FILE" | jq -r '.payload // empty' 2>/dev/null)"
rm -f "$REPLY_FILE"

# Parse each payload JSON and extract states
STATES="$(echo "$PAYLOADS" | jq -r '.status.state // empty' 2>/dev/null)"
MESSAGES="$(echo "$PAYLOADS" | jq -r '.status.message // empty' 2>/dev/null)"

echo "[smoke][e2e] events received:"
echo "$PAYLOADS" | jq -c '{state: .status.state, message: .status.message}' 2>/dev/null | sed 's/^/  /'

# Validate we got a submitted event
if echo "$STATES" | grep -q 'submitted'; then
    echo "[smoke][e2e] OK: received 'submitted' status"
else
    echo "[smoke][e2e] FAIL: no 'submitted' status event" >&2
    http DELETE /agent_configs/e2e-echo "" "$TOKEN" >/dev/null
    exit 1
fi

# Validate we got a completed event with actual LLM output
if echo "$STATES" | grep -q 'completed'; then
    FINAL_MSG="$(echo "$PAYLOADS" | jq -r 'select(.status.state == "completed") | .status.message' 2>/dev/null)"
    echo "[smoke][e2e] OK: received 'completed' status"
    echo "[smoke][e2e] LLM response: $FINAL_MSG"
else
    if echo "$STATES" | grep -q 'failed'; then
        FAIL_MSG="$(echo "$PAYLOADS" | jq -r 'select(.status.state == "failed") | .status.message' 2>/dev/null)"
        echo "[smoke][e2e] FAIL: task failed: $FAIL_MSG" >&2
        http DELETE /agent_configs/e2e-echo "" "$TOKEN" >/dev/null
        exit 1
    fi
    echo "[smoke][e2e] WARN: no 'completed' event (timeout or still running)"
fi

# Cleanup
echo "[smoke][e2e] cleanup: deleting e2e-echo"
http DELETE /agent_configs/e2e-echo "" "$TOKEN" >/dev/null

echo ""
echo "[smoke] ALL PASS"

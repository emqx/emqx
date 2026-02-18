#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
BASE_URL="${BASE_URL:-http://127.0.0.1:18083}"
LOGIN_USERNAME="${LOGIN_USERNAME:-smoke_admin}"
LOGIN_PASSWORD="${LOGIN_PASSWORD:-smoke_pass}"
PLUGIN_BASE="$BASE_URL/api/v5/plugin_api/emqx_uns_gate"
MODEL_V1="$ROOT_DIR/plugins/emqx_uns_gate/smoke/uns-model-v1.json"

MQTT_HOST="${MQTT_HOST:-127.0.0.1}"
MQTT_PORT="${MQTT_PORT:-1883}"

wait_api() {
    local i
    for i in $(seq 1 60); do
        if curl -sS "$BASE_URL/api/v5/status" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "EMQX API not ready at $BASE_URL" >&2
    return 1
}

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

api_post_model() {
    local token="$1"
    curl -sS --fail -H "Authorization: Bearer $token" -H 'content-type: application/json' \
      -X POST "$PLUGIN_BASE/models" \
      -d "$(printf '{"activate":true,"model":%s}' "$(cat "$MODEL_V1")")" >/dev/null
}

mqtt_pub_capture() {
    local topic="$1"
    local payload="$2"
    mqttx pub -h "$MQTT_HOST" -p "$MQTT_PORT" -V 5 -q 1 -t "$topic" -m "$payload" 2>&1
}

subscribe_capture() {
    local topic="$1"
    local timeout_s="$2"
    local outfile="$3"
    timeout "$timeout_s" mqttx sub -h "$MQTT_HOST" -p "$MQTT_PORT" -V 5 -q 1 -t "$topic" \
        --output-mode clean >"$outfile" 2>&1
}

publish_with_sub_capture() {
    local topic="$1"
    local payload="$2"
    local timeout_s="$3"
    local sub_file="$4"
    local pub_file="$5"

    (subscribe_capture "$topic" "$timeout_s" "$sub_file") &
    local sub_pid=$!
    sleep 1
    mqtt_pub_capture "$topic" "$payload" >"$pub_file"
    local pub_rc=$?
    local sub_rc
    if wait "$sub_pid"; then
        sub_rc=0
    else
        sub_rc=$?
    fi
    echo "$pub_rc:$sub_rc"
}

format_sub_output() {
    local raw="$1"
    local topic_line payload_line
    topic_line="$(printf '%s\n' "$raw" | grep -m1 '"topic":' || true)"
    payload_line="$(printf '%s\n' "$raw" | grep -m1 '"payload":' || true)"
    if [[ -z "$topic_line" && -z "$payload_line" ]]; then
        printf '<no message>'
    else
        printf '%s\n%s' "$topic_line" "$payload_line" | sed '/^$/d'
    fi
}

format_pub_output() {
    local raw="$1"
    printf '%s\n' "$raw" | sed '/^[[:space:]]\+at /d'
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    if ! grep -Fq "$needle" <<<"$haystack"; then
        echo "Expected output to contain: $needle" >&2
        echo "Actual output:" >&2
        echo "$haystack" >&2
        return 1
    fi
}

assert_not_contains() {
    local haystack="$1"
    local needle="$2"
    if grep -Fq "$needle" <<<"$haystack"; then
        echo "Expected output NOT to contain: $needle" >&2
        echo "Actual output:" >&2
        echo "$haystack" >&2
        return 1
    fi
}

assert_denied() {
    local output="$1"
    if grep -Fq "Publish error: Not authorized" <<<"$output"; then
        return 0
    fi
    if grep -Fq "Publish error" <<<"$output"; then
        return 0
    fi
    echo "Expected publish to be denied, but no publish error was found." >&2
    echo "Actual output:" >&2
    echo "$output" >&2
    return 1
}

run_case() {
    local name="$1"
    local expected="$2"
    local topic="$3"
    local payload="$4"

    echo "[smoke][mqtt] CASE: $name"
    echo "  expect : $expected"
    echo "  topic  : $topic"
    echo "  payload: $payload"
    local out pub_sub_rc pub_rc sub_rc
    local pub_file sub_file
    pub_file="$(mktemp)"
    sub_file="$(mktemp)"
    pub_sub_rc="$(publish_with_sub_capture "$topic" "$payload" 4 "$sub_file" "$pub_file")"
    pub_rc="${pub_sub_rc%%:*}"
    sub_rc="${pub_sub_rc##*:}"
    out="$(cat "$pub_file")"
    local sub_out
    sub_out="$(cat "$sub_file")"
    local sub_log
    sub_log="$(format_sub_output "$sub_out")"
    local pub_log
    pub_log="$(format_pub_output "$out")"
    rm -f "$pub_file" "$sub_file"

    echo "$pub_log" | sed 's/^/  pub    : /'
    echo "$sub_log" | sed 's/^/  sub    : /'
    echo "  rc(pub/sub): $pub_rc/$sub_rc"

    if [[ "$expected" == "allow" ]]; then
        local escaped_payload
        escaped_payload="$(printf '%s' "$payload" | sed 's/\\/\\\\/g; s/"/\\"/g')"
        assert_contains "$out" "Message published"
        assert_not_contains "$out" "Publish error: Not authorized"
        assert_contains "$sub_out" "\"topic\": \"$topic\""
        assert_contains "$sub_out" "\"payload\": \"$escaped_payload\""
    elif [[ "$expected" == "deny_auth" ]]; then
        assert_denied "$out"
    else
        assert_contains "$out" "Message published"
        assert_not_contains "$sub_out" "\"topic\": \"$topic\""
    fi
}

echo "[smoke][mqtt] waiting EMQX API"
wait_api
TOKEN="$(login_token)"

echo "[smoke][mqtt] setup gate model via runtime API (v1)"
api_post_model "$TOKEN"

run_case \
    "allow: valid topic + valid line_control payload" \
    "allow" \
    "default/Plant1/Area1/Furnaces/F1/Lines/Line1/LineControl" \
    '{"Status":"running","Mode":"auto"}'

run_case \
    "deny(topic): line_id violates regex (^Line[0-9]{1,4}$)" \
    "deny_auth" \
    "default/Plant1/Area1/Furnaces/F1/Lines/Bad/LineControl" \
    '{"Status":"running","Mode":"auto"}'

run_case \
    "deny(topic): unknown namespace branch" \
    "deny_auth" \
    "default/Plant1/Area1/Unknown/F1/Lines/Line1/LineControl" \
    '{"Status":"running","Mode":"auto"}'

run_case \
    "deny(topic): publish to intermediate node (not endpoint)" \
    "deny_auth" \
    "default/Plant1/Area1/Furnaces/F1/Lines/Line1" \
    '{"Status":"running","Mode":"auto"}'

run_case \
    "deny(payload): missing required field Mode (dropped, not delivered)" \
    "deny_drop" \
    "default/Plant1/Area1/Furnaces/F1/Lines/Line1/LineControl" \
    '{"Status":"running"}'

run_case \
    "deny(payload): enum mismatch Status=ACTIVE (dropped, not delivered)" \
    "deny_drop" \
    "default/Plant1/Area1/Furnaces/F1/Lines/Line1/LineControl" \
    '{"Status":"ACTIVE","Mode":"auto"}'

run_case \
    "deny(payload): additional property not allowed (dropped, not delivered)" \
    "deny_drop" \
    "default/Plant1/Area1/Furnaces/F1/Lines/Line1/LineControl" \
    '{"Status":"running","Mode":"auto","Extra":1}'

echo "[smoke][mqtt] PASS"

#!/usr/bin/env bash
set -euo pipefail

## Smoke test for emqx_bridge_mqtt_dq plugin.
##
## Assumes EMQX is running with the plugin started and a bridge configured
## that forwards "devices/#" to "forwarded/${topic}" on the same broker.
##
## The test publishes via mqttx to "devices/sensor/1" and subscribes to
## "forwarded/devices/sensor/1" to verify message forwarding.

BASE_URL="${BASE_URL:-http://127.0.0.1:18083}"
MQTT_HOST="${MQTT_HOST:-127.0.0.1}"
MQTT_PORT="${MQTT_PORT:-1883}"
MQTT_USERNAME="${MQTT_USERNAME:-smoke_admin}"
MQTT_PASSWORD="${MQTT_PASSWORD:-smoke_pass}"

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

subscribe_capture() {
    local topic="$1"
    local timeout_s="$2"
    local outfile="$3"
    timeout "$timeout_s" mqttx sub -h "$MQTT_HOST" -p "$MQTT_PORT" -V 5 \
        -u "$MQTT_USERNAME" -P "$MQTT_PASSWORD" -q 1 -t "$topic" \
        --output-mode clean >"$outfile" 2>&1
}

publish_and_capture() {
    local pub_topic="$1"
    local sub_topic="$2"
    local payload="$3"
    local timeout_s="${4:-5}"
    local sub_file pub_file
    sub_file="$(mktemp)"
    pub_file="$(mktemp)"

    (subscribe_capture "$sub_topic" "$timeout_s" "$sub_file") &
    local sub_pid=$!
    sleep 1

    mqttx pub -h "$MQTT_HOST" -p "$MQTT_PORT" -V 5 \
        -u "$MQTT_USERNAME" -P "$MQTT_PASSWORD" -q 1 \
        -t "$pub_topic" -m "$payload" >"$pub_file" 2>&1 || true

    local sub_rc
    if wait "$sub_pid"; then
        sub_rc=0
    else
        sub_rc=$?
    fi

    local sub_out pub_out
    sub_out="$(cat "$sub_file")"
    pub_out="$(cat "$pub_file")"
    rm -f "$sub_file" "$pub_file"

    echo "  pub: $(echo "$pub_out" | head -3 | tr '\n' ' ')"
    echo "  sub: $(echo "$sub_out" | head -5 | tr '\n' ' ')"
    echo "  sub_rc: $sub_rc"

    SUB_OUTPUT="$sub_out"
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    if ! grep -Fq "$needle" <<<"$haystack"; then
        echo "FAIL: expected output to contain: $needle" >&2
        echo "actual: $haystack" >&2
        return 1
    fi
}

assert_not_empty() {
    local value="$1"
    local label="$2"
    if [[ -z "$value" ]]; then
        echo "FAIL: $label is empty" >&2
        return 1
    fi
}

echo "[smoke][mqtt] waiting EMQX API"
wait_api

echo ""
echo "[smoke][mqtt] CASE 1: basic message forwarding"
echo "  publish to: devices/sensor/1"
echo "  expect on:  forwarded/devices/sensor/1"
SUB_OUTPUT=""
publish_and_capture \
    "devices/sensor/1" \
    "forwarded/devices/sensor/1" \
    '{"temp":22.5,"unit":"C"}'
assert_contains "$SUB_OUTPUT" '"payload"'
assert_contains "$SUB_OUTPUT" '"topic": "forwarded/devices/sensor/1"'
echo "  CASE 1: OK"

echo ""
echo "[smoke][mqtt] CASE 2: topic filter mismatch (no forwarding)"
echo "  publish to: other/topic"
echo "  expect:     NOT forwarded"
sub_file="$(mktemp)"
(subscribe_capture "forwarded/other/topic" 3 "$sub_file") &
sub_pid=$!
sleep 1
mqttx pub -h "$MQTT_HOST" -p "$MQTT_PORT" -V 5 \
    -u "$MQTT_USERNAME" -P "$MQTT_PASSWORD" -q 1 \
    -t "other/topic" -m '{"x":1}' >/dev/null 2>&1 || true
wait "$sub_pid" || true
no_fwd_out="$(cat "$sub_file")"
rm -f "$sub_file"
if echo "$no_fwd_out" | grep -Fq '"topic": "forwarded/other/topic"'; then
    echo "  FAIL: message was unexpectedly forwarded" >&2
    exit 1
fi
echo "  CASE 2: OK (no message forwarded)"

echo ""
echo "[smoke][mqtt] CASE 3: payload preserved"
echo "  publish to: devices/sensor/2"
PAYLOAD='{"status":"online","ts":1234567890}'
SUB_OUTPUT=""
publish_and_capture \
    "devices/sensor/2" \
    "forwarded/devices/sensor/2" \
    "$PAYLOAD"
assert_contains "$SUB_OUTPUT" '"payload"'
# The payload should be preserved in the forwarded message
assert_contains "$SUB_OUTPUT" 'status'
echo "  CASE 3: OK"

echo ""
echo "[smoke][mqtt] PASS"

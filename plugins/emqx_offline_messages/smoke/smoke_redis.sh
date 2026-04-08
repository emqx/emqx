#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PLUGIN_APP="emqx_offline_messages"
PLUGIN_VSN="$(tr -d '[:space:]' < "$ROOT_DIR/plugins/$PLUGIN_APP/VERSION")"
PLUGIN="$PLUGIN_APP-$PLUGIN_VSN"
PLUGIN_TAR="$ROOT_DIR/_build/plugins/$PLUGIN.tar.gz"
CERT_GEN="$ROOT_DIR/plugins/$PLUGIN_APP/test/assets/gen-certs.sh"
EMQX_API_PORT="${EMQX_API_PORT:-58083}"
EMQX_MQTT_PORT="${EMQX_MQTT_PORT:-5883}"
BASE_URL="http://127.0.0.1:$EMQX_API_PORT"
KEEP=false

IFS=: read -r API_KEY API_SECRET < "$SCRIPT_DIR/bootstrap-api-keys.txt"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -c)
            cd "$SCRIPT_DIR"
            docker compose down -v --remove-orphans 2>/dev/null || true
            exit 0
            ;;
        -k)
            KEEP=true
            shift
            ;;
        *)
            echo "Unknown flag: $1" >&2
            exit 1
            ;;
    esac
done

if [[ ! -f "$PLUGIN_TAR" ]]; then
    echo "[redis] Plugin tarball not found: $PLUGIN_TAR" >&2
    echo "[redis] Run: PROFILE=emqx-enterprise ./scripts/run-plugin-dev.sh $PLUGIN_APP" >&2
    exit 1
fi

"$CERT_GEN"

cleanup() {
    if [[ "$KEEP" == "true" ]]; then
        echo ""
        echo "[redis] containers kept running (-k). Clean up with:"
        echo "  $0 -c"
        return
    fi
    cd "$SCRIPT_DIR"
    docker compose down -v --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

wait_api() {
    local attempts=60
    while ((attempts > 0)); do
        if curl -sS "$BASE_URL/api/v5/status" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        ((attempts--))
    done
    echo "[redis] API not ready at $BASE_URL" >&2
    exit 1
}

wait_redis() {
    local attempts=60
    while ((attempts > 0)); do
        if docker compose exec -T redis redis-cli -a public PING >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        ((attempts--))
    done
    echo "[redis] Redis did not become ready" >&2
    exit 1
}

subscribe_capture() {
    local topic="$1"
    local timeout_s="$2"
    local outfile="$3"
    timeout "$timeout_s" mqttx sub -h 127.0.0.1 -p "$EMQX_MQTT_PORT" -V 5 -q 1 \
        -t "$topic" --output-mode clean >"$outfile" 2>&1
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    if ! grep -Fq "$needle" <<<"$haystack"; then
        echo "[redis] FAIL: expected output to contain: $needle" >&2
        echo "[redis] output: $haystack" >&2
        exit 1
    fi
}

put_config() {
    local body_file response_file http_code attempts=10
    body_file="$(mktemp)"
    response_file="$(mktemp)"
    cat >"$body_file"
    while ((attempts > 0)); do
        http_code="$(curl -sS -o "$response_file" -w '%{http_code}' \
            -X PUT "$BASE_URL/api/v5/plugins/$PLUGIN/config" \
            -u "$API_KEY:$API_SECRET" \
            -H 'content-type: application/json' \
            --data @"$body_file" || true)"
        if [[ "$http_code" == "200" || "$http_code" == "204" ]]; then
            rm -f "$body_file" "$response_file"
            return 0
        fi
        sleep 1
        ((attempts--))
    done
    echo "[redis] FAIL: config update returned HTTP ${http_code:-unknown}" >&2
    if [[ -s "$response_file" ]]; then
        cat "$response_file" >&2
        echo >&2
    fi
    rm -f "$body_file" "$response_file"
    exit 1
}

echo "[redis] starting EMQX + Redis"
cd "$SCRIPT_DIR"
export PLUGIN
docker compose down -v --remove-orphans 2>/dev/null || true
docker compose up -d emqx redis

wait_redis
wait_api

echo "[redis] configuring plugin"
put_config <<'JSON'
{
  "mysql": {
    "enable": false,
    "init_default_schema": false,
    "server": "mysql:3306",
    "database": "emqx",
    "pool_size": 1,
    "username": "emqx",
    "password": "public",
    "topics": [],
    "insert_message_sql": "insert into mqtt_msg(msgid, sender, topic, qos, retain, payload, arrived) values(${id}, ${from}, ${topic}, ${qos}, ${flags.retain}, ${payload}, FROM_UNIXTIME(${timestamp}/1000))",
    "delete_message_sql": "delete from mqtt_msg where msgid = ${id}",
    "select_message_sql": "select * from mqtt_msg where topic = ${topic}",
    "insert_subscription_sql": "insert into mqtt_sub(clientid, topic, qos) values(${clientid}, ${topic}, ${qos}) on duplicate key update qos = ${qos}",
    "select_subscriptions_sql": "select topic, qos from mqtt_sub where clientid = ${clientid}",
    "delete_subscription_sql": "delete from mqtt_sub where clientid = ${clientid} and topic = ${topic}",
    "batch_size": 1,
    "batch_time": 100,
    "ssl": {
      "enable": false,
      "server_name_indication": "disable",
      "cacertfile": "",
      "certfile": "",
      "keyfile": ""
    }
  },
  "redis": {
    "enable": true,
    "servers": "redis:6379",
    "redis_type": "single",
    "pool_size": 4,
    "username": "",
    "password": "public",
    "topics": ["offline/#"],
    "message_key_prefix": "mqtt:msg",
    "subscription_key_prefix": "mqtt:sub",
    "message_ttl": 7200,
    "database": 0,
    "batch_size": 1,
    "batch_time": 0,
    "ssl": {
      "enable": false,
      "server_name_indication": "disable",
      "cacertfile": "",
      "certfile": "",
      "keyfile": ""
    }
  }
}
JSON
sleep 3

echo "[redis] publishing before subscriber exists"
mqttx pub -h 127.0.0.1 -p "$EMQX_MQTT_PORT" -V 5 -q 1 \
    -t "offline/redis" -m '{"backend":"redis","seq":1}' >/dev/null

echo "[redis] subscribing and expecting replay"
SUB_FILE="$(mktemp)"
(subscribe_capture "offline/redis" 10 "$SUB_FILE") &
SUB_PID=$!
sleep 2
wait "$SUB_PID" || true
SUB_OUTPUT="$(cat "$SUB_FILE")"
rm -f "$SUB_FILE"

assert_contains "$SUB_OUTPUT" '"topic": "offline/redis"'
assert_contains "$SUB_OUTPUT" 'redis'

echo "[redis] verifying non-matching topic is not replayed"
mqttx pub -h 127.0.0.1 -p "$EMQX_MQTT_PORT" -V 5 -q 1 \
    -t "other/topic" -m '{"backend":"redis","seq":2}' >/dev/null
SUB_FILE="$(mktemp)"
(subscribe_capture "other/topic" 4 "$SUB_FILE") &
SUB_PID=$!
sleep 2
wait "$SUB_PID" || true
NONMATCH_OUTPUT="$(cat "$SUB_FILE")"
rm -f "$SUB_FILE"
if grep -Fq '"topic": "other/topic"' <<<"$NONMATCH_OUTPUT"; then
    echo "[redis] FAIL: non-matching topic was replayed" >&2
    exit 1
fi

echo "[redis] PASS"

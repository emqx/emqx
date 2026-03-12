#!/usr/bin/env bash
set -euo pipefail

## Docker-compose smoke test for emqx_bridge_mqtt_dq plugin.
##
## Proves disk-queue durability: messages published while the remote broker
## is down are persisted on disk and delivered after both brokers restart.
##
## Usage:
##   ./smoke_docker.sh           # run the full test
##   ./smoke_docker.sh -k        # keep containers running after test
##   ./smoke_docker.sh -c        # cleanup only (remove containers/volumes)

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PLUGIN_APP="emqx_bridge_mqtt_dq"
PLUGIN_VSN="$(tr -d '[:space:]' < "$ROOT_DIR/plugins/$PLUGIN_APP/VERSION")"
PLUGIN="$PLUGIN_APP-$PLUGIN_VSN"
PLUGIN_TAR="$ROOT_DIR/_build/plugins/$PLUGIN.tar.gz"
IMAGE="${IMAGE:-emqx/emqx-enterprise:6.1.1}"
EMQX_VERSION="${IMAGE##*:}"

LOCAL_API_PORT="${LOCAL_API_PORT:-38083}"
LOCAL_API="http://127.0.0.1:$LOCAL_API_PORT"
REMOTE_API="http://127.0.0.1:48083"
LOCAL_MQTT_PORT=3883
REMOTE_MQTT_PORT=4883

IFS=: read -r API_KEY API_SECRET < "$SCRIPT_DIR/bootstrap-api-keys.txt"

KEEP=false

## ---------- parse flags ----------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -c)
            cd "$SCRIPT_DIR"
            echo "[docker] cleaning up"
            docker compose down -v --remove-orphans 2>/dev/null || true
            exit 0
            ;;
        -k) KEEP=true; shift ;;
        *)  echo "Unknown flag: $1" >&2; exit 1 ;;
    esac
done

if [[ ! -f "$PLUGIN_TAR" ]]; then
    echo "[docker] Plugin tarball not found: $PLUGIN_TAR" >&2
    echo "[docker] Run: PROFILE=emqx-enterprise ./scripts/run-plugin-dev.sh $PLUGIN_APP" >&2
    exit 1
fi

## ---------- helpers ----------
cleanup() {
    if [[ "$KEEP" == "true" ]]; then
        echo ""
        echo "[docker] containers kept running (-k). Clean up with:"
        echo "  $0 -c"
        return
    fi
    echo "[docker] cleaning up"
    cd "$SCRIPT_DIR"
    docker compose down -v --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

wait_api() {
    local url="$1" _i
    # shellcheck disable=SC2034
    for _i in $(seq 1 60); do
        if curl -sS "$url/api/v5/status" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "[docker] API not ready at $url" >&2
    return 1
}

## ================================================================
## Phase 1: Start both brokers
## ================================================================
echo "[docker] Phase 1: starting brokers"
cd "$SCRIPT_DIR"
export PLUGIN
export PLUGIN_VSN
export EMQX_VERSION
docker compose down -v --remove-orphans 2>/dev/null || true
docker compose up -d

echo "[docker] waiting for remote API"
wait_api "$REMOTE_API"
echo "[docker] waiting for local API"
wait_api "$LOCAL_API"

## ================================================================
## Phase 2: Configure bridge
## ================================================================
echo "[docker] Phase 2: configure bridge"

# shellcheck disable=SC2016
HTTP_CODE=$(curl -sf -o /dev/null -w '%{http_code}' \
    -X PUT "$LOCAL_API/api/v5/plugins/$PLUGIN/config" \
    -u "$API_KEY:$API_SECRET" \
    -H 'content-type: application/json' \
    -d '{
  "remotes": {
    "cloud": {
      "server": "${EMQXDQ_REMOTE_HOST}",
      "ssl": {"enable": false}
    }
  },
  "bridges": {
    "smoke": {
      "enable": true,
      "remote": "cloud",
      "proto_ver": "v4",
      "clientid_prefix": "dq_smoke",
      "keepalive_s": 60,
      "pool_size": 2,
      "filter_topic": "devices/#",
      "remote_topic": "forwarded/${topic}",
      "remote_qos": "${qos}",
      "remote_retain": "${retain}",
      "queue": {
        "base_dir": "emqx_bridge_mqtt_dq",
        "seg_bytes": "10MB",
        "max_total_bytes": "50MB"
      }
    }
  }
}')
if [[ "$HTTP_CODE" != "200" && "$HTTP_CODE" != "204" ]]; then
    echo "[docker] FAIL: config update returned HTTP $HTTP_CODE" >&2
    exit 1
fi
echo "[docker] bridge configured (HTTP $HTTP_CODE)"
sleep 2

## ================================================================
## Phase 3: Verify live forwarding
## ================================================================
echo "[docker] Phase 3: verify live forwarding"
SUB_FILE="$(mktemp)"
(timeout 10 mqttx sub -h 127.0.0.1 -p "$REMOTE_MQTT_PORT" -V 5 -q 1 \
    -t "forwarded/devices/sensor/1" --output-mode clean >"$SUB_FILE" 2>&1) &
SUB_PID=$!
sleep 2

PUB_OUT="$(mqttx pub -h 127.0.0.1 -p "$LOCAL_MQTT_PORT" -V 5 -q 1 \
    -t "devices/sensor/1" -m '{"phase":"live","seq":1}' 2>&1 || true)"
echo "$PUB_OUT" | sed -n '1,3p'
sleep 2

# Subscriber should have received the message; give it a moment then kill
kill "$SUB_PID" 2>/dev/null || true
wait "$SUB_PID" 2>/dev/null || true

LIVE_OUT="$(cat "$SUB_FILE")"
rm -f "$SUB_FILE"
echo "  sub output: $(echo "$LIVE_OUT" | head -5 | tr '\n' ' ')"

if ! echo "$LIVE_OUT" | grep -Fq 'live'; then
    echo "[docker] FAIL: live forwarding not working" >&2
    echo "  subscriber saw: $LIVE_OUT" >&2
    exit 1
fi
echo "[docker] Phase 3: OK (live forwarding works)"

## ================================================================
## Phase 4: Stop remote, publish messages (queued on disk)
## ================================================================
echo "[docker] Phase 4: stop remote, publish messages to queue"
cd "$SCRIPT_DIR"
docker compose stop remote
sleep 1

for seq in 1 2 3 4 5; do
    PUB_OUT="$(mqttx pub -h 127.0.0.1 -p "$LOCAL_MQTT_PORT" -V 5 -q 1 \
        -t "devices/sensor/1" -m "{\"phase\":\"queued\",\"seq\":$seq}" 2>&1 || true)"
    echo "$PUB_OUT" | sed -n '1p'
done
echo "[docker] 5 messages published while remote is down"
sleep 1

## ================================================================
## Phase 5: Stop local (disk queue persists in named volume)
## ================================================================
echo "[docker] Phase 5: stop local broker"
docker compose stop local
sleep 1

## ================================================================
## Phase 6: Start remote
## ================================================================
echo "[docker] Phase 6: start remote broker"
docker compose start remote
wait_api "$REMOTE_API"

## ================================================================
## Phase 7: Start background subscriber on remote
## ================================================================
echo "[docker] Phase 7: start subscriber on remote"
SUB_FILE="$(mktemp)"
(timeout 60 mqttx sub -h 127.0.0.1 -p "$REMOTE_MQTT_PORT" -V 5 -q 1 \
    -t "forwarded/devices/sensor/1" --output-mode clean >"$SUB_FILE" 2>&1) &
SUB_PID=$!
sleep 1

## ================================================================
## Phase 8: Start local (reconnects, flushes queue)
## ================================================================
echo "[docker] Phase 8: start local broker (should flush queue)"
cd "$SCRIPT_DIR"
docker compose start local
wait_api "$LOCAL_API"

# Wait for queued messages to be flushed (up to 30s)
echo "[docker] waiting for queued messages to arrive..."
# shellcheck disable=SC2034
for _i in $(seq 1 30); do
    if [[ -f "$SUB_FILE" ]] && grep -cF '"phase\":\"queued\"' "$SUB_FILE" 2>/dev/null | grep -q '[5-9]'; then
        break
    fi
    sleep 1
done

kill "$SUB_PID" 2>/dev/null || true
wait "$SUB_PID" 2>/dev/null || true

## ================================================================
## Phase 9: Assert queued messages received
## ================================================================
echo "[docker] Phase 9: verify queued messages"
QUEUE_OUT="$(cat "$SUB_FILE")"
rm -f "$SUB_FILE"

echo "  subscriber output (last 20 lines):"
echo "$QUEUE_OUT" | tail -20 | sed 's/^/    /'

QUEUE_COUNT="$(echo "$QUEUE_OUT" | grep -cF '"phase\":\"queued\"' || true)"
echo "  queued messages received: $QUEUE_COUNT / 5"

if [[ "$QUEUE_COUNT" -lt 5 ]]; then
    echo "[docker] FAIL: expected at least 5 queued messages, got $QUEUE_COUNT" >&2
    exit 1
fi

# Verify each seq number is present
for seq in 1 2 3 4 5; do
    if ! echo "$QUEUE_OUT" | grep -Fq "\"phase\\\":\\\"queued\\\",\\\"seq\\\":$seq"; then
        echo "[docker] FAIL: missing queued message seq=$seq" >&2
        exit 1
    fi
done

echo ""
echo "[docker] PASS — all 5 queued messages delivered after restart"
echo ""

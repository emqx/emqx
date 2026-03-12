#!/usr/bin/env bash
set -euo pipefail

## Smoke test: sustained publish load with remote broker restart.
##
## Uses emqtt-bench to publish 100 msg/s for 30s through the bridge while
## the remote broker is restarted mid-way. Asserts counters afterwards.
##
## Prerequisites:
##   - Plugin tarball built in _build/plugins/
##   - docker & docker compose available
##
## Usage:
##   ./smoke_bench.sh           # run the test
##   ./smoke_bench.sh -k        # keep containers after test
##   ./smoke_bench.sh -c        # cleanup only

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PLUGIN_APP="emqx_bridge_mqtt_dq"
PLUGIN_VSN="$(tr -d '[:space:]' < "$ROOT_DIR/plugins/$PLUGIN_APP/VERSION")"
PLUGIN="$PLUGIN_APP-$PLUGIN_VSN"
PLUGIN_TAR="$ROOT_DIR/_build/plugins/$PLUGIN.tar.gz"

LOCAL_API_PORT="${LOCAL_API_PORT:-38083}"
LOCAL_API="http://127.0.0.1:$LOCAL_API_PORT"
REMOTE_API="http://127.0.0.1:48083"

IFS=: read -r API_KEY API_SECRET < "$SCRIPT_DIR/bootstrap-api-keys.txt"

BENCH_IMAGE="docker.io/emqx/emqtt-bench:latest"
BENCH_CLIENTS=10
BENCH_INTERVAL=100  # ms between publishes per client → 10 clients × 10 msg/s = 100 msg/s
BENCH_DURATION=30   # seconds
BENCH_TOTAL=$((BENCH_CLIENTS * BENCH_DURATION * 1000 / BENCH_INTERVAL))
# Allow some tolerance: messages published during remote-down may be retried
# but should not be lost (disk queue). We require at least 90% delivered.
MIN_PUBLISH=$((BENCH_TOTAL * 90 / 100))

KEEP=false

## ---------- parse flags ----------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -c)
            cd "$SCRIPT_DIR"
            echo "[bench] cleaning up"
            docker compose down -v --remove-orphans 2>/dev/null || true
            docker rm -f dq-bench 2>/dev/null || true
            exit 0
            ;;
        -k) KEEP=true; shift ;;
        *)  echo "Unknown flag: $1" >&2; exit 1 ;;
    esac
done

if [[ ! -f "$PLUGIN_TAR" ]]; then
    echo "[bench] Plugin tarball not found: $PLUGIN_TAR" >&2
    echo "[bench] Run: PROFILE=emqx-enterprise ./scripts/run-plugin-dev.sh $PLUGIN_APP" >&2
    exit 1
fi

## ---------- helpers ----------
cleanup() {
    docker rm -f dq-bench 2>/dev/null || true
    if [[ "$KEEP" == "true" ]]; then
        echo ""
        echo "[bench] containers kept running (-k). Clean up with:"
        echo "  $0 -c"
        return
    fi
    echo "[bench] cleaning up"
    cd "$SCRIPT_DIR"
    docker compose down -v --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

wait_api() {
    local url="$1" attempts=60
    while ((attempts > 0)); do
        if curl -sS "$url/api/v5/status" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        ((attempts--))
    done
    echo "[bench] API not ready at $url" >&2
    return 1
}

get_stats() {
    curl -sS "$LOCAL_API/api/v5/plugin_api/$PLUGIN_APP/stats/bench" \
        -u "$API_KEY:$API_SECRET" \
        -H 'Accept: application/json'
}

jq_field() {
    local json="$1" field="$2"
    printf '%s' "$json" | jq -r "$field"
}

## ================================================================
## Phase 1: Start both brokers
## ================================================================
echo "[bench] Phase 1: starting brokers"
cd "$SCRIPT_DIR"
export PLUGIN
docker compose down -v --remove-orphans 2>/dev/null || true
docker compose up -d

echo "[bench] waiting for remote API"
wait_api "$REMOTE_API"
echo "[bench] waiting for local API"
wait_api "$LOCAL_API"

## ================================================================
## Phase 2: Configure bridge
## ================================================================
echo "[bench] Phase 2: configure bridge"

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
    "bench": {
      "enable": true,
      "remote": "cloud",
      "proto_ver": "v4",
      "clientid_prefix": "dq_bench_",
      "keepalive_s": 10,
      "pool_size": 4,
      "filter_topic": "bench/#",
      "remote_topic": "forwarded/${topic}",
      "remote_qos": "${qos}",
      "remote_retain": "${retain}",
      "max_inflight": 64,
      "queue": {
        "base_dir": "emqx_bridge_mqtt_dq",
        "seg_bytes": "10MB",
        "max_total_bytes": "100MB"
      }
    }
  }
}')
if [[ "$HTTP_CODE" != "200" && "$HTTP_CODE" != "204" ]]; then
    echo "[bench] FAIL: config update returned HTTP $HTTP_CODE" >&2
    exit 1
fi
echo "[bench] bridge configured (HTTP $HTTP_CODE)"
sleep 3

## ================================================================
## Phase 3: Start emqtt-bench publisher
## ================================================================
echo "[bench] Phase 3: start emqtt-bench (${BENCH_CLIENTS} clients, ${BENCH_INTERVAL}ms interval, ${BENCH_TOTAL} total messages)"
docker run -d --rm \
    --name dq-bench \
    --network dq-smoke \
    "$BENCH_IMAGE" \
    pub \
    -h dq-local \
    -p 1883 \
    -c "$BENCH_CLIENTS" \
    -I "$BENCH_INTERVAL" \
    -t "bench/%i/data" \
    -s 128 \
    -q 1 \
    -L "$BENCH_TOTAL" \
    -w true

echo "[bench] publishing for ~${BENCH_DURATION}s..."

## ================================================================
## Phase 4: Wait ~10s, then restart remote broker
## ================================================================
sleep 10
echo "[bench] Phase 4: restarting remote broker"
cd "$SCRIPT_DIR"
docker compose stop remote
echo "[bench] remote stopped, waiting 5s..."
sleep 5
echo "[bench] starting remote again"
docker compose start remote
wait_api "$REMOTE_API"
echo "[bench] remote is back"

## ================================================================
## Phase 5: Wait for emqtt-bench to finish
## ================================================================
echo "[bench] Phase 5: waiting for emqtt-bench to finish..."
# emqtt-bench will exit when -L limit is reached (--rm cleans the container)
for _ in $(seq 1 120); do
    if ! docker ps -q -f name=dq-bench | grep -q .; then
        break
    fi
    sleep 1
done
# If still running, kill it
if docker ps -q -f name=dq-bench | grep -q .; then
    echo "[bench] emqtt-bench still running after timeout, stopping"
    docker rm -f dq-bench 2>/dev/null || true
fi
echo "[bench] publishing done"

## ================================================================
## Phase 6: Wait for bridge to drain
## ================================================================
echo "[bench] Phase 6: waiting for bridge to drain..."
for _ in $(seq 1 60); do
    STATS="$(get_stats)"
    BUFFERED="$(jq_field "$STATS" '.bridge.buffered // 0')"
    BACKLOG="$(jq_field "$STATS" '.bridge.backlog // 0')"
    INFLIGHT="$(jq_field "$STATS" '.bridge.inflight // 0')"
    PENDING=$((BUFFERED + BACKLOG + INFLIGHT))
    if [[ "$PENDING" -eq 0 ]]; then
        break
    fi
    echo "  pending: buffered=$BUFFERED backlog=$BACKLOG inflight=$INFLIGHT"
    sleep 2
done

## ================================================================
## Phase 7: Assert counters
## ================================================================
echo "[bench] Phase 7: assert counters"
STATS="$(get_stats)"
echo "  stats response:"
echo "$STATS" | jq .

ENQUEUE="$(jq_field "$STATS" '.bridge.enqueue')"
DEQUEUE="$(jq_field "$STATS" '.bridge.dequeue')"
PUBLISH="$(jq_field "$STATS" '.bridge.publish')"
DROP="$(jq_field "$STATS" '.bridge.drop')"
STATUS="$(jq_field "$STATS" '.bridge.status')"

echo ""
echo "  enqueue:  $ENQUEUE"
echo "  dequeue:  $DEQUEUE"
echo "  publish:  $PUBLISH"
echo "  drop:     $DROP"
echo "  status:   $STATUS"
echo "  expected: >= $MIN_PUBLISH published out of $BENCH_TOTAL"

FAIL=0

# enqueue should match total published by bench
if [[ "$ENQUEUE" -lt "$MIN_PUBLISH" ]]; then
    echo "  FAIL: enqueue ($ENQUEUE) < min expected ($MIN_PUBLISH)" >&2
    FAIL=1
fi

# publish should be at least 90% of enqueue (some may still be retrying)
if [[ "$PUBLISH" -lt "$MIN_PUBLISH" ]]; then
    echo "  FAIL: publish ($PUBLISH) < min expected ($MIN_PUBLISH)" >&2
    FAIL=1
fi

# enqueue = dequeue after drain
if [[ "$ENQUEUE" -ne "$DEQUEUE" ]]; then
    echo "  FAIL: enqueue ($ENQUEUE) != dequeue ($DEQUEUE) — bridge not fully drained" >&2
    FAIL=1
fi

# publish + drop = dequeue
PUBLISH_PLUS_DROP=$((PUBLISH + DROP))
if [[ "$PUBLISH_PLUS_DROP" -ne "$DEQUEUE" ]]; then
    echo "  FAIL: publish+drop ($PUBLISH_PLUS_DROP) != dequeue ($DEQUEUE)" >&2
    FAIL=1
fi

# status should be ok
if [[ "$STATUS" != "ok" ]]; then
    echo "  FAIL: bridge status is '$STATUS', expected 'ok'" >&2
    FAIL=1
fi

if [[ "$FAIL" -ne 0 ]]; then
    echo ""
    echo "[bench] FAIL"
    exit 1
fi

echo ""
echo "[bench] PASS — $PUBLISH/$ENQUEUE messages published, drop=$DROP, remote restarted mid-test"
echo ""

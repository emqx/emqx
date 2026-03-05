#!/usr/bin/env bash
set -euo pipefail

## Cluster smoke test for emqx_unsgov plugin.
##
## 1) Starts a 2-node EMQX cluster in Docker.
## 2) Installs and starts the plugin on both nodes.
## 3) Provisions a model via Node1 API.
## 4) Publishes MQTT messages to Node2.
## 5) Queries Prometheus metrics from Node1 and verifies cluster-aggregated counts.

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
PLUGIN_APP="emqx_unsgov"
PLUGIN_VSN="$(tr -d '[:space:]' < "$ROOT_DIR/plugins/$PLUGIN_APP/VERSION")"
PLUGIN="$PLUGIN_APP-$PLUGIN_VSN"
PLUGIN_TAR="$ROOT_DIR/_build/plugins/$PLUGIN.tar.gz"
IMAGE="${IMAGE:-emqx/emqx:6.1.1}"
MODEL_V1="$ROOT_DIR/plugins/$PLUGIN_APP/smoke/uns-model-v1.json"

NET='emqx.io'
NODE1="node1.$NET"
NODE2="node2.$NET"

NODE1_API="http://127.0.0.1:18083"
NODE2_API="http://127.0.0.1:19083"
NODE2_MQTT_PORT=2883

LOGIN_USERNAME="admin"
LOGIN_PASSWORD="public"

if [[ ! -f "$PLUGIN_TAR" ]]; then
    echo "[cluster] Plugin tarball not found: $PLUGIN_TAR" >&2
    echo "[cluster] Run: PROFILE=emqx-enterprise ./scripts/run-plugin-dev.sh $PLUGIN_APP" >&2
    exit 1
fi

cleanup() {
    echo "[cluster] cleaning up containers and network"
    docker rm -f "$NODE1" >/dev/null 2>&1 || true
    docker rm -f "$NODE2" >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
}

if [[ "${1:-}" == "-c" ]]; then
    cleanup
    exit 0
fi

## ---------- start cluster ----------
echo "[cluster] starting 2-node EMQX cluster with image $IMAGE"
"$ROOT_DIR/scripts/test/start-two-nodes-in-docker-no-proxy.sh" "$IMAGE"

## ---------- helpers ----------
wait_api() {
    # shellcheck disable=SC2034
    local url="$1" _i
    for _i in $(seq 1 60); do
        if curl -sS "$url/api/v5/status" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "[cluster] API not ready at $url" >&2
    return 1
}

login_token() {
    local url="$1"
    local res token
    res="$(curl -sS --fail -H 'content-type: application/json' \
        -X POST "$url/api/v5/login" \
        -d "{\"username\":\"$LOGIN_USERNAME\",\"password\":\"$LOGIN_PASSWORD\"}")"
    token="$(printf '%s' "$res" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')"
    if [[ -z "$token" ]]; then
        echo "[cluster] failed to get token from $url" >&2
        echo "Response: $res" >&2
        return 1
    fi
    printf '%s' "$token"
}

install_plugin() {
    local container="$1"
    echo "[cluster] installing plugin on $container"
    docker cp "$PLUGIN_TAR" "$container:/opt/emqx/plugins/"
    docker exec "$container" emqx ctl plugins install "$PLUGIN"
    docker exec "$container" emqx ctl plugins enable "$PLUGIN"
    docker exec "$container" emqx ctl plugins start "$PLUGIN"
}

## ---------- install plugin on both nodes ----------
install_plugin "$NODE1"
install_plugin "$NODE2"

## ---------- provision model via Node1 API ----------
echo "[cluster] waiting for Node1 API"
wait_api "$NODE1_API"
TOKEN1="$(login_token "$NODE1_API")"

PLUGIN_BASE="$NODE1_API/api/v5/plugin_api/$PLUGIN_APP"

echo "[cluster] resetting store and metrics on both nodes (clear bootstrap models)"
docker exec "$NODE1" emqx eval 'emqx_unsgov_store:reset(), emqx_unsgov_metrics:reset().' >/dev/null
docker exec "$NODE2" emqx eval 'emqx_unsgov_store:reset(), emqx_unsgov_metrics:reset().' >/dev/null

echo "[cluster] POST model to Node1"
post_res="$(curl -sS --fail \
    -H "Authorization: Bearer $TOKEN1" \
    -H 'content-type: application/json' \
    -X POST "$PLUGIN_BASE/models" \
    -d "$(printf '{"activate":true,"model":%s}' "$(cat "$MODEL_V1")")")"
echo "$post_res" | grep -q '"active":true'
echo "[cluster] model provisioned: $(echo "$post_res" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"

## ---------- verify model visible on Node2 ----------
echo "[cluster] waiting for Node2 API"
wait_api "$NODE2_API"
TOKEN2="$(login_token "$NODE2_API")"

PLUGIN_BASE2="$NODE2_API/api/v5/plugin_api/$PLUGIN_APP"
echo "[cluster] GET /models from Node2"
models2="$(curl -sS --fail \
    -H "Authorization: Bearer $TOKEN2" \
    "$PLUGIN_BASE2/models")"
echo "$models2" | grep -q 'smoke-model-v1'
echo "[cluster] model replicated to Node2: OK"

## ---------- publish MQTT messages to Node2 ----------
echo "[cluster] publishing MQTT messages to Node2 (port $NODE2_MQTT_PORT)"

echo "[cluster] CASE: allow - valid topic + payload -> Node2"
mqttx pub -h 127.0.0.1 -p "$NODE2_MQTT_PORT" -V 5 -q 1 \
    -t "default/Plant1/Area1/Furnaces/F1/Lines/Line1/LineControl" \
    -m '{"Status":"running","Mode":"auto"}' 2>&1 | head -5
echo "[cluster] allow: OK"

echo "[cluster] CASE: deny(topic) - bad line_id -> Node2"
deny_out="$(mqttx pub -h 127.0.0.1 -p "$NODE2_MQTT_PORT" -V 5 -q 1 \
    -t "default/Plant1/Area1/Furnaces/F1/Lines/Bad/LineControl" \
    -m '{"Status":"running","Mode":"auto"}' 2>&1 || true)"
if echo "$deny_out" | grep -q "Not authorized"; then
    echo "[cluster] deny(topic): OK"
else
    echo "[cluster] deny(topic): UNEXPECTED - expected Not authorized" >&2
    echo "$deny_out" >&2
    exit 1
fi

echo "[cluster] CASE: deny(payload) - missing Mode -> Node2"
mqttx pub -h 127.0.0.1 -p "$NODE2_MQTT_PORT" -V 5 -q 1 \
    -t "default/Plant1/Area1/Furnaces/F1/Lines/Line1/LineControl" \
    -m '{"Status":"running"}' 2>&1 | head -5
echo "[cluster] deny(payload) dropped: OK"

sleep 1

## ---------- query Prometheus from Node1 ----------
echo "[cluster] GET /metrics (Prometheus) from Node1"
prom_body="$(curl -sS --fail \
    -H "Authorization: Bearer $TOKEN1" \
    "$PLUGIN_BASE/metrics")"

echo "[cluster] verifying cluster-aggregated Prometheus metrics"

assert_prom() {
    local name="$1"
    local min_value="$2"
    local line
    line="$(echo "$prom_body" | grep -E "^${name}( |\\{)" | head -1 || true)"
    if [[ -z "$line" ]]; then
        echo "  FAIL: metric $name not found in Prometheus output" >&2
        echo "$prom_body" >&2
        exit 1
    fi
    local value
    value="$(echo "$line" | awk '{print $NF}')"
    if [[ "$value" -lt "$min_value" ]]; then
        echo "  FAIL: $name=$value, expected >= $min_value" >&2
        exit 1
    fi
    echo "  $name=$value (>= $min_value): OK"
}

assert_prom_help() {
    local name="$1"
    if ! echo "$prom_body" | grep -q "^# HELP ${name} .*cluster-aggregated"; then
        echo "  FAIL: HELP for $name missing 'cluster-aggregated'" >&2
        echo "  actual: $(echo "$prom_body" | grep "^# HELP ${name}" || echo '<missing>')" >&2
        exit 1
    fi
    echo "  HELP $name contains 'cluster-aggregated': OK"
}

# We sent 3 messages total (1 allow + 1 deny_topic + 1 deny_payload)
assert_prom "emqx_unsgov_messages_total" 3
assert_prom "emqx_unsgov_messages_allowed_total" 1
assert_prom "emqx_unsgov_messages_dropped_total" 2
assert_prom "emqx_unsgov_topic_invalid_total" 1
assert_prom "emqx_unsgov_payload_invalid_total" 1

# Per-model metrics
assert_prom "emqx_unsgov_model_messages_total{model_id=\"smoke-model-v1\"}" 2

# HELP text includes cluster-aggregated
assert_prom_help "emqx_unsgov_messages_total"
assert_prom_help "emqx_unsgov_uptime_seconds"

# TYPE for uptime should be gauge
if ! echo "$prom_body" | grep -q "^# TYPE emqx_unsgov_uptime_seconds gauge"; then
    echo "  FAIL: uptime_seconds should be TYPE gauge" >&2
    exit 1
fi
echo "  TYPE emqx_unsgov_uptime_seconds gauge: OK"

echo ""
echo "[cluster] PASS"
echo ""
echo "Containers still running. Clean up with:"
echo "  $0 -c"

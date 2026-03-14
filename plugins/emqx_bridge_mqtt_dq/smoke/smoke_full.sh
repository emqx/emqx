#!/usr/bin/env bash
set -euo pipefail

## Full smoke test for emqx_bridge_mqtt_dq plugin.
##
## 1) Installs and starts the plugin via run-plugin-dev.sh.
## 2) Ensures a local dashboard user and obtains a bearer token from /api/v5/login.
## 3) Configures a self-bridge (local -> local) via REST API.
## 4) Runs MQTT smoke tests.

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
PROFILE="${PROFILE:-emqx-enterprise}"
PLUGIN_APP="emqx_bridge_mqtt_dq"
PLUGIN_VSN="$(tr -d '[:space:]' < "$ROOT_DIR/plugins/$PLUGIN_APP/VERSION")"
PLUGIN="$PLUGIN_APP-$PLUGIN_VSN"
CLUSTER_HOCON="$ROOT_DIR/_build/$PROFILE/rel/emqx/data/configs/cluster.hocon"
BASE_URL="${BASE_URL:-http://127.0.0.1:18083}"
EMQX_BIN="$ROOT_DIR/_build/$PROFILE/rel/emqx/bin/emqx"
TLS_CERT_DIR="$ROOT_DIR/_build/$PROFILE/rel/emqx/etc/certs"
LOGIN_USERNAME="${LOGIN_USERNAME:-smoke_admin}"
LOGIN_PASSWORD="${LOGIN_PASSWORD:-smoke_pass}"

login_token() {
    local res token
    res="$(curl -sS --fail -H 'content-type: application/json' \
        -X POST "$BASE_URL/api/v5/login" \
        -d "{\"username\":\"$LOGIN_USERNAME\",\"password\":\"$LOGIN_PASSWORD\"}")"
    token="$(printf '%s' "$res" | jq -r '.token // empty')"
    if [[ -z "$token" ]]; then
        echo "Failed to obtain login token from /api/v5/login" >&2
        echo "Response: $res" >&2
        exit 1
    fi
    printf '%s' "$token"
}

reset_node_state() {
    local attempts=60
    echo "[smoke] resetting EMQX node state"
    "$EMQX_BIN" stop >/dev/null 2>&1 || true
    rm -f "$CLUSTER_HOCON"
    "$EMQX_BIN" start
    while ((attempts > 0)); do
        if "$EMQX_BIN" ctl status >/dev/null 2>&1 && curl -sS "$BASE_URL/api/v5/status" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        ((attempts--))
    done
    echo "[smoke] EMQX did not become ready after reset" >&2
    exit 1
}

cd "$ROOT_DIR"

reset_node_state

echo "[smoke] installing and starting plugin via run-plugin-dev"
"$EMQX_BIN" ctl plugins stop "$PLUGIN" >/dev/null 2>&1 || true
"$EMQX_BIN" ctl plugins uninstall "$PLUGIN" >/dev/null 2>&1 || true
rm -rf "$ROOT_DIR/_build/$PROFILE/rel/emqx/plugins/$PLUGIN" || true
rm -f "$ROOT_DIR/_build/$PROFILE/rel/emqx/plugins/$PLUGIN.tar.gz" || true
PROFILE="$PROFILE" ./scripts/run-plugin-dev.sh "$PLUGIN_APP"

echo "[smoke] ensuring dashboard user for API login"
"$EMQX_BIN" ctl admins add "$LOGIN_USERNAME" "$LOGIN_PASSWORD" "smoke user" administrator >/dev/null 2>&1 || \
    "$EMQX_BIN" ctl admins passwd "$LOGIN_USERNAME" "$LOGIN_PASSWORD" >/dev/null
TOKEN="$(login_token)"

echo "[smoke] updating plugin config via REST API"
# shellcheck disable=SC2016
HTTP_CODE=$(curl -sf -o /dev/null -w '%{http_code}' \
    -X PUT "$BASE_URL/api/v5/plugins/$PLUGIN/config" \
    -H "Authorization: Bearer $TOKEN" \
    -H 'content-type: application/json' \
    -d '{
  "bridges": {
    "smoke": {
      "enable": true,
      "remote": "loopback_tls",
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
  },
  "remotes": {
    "loopback_tls": {
      "server": "127.0.0.1:8883",
      "username": "",
      "password": "",
      "ssl": {
        "enable": true,
        "verify": "verify_none",
        "cacertfile": "'"$TLS_CERT_DIR"'/cacert.pem"
      }
    }
  }
}')
if [[ "$HTTP_CODE" != "200" && "$HTTP_CODE" != "204" ]]; then
    echo "[smoke] FAIL: config update returned HTTP $HTTP_CODE" >&2
    exit 1
fi
echo "[smoke] config updated (HTTP $HTTP_CODE)"

# Give it a moment to connect
sleep 2

echo "[smoke] running MQTT DQ Bridge smoke checks"
MQTT_USERNAME="$LOGIN_USERNAME" MQTT_PASSWORD="$LOGIN_PASSWORD" ./plugins/emqx_bridge_mqtt_dq/smoke/smoke_mqtt.sh

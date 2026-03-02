#!/usr/bin/env bash
set -euo pipefail

## Full smoke test for emqx_bridge_mqtt_dq plugin.
##
## 1) Installs and starts the plugin via run-plugin-dev.sh.
## 2) Configures a self-bridge (local -> local) via REST API.
## 3) Runs MQTT smoke tests.

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
PROFILE="${PROFILE:-emqx-enterprise}"
PLUGIN_APP="emqx_bridge_mqtt_dq"
PLUGIN_VSN="$(tr -d '[:space:]' < "$ROOT_DIR/plugins/$PLUGIN_APP/VERSION")"
PLUGIN="$PLUGIN_APP-$PLUGIN_VSN"
BASE_URL="${BASE_URL:-http://127.0.0.1:18083}"
LOGIN_USERNAME="${LOGIN_USERNAME:-smoke_admin}"
LOGIN_PASSWORD="${LOGIN_PASSWORD:-smoke_pass}"

cd "$ROOT_DIR"
echo "[smoke] installing and starting plugin via run-plugin-dev"
EMQX_BIN="$ROOT_DIR/_build/$PROFILE/rel/emqx/bin/emqx"
"$EMQX_BIN" ctl plugins stop "$PLUGIN" >/dev/null 2>&1 || true
"$EMQX_BIN" ctl plugins uninstall "$PLUGIN" >/dev/null 2>&1 || true
rm -rf "$ROOT_DIR/_build/$PROFILE/rel/emqx/plugins/$PLUGIN" || true
rm -f "$ROOT_DIR/_build/$PROFILE/rel/emqx/plugins/$PLUGIN.tar.gz" || true
PROFILE="$PROFILE" ./scripts/run-plugin-dev.sh "$PLUGIN_APP"

echo "[smoke] ensuring dashboard user for API smoke"
"$EMQX_BIN" ctl admins add "$LOGIN_USERNAME" "$LOGIN_PASSWORD" "smoke user" administrator >/dev/null 2>&1 || \
    "$EMQX_BIN" ctl admins passwd "$LOGIN_USERNAME" "$LOGIN_PASSWORD" >/dev/null

login_token() {
    curl -sf -X POST "$BASE_URL/api/v5/login" \
        -H 'content-type: application/json' \
        -d "{\"username\":\"$LOGIN_USERNAME\",\"password\":\"$LOGIN_PASSWORD\"}" \
        | grep -o '"token":"[^"]*"' | cut -d'"' -f4
}

TOKEN="$(login_token)"
if [[ -z "$TOKEN" ]]; then
    echo "[smoke] FAIL: could not login to EMQX API" >&2
    exit 1
fi

echo "[smoke] updating plugin config via REST API"
HTTP_CODE=$(curl -sf -o /dev/null -w '%{http_code}' \
    -X PUT "$BASE_URL/api/v5/plugins/$PLUGIN/config" \
    -H "Authorization: Bearer $TOKEN" \
    -H 'content-type: application/json' \
    -d '{
  "bridges": {
    "smoke": {
      "enable": true,
      "server": "127.0.0.1:1883",
      "proto_ver": "v4",
      "clientid_prefix": "dq_smoke",
      "clean_start": true,
      "keepalive_s": 60,
      "pool_size": 2,
      "filter_topic": "devices/#",
      "remote_topic": "forwarded/${topic}",
      "remote_qos": 1,
      "remote_retain": false,
      "queue": {
        "dir": "data/bridge_mqtt_dq/smoke",
        "seg_bytes": "10MB",
        "max_total_bytes": "50MB"
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
LOGIN_USERNAME="$LOGIN_USERNAME" LOGIN_PASSWORD="$LOGIN_PASSWORD" \
    ./plugins/emqx_bridge_mqtt_dq/smoke/smoke_mqtt.sh

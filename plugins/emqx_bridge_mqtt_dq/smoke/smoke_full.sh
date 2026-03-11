#!/usr/bin/env bash
set -euo pipefail

## Full smoke test for emqx_bridge_mqtt_dq plugin.
##
## 1) Installs and starts the plugin via run-plugin-dev.sh.
## 2) Bootstraps an API key so curl can use basic auth (-u admin:public).
## 3) Configures a self-bridge (local -> local) via REST API.
## 4) Runs MQTT smoke tests.

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROFILE="${PROFILE:-emqx-enterprise}"
PLUGIN_APP="emqx_bridge_mqtt_dq"
PLUGIN_VSN="$(tr -d '[:space:]' < "$ROOT_DIR/plugins/$PLUGIN_APP/VERSION")"
PLUGIN="$PLUGIN_APP-$PLUGIN_VSN"
BASE_URL="${BASE_URL:-http://127.0.0.1:18083}"
TLS_CERT_DIR="$ROOT_DIR/_build/$PROFILE/rel/emqx/etc/certs"
EMQX_BIN="$ROOT_DIR/_build/$PROFILE/rel/emqx/bin/emqx"

IFS=: read -r API_KEY API_SECRET < "$SCRIPT_DIR/bootstrap-api-keys.txt"

cd "$ROOT_DIR"

## Bootstrap API key so we can use basic auth
BOOTSTRAP_FILE="$ROOT_DIR/_build/$PROFILE/rel/emqx/bootstrap-api-keys.txt"
cp "$SCRIPT_DIR/bootstrap-api-keys.txt" "$BOOTSTRAP_FILE"
export EMQX_API_KEY__BOOTSTRAP_FILE="$BOOTSTRAP_FILE"

echo "[smoke] installing and starting plugin via run-plugin-dev"
"$EMQX_BIN" ctl plugins stop "$PLUGIN" >/dev/null 2>&1 || true
"$EMQX_BIN" ctl plugins uninstall "$PLUGIN" >/dev/null 2>&1 || true
rm -rf "$ROOT_DIR/_build/$PROFILE/rel/emqx/plugins/$PLUGIN" || true
rm -f "$ROOT_DIR/_build/$PROFILE/rel/emqx/plugins/$PLUGIN.tar.gz" || true
PROFILE="$PROFILE" ./scripts/run-plugin-dev.sh "$PLUGIN_APP"

echo "[smoke] updating plugin config via REST API"
# shellcheck disable=SC2016
HTTP_CODE=$(curl -sf -o /dev/null -w '%{http_code}' \
    -X PUT "$BASE_URL/api/v5/plugins/$PLUGIN/config" \
    -u "$API_KEY:$API_SECRET" \
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
      "remote_qos": 1,
      "remote_retain": false,
      "queue": {
        "dir": "bridge_mqtt_dq/smoke",
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
        "verify": "verify_peer",
        "sni": "localhost",
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
./plugins/emqx_bridge_mqtt_dq/smoke/smoke_mqtt.sh

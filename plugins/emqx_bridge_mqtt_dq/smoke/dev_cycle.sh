#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage:
  plugins/emqx_bridge_mqtt_dq/smoke/dev_cycle.sh [--skip-ct]

What it does:
  1) Run MQTT DQ Bridge CT (unless --skip-ct)
  2) Rebuild + reinstall plugin (with clean uninstall/remove/install cycle)
  3) Configure self-bridge via REST API and run MQTT smoke
USAGE
}

RUN_CT=1
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-ct)
            RUN_CT=0
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
    shift
done

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROFILE="${PROFILE:-emqx-enterprise}"
EMQX_BIN="$ROOT_DIR/_build/$PROFILE/rel/emqx/bin/emqx"
PLUGIN_APP="emqx_bridge_mqtt_dq"
PLUGIN_VSN="$(tr -d '[:space:]' < "$ROOT_DIR/plugins/$PLUGIN_APP/VERSION")"
PLUGIN="$PLUGIN_APP-$PLUGIN_VSN"
PLUGIN_TAR="$ROOT_DIR/_build/plugins/$PLUGIN.tar.gz"
PLUGIN_DIR="$ROOT_DIR/_build/$PROFILE/rel/emqx/plugins/$PLUGIN"
PLUGIN_DST_DIR="$ROOT_DIR/_build/$PROFILE/rel/emqx/plugins"
BASE_URL="${BASE_URL:-http://127.0.0.1:18083}"
TLS_CERT_DIR="$ROOT_DIR/_build/$PROFILE/rel/emqx/etc/certs"

IFS=: read -r API_KEY API_SECRET < "$SCRIPT_DIR/bootstrap-api-keys.txt"

cd "$ROOT_DIR"

if [[ ! -x "$EMQX_BIN" ]]; then
    echo "EMQX binary not found: $EMQX_BIN" >&2
    echo "Run: make $PROFILE" >&2
    exit 1
fi

if [[ "$RUN_CT" -eq 1 ]]; then
    echo "[dev-cycle] running CT"
    make -C "$ROOT_DIR" plugins/emqx_bridge_mqtt_dq-ct
fi

echo "[dev-cycle] rebuilding and installing plugin"
PROFILE="$PROFILE" ./scripts/run-plugin-dev.sh "$PLUGIN_APP"

if [[ ! -f "$PLUGIN_TAR" ]]; then
    echo "Plugin tarball not found: $PLUGIN_TAR" >&2
    exit 1
fi

echo "[dev-cycle] forcing clean reinstall"
"$EMQX_BIN" ctl plugins stop "$PLUGIN" >/dev/null 2>&1 || true
"$EMQX_BIN" ctl plugins disable "$PLUGIN" >/dev/null 2>&1 || true
"$EMQX_BIN" ctl plugins uninstall "$PLUGIN" >/dev/null 2>&1 || true

rm -rf "$PLUGIN_DIR"
mkdir -p "$PLUGIN_DST_DIR"
cp -f "$PLUGIN_TAR" "$PLUGIN_DST_DIR/"
"$EMQX_BIN" ctl plugins install "$PLUGIN"
"$EMQX_BIN" ctl plugins enable "$PLUGIN"
"$EMQX_BIN" ctl plugins start "$PLUGIN"

## Bootstrap API key so we can use basic auth
BOOTSTRAP_FILE="$ROOT_DIR/_build/$PROFILE/rel/emqx/bootstrap-api-keys.txt"
cp "$SCRIPT_DIR/bootstrap-api-keys.txt" "$BOOTSTRAP_FILE"
export EMQX_API_KEY__BOOTSTRAP_FILE="$BOOTSTRAP_FILE"

echo "[dev-cycle] configuring self-bridge via REST API"
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
    echo "[dev-cycle] FAIL: config update returned HTTP $HTTP_CODE" >&2
    exit 1
fi
echo "[dev-cycle] config updated (HTTP $HTTP_CODE)"

sleep 2

echo "[dev-cycle] running MQTT smoke"
./plugins/emqx_bridge_mqtt_dq/smoke/smoke_mqtt.sh

echo "[dev-cycle] PASS"

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
PROFILE="${PROFILE:-emqx-enterprise}"
EMQX_BIN="$ROOT_DIR/_build/$PROFILE/rel/emqx/bin/emqx"
PLUGIN_APP="emqx_bridge_mqtt_dq"
PLUGIN_VSN="$(tr -d '[:space:]' < "$ROOT_DIR/plugins/$PLUGIN_APP/VERSION")"
PLUGIN="$PLUGIN_APP-$PLUGIN_VSN"
PLUGIN_TAR="$ROOT_DIR/_build/plugins/$PLUGIN.tar.gz"
PLUGIN_DIR="$ROOT_DIR/_build/$PROFILE/rel/emqx/plugins/$PLUGIN"
PLUGIN_DST_DIR="$ROOT_DIR/_build/$PROFILE/rel/emqx/plugins"
CLUSTER_HOCON="$ROOT_DIR/_build/$PROFILE/rel/emqx/data/configs/cluster.hocon"
BASE_URL="${BASE_URL:-http://127.0.0.1:18083}"
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
    echo "[dev-cycle] resetting EMQX node state"
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
    echo "[dev-cycle] EMQX did not become ready after reset" >&2
    exit 1
}

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

reset_node_state

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

echo "[dev-cycle] ensuring dashboard user for API login"
"$EMQX_BIN" ctl admins add "$LOGIN_USERNAME" "$LOGIN_PASSWORD" "smoke user" administrator >/dev/null 2>&1 || \
    "$EMQX_BIN" ctl admins passwd "$LOGIN_USERNAME" "$LOGIN_PASSWORD" >/dev/null
TOKEN="$(login_token)"

echo "[dev-cycle] configuring self-bridge via REST API"
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
    echo "[dev-cycle] FAIL: config update returned HTTP $HTTP_CODE" >&2
    exit 1
fi
echo "[dev-cycle] config updated (HTTP $HTTP_CODE)"

sleep 2

echo "[dev-cycle] running MQTT smoke"
MQTT_USERNAME="$LOGIN_USERNAME" MQTT_PASSWORD="$LOGIN_PASSWORD" ./plugins/emqx_bridge_mqtt_dq/smoke/smoke_mqtt.sh

echo "[dev-cycle] PASS"

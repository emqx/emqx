#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage:
  plugins/emqx_username_quota/smoke/dev_cycle.sh

What it does:
  1) Rebuild + reinstall plugin (with clean uninstall/remove/install cycle)
  2) Run MQTT integration smoke via mqttx:
     - set max_sessions_per_username=1
     - verify first client connects and stays connected
     - verify second client with same username is rejected
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
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
MQTT_HOST="${MQTT_HOST:-127.0.0.1}"
MQTT_PORT="${MQTT_PORT:-1883}"
PLUGIN_APP="emqx_username_quota"
PLUGIN_VSN="$(tr -d '[:space:]' < "$ROOT_DIR/plugins/$PLUGIN_APP/VERSION")"
PLUGIN="$PLUGIN_APP-$PLUGIN_VSN"
PLUGIN_TAR="$ROOT_DIR/_build/plugins/$PLUGIN.tar.gz"
PLUGIN_DIR="$ROOT_DIR/_build/$PROFILE/rel/emqx/plugins/$PLUGIN"
PLUGIN_DST_DIR="$ROOT_DIR/_build/$PROFILE/rel/emqx/plugins"

cd "$ROOT_DIR"

if [[ ! -x "$EMQX_BIN" ]]; then
    echo "EMQX binary not found: $EMQX_BIN" >&2
    echo "Run: make $PROFILE" >&2
    exit 1
fi

if ! command -v mqttx >/dev/null 2>&1; then
    echo "mqttx is required but not found in PATH" >&2
    exit 1
fi

echo "[dev-cycle] rebuilding and installing plugin"
PROFILE="$PROFILE" ./scripts/run-plugin-dev.sh "$PLUGIN_APP"

if [[ ! -f "$PLUGIN_TAR" ]]; then
    echo "Plugin tarball not found: $PLUGIN_TAR" >&2
    exit 1
fi

echo "[dev-cycle] forcing clean reinstall to avoid stale unpacked plugin files"
"$EMQX_BIN" ctl plugins stop "$PLUGIN" >/dev/null 2>&1 || true
"$EMQX_BIN" ctl plugins disable "$PLUGIN" >/dev/null 2>&1 || true
"$EMQX_BIN" ctl plugins uninstall "$PLUGIN" >/dev/null 2>&1 || true
rm -rf "$PLUGIN_DIR"
mkdir -p "$PLUGIN_DST_DIR"
cp -f "$PLUGIN_TAR" "$PLUGIN_DST_DIR/"
"$EMQX_BIN" ctl plugins install "$PLUGIN"
"$EMQX_BIN" ctl plugins enable "$PLUGIN"
"$EMQX_BIN" ctl plugins start "$PLUGIN"

echo "[dev-cycle] running mqttx integration smoke"
SMOKE_USER="quota_smoke_$(date +%s)"
CID1="${PLUGIN_APP}_smoke_c1"
CID2="${PLUGIN_APP}_smoke_c2"
LOG_DIR="$ROOT_DIR/_build/plugins/smoke-logs/$PLUGIN_APP"
mkdir -p "$LOG_DIR"
LOG1="$LOG_DIR/conn1.log"
LOG2="$LOG_DIR/conn2.log"

"$EMQX_BIN" eval "io:format(\"~p~n\", [emqx_username_quota:reset()])." >/dev/null
"$EMQX_BIN" eval "io:format(\"~p~n\", [emqx_username_quota_config:update(#{<<\"max_sessions_per_username\">> => 1, <<\"username_white_list\">> => []})])." >/dev/null

mqttx conn -h "$MQTT_HOST" -p "$MQTT_PORT" -u "$SMOKE_USER" -i "$CID1" -rp 0 >"$LOG1" 2>&1 &
PID1=$!
sleep 1
if ! kill -0 "$PID1" 2>/dev/null; then
    echo "First mqttx client failed to stay connected" >&2
    cat "$LOG1" >&2 || true
    exit 1
fi

set +e
timeout 5 mqttx conn -h "$MQTT_HOST" -p "$MQTT_PORT" -u "$SMOKE_USER" -i "$CID2" -rp 0 >"$LOG2" 2>&1
RC2=$?
set -e

kill "$PID1" >/dev/null 2>&1 || true
wait "$PID1" >/dev/null 2>&1 || true

"$EMQX_BIN" eval "io:format(\"~p~n\", [emqx_username_quota:reset()])." >/dev/null
"$EMQX_BIN" eval "io:format(\"~p~n\", [emqx_username_quota_config:update(#{})])." >/dev/null

if [[ "$RC2" -eq 0 || "$RC2" -eq 124 ]]; then
    echo "Second mqttx client was not rejected as expected (rc=$RC2)" >&2
    cat "$LOG2" >&2 || true
    exit 1
fi

echo "[dev-cycle] PASS"

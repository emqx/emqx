#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage:
  plugins/emqx_a2a/smoke/dev_cycle.sh [--api] [--mqtt] [--all]

What it does:
  1) Rebuild + reinstall plugin (clean uninstall/remove/install cycle)
  2) Run API smoke (--api or --all)
  3) Run MQTT smoke (--mqtt or --all)

  Default (no flags): runs both API and MQTT smoke tests.
USAGE
}

RUN_API=0
RUN_MQTT=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --api)
            RUN_API=1
            ;;
        --mqtt)
            RUN_MQTT=1
            ;;
        --all)
            RUN_API=1
            RUN_MQTT=1
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

# Default: run both
if [[ "$RUN_API" -eq 0 && "$RUN_MQTT" -eq 0 ]]; then
    RUN_API=1
    RUN_MQTT=1
fi

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
PROFILE="${PROFILE:-emqx-enterprise}"
EMQX_BIN="$ROOT_DIR/_build/$PROFILE/rel/emqx/bin/emqx"
PLUGIN_APP="emqx_a2a"
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

echo "[dev-cycle] rebuilding and installing plugin"
"$EMQX_BIN" ctl plugins stop "$PLUGIN" >/dev/null 2>&1 || true
"$EMQX_BIN" ctl plugins disable "$PLUGIN" >/dev/null 2>&1 || true
"$EMQX_BIN" ctl plugins uninstall "$PLUGIN" >/dev/null 2>&1 || true

PROFILE="$PROFILE" ./scripts/run-plugin-dev.sh "$PLUGIN_APP"

if [[ ! -f "$PLUGIN_TAR" ]]; then
    echo "Plugin tarball not found: $PLUGIN_TAR" >&2
    exit 1
fi

echo "[dev-cycle] forcing clean reinstall"
rm -rf "$PLUGIN_DIR"
mkdir -p "$PLUGIN_DST_DIR"
cp -f "$PLUGIN_TAR" "$PLUGIN_DST_DIR/"
"$EMQX_BIN" ctl plugins install "$PLUGIN"
"$EMQX_BIN" ctl plugins enable "$PLUGIN"
"$EMQX_BIN" ctl plugins start "$PLUGIN"

echo "[dev-cycle] ensuring smoke admin"
"$EMQX_BIN" ctl admins add smoke_admin smoke_pass "smoke user" administrator >/dev/null 2>&1 || \
    "$EMQX_BIN" ctl admins passwd smoke_admin smoke_pass >/dev/null

if [[ "$RUN_API" -eq 1 ]]; then
    echo "[dev-cycle] running API smoke"
    LOGIN_USERNAME=smoke_admin LOGIN_PASSWORD=smoke_pass ./plugins/emqx_a2a/smoke/smoke_api.sh
fi

if [[ "$RUN_MQTT" -eq 1 ]]; then
    echo "[dev-cycle] running MQTT smoke"
    LOGIN_USERNAME=smoke_admin LOGIN_PASSWORD=smoke_pass ./plugins/emqx_a2a/smoke/smoke_mqtt.sh
fi

echo "[dev-cycle] PASS"

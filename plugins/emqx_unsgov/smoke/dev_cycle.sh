#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage:
  plugins/emqx_unsgov/smoke/dev_cycle.sh [--skip-ct] [--api]

What it does:
  1) Run UNS Governance CT (unless --skip-ct)
  2) Rebuild + reinstall plugin (with clean uninstall/remove/install cycle)
  3) Run MQTT smoke
  4) Optionally run API smoke (--api)
USAGE
}

RUN_CT=1
RUN_API=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-ct)
            RUN_CT=0
            ;;
        --api)
            RUN_API=1
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
PLUGIN_APP="emqx_unsgov"
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

if [[ "$RUN_CT" -eq 1 ]]; then
    echo "[dev-cycle] running CT"
    make -C "$ROOT_DIR" plugins/emqx_unsgov-ct
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
echo "[dev-cycle] nuking mnesia data to avoid stale table schemas"
"$EMQX_BIN" stop >/dev/null 2>&1 || true
rm -rf "$ROOT_DIR/_build/$PROFILE/rel/emqx/data/mnesia/"
rm -rf "$ROOT_DIR/_build/$PROFILE/rel/emqx/data/plugins/"
"$EMQX_BIN" start
"$EMQX_BIN" ctl status >/dev/null 2>&1 || sleep 5
rm -rf "$PLUGIN_DIR"
mkdir -p "$PLUGIN_DST_DIR"
cp -f "$PLUGIN_TAR" "$PLUGIN_DST_DIR/"
"$EMQX_BIN" ctl plugins install "$PLUGIN"
"$EMQX_BIN" ctl plugins enable "$PLUGIN"
"$EMQX_BIN" ctl plugins start "$PLUGIN"

echo "[dev-cycle] ensuring API smoke admin"
"$EMQX_BIN" ctl admins add smoke_admin smoke_pass "smoke user" administrator >/dev/null 2>&1 || \
    "$EMQX_BIN" ctl admins passwd smoke_admin smoke_pass >/dev/null

echo "[dev-cycle] running MQTT smoke"
echo "[dev-cycle] resetting UNS Governance store before smoke"
"$EMQX_BIN" eval "io:format(\"~p~n\", [emqx_unsgov_store:reset()])." >/dev/null
./plugins/emqx_unsgov/smoke/smoke_mqtt.sh

if [[ "$RUN_API" -eq 1 ]]; then
    echo "[dev-cycle] running API smoke"
    LOGIN_USERNAME=smoke_admin LOGIN_PASSWORD=smoke_pass ./plugins/emqx_unsgov/smoke/smoke_api.sh
fi

echo "[dev-cycle] PASS"

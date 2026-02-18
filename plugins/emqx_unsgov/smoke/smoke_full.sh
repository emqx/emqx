#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
PROFILE="${PROFILE:-emqx-enterprise}"
PLUGIN_APP="emqx_unsgov"
PLUGIN_VSN="$(tr -d '[:space:]' < "$ROOT_DIR/plugins/$PLUGIN_APP/VERSION")"
PLUGIN="$PLUGIN_APP-$PLUGIN_VSN"

cd "$ROOT_DIR"
echo "[smoke] installing and starting plugin via run-plugin-dev"
EMQX_BIN="$ROOT_DIR/_build/$PROFILE/rel/emqx/bin/emqx"
"$EMQX_BIN" ctl plugins stop "$PLUGIN" >/dev/null 2>&1 || true
"$EMQX_BIN" ctl plugins uninstall "$PLUGIN" >/dev/null 2>&1 || true
"$EMQX_BIN" eval "emqx_hooks:del('message.publish', {emqx_unsgov, on_message_publish}), ok." >/dev/null 2>&1 || true
rm -rf "$ROOT_DIR/_build/$PROFILE/rel/emqx/plugins/$PLUGIN" || true
rm -f "$ROOT_DIR/_build/$PROFILE/rel/emqx/plugins/$PLUGIN.tar.gz" || true
PROFILE="$PROFILE" ./scripts/run-plugin-dev.sh "$PLUGIN_APP"

echo "[smoke] ensuring dashboard user for API smoke"
"$EMQX_BIN" ctl admins add smoke_admin smoke_pass "smoke user" administrator >/dev/null 2>&1 || \
    "$EMQX_BIN" ctl admins passwd smoke_admin smoke_pass >/dev/null

echo "[smoke] running UNS Governance API smoke checks"
LOGIN_USERNAME=smoke_admin LOGIN_PASSWORD=smoke_pass ./plugins/emqx_unsgov/smoke/smoke_api.sh

echo "[smoke] running UNS Governance MQTTX smoke checks"
LOGIN_USERNAME=smoke_admin LOGIN_PASSWORD=smoke_pass ./plugins/emqx_unsgov/smoke/smoke_mqtt.sh

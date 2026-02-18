#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"

cd "$ROOT_DIR"
echo "[smoke] installing and starting plugin via run-plugin-dev"
EMQX_BIN="$ROOT_DIR/_build/emqx-enterprise/rel/emqx/bin/emqx"
"$EMQX_BIN" ctl plugins stop emqx_uns_gate-1.0.0 >/dev/null 2>&1 || true
"$EMQX_BIN" ctl plugins uninstall emqx_uns_gate-1.0.0 >/dev/null 2>&1 || true
"$EMQX_BIN" eval "emqx_hooks:del('message.publish', {emqx_uns_gate, on_message_publish}), ok." >/dev/null 2>&1 || true
rm -rf "$ROOT_DIR/_build/emqx-enterprise/rel/emqx/plugins/emqx_uns_gate-1.0.0" || true
rm -f "$ROOT_DIR/_build/emqx-enterprise/rel/emqx/plugins/emqx_uns_gate-1.0.0.tar.gz" || true
./scripts/run-plugin-dev.sh emqx_uns_gate

echo "[smoke] ensuring dashboard user for API smoke"
"$EMQX_BIN" ctl admins add smoke_admin smoke_pass "smoke user" administrator >/dev/null 2>&1 || \
    "$EMQX_BIN" ctl admins passwd smoke_admin smoke_pass >/dev/null

echo "[smoke] running UNS Gate API smoke checks"
LOGIN_USERNAME=smoke_admin LOGIN_PASSWORD=smoke_pass ./plugins/emqx_uns_gate/smoke/smoke_api.sh

echo "[smoke] running UNS Gate MQTTX smoke checks"
LOGIN_USERNAME=smoke_admin LOGIN_PASSWORD=smoke_pass ./plugins/emqx_uns_gate/smoke/smoke_mqtt.sh

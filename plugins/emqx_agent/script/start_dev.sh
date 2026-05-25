#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
PROFILE="${PROFILE:-emqx-enterprise}"
EMQX_DIR="$ROOT_DIR/_build/$PROFILE/rel/emqx"
EMQX_BIN="$EMQX_DIR/bin/emqx"
EMQX_HOST="${EMQX_HOST:-localhost}"

cd "$ROOT_DIR"
if [[ -x "$EMQX_BIN" ]]; then
    echo "Stopping previous EMQX node if running..."
    "$EMQX_BIN" stop >/dev/null 2>&1 || true
fi

rm -rf "$ROOT_DIR/_build/plugins"
rm -rf "$EMQX_DIR/plugins"
rm -rf "$EMQX_DIR/data/plugins"

PROFILE="$PROFILE" make
PROFILE="$PROFILE" ./scripts/run-plugin-dev.sh emqx_agent "$@"

echo "Agent UI: http://$EMQX_HOST:18083/api/v5/plugin_api/emqx_agent/ui"

exec tail -F "$EMQX_DIR/log/emqx.log".{1,2,3,4,5}

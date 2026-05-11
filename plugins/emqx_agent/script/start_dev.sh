#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
PROFILE="${PROFILE:-emqx-enterprise}"
EMQX_BIN="$ROOT_DIR/_build/$PROFILE/rel/emqx/bin/emqx"

cd "$ROOT_DIR"
if [[ -x "$EMQX_BIN" ]]; then
    echo "Stopping previous EMQX node if running..."
    "$EMQX_BIN" stop >/dev/null 2>&1 || true
fi

PROFILE="$PROFILE" ./scripts/run-plugin-dev.sh emqx_agent "$@"

echo "Agent UI: http://localhost:18083/api/v5/plugin_api/emqx_agent/ui"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
PROFILE="${PROFILE:-emqx-enterprise}"

cd "$ROOT_DIR"
PROFILE="$PROFILE" ./scripts/run-plugin-dev.sh emqx_agent "$@"

echo "Agent UI: http://localhost:18083/api/v5/plugin_api/emqx_agent/ui"

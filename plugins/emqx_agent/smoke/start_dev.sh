#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
PROFILE="${PROFILE:-emqx-enterprise}"
EMQX_BIN="$ROOT_DIR/_build/$PROFILE/rel/emqx/bin/emqx"

if [[ ! -x "$EMQX_BIN" ]]; then
    echo "EMQX binary not found: $EMQX_BIN" >&2
    echo "Run: make $PROFILE" >&2
    exit 1
fi

if "$EMQX_BIN" ping >/dev/null 2>&1; then
    echo "EMQX already running for profile $PROFILE"
else
    echo "Starting EMQX for profile $PROFILE"
    "$EMQX_BIN" start
fi

"$EMQX_BIN" ctl status
echo "Dashboard: http://localhost:18083"

#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage:
  plugins/emqx_agent/smoke/dev_cycle.sh [--skip-ct] [--attach]

What it does:
  1) Run EMQX Agent plugin CT with its docker-ct services unless --skip-ct is set
  2) Start the local EMQX release node if needed
  3) Build, install, enable, and start the plugin in dev mode
USAGE
}

RUN_CT=1
ATTACH=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-ct)
            RUN_CT=0
            ;;
        --attach)
            ATTACH=1
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

cd "$ROOT_DIR"

if [[ "$RUN_CT" -eq 1 ]]; then
    echo "[dev-cycle] running CT"
    ./scripts/ct/run.sh --app "plugins/emqx_agent" -- env TERM=dumb make plugins/emqx_agent-ct
fi

echo "[dev-cycle] starting EMQX"
PROFILE="$PROFILE" plugins/emqx_agent/smoke/start_dev.sh

echo "[dev-cycle] loading plugin"
if [[ "$ATTACH" -eq 1 ]]; then
    PROFILE="$PROFILE" plugins/emqx_agent/smoke/load_dev.sh --attach
else
    PROFILE="$PROFILE" plugins/emqx_agent/smoke/load_dev.sh
fi

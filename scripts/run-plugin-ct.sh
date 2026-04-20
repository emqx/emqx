#!/usr/bin/env bash

## Run Common Test suites for a single monorepo plugin.
## Each plugin is a self-contained rebar3 project; run rebar3 ct
## from inside the plugin directory.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $(basename "$0") <plugin_app>" >&2
    exit 1
fi

APP="$1"

if [[ ! "$APP" =~ ^[a-z][a-z0-9_]*$ ]]; then
    echo "Invalid plugin app name: $APP" >&2
    exit 1
fi

ROOT_DIR="$(cd -P -- "$(dirname -- "$0")/.." && pwd)"
PLUGIN_DIR="$ROOT_DIR/plugins/$APP"

if [[ ! -d "$PLUGIN_DIR" ]]; then
    echo "Error: No such plugin app: plugins/$APP" >&2
    exit 1
fi

if [[ ! -d "$PLUGIN_DIR/test" ]]; then
    echo "No test/ dir under plugins/$APP; skipping"
    exit 0
fi

REBAR="$ROOT_DIR/rebar3"
if [[ ! -x "$REBAR" ]]; then
    "$ROOT_DIR/scripts/ensure-rebar3.sh"
fi

echo "Running plugin CT: $APP"
cd "$PLUGIN_DIR"
exec "$REBAR" as test ct -v

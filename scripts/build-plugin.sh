#!/usr/bin/env bash

## Build a single monorepo plugin under plugins/<name>/.
## Invokes the plugin's own rebar3 emqx_plugrel tar and copies the
## resulting .tar.gz into $ROOT/_build/plugins/ for easy consumption.

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

if [[ ! -f "$PLUGIN_DIR/rebar.config" ]]; then
    echo "Error: $PLUGIN_DIR/rebar.config not found." >&2
    exit 1
fi

OUT_DIR="$ROOT_DIR/_build/plugins"
mkdir -p "$OUT_DIR"

REBAR="$ROOT_DIR/rebar3"
if [[ ! -x "$REBAR" ]]; then
    "$ROOT_DIR/scripts/ensure-rebar3.sh"
fi

echo "Building plugin $APP"
(
    cd "$PLUGIN_DIR"
    "$REBAR" emqx_plugrel tar
)

shopt -s nullglob
tars=("$PLUGIN_DIR"/_build/default/emqx_plugrel/"$APP"-*.tar.gz)
shopt -u nullglob

if [[ ${#tars[@]} -eq 0 ]]; then
    echo "Error: No plugin tarball produced for $APP." >&2
    echo "Searched under $PLUGIN_DIR/_build/..." >&2
    exit 1
fi

newest=""
for t in "${tars[@]}"; do
    if [[ -z "$newest" || "$t" -nt "$newest" ]]; then
        newest="$t"
    fi
done

cp -f "$newest" "$OUT_DIR/"
echo "Copied $(basename "$newest") to _build/plugins/"

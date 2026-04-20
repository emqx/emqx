#!/usr/bin/env bash

## Walk plugins/* and run Common Test for each plugin that has tests.
## No-op when plugins/ is empty.

set -euo pipefail

ROOT_DIR="$(cd -P -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

ran_any=0
for plugin_app_dir in plugins/*; do
    if [ ! -d "$plugin_app_dir" ] || [ ! -f "$plugin_app_dir/rebar.config" ] || [ ! -d "$plugin_app_dir/test" ]; then
        continue
    fi

    plugin_app="$(basename "$plugin_app_dir")"
    ran_any=1
    echo "Running plugin CT for ${plugin_app}"
    "$ROOT_DIR/scripts/run-plugin-ct.sh" "$plugin_app"
done

if [ "$ran_any" -eq 0 ]; then
    echo "No plugin apps with rebar.config and test/ found; skipping"
fi

#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd -P -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p _build/plugins

for plugin_app_dir in plugins/*; do
    if [ ! -d "$plugin_app_dir" ] || [ ! -f "$plugin_app_dir/mix.exs" ]; then
        continue
    fi
    if ! grep -q "emqx_plugin:" "$plugin_app_dir/mix.exs"; then
        continue
    fi

    plugin_app="$(basename "$plugin_app_dir")"
    echo "Building plugin ${plugin_app}"
    make "plugin-${plugin_app}"

    if ! find _build/plugins -maxdepth 1 -type f -name "${plugin_app}-*.tar.gz" | grep -q .; then
        echo "No plugin package (*.tar.gz) found under _build/plugins for ${plugin_app}"
        exit 1
    fi
done

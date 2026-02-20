#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd -P -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

mapfile -t plugins < <(find plugins -mindepth 1 -maxdepth 1 -type d | sort)
ran_any=0

for app in "${plugins[@]}"; do
    if [ ! -f "$app/mix.exs" ] || [ ! -d "$app/test" ]; then
        continue
    fi

    ran_any=1
    echo "Running plugin CT for ${app}"
    make "${app}-ct"
done

if [ "$ran_any" -eq 0 ]; then
    echo "No plugin apps with mix.exs and test/ found; skipping"
fi

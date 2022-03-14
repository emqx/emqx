#!/usr/bin/env bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "$0")/.."

if [ "$(./scripts/get-distro.sh)" = 'windows' ]; then
    # Otherwise windows may resolve to find.exe
    FIND="/usr/bin/find"
else
    FIND='find'
fi

find_app() {
    local appdir="$1"
    "$FIND" "${appdir}" -mindepth 1 -maxdepth 1 -type d
}

CE="$(find_app 'apps')"
EE="$(find_app 'lib-ee')"

if [ "${1:-}" = 'json' ]; then
    echo -e "${CE}\n${EE} " | xargs | tr -d '\n' | jq -R -s -c 'split(" ")'
else
    echo -e "${CE}\n${EE}"
fi

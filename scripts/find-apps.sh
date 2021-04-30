#!/usr/bin/env bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "$0")/.."

find_app() {
    local appdir="$1"
    find "${appdir}" -mindepth 1 -maxdepth 1 -type d
}

# append emqx application first
echo 'emqx'

find_app 'apps'
if [ -f 'EMQX_ENTERPRISE' ]; then
    find_app 'lib-ee'
else
    find_app 'lib-ce'
fi

## find directories in lib-extra
find_app 'lib-extra'
## find symlinks in lib-extra
find 'lib-extra' -mindepth 1 -maxdepth 1 -type l -exec test -e {} \; -print

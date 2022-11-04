#!/usr/bin/env bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "$0")/.."

help() {
    echo
    echo "-h|--help:        To display this usage info"
    echo "--json:           Print apps in json"
}

WANT_JSON='no'
while [ "$#" -gt 0 ]; do
    case $1 in
        -h|--help)
            help
            exit 0
            ;;
        --json)
            WANT_JSON='yes'
            shift 1
            ;;
        *)
            echo "unknown option $1"
            exit 1
            ;;
    esac
done
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

EM="emqx"
CE="$(find_app 'apps')"

if [ -f 'EMQX_ENTERPRISE' ]; then
    LIB="$(find_app 'lib-ee')"
else
    LIB="$(find_app 'lib-ce')"
fi

## find directories in lib-extra
LIBE="$(find_app 'lib-extra')"

## find symlinks in lib-extra
LIBES="$("$FIND" 'lib-extra' -mindepth 1 -maxdepth 1 -type l -exec test -e {} \; -print)"

APPS_ALL="$(echo -e "${EM}\n${CE}\n${LIB}\n${LIBE}\n${LIBES}")"

if [ "$WANT_JSON" = 'yes' ]; then
    echo "${APPS_ALL}" | xargs | tr -d '\n' | jq -R -s -c 'split(" ")'
else
    echo "${APPS_ALL}"
fi


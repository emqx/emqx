#!/usr/bin/env bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "$0")/.."

help() {
    echo
    echo "-h|--help:        To display this usage info"
    echo "--ct fast|docker: Print apps which needs docker-compose to run ct"
    echo "--json:           Print apps in json"
}

WANT_JSON='no'
CT='novalue'
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

        --ct)
            CT="$2"
            shift 2
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

CE="$(find_app 'apps')"
EE="$(find_app 'lib-ee')"
APPS_ALL="$(echo -e "${CE}\n${EE}")"

if [ "$CT" = 'novalue' ]; then
    RESULT="${APPS_ALL}"
else
    APPS_NORMAL_CT=( )
    APPS_DOCKER_CT=( )
    for app in ${APPS_ALL}; do
        if [ -f "${app}/docker-ct" ]; then
            APPS_DOCKER_CT+=("$app")
        else
            APPS_NORMAL_CT+=("$app")
        fi
    done
    if [ "$CT" = 'docker' ]; then
        RESULT="${APPS_DOCKER_CT[*]}"
    else
        RESULT="${APPS_NORMAL_CT[*]}"
    fi
fi

if [ "$WANT_JSON" = 'yes' ]; then
    echo "${RESULT}" | xargs | tr -d '\n' | jq -R -s -c 'split(" ")'
else
    echo "${RESULT}" | xargs
fi

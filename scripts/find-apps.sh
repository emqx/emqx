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

if [ "$CT" = 'novalue' ]; then
    echo -e "${CE}\n${EE}"
    exit 0
fi

APPS_ALL="$(echo -e "${CE}\n${EE}")"
APPS_DOCKER_CT="$(grep -v -E '^#.*' scripts/docker-ct-apps)"

# shellcheck disable=SC2068
for app in ${APPS_DOCKER_CT[@]}; do
    APPS_ALL=("${APPS_ALL[@]/$app}")
done

if [ "$CT" = 'docker' ]; then
    RESULT="${APPS_DOCKER_CT}"
else
    RESULT="${APPS_ALL[*]}"
fi

if [ "$WANT_JSON" = 'yes' ]; then
    echo "${RESULT}" | xargs | tr -d '\n' | jq -R -s -c 'split(" ")'
else
    echo "${RESULT}" | xargs
fi

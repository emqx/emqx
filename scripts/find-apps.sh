#!/usr/bin/env bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "$0")/.."

help() {
    echo
    echo "-h|--help:        To display this usage info"
    echo "--ci:             Print apps in json format for github ci matrix"
}

MODE='list'
while [ "$#" -gt 0 ]; do
    case $1 in
        -h|--help)
            help
            exit 0
            ;;
        --ci)
            MODE='ci'
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

CE="$(find_app 'apps')"
EE="$(find_app 'lib-ee')"
APPS_ALL="$(echo -e "${CE}\n${EE}")"

if [ "$MODE" = 'list' ]; then
    echo "${APPS_ALL}"
    exit 0
fi

##################################################
###### now deal with the github action's matrix.
##################################################

format_app_description() {
    ## prefix is for github actions (they don't like slash in variables)
    local prefix=${1//\//_}
    echo -n -e "$(
cat <<END
    {"app": "${1}", "profile": "${2}", "runner": "${3}", "prefix": "${prefix}"}
END
    )"
}

describe_app() {
    app="$1"
    local runner="host"
    local profile
    if [ -f "${app}/docker-ct" ]; then
        runner="docker"
    fi
    case "${app}" in
        apps/emqx_bridge_kafka)
            profile='emqx-enterprise'
            ;;
        apps/emqx_bridge_gcp_pubsub)
            profile='emqx-enterprise'
            ;;
        apps/*)
            profile='emqx'
            ;;
        lib-ee/*)
            profile='emqx-enterprise'
            ;;
        *)
            echo "unknown app: $app"
            exit 1
            ;;
    esac
    format_app_description "$app" "$profile" "$runner"
}

matrix() {
    local sep='['
    for app in ${APPS_ALL}; do
        row="$(describe_app "$app")"
        if [ -z "$row" ]; then
            continue
        fi
        echo -n "${sep}${row}"
        sep=', '
    done
    echo ']'
}

matrix

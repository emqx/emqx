#!/usr/bin/env bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "$0")/.."

help() {
    echo
    echo "-h|--help:        To display this usage info"
    echo "--ci fast|docker: Print apps in json format for github ci mtrix"
}

CI='novalue'
while [ "$#" -gt 0 ]; do
    case $1 in
        -h|--help)
            help
            exit 0
            ;;
        --ci)
            CI="$2"
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

if [ "$CI" = 'novalue' ]; then
    echo "${APPS_ALL}"
    exit 0
fi

##################################################
###### now deal with the github action's matrix.
##################################################

dimensions() {
    app="$1"
    if [ -f "${app}/docker-ct" ]; then
        if [[ "$CI" != 'docker' ]]; then
            return
        fi
    else
        if [[ "$CI" != 'fast' ]]; then
            return
        fi
    fi
    case "${app}" in
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
    ## poor-man's json formatter
    echo -n -e "[\"$app\", \"$profile\"]"
}

matrix() {
    first_row='yes'
    for app in ${APPS_ALL}; do
        row="$(dimensions "$app")"
        if [ -z "$row" ]; then
            continue
        fi
        if [ "$first_row" = 'yes' ]; then
            first_row='no'
            echo -n "$row"
        else
            echo -n ",${row}"
        fi
    done
}

echo -n '['
matrix
echo ']'

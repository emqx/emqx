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

APPS_ALL="$(find_app 'apps')"

if [ "$MODE" = 'list' ]; then
    echo "${APPS_ALL}"
    exit 0
fi

##################################################
###### now deal with the github action's matrix.
##################################################

LINESEP=''

print_line() {
    echo -n "${LINESEP}"
    echo "${1}"
    LINESEP=', '
}

format_app_description() {
    ## prefix is for github actions (they don't like slash in variables)
    local prefix=${1//\//_}
    local groups="$2"
    local group=0
    while [ "$groups" -gt $group ]; do
        group=$(( group + 1 ))
        print_line "$(
cat <<END
{"app": "${1}", "suitegroup": "${group}_${groups}", "profile": "${3}", "runner": "${4}", "prefix": "${prefix}", "compat": ${5}}
END
        )"
    done
}

describe_app() {
    app="$1"
    local runner="host"
    local profile
    local suitegroups=1
    local compat="false"
    if [ -f "${app}/docker-ct" ]; then
        runner="docker"
    fi
    case "${app}" in
        apps/*)
            if [[ -f "${app}/BSL.txt" ]]; then
              profile='emqx-enterprise'
            else
              profile='emqx'
            fi
            ;;
        *)
            echo "unknown app: $app"
            exit 1
            ;;
    esac
    if [[ "$app" == "apps/emqx" ]]; then
        suitegroups=5
    fi
    if grep -rq '-behaviour(emqx_bpapi)' "${app}/" ; then
        # NOTE: Implicitly assuming that any app with BPAPI protocols might want compat testing.
        compat="true"
    fi
    format_app_description "$app" "$suitegroups" "$profile" "$runner" "$compat"
}

matrix() {
    echo -n '[ '
    for app in ${APPS_ALL}; do
        describe_app "${app}"
    done
    echo ']'
}

matrix

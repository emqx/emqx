#!/usr/bin/env bash

set -e

function parseSemver() {
    local RE='^([0-9]+)\.([0-9]+)\.([0-9]+)(-([a-z]+\.[0-9]+|M[0-9]+\.[0-9]+|M[0-9]+\.[0-9]+-[a-z]+\.[0-9]+))?(-g[0-9a-f]+)?$'
    echo "$1" | grep -qE "$RE" || exit 1
    #shellcheck disable=SC2155
    local MAJOR=$(echo "$1" | sed -r "s#$RE#\1#")
    #shellcheck disable=SC2155
    local MINOR=$(echo "$1" | sed -r "s#$RE#\2#")
    #shellcheck disable=SC2155
    local PATCH=$(echo "$1" | sed -r "s#$RE#\3#")
    #shellcheck disable=SC2155
    local BUILD=$(echo "$1" | sed -r "s#$RE#\5#")
    case "${2}" in
        --major) echo "${MAJOR}" ;;
        --minor) echo "${MINOR}" ;;
        --patch) echo "${PATCH}" ;;
        --build) echo "${BUILD}" ;;
        *)
            cat <<EOF
{"major": ${MAJOR}, "minor": ${MINOR}, "patch": ${PATCH}, "build": "${BUILD}"}
EOF
            ;;
    esac
}

parseSemver "$1" "$2"

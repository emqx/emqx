#!/usr/bin/env bash

## This script prints Linux distro name and its version number
## e.g. macos, el8, ubuntu20.04

set -euo pipefail

UNAME="$(uname -s)"

case "$UNAME" in
    Darwin)
        DIST='macos'
        VERSION_ID=$(sw_vers | gsed -n '/^ProductVersion:/p' | gsed -r 's/ProductVersion:(.*)/\1/g' | gsed -r 's/([0-9]+).*/\1/g' | gsed 's/^[ \t]*//g')
        SYSTEM="$(echo "${DIST}${VERSION_ID}" | gsed -r 's/([a-zA-Z]*)-.*/\1/g')"
        ;;
    Linux)
        if grep -q -i 'rhel' /etc/*-release; then
            DIST='el'
            VERSION_ID="$(rpm --eval '%{rhel}')"
        else
            DIST="$(sed -n '/^ID=/p' /etc/os-release | sed -r 's/ID=(.*)/\1/g' | sed 's/"//g')"
            VERSION_ID="$(sed -n '/^VERSION_ID=/p' /etc/os-release | sed -r 's/VERSION_ID=(.*)/\1/g' | sed 's/"//g')"
        fi
        SYSTEM="$(echo "${DIST}${VERSION_ID}" | sed -r 's/([a-zA-Z]*)-.*/\1/g')"
        ;;
    CYGWIN*|MSYS*|MINGW*)
        SYSTEM="windows"
        ;;
esac

echo "$SYSTEM"

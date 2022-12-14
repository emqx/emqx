#!/usr/bin/env bash

## This script prints Linux distro name and its version number
## e.g. macos, centos8, ubuntu20.04

set -euo pipefail

UNAME="$(uname -s)"

case "$UNAME" in
    Darwin)
        DIST='macos'
        VERSION_ID="$(sw_vers | grep 'ProductVersion' | cut -d ':' -f2 | cut -d'.' -f1 | tr -d ' \t')"
        SYSTEM="${DIST}${VERSION_ID}"
        ;;
    Linux)
        # /etc/os-release on amazon linux 2 contains both rhel and centos strings
        if grep -q -i 'amzn' /etc/*-release; then
            DIST='amzn'
            VERSION_ID="$(sed -n '/^VERSION_ID=/p' /etc/os-release | sed -r 's/VERSION_ID=(.*)/\1/g' | sed 's/"//g')"
        elif grep -q -i 'rhel' /etc/*-release; then
            DIST='el'
            VERSION_ID="$(rpm --eval '%{rhel}')"
        elif grep -q -i 'centos' /etc/*-release; then
            DIST='centos'
            VERSION_ID="$(rpm --eval '%{centos_ver}')"
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

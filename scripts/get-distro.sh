#!/usr/bin/env bash

## This script prints Linux distro name and its version number
## e.g. macos, centos8, ubuntu20.04

set -euo pipefail

if [ "$(uname -s)" = 'Darwin' ]; then
    DIST='macos'
    VERSION_ID=$(sw_vers | gsed -n '/^ProductVersion:/p' | gsed -r 's/ProductVersion:(.*)/\1/g' | gsed -r 's/([0-9]+).*/\1/g' | gsed 's/^[ \t]*//g')
    SYSTEM="$(echo "${DIST}${VERSION_ID}" | gsed -r 's/([a-zA-Z]*)-.*/\1/g')"
elif [ "$(uname -s)" = 'Linux' ]; then
    if grep -q -i 'rhel' /etc/*-release; then
        DIST='rhel'
        VERSION_ID="$(rpm --eval '%{rhel}')"
    else
        DIST="$(sed -n '/^ID=/p' /etc/os-release | sed -r 's/ID=(.*)/\1/g' | sed 's/"//g')"
        VERSION_ID="$(sed -n '/^VERSION_ID=/p' /etc/os-release | sed -r 's/VERSION_ID=(.*)/\1/g' | sed 's/"//g')"
    fi
    SYSTEM="$(echo "${DIST}${VERSION_ID}" | sed -r 's/([a-zA-Z]*)-.*/\1/g')"
fi
echo "$SYSTEM"

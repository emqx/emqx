#!/bin/bash

## This script prints Linux distro name and its version number
## e.g. macos, centos8, ubuntu20.04

set -euo pipefail

if [ "$(uname -s)" = 'Darwin' ]; then
	echo 'macos'
elif [ "$(uname -s)" = 'Linux' ]; then
    if grep -q -i 'centos' /etc/*-release; then
        DIST='centos'
        VERSION_ID="$(rpm --eval '%{centos_ver}')"
    else
        DIST="$(sed -n '/^ID=/p' /etc/os-release | sed -r 's/ID=(.*)/\1/g' | sed 's/"//g')"
        VERSION_ID="$(sed -n '/^VERSION_ID=/p' /etc/os-release | sed -r 's/VERSION_ID=(.*)/\1/g' | sed 's/"//g')"
    fi
    echo "${DIST}${VERSION_ID}" | sed -r 's/([a-zA-Z]*)-.*/\1/g'
fi

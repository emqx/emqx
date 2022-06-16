#!/usr/bin/env bash

set -euo pipefail

AUTO_INSTALL_BUILD_DEPS="${AUTO_INSTALL_BUILD_DEPS:-0}"

required_packages_mac_osx="freetds unixodbc coreutils"
required_cmds_mac_osx="curl zip unzip autoconf automake cmake openssl"

dependency_missing() {
    echo "error: $1 is not found in the system"
    if [ "${AUTO_INSTALL_BUILD_DEPS}" = "1" ]; then
        echo "brew install $1"
        brew install "$1"
    else
        echo "You can install it by running:"
        echo "  brew install $1"
        exit 1
    fi
}

prepare_mac_osx() {
    current_packages=$(brew list)
    for package in ${required_packages_mac_osx}
    do
        if ! echo "${current_packages}" | grep -q "${package}"; then
            dependency_missing "${package}"
        fi
    done
    for cmd in ${required_cmds_mac_osx}
    do
        if ! command -v "${cmd}" &> /dev/null; then
            dependency_missing "${cmd}"
        fi
    done
}

case $(uname) in
    *Darwin*) prepare_mac_osx;;
    *) exit 0;;
esac

#!/usr/bin/env bash

## This script is intended to run in docker
## extracts a .tar.gz package and runs EMQX in console mode

set -euo pipefail

PKG="$1"

mkdir -p emqx
tar -C emqx -zxf "$PKG"

ln -s "$(pwd)/emqx/bin/emqx" /usr/bin/emqx
ln -s "$(pwd)/emqx/bin/emqx_ctl" /usr/bin/emqx_ctl

if command -v apt; then
    apt update -y
    apt install -y \
        curl \
        jq \
        libffi-dev \
        libkrb5-3 \
        libkrb5-dev \
        libncurses5-dev \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules-gssapi-mit \
        libssl-dev \
        zip
fi

emqx console

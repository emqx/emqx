#!/usr/bin/env bash

## This script is intended to run in docker
## extracts a .tar.gz package and runs EMQX in console mode

set -euo pipefail

PKG="$1"

mkdir -p emqx
tar -C emqx -zxf "$PKG"

ln -s "$(pwd)/emqx/bin/emqx" /usr/bin/emqx
ln -s "$(pwd)/emqx/bin/emqx_ctl" /usr/bin/emqx_ctl

emqx console

#!/usr/bin/env bash

## This script needs the 'lux' command in PATH
## it runs the .ci/fvt_tests/relup.lux script

set -euo pipefail

old_vsn="${1}"

# ensure dir
cd -P -- "$(dirname -- "$0")/.."

set -x

case "$old_vsn" in
    e*)
        cur_vsn="$(./pkg-vsn.sh emqx-enterprise)"
        profile='emqx-enterprise'
        ;;
    v*)
        cur_vsn="$(./pkg-vsn.sh emqx)"
        profile='emqx'
        ;;
    *)
        echo "unknown old version $old_vsn"
        exit 1
        ;;
esac

old_pkg="$(pwd)/_upgrade_base/${profile}-${old_vsn#[e|v]}-otp24.2.1-1-ubuntu20.04-amd64.tar.gz"
cur_pkg="$(pwd)/_packages/${profile}/${profile}-${cur_vsn}-otp24.2.1-1-ubuntu20.04-amd64.tar.gz"

lux \
    --progress verbose \
    --case_timeout infinity \
    --var PROJ_ROOT="$(pwd)" \
    --var PROFILE="$profile" \
    --var VSN="$cur_vsn" \
    --var OLD_VSN="$old_vsn" \
    --var CUR_PKG="$cur_pkg" \
    --var OLD_PKG="$old_pkg" \
    .ci/fvt_tests/relup.lux

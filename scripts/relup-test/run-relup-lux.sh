#!/usr/bin/env bash

## This script needs the 'lux' command in PATH
## it runs the scripts/relup-test/relup.lux script

set -euo pipefail

old_vsn="${1:-}"
if [ -z "$old_vsn" ]; then
    echo "arg1 should be the upgrade base version"
    exit 1
fi

rebuild="${2:-no_rebuild}"

# ensure dir
cd -P -- "$(dirname -- "$0")/../.."

set -x

if [ ! -d '.git' ] && [ -z "${CUR_VSN:-}" ]; then
    echo "Unable to resolve current version, because it's not a git repo, and CUR_VSN is not set"
    exit 1
fi

case "$old_vsn" in
    e*)
        cur_vsn="${CUR_VSN:-$(./pkg-vsn.sh emqx-enterprise)}"
        profile='emqx-enterprise'
        ;;
    v*)
        cur_vsn="${CUR_VSN:-$(./pkg-vsn.sh emqx)}"
        profile='emqx'
        ;;
    *)
        echo "unknown old version $old_vsn"
        exit 1
        ;;
esac

if [ "$rebuild" = "--build" ]; then
    make "${profile}-tgz"
fi

# From now on, no need for the v|e prefix
OLD_VSN="${old_vsn#[e|v]}"

OLD_PKG="$(pwd)/_upgrade_base/${profile}-${OLD_VSN}-otp24.2.1-1-ubuntu20.04-amd64.tar.gz"
CUR_PKG="$(pwd)/_packages/${profile}/${profile}-${cur_vsn}-otp24.2.1-1-ubuntu20.04-amd64.tar.gz"

if [ ! -f "$OLD_PKG" ]; then
    echo "$OLD_PKG not found"
    exit 1
fi

if [ ! -f "$CUR_PKG" ]; then
    echo "$CUR_PKG not found"
    exit 1
fi

# start two nodes and their friends (webhook server and a bench) in docker
./scripts/relup-test/start-relup-test-cluster.sh 'ubuntu:20.04' "$OLD_PKG"

# run relup tests
lux \
    --progress verbose \
    --case_timeout infinity \
    --var PROJ_ROOT="$(pwd)" \
    --var VSN="$cur_vsn" \
    --var CUR_PKG="$CUR_PKG" \
    --var OLD_VSN="$OLD_VSN" \
    --var NODE1="node1.emqx.io" \
    --var NODE2="node2.emqx.io" \
    --var BENCH="bench.emqx.io" \
    ./scripts/relup-test/relup.lux

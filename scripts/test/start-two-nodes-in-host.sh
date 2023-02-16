#!/usr/bin/env bash

set -euo pipefail

## This starts two nodes on the same host (not in docker).
## The listener ports are shifted with an offset to avoid clashing.
## The data and log directories are configured to use ./tmp/

## By default, the boot script is ./_build/emqx/rel/emqx
## it can be overriden with arg1 and arg2 for the two nodes respectfully

# ensure dir
cd -P -- "$(dirname -- "$0")/../../"

DEFAULT_BOOT='./_build/emqx/rel/emqx/bin/emqx'
BOOT1="${1:-$DEFAULT_BOOT}"
BOOT2="${2:-$BOOT1}"

DATA1="$(pwd)/tmp/emqx1/data"
LOG1="$(pwd)/tmp/emqx1/log"
DATA2="$(pwd)/tmp/emqx2/data"
LOG2="$(pwd)/tmp/emqx2/log"

mkdir -p "$DATA1" "$DATA2" "$LOG1" "$LOG2"

echo "Stopping emqx1"
env EMQX_NODE_NAME='emqx1@127.0.0.1' \
    ./_build/emqx/rel/emqx/bin/emqx stop || true

echo "Stopping emqx2"
env EMQX_NODE_NAME='emqx2@127.0.0.1' \
    ./_build/emqx/rel/emqx/bin/emqx stop || true

## Fork-start node1, otherwise it'll keep waiting for node2 because we are using static cluster
env DEBUG="${DEBUG:-0}" \
    EMQX_CLUSTER__STATIC__SEEDS='["emqx1@127.0.0.1","emqx2@127.0.0.1"]' \
    EMQX_CLUSTER__DISCOVERY_STRATEGY=static \
    EMQX_LOG__FILE_HANDLERS__DEFAULT__LEVEL="${EMQX_LOG__FILE_HANDLERS__DEFAULT__LEVEL:-debug}" \
    EMQX_LOG__FILE_HANDLERS__DEFAULT__FILE="$LOG1/emqx.log" \
    EMQX_NODE_NAME='emqx1@127.0.0.1' \
    EMQX_LOG_DIR="$LOG1" \
    EMQX_NODE__DATA_DIR="$DATA1" \
    EMQX_LISTENERS__TCP__DEFAULT__BIND='0.0.0.0:1881' \
    EMQX_LISTENERS__SSL__DEFAULT__BIND='0.0.0.0:8881' \
    EMQX_LISTENERS__WS__DEFAULT__BIND='0.0.0.0:8081' \
    EMQX_LISTENERS__WSS__DEFAULT__BIND='0.0.0.0:8084' \
    EMQX_DASHBOARD__LISTENERS__HTTP__BIND='0.0.0.0:18081' \
    "$BOOT1" start &

env DEBUG="${DEBUG:-0}" \
    EMQX_CLUSTER__STATIC__SEEDS='["emqx1@127.0.0.1","emqx2@127.0.0.1"]' \
    EMQX_CLUSTER__DISCOVERY_STRATEGY=static \
    EMQX_LOG__FILE_HANDLERS__DEFAULT__LEVEL="${EMQX_LOG__FILE_HANDLERS__DEFAULT__LEVEL:-debug}" \
    EMQX_LOG__FILE_HANDLERS__DEFAULT__FILE="$LOG2/emqx.log" \
    EMQX_NODE_NAME='emqx2@127.0.0.1' \
    EMQX_LOG_DIR="$LOG2" \
    EMQX_NODE__DATA_DIR="$DATA2" \
    EMQX_LISTENERS__TCP__DEFAULT__BIND='0.0.0.0:1882' \
    EMQX_LISTENERS__SSL__DEFAULT__BIND='0.0.0.0:8882' \
    EMQX_LISTENERS__WS__DEFAULT__BIND='0.0.0.0:8082' \
    EMQX_LISTENERS__WSS__DEFAULT__BIND='0.0.0.0:8085' \
    EMQX_DASHBOARD__LISTENERS__HTTP__BIND='0.0.0.0:18082' \
    "$BOOT2" start

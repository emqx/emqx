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

export IP1='127.0.0.1'
export IP2='127.0.0.2'

# cannot use the same node name even IPs are different because Erlang distribution listens on 0.0.0.0
NODE1="emqx1@$IP1"
NODE2="emqx2@$IP2"

start_cmd() {
    local index="$1"
    local nodehome
    nodehome="$(pwd)/tmp/emqx${index}"
    [ "$index" -eq 1 ] && BOOT_SCRIPT="$BOOT1"
    [ "$index" -eq 2 ] && BOOT_SCRIPT="$BOOT2"
    mkdir -p "${nodehome}/data" "${nodehome}/log"
    cat <<-EOF
env DEBUG="${DEBUG:-0}" \
EMQX_CLUSTER__STATIC__SEEDS="[\"$NODE1\",\"$NODE2\"]" \
EMQX_CLUSTER__DISCOVERY_STRATEGY=static \
EMQX_LOG__FILE_HANDLERS__DEFAULT__LEVEL="${EMQX_LOG__FILE_HANDLERS__DEFAULT__LEVEL:-debug}" \
EMQX_LOG__FILE_HANDLERS__DEFAULT__FILE="${nodehome}/log/emqx.log" \
EMQX_NODE_NAME="emqx${index}@\$IP${index}" \
EMQX_NODE__COOKIE="${EMQX_NODE__COOKIE:-cookie1}" \
EMQX_LOG_DIR="${nodehome}/log" \
EMQX_NODE__DATA_DIR="${nodehome}/data" \
EMQX_LISTENERS__TCP__DEFAULT__BIND="\$IP${index}:1883" \
EMQX_LISTENERS__SSL__DEFAULT__BIND="\$IP${index}:8883" \
EMQX_LISTENERS__WS__DEFAULT__BIND="\$IP${index}:8083" \
EMQX_LISTENERS__WSS__DEFAULT__BIND="\$IP${index}:8084" \
EMQX_DASHBOARD__LISTENERS__HTTP__BIND="\$IP${index}:18083" \
"$BOOT_SCRIPT" start
EOF
}

echo "Stopping $NODE1"
env EMQX_NODE_NAME="$NODE1" "$BOOT1" stop || true

echo "Stopping $NODE2"
env EMQX_NODE_NAME="$NODE2" "$BOOT2" stop || true

start_one_node() {
    local index="$1"
    local cmd
    cmd="$(start_cmd "$index" | envsubst)"
    echo "$cmd"
    eval "$cmd"
}

## Fork-start node1, otherwise it'll keep waiting for node2 because we are using static cluster
start_one_node 1 &
start_one_node 2

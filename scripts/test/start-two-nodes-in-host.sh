#!/usr/bin/env bash

set -euo pipefail

## Starts tow nodes named emqx1@127.0.1 and emqx2@127.0.0.2 on the same host.
## The data and log directories are configured to use $DATADIR/emqx{1,2},
## where DATADIR is by default ./tmp.

logerr() {
    if [ "${TERM:-dumb}" = dumb ]; then
        echo -e "ERROR: $*" 1>&2
    else
        echo -e "$(tput setaf 1)ERROR: $*$(tput sgr0)" 1>&2
    fi
}

help() {
    echo ""
    echo "Start a 2-node cluster in one host, node names are emqx1@127.0.1 and emqx2@127.0.0.2"
    echo ""
    echo "-h|--help: To display this usage info."
    echo "-b|--boots: Optional, default is ./_build/emqx/rel/emqx/bin/emqx"
    echo "            If it is not found in _build dir, the 'which emqx' command output is used"
    echo "            Supports multiple commands, one for each node, separate with a comma."
    echo "-r|--roles: Optional, node (db) roles, comma separate."
    echo "            Default is 'core' for both."
    echo "-d|--data:  Optional, base dirs for node data and log directories."
    echo "            Nodes will save data to ./tmp/emqx1 and ./tmp/emqx2"
    echo "            Default is './tmp'"
}

BUILT_BOOT='./_build/emqx/rel/emqx/bin/emqx'
if [ -f "$BUILT_BOOT" ]; then
    DEFAULT_BOOT="$BUILT_BOOT"
else
    DEFAULT_BOOT="$(which emqx || true)"
fi
DATADIR="$(pwd)/tmp"
ROLES='core,core'
while [ "$#" -gt 0 ]; do
    case $1 in
        -h|--help)
            help
            exit 0
            ;;
        -b|--boots)
            BOOT="$2"
            shift 2
            ;;
        -r|--roles)
            ROLES="$2"
            shift 2
            ;;
        -d|--data)
            DATADIR="$2"
            shift 2
            ;;
        *)
            echo "unknown option $1"
            exit 1
            ;;
    esac
done

if [ -z "${BOOT:-}" ]; then
    if [ -z "${DEFAULT_BOOT}" ]; then
        help
        logerr "Missing -b|--boots option."
        exit 1
    fi
    BOOT="${DEFAULT_BOOT}"
fi

BOOT1="$(echo "$BOOT" | cut -d ',' -f1)"
BOOT2="$(echo "$BOOT" | cut -d ',' -f2)"
[ -z "$BOOT2" ] && BOOT2="$BOOT1"

export IP1='127.0.0.1'
export IP2='127.0.0.2'

# cannot use the same node name even IPs are different because Erlang distribution listens on 0.0.0.0
NODE1="emqx1@$IP1"
NODE2="emqx2@$IP2"

ROLE1="$(echo "$ROLES" | cut -d ',' -f1)"
ROLE2="$(echo "$ROLES" | cut -d ',' -f2)"
export ROLE1 ROLE2

if [ "$ROLE1" = 'core' ] && [ "$ROLE2" = 'core' ]; then
    SEEDS="$NODE1,$NODE2"
elif [ "$ROLE1" = 'core' ]; then
    SEEDS="$NODE1"
elif [ "$ROLE2" = 'core' ]; then
    SEEDS="$NODE2"
else
    echo "missing 'core' role in -r|--roles option"
    exit 1
fi
export SEEDS

export DATADIR

start_cmd() {
    local index="$1"
    local nodehome
    nodehome="${DATADIR}/emqx${index}"
    [ "$index" -eq 1 ] && BOOT_SCRIPT="$BOOT1"
    [ "$index" -eq 2 ] && BOOT_SCRIPT="$BOOT2"
    mkdir -p "${nodehome}/data" "${nodehome}/log"
    cat <<-EOF
env DEBUG="${DEBUG:-0}" \
EMQX_NODE_NAME="emqx${index}@\$IP${index}" \
EMQX_CLUSTER__STATIC__SEEDS="$SEEDS" \
EMQX_CLUSTER__DISCOVERY_STRATEGY=static \
EMQX_NODE__ROLE="\$ROLE${index}" \
EMQX_LOG__FILE_HANDLERS__DEFAULT__LEVEL="${EMQX_LOG__FILE_HANDLERS__DEFAULT__LEVEL:-debug}" \
EMQX_LOG__FILE_HANDLERS__DEFAULT__FILE="${nodehome}/log/emqx.log" \
EMQX_NODE__COOKIE="${EMQX_NODE__COOKIE:-cookie1}" \
EMQX_LOG_DIR="${nodehome}/log" \
EMQX_NODE__DATA_DIR="${nodehome}/data" \
EMQX_LISTENERS__TCP__DEFAULT__BIND="\$IP${index}:1883" \
EMQX_LISTENERS__SSL__DEFAULT__BIND="\$IP${index}:8883" \
EMQX_LISTENERS__WS__DEFAULT__BIND="\$IP${index}:8083" \
EMQX_LISTENERS__WSS__DEFAULT__BIND="\$IP${index}:8084" \
EMQX_DASHBOARD__LISTENERS__HTTP__BIND="\$IP${index}:18083" \
EMQX_LICENSE__KEY=evaluation \
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

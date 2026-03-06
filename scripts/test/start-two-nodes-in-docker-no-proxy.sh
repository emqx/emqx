#!/usr/bin/env bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "$0")/../../"

# Node1 ports are default EMQX ports on localhost.
# Node2 uses offset host ports to avoid conflicts.
NODE1_PORTS=(-p 18083:18083 -p 1883:1883 -p 8083:8083)
NODE2_PORTS=(-p 19083:18083 -p 2883:1883 -p 9083:8083)

NET='emqx.io'
NODE1="node1.$NET"
NODE2="node2.$NET"
COOKIE='this-is-a-secret'
IPV6=0
USE_NET=''

cleanup() {
    docker rm -f "$NODE1" >/dev/null 2>&1 || true
    docker rm -f "$NODE2" >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
}

show_help() {
    echo "Usage: $0 [options] EMQX_IMAGE1 [EMQX_IMAGE2]"
    echo ""
    echo "Start a 2-node EMQX cluster in Docker without HAProxy."
    echo ""
    echo "Options:"
    echo "  -h, --help: Show this help message and exit."
    echo "  -P: Publish container ports to random host ports (docker -P style)."
    echo "  -6: Test with IPv6"
    echo "  -c: Cleanup: delete docker network, force delete containers."
    echo "  -n: <docker_net_name>"
    echo "      use existing docker network, do not create a network for this test"
}

while getopts "hc6Pn:" opt
do
    case $opt in
        P)
            NODE1_PORTS=(-p 18083 -p 1883 -p 8083)
            NODE2_PORTS=(-p 18083 -p 1883 -p 8083)
            ;;
        c) cleanup; exit 0;;
        h) show_help; exit 0;;
        6) IPV6=1;;
        n) USE_NET="$OPTARG";;
        *) ;;
    esac
done
shift $((OPTIND - 1))

IMAGE1="${1:-}"
IMAGE2="${2:-${IMAGE1}}"

if [ -z "${IMAGE1:-}" ] || [ -z "${IMAGE2:-}" ]; then
    show_help
    exit 1
fi

cleanup

if [ -z "${USE_NET}" ]; then
    if [ ${IPV6} = 1 ]; then
        docker network create --ipv6 --subnet 2001:0DB8::/112 "$NET"
        RPC_ADDRESS="::"
        PROTO_DIST='inet6_tls'
    else
        docker network create "$NET"
        RPC_ADDRESS="0.0.0.0"
        PROTO_DIST='inet_tls'
    fi
else
    NET="${USE_NET}"
    RPC_ADDRESS="0.0.0.0"
    PROTO_DIST='inet_tls'
fi

docker run -d -t --restart=always --name "$NODE1" \
  --net "$NET" \
  "${NODE1_PORTS[@]}" \
  -e EMQX_LOG__CONSOLE_HANDLER__LEVEL=debug \
  -e EMQX_NODE_NAME="emqx@$NODE1" \
  -e EMQX_NODE_COOKIE="$COOKIE" \
  -e EMQX_CLUSTER__PROTO_DIST="${PROTO_DIST}" \
  -e EMQX_RPC__LISTEN_ADDRESS="${RPC_ADDRESS}" \
  -e EMQX_RPC__IPV6_ONLY="true" \
  -e EMQX_listeners__ssl__default__enable=false \
  -e EMQX_listeners__wss__default__enable=false \
  -e EMQX_LICENSE__KEY="${LICENSE_KEY1:-evaluation}" \
  "$IMAGE1"

docker run -d -t --restart=always --name "$NODE2" \
  --net "$NET" \
  "${NODE2_PORTS[@]}" \
  -e EMQX_LOG__CONSOLE_HANDLER__LEVEL=debug \
  -e EMQX_NODE_NAME="emqx@$NODE2" \
  -e EMQX_NODE_COOKIE="$COOKIE" \
  -e EMQX_CLUSTER__PROTO_DIST="${PROTO_DIST}" \
  -e EMQX_RPC__LISTEN_ADDRESS="${RPC_ADDRESS}" \
  -e EMQX_RPC__IPV6_ONLY="true" \
  -e EMQX_listeners__ssl__default__enable=false \
  -e EMQX_listeners__wss__default__enable=false \
  -e EMQX_LICENSE__KEY="${LICENSE_KEY2:-evaluation}" \
  "$IMAGE2"

wait_for_emqx() {
    container="$1"
    wait_limit="$2"
    wait_sec=0
    while ! docker exec "$container" emqx ctl status >/dev/null 2>&1; do
        wait_sec=$(( wait_sec + 1 ))
        if [ $wait_sec -gt "$wait_limit" ]; then
            echo "timeout wait for EMQX"
            docker logs "$container"
            exit 1
        fi
        echo -n '.'
        sleep 1
    done
}

wait_for_running_nodes() {
    local container="$1"
    local expected_running_nodes="$2"
    local wait_limit="$3"
    local wait_sec=0
    local running_nodes=0
    while [ "${wait_sec}" -lt "${wait_limit}" ]; do
        running_nodes="$(docker exec -t "$container" emqx ctl cluster status --json | jq '.running_nodes | length')"
        if [ "${running_nodes}" -eq "${expected_running_nodes}" ]; then
            echo "Successfully confirmed ${running_nodes} running nodes"
            return 0
        fi
        wait_sec=$(( wait_sec + 1 ))
        echo -n '.'
        sleep 1
    done
    echo "Expected running nodes is ${expected_running_nodes}, but got ${running_nodes} after ${wait_limit} seconds"
    docker logs "$container"
    exit 1
}

wait_for_emqx "$NODE1" 60
wait_for_emqx "$NODE2" 30

echo

docker exec "${NODE2}" emqx ctl cluster join "emqx@$NODE1"

wait_for_running_nodes "$NODE1" "${EXPECTED_RUNNING_NODES:-2}" 30

echo "Node1 MQTT: localhost:1883, Dashboard: localhost:18083"
echo "Node2 MQTT: localhost:2883, Dashboard: localhost:19083"

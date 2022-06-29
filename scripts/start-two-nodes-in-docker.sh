#!/usr/bin/env bash

set -euo pipefail

## EMQX can only start with longname (https://erlang.org/doc/reference_manual/distributed.html)
## The host name part of EMQX's node name has to be static, this means we should either
## pre-assign static IP for containers, or ensure containers can communiate with each other by name
## this is why a docker network is created, and the containers's names have a dot.

# ensure dir
cd -P -- "$(dirname -- "$0")/.."

IMAGE="${1}"

NET='emqx.io'
NODE1="node1.$NET"
NODE2="node2.$NET"
COOKIE='this-is-a-secret'

## clean up
docker rm -f "$NODE1" >/dev/null 2>&1 || true
docker rm -f "$NODE2" >/dev/null 2>&1 || true
docker network rm "$NET" >/dev/null 2>&1 || true

docker network create "$NET"

docker run -d -t --restart=always --name "$NODE1" \
  --net "$NET" \
  -e EMQX_LOG__CONSOLE_HANDLER__LEVEL=debug \
  -e EMQX_NODE_NAME="emqx@$NODE1" \
  -e EMQX_NODE_COOKIE="$COOKIE" \
  -e EMQX_CLUSTER__PROTO_DIST='inet_tls' \
  -p 18083:18083 \
  "$IMAGE"

docker run -d -t --restart=always --name "$NODE2" \
  --net "$NET" \
  -e EMQX_LOG__CONSOLE_HANDLER__LEVEL=debug \
  -e EMQX_NODE_NAME="emqx@$NODE2" \
  -e EMQX_NODE_COOKIE="$COOKIE" \
  -e EMQX_CLUSTER__PROTO_DIST='inet_tls' \
  -p 18084:18083 \
  "$IMAGE"

wait () {
  container="$1"
  while ! docker exec "$container" emqx_ctl status >/dev/null 2>&1; do
    echo -n '.'
    sleep 1
  done
}

wait $NODE1
wait $NODE2
echo

docker exec $NODE1 emqx_ctl cluster join "emqx@$NODE2"

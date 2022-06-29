#!/usr/bin/env bash

set -euo pipefail

## EMQX can only start with longname (https://erlang.org/doc/reference_manual/distributed.html)
## The host name part of EMQX's node name has to be static, this means we should either
## pre-assign static IP for containers, or ensure containers can communiate with each other by name
## this is why a docker network is created, and the containers's names have a dot.

# ensure dir
cd -P -- "$(dirname -- "$0")/../.."

set -x

IMAGE="${1}"
PKG="$(readlink -f "${2}")"

NET='emqx.io'
NODE1="node1.$NET"
NODE2="node2.$NET"
WEBHOOK="webhook.$NET"
BENCH="bench.$NET"
COOKIE='this-is-a-secret'
## Erlang image is needed to run webhook server and emqtt-bench
ERLANG_IMAGE="ghcr.io/emqx/emqx-builder/5.0-17:1.13.4-24.2.1-1-ubuntu20.04"
# builder has emqtt-bench installed
BENCH_IMAGE="$ERLANG_IMAGE"

## clean up
docker rm -f "$BENCH" >/dev/null 2>&1 || true
docker rm -f "$WEBHOOK" >/dev/null 2>&1 || true
docker rm -f "$NODE1" >/dev/null 2>&1 || true
docker rm -f "$NODE2" >/dev/null 2>&1 || true
docker network rm "$NET" >/dev/null 2>&1 || true

docker network create "$NET"

docker run -d -t --name "$NODE1" \
  --net "$NET" \
  -e EMQX_LOG__CONSOLE_HANDLER__LEVEL=warning \
  -e EMQX_NODE_NAME="emqx@$NODE1" \
  -e EMQX_NODE_COOKIE="$COOKIE" \
  -p 18083:18083 \
  -v "$PKG:/emqx.tar.gz" \
  -v "$(pwd)/scripts/relup-test/run-pkg.sh:/run-pkg.sh" \
  "$IMAGE" /run-pkg.sh emqx.tar.gz

docker run -d -t --name "$NODE2" \
  --net "$NET" \
  -e EMQX_LOG__CONSOLE_HANDLER__LEVEL=warning \
  -e EMQX_NODE_NAME="emqx@$NODE2" \
  -e EMQX_NODE_COOKIE="$COOKIE" \
  -p 18084:18083 \
  -v "$PKG:/emqx.tar.gz" \
  -v "$(pwd)/scripts/relup-test/run-pkg.sh:/run-pkg.sh" \
  "$IMAGE" /run-pkg.sh emqx.tar.gz

docker run -d -t --name "$WEBHOOK" \
  --net "$NET" \
  -v "$(pwd)/.ci/fvt_tests/http_server:/http_server" \
  -w /http_server \
  -p 7077:7077 \
  "$ERLANG_IMAGE" bash -c 'rebar3 compile; erl -pa _build/default/lib/*/ebin -eval "http_server:start()"'

docker run -d -t --name "$BENCH" \
    --net "$NET" \
    "$BENCH_IMAGE" \
    bash -c 'sleep 10000; exit 1'

wait_limit=60
wait_for_emqx() {
    wait_sec=0
    container="$1"
    wait_limit="$2"
    set +x
    while ! docker exec "$container" emqx_ctl status >/dev/null 2>&1; do
        wait_sec=$(( wait_sec + 1 ))
        if [ $wait_sec -gt "$wait_limit" ]; then
            echo "timeout wait for EMQX"
            exit 1
        fi
        echo -n '.'
        sleep 1
    done
}

wait_for_webhook() {
    wait_sec=0
    wait_limit="$1"
    set +x
    while ! curl -f -s localhost:7077; do
        wait_sec=$(( wait_sec + 1 ))
        if [ $wait_sec -gt "$wait_limit" ]; then
            echo "timeout wait for EMQX"
            exit 1
        fi
        echo -n '.'
        sleep 1
    done
}

# wait for webhook http server to start,
# it may take a while because it needs to compile from source code
wait_for_webhook 120
# after webhook start, it should not cost more than 30 seconds
wait_for_emqx $NODE1 30
# afer node1 is up, it should not cost more than 10 seconds
wait_for_emqx $NODE2 10
echo

docker exec $NODE1 emqx_ctl cluster join "emqx@$NODE2"

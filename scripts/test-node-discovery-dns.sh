#!/usr/bin/env bash

## Test two-nodes cluster discover each other using DNS A records lookup result.

set -euo pipefail

cd -P -- "$(dirname -- "$0")/.."

IMAGE="${1:-}"

if [ -z "$IMAGE" ]; then
    echo "Usage: $0 <EMQX_IMAGE_TAG>"
    echo "e.g. $0 docker.io/emqx/emqx:5.0.8"
    exit 1
fi

NET='test_node_discovery_dns'
NODE1='emqx1'
NODE2='emqx2'
COOKIE='this-is-a-secret'

# cleanup
docker rm -f dnsmasq >/dev/null 2>&1 || true
docker rm -f "$NODE1" >/dev/null 2>&1 || true
docker rm -f "$NODE2" >/dev/null 2>&1 || true
docker network rm "$NET" >/dev/null 2>&1 || true

docker network create --subnet=172.18.0.0/16 $NET

IP0="172.18.0.100"
IP1="172.18.0.101"
IP2="172.18.0.102"
DOMAIN="dnstest.mynet"

# create configs for dnsmasq
cat <<-EOF > "/tmp/dnsmasq.conf"
conf-dir=/etc/dnsmasq,*.conf
addn-hosts=/etc/hosts.$DOMAIN
EOF

cat <<-EOF > "/tmp/hosts.$DOMAIN"
$IP1 $DOMAIN
$IP2 $DOMAIN
EOF

cat <<-EOF > /tmp/dnsmasq.base.conf
domain-needed
bogus-priv
no-hosts
keep-in-foreground
no-resolv
expand-hosts
server=8.8.8.8
EOF

docker run -d -t --name dnsmasq \
    --net "$NET" \
    --ip "$IP0" \
    -v /tmp/dnsmasq.conf:/etc/dnsmasq.conf \
    -v "/tmp/hosts.$DOMAIN:/etc/hosts.$DOMAIN" \
    -v "/tmp/dnsmasq.base.conf:/etc/dnsmasq/0.base.conf" \
    --cap-add=NET_ADMIN \
    storytel/dnsmasq dnsmasq --no-daemon --log-queries

# Node names (the part before '@') should be all the same in the cluster
# e.g. emqx@${IP}
start_emqx_v5() {
    NAME="$1"
    IP="$2"
    DASHBOARD_PORT="$3"
    docker run -d -t \
        --name "$NAME" \
        --net "$NET" \
        --ip "$IP" \
        --dns "$IP0" \
        -p "$DASHBOARD_PORT:18083" \
        -e EMQX_NODE_NAME="emqx@${IP}" \
        -e EMQX_LOG__CONSOLE_HANDLER__LEVEL=debug \
        -e EMQX_NODE_COOKIE="$COOKIE" \
        -e EMQX_cluster__discovery_strategy='dns' \
        -e EMQX_cluster__dns__name="$DOMAIN" \
        -e EMQX_cluster__dns__record_type="a" \
        "$IMAGE"
}

## EMQX v4 has different configuration schema:
# EMQX_NODE_NAME="emqx@${IP}":
#   This is necessary because 4.x docker entrypoint
#   by default uses docker container ID as node name
#   (the part before @ of e.g. emqx@172.18.0.101)
# EMQX_cluster__dns__app
#   This must be the same as node name in 4.x
# EMQX_cluster__discovery
#   This in 5.0 is EMQX_cluster__discovery_strategy
# EMQX_cluster__dns__name
#   The DNS domain to lookup for peer nodes
# EMQX_cluster__dns__record_type
#   The DNS record type. (only 'a' type is tested)
start_emqx_v4() {
    NAME="$1"
    IP="$2"
    APP_NAME="emqx"
    DASHBOARD_PORT="$3"
    docker run -d -t \
        --name "$NAME" \
        --net "$NET" \
        --ip "$IP" \
        --dns "$IP0" \
        -p "$DASHBOARD_PORT:18083" \
        -e EMQX_NODE_NAME="${APP_NAME}@${IP}" \
        -e EMQX_LOG__LEVEL=debug \
        -e EMQX_NODE_COOKIE="$COOKIE" \
        -e EMQX_cluster__discovery='dns' \
        -e EMQX_cluster__dns__name="$DOMAIN" \
        -e EMQX_cluster__dns__app="${APP_NAME}" \
        -e EMQX_cluster__dns__record_type="a" \
        "$IMAGE"
}

case "${IMAGE}" in
    *emqx:4.*|*emqx-ee:4.*)
        start_emqx_v4 "$NODE1" "$IP1" 18083
        start_emqx_v4 "$NODE2" "$IP2" 18084
        ;;
    *)
        start_emqx_v5 "$NODE1" "$IP1" 18083
        start_emqx_v5 "$NODE2" "$IP2" 18084
        ;;

esac

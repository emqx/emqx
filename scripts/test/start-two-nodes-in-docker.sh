#!/usr/bin/env bash

set -euo pipefail

## EMQX can only start with longname (https://erlang.org/doc/reference_manual/distributed.html)
## The host name part of EMQX's node name has to be static, this means we should either
## pre-assign static IP for containers, or ensure containers can communiate with each other by name
## this is why a docker network is created, and the containers's names have a dot.

# ensure dir
cd -P -- "$(dirname -- "$0")/../../"

HAPROXY_PORTS=(-p 18083:18083 -p 1883:1883 -p 8883:8883 -p 8884:8884)

NET='emqx.io'
NODE1="node1.$NET"
NODE2="node2.$NET"
COOKIE='this-is-a-secret'
IPV6=0
DASHBOARD_NODES='both'

cleanup() {
    docker rm -f haproxy >/dev/null 2>&1 || true
    docker rm -f "$NODE1" >/dev/null 2>&1 || true
    docker rm -f "$NODE2" >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
}

show_help() {
    echo "Usage: $0 [options] EMQX_IMAGE1 [EMQX_IAMGE2]"
    echo ""
    echo "Specifiy which docker image to run with EMQX_IMAGE1"
    echo "EMQX_IMAGE2 is the same as EMQX_IMAGE1 if not set"
    echo ""
    echo "Options:"
    echo "  -h, --help: Show this help message and exit."
    echo "  -P: Add -p options for docker run to expose more HAProxy container ports."
    echo "  -6: Test with IPv6"
    echo "  -c: Cleanup: delete docker network, force delete the containers."
    echo "  -d: '1', '2', or 'both' (defualt = 'both')"
    echo "          1: Only put node 1 behind haproxy"
    echo "          2: Only put node 2 behind haproxy"
    echo "       both: This is the default value, which means both nodes serve dashboard"
    echo "       This is often needed for tests which want to check one dashboard version"
    echo "       when starting two different versions of EMQX."
}

while getopts "hc6P:d:" opt
do
    case $opt in
        # -P option is treated similarly to docker run -P:
        # publish ports to random available host ports
        P) HAPROXY_PORTS=(-p 18083 -p 1883 -p 8883 -p 8884);;
        c) cleanup; exit 0;;
        h) show_help; exit 0;;
        6) IPV6=1;;
        d) DASHBOARD_NODES="$OPTARG";;
        *) ;;
    esac
done
shift $((OPTIND - 1))

IMAGE1="${1:-}"
IMAGE2="${2:-${IMAGE1}}"

DASHBOARD_BACKEND1="server emqx-1 $NODE1:18083"
DASHBOARD_BACKEND2="server emqx-2 $NODE2:18083"
case "${DASHBOARD_NODES}" in
    1)
        DASHBOARD_BACKEND2=""
        ;;
    2)
        DASHBOARD_BACKEND1=""
        ;;
    both)
        ;;
esac

if [ -z "${IMAGE1:-}" ] || [ -z "${IMAGE2:-}" ]; then
    show_help
    exit 1
fi

cleanup

if [ ${IPV6} = 1 ]; then
    docker network create --ipv6 --subnet 2001:0DB8::/112 "$NET"
    RPC_ADDRESS="::"
    PROTO_DIST='inet6_tls'
else
    docker network create "$NET"
    RPC_ADDRESS="0.0.0.0"
    PROTO_DIST='inet_tls'
fi

docker run -d -t --restart=always --name "$NODE1" \
  --net "$NET" \
  -e EMQX_LOG__CONSOLE_HANDLER__LEVEL=debug \
  -e EMQX_NODE_NAME="emqx@$NODE1" \
  -e EMQX_NODE_COOKIE="$COOKIE" \
  -e EMQX_CLUSTER__PROTO_DIST="${PROTO_DIST}" \
  -e EMQX_RPC__LISTEN_ADDRESS="${RPC_ADDRESS}" \
  -e EMQX_RPC__IPV6_ONLY="true" \
  -e EMQX_listeners__ssl__default__enable=false \
  -e EMQX_listeners__wss__default__enable=false \
  -e EMQX_listeners__tcp__default__proxy_protocol=true \
  -e EMQX_listeners__ws__default__proxy_protocol=true \
  -e EMQX_LICENSE__KEY="${EMQX_LICENSE__KEY1:-evaluation}" \
  "$IMAGE1"

docker run -d -t --restart=always --name "$NODE2" \
  --net "$NET" \
  -e EMQX_LOG__CONSOLE_HANDLER__LEVEL=debug \
  -e EMQX_NODE_NAME="emqx@$NODE2" \
  -e EMQX_NODE_COOKIE="$COOKIE" \
  -e EMQX_CLUSTER__PROTO_DIST="${PROTO_DIST}" \
  -e EMQX_RPC__LISTEN_ADDRESS="${RPC_ADDRESS}" \
  -e EMQX_RPC__IPV6_ONLY="true" \
  -e EMQX_listeners__ssl__default__enable=false \
  -e EMQX_listeners__wss__default__enable=false \
  -e EMQX_listeners__tcp__default__proxy_protocol=true \
  -e EMQX_listeners__ws__default__proxy_protocol=true \
  -e EMQX_LICENSE__KEY="${EMQX_LICENSE__KEY2:-evaluation}" \
  "$IMAGE2"

mkdir -p tmp
cat <<EOF > tmp/haproxy.cfg
##----------------------------------------------------------------
## global 2021/04/05
##----------------------------------------------------------------
global
    log stdout format raw daemon debug
    # Replace 1024000 with deployment connections
    maxconn 102400
    nbproc 1
    nbthread 2
    cpu-map auto:1/1-2 0-1
    tune.ssl.default-dh-param 2048
    ssl-default-bind-ciphers ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES256-GCM-SHA384:DHE-RSA-AES128-GCM-SHA256:DHE-DSS-AES128-GCM-SHA256:kEDH+AESGCM:ECDHE-RSA-AES128-SHA256:ECDHE-ECDSA-AES128-SHA256:ECDHE-RSA-AES128-SHA:ECDHE-ECDSA-AES128-SHA:ECDHE-RSA-AES256-SHA384:ECDHE-ECDSA-AES256-SHA384:ECDHE-RSA-AES256-SHA:ECDHE-ECDSA-AES256-SHA:DHE-RSA-AES128-SHA256:DHE-RSA-AES128-SHA:DHE-DSS-AES128-SHA256:DHE-RSA-AES256-SHA256:DHE-DSS-AES256-SHA:DHE-RSA-AES256-SHA:ECDHE-RSA-DES-CBC3-SHA:ECDHE-ECDSA-DES-CBC3-SHA:EDH-RSA-DES-CBC3-SHA:AES128-GCM-SHA256:AES256-GCM-SHA384:AES128-SHA256:AES256-SHA256:AES128-SHA:AES256-SHA:AES:DES-CBC3-SHA:HIGH:SEED:!aNULL:!eNULL:!EXPORT:!DES:!RC4:!MD5:!PSK:!RSAPSK:!aDH:!aECDH:!EDH-DSS-DES-CBC3-SHA:!KRB5-DES-CBC3-SHA:!SRP
    # Enable the HAProxy Runtime API
    # e.g. echo "show table emqx_tcp_back" | sudo socat stdio tcp4-connect:172.100.239.4:9999
    stats socket :9999 level admin expose-fd listeners

##----------------------------------------------------------------
## defaults
##----------------------------------------------------------------
defaults
    log global
    mode tcp
    option tcplog
    # Replace 1024000 with deployment connections
    maxconn 102400
    timeout connect 30000
    timeout client 600s
    timeout server 600s

##----------------------------------------------------------------
## API
##----------------------------------------------------------------
frontend emqx_dashboard
    mode tcp
    option tcplog
    bind *:18083
    default_backend emqx_dashboard_back

backend emqx_dashboard_back
    # Must use a consistent dispatch when EMQX is running on different versions
    # because the js files for the dashboard is chunked, having the backends sharing
    # load randomly will cause the browser fail to GET some chunks (or get bad chunks if names clash)
    balance source
    mode http
    ${DASHBOARD_BACKEND1}
    ${DASHBOARD_BACKEND2}

##----------------------------------------------------------------
## MQTT Listeners
##----------------------------------------------------------------
frontend emqx_tcp
    mode tcp
    option tcplog
    bind *:1883
    default_backend emqx_backend_tcp

frontend emqx_ssl
    mode tcp
    option tcplog
    bind *:8883 ssl crt /tmp/emqx.pem ca-file /usr/local/etc/haproxy/certs/cacert.pem verify required no-sslv3
    default_backend emqx_backend_tcp

frontend emqx_wss
    mode tcp
    option tcplog
    bind *:8884 ssl crt /tmp/emqx.pem ca-file /usr/local/etc/haproxy/certs/cacert.pem verify required no-sslv3
    default_backend emqx_backend_ws

backend emqx_backend_tcp
    mode tcp
    balance static-rr
    server emqx-1 $NODE1:1883 check-send-proxy send-proxy-v2-ssl-cn
    server emqx-2 $NODE2:1883 check-send-proxy send-proxy-v2-ssl-cn

backend emqx_backend_ws
    mode tcp
    balance static-rr
    server emqx-1 $NODE1:8083 check-send-proxy send-proxy-v2-ssl-cn
    server emqx-2 $NODE2:8083 check-send-proxy send-proxy-v2-ssl-cn
EOF

HAPROXY_IMAGE='ghcr.io/haproxytech/haproxy-docker-alpine:2.4.27'

haproxy_cid=$(docker run -d --name haproxy \
                     --net "$NET" \
                     -v "$(pwd)/tmp/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg" \
                     -v "$(pwd)/apps/emqx/etc/certs:/usr/local/etc/haproxy/certs" \
                     -w /usr/local/etc/haproxy \
                     "${HAPROXY_PORTS[@]}" \
                     "${HAPROXY_IMAGE}" \
                     sh -c 'set -euo pipefail;
                              cat certs/cert.pem certs/key.pem > /tmp/emqx.pem;
                              haproxy -f haproxy.cfg')

haproxy_ssl_port=$(docker inspect --format='{{(index (index .NetworkSettings.Ports "8884/tcp") 0).HostPort}}' "$haproxy_cid")

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

## Probe wss listener by haproxy.
probe_wss_listener() {
    openssl s_client \
        -CAfile apps/emqx/etc/certs/cacert.pem \
        -cert apps/emqx/etc/certs/client-cert.pem \
        -key apps/emqx/etc/certs/client-key.pem \
        -connect localhost:"$haproxy_ssl_port" </dev/null >/dev/null 2>&1
}

wait_for_haproxy() {
    wait_sec=0
    wait_limit="$1"
    set +x
    while ! probe_wss_listener; do
        wait_sec=$(( wait_sec + 1 ))
        if [ $wait_sec -gt "$wait_limit" ]; then
            echo "timeout wait for haproxy"
            exit 1
        fi
        echo -n '.'
        sleep 1
    done
}

wait_for_emqx "$NODE1" 60
wait_for_emqx "$NODE2" 30
wait_for_haproxy 30

echo

docker exec "${NODE2}" emqx ctl cluster join "emqx@$NODE1"

RUNNING_NODES="$(docker exec -t "$NODE1" emqx ctl cluster status --json | jq '.running_nodes | length')"
if ! [ "${RUNNING_NODES}" -eq "${EXPECTED_RUNNING_NODES:-2}" ]; then
    echo "Expected running nodes is ${EXPECTED_RUNNING_NODES}, but got ${RUNNING_NODES}"
    exit 1
fi

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
docker rm -f haproxy >/dev/null 2>&1 || true
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
  -e EMQX_listeners__ssl__default__enable=false \
  -e EMQX_listeners__wss__default__enable=false \
  -e EMQX_listeners__tcp__default__proxy_protocol=true \
  -e EMQX_listeners__ws__default__proxy_protocol=true \
  "$IMAGE"

docker run -d -t --restart=always --name "$NODE2" \
  --net "$NET" \
  -e EMQX_LOG__CONSOLE_HANDLER__LEVEL=debug \
  -e EMQX_NODE_NAME="emqx@$NODE2" \
  -e EMQX_NODE_COOKIE="$COOKIE" \
  -e EMQX_CLUSTER__PROTO_DIST='inet_tls' \
  -e EMQX_listeners__ssl__default__enable=false \
  -e EMQX_listeners__wss__default__enable=false \
  -e EMQX_listeners__tcp__default__proxy_protocol=true \
  -e EMQX_listeners__ws__default__proxy_protocol=true \
  "$IMAGE"

mkdir -p tmp
cat <<EOF > tmp/haproxy.cfg
##----------------------------------------------------------------
## global 2021/04/05
##----------------------------------------------------------------
global
    log stdout format raw daemon debug
    # Replace 1024000 with deployment connections
    maxconn 1000
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
    maxconn 1000
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
    mode http
    # balance static-rr
    server emqx-1 $NODE1:18083
    server emqx-2 $NODE2:18083

##----------------------------------------------------------------
## TLS
##----------------------------------------------------------------
frontend emqx_ssl
    mode tcp
    option tcplog
    bind *:8883 ssl crt /tmp/emqx.pem ca-file /usr/local/etc/haproxy/certs/cacert.pem verify required no-sslv3
    default_backend emqx_ssl_back

frontend emqx_wss
    mode tcp
    option tcplog
    bind *:8084 ssl crt /tmp/emqx.pem ca-file /usr/local/etc/haproxy/certs/cacert.pem verify required no-sslv3
    default_backend emqx_wss_back

backend emqx_ssl_back
    mode tcp
    balance static-rr
    server emqx-1 $NODE1:1883 check-send-proxy send-proxy-v2-ssl-cn
    server emqx-2 $NODE2:1883 check-send-proxy send-proxy-v2-ssl-cn

backend emqx_wss_back
    mode tcp
    balance static-rr
    server emqx-1 $NODE1:8083 check-send-proxy send-proxy-v2-ssl-cn
    server emqx-2 $NODE2:8083 check-send-proxy send-proxy-v2-ssl-cn
EOF


docker run -d --name haproxy \
    --net "$NET" \
    -v "$(pwd)/tmp/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg" \
    -v "$(pwd)/apps/emqx/etc/certs:/usr/local/etc/haproxy/certs" \
    -w /usr/local/etc/haproxy \
    -p 18083:18083 \
    -p 8883:8883 \
    -p 8084:8084 \
    "haproxy:2.4" \
    bash -c 'set -euo pipefail;
             cat certs/cert.pem certs/key.pem > /tmp/emqx.pem;
             haproxy -f haproxy.cfg'

wait_limit=60
wait_for_emqx() {
    container="$1"
    wait_limit="$2"
    wait_sec=0
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

wait_for_haproxy() {
    wait_sec=0
    wait_limit="$1"
    set +x
    while ! openssl s_client \
                -CAfile apps/emqx/etc/certs/cacert.pem \
                -cert apps/emqx/etc/certs/cert.pem \
                -key apps/emqx/etc/certs/key.pem \
                localhost:8084 </dev/null; do
        wait_sec=$(( wait_sec + 1 ))
        if [ $wait_sec -gt "$wait_limit" ]; then
            echo "timeout wait for haproxy"
            exit 1
        fi
        echo -n '.'
        sleep 1
    done
}

wait_for_emqx "$NODE1" 30
wait_for_emqx "$NODE2" 30
wait_for_haproxy 10

echo

docker exec $NODE1 emqx_ctl cluster join "emqx@$NODE2"

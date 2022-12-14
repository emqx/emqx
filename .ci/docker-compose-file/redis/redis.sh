#!/bin/bash

set -x

LOCAL_IP=$(hostname -i | grep -oE '((25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])\.){3}(25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])' | head -n 1)

node=single
tls=false
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -n|--node)
    node="$2"
    shift # past argument
    shift # past value
    ;;
    --tls-enabled)
    tls=true
    shift # past argument
    ;;
    *)
    shift # past argument
    ;;
esac
done

rm -f \
    /data/conf/r7000i.log \
    /data/conf/r7001i.log \
    /data/conf/r7002i.log \
    /data/conf/nodes.7000.conf \
    /data/conf/nodes.7001.conf \
    /data/conf/nodes.7002.conf

if [ "$node" = "cluster" ]; then
  if $tls; then
    redis-server /data/conf/redis-tls.conf --port 7000 --cluster-config-file /data/conf/nodes.7000.conf \
                                           --tls-port 8000 --cluster-enabled yes
    redis-server /data/conf/redis-tls.conf --port 7001 --cluster-config-file /data/conf/nodes.7001.conf \
                                           --tls-port 8001 --cluster-enabled yes
    redis-server /data/conf/redis-tls.conf --port 7002 --cluster-config-file /data/conf/nodes.7002.conf \
                                           --tls-port 8002 --cluster-enabled yes
  else
    redis-server /data/conf/redis.conf --port 7000 --cluster-config-file /data/conf/nodes.7000.conf \
                                       --cluster-enabled yes
    redis-server /data/conf/redis.conf --port 7001 --cluster-config-file /data/conf/nodes.7001.conf \
                                       --cluster-enabled yes
    redis-server /data/conf/redis.conf --port 7002 --cluster-config-file /data/conf/nodes.7002.conf \
                                       --cluster-enabled yes
  fi
elif [ "$node" = "sentinel" ]; then
  if $tls; then
    redis-server /data/conf/redis-tls.conf --port 7000 --cluster-config-file /data/conf/nodes.7000.conf \
                                           --tls-port 8000 --cluster-enabled no
    redis-server /data/conf/redis-tls.conf --port 7001 --cluster-config-file /data/conf/nodes.7001.conf \
                                           --tls-port 8001 --cluster-enabled no --slaveof "$LOCAL_IP" 8000
    redis-server /data/conf/redis-tls.conf --port 7002 --cluster-config-file /data/conf/nodes.7002.conf \
                                           --tls-port 8002 --cluster-enabled no --slaveof "$LOCAL_IP" 8000

  else
    redis-server /data/conf/redis.conf --port 7000 --cluster-config-file /data/conf/nodes.7000.conf \
                                       --cluster-enabled no
    redis-server /data/conf/redis.conf --port 7001 --cluster-config-file /data/conf/nodes.7001.conf \
                                       --cluster-enabled no --slaveof "$LOCAL_IP" 7000
    redis-server /data/conf/redis.conf --port 7002 --cluster-config-file /data/conf/nodes.7002.conf \
                                       --cluster-enabled no --slaveof "$LOCAL_IP" 7000
  fi
fi

REDIS_LOAD_FLG=true

while $REDIS_LOAD_FLG;
do
    sleep 1
    redis-cli --pass public --no-auth-warning -p 7000 info 1> /data/conf/r7000i.log 2> /dev/null
    if ! [ -s /data/conf/r7000i.log ]; then
        continue
    fi
    redis-cli --pass public --no-auth-warning -p 7001 info 1> /data/conf/r7001i.log 2> /dev/null
    if ! [ -s /data/conf/r7001i.log ]; then
        continue
    fi
    redis-cli --pass public --no-auth-warning -p 7002 info 1> /data/conf/r7002i.log 2> /dev/null;
    if ! [ -s /data/conf/r7002i.log ]; then
        continue
    fi
    if [ "$node" = "cluster" ] ; then
      if $tls; then
        yes "yes" | redis-cli --cluster create "$LOCAL_IP:8000" "$LOCAL_IP:8001" "$LOCAL_IP:8002" \
                              --pass public --no-auth-warning \
                              --tls true --cacert /etc/certs/ca.crt \
                                         --cert /etc/certs/redis.crt --key /etc/certs/redis.key
      else
        yes "yes" | redis-cli --cluster create "$LOCAL_IP:7000" "$LOCAL_IP:7001" "$LOCAL_IP:7002" \
                              --pass public --no-auth-warning
      fi
    elif [ "$node" = "sentinel" ]; then
      tee /_sentinel.conf>/dev/null << EOF
port 26379
bind 0.0.0.0 ::
daemonize yes
logfile /var/log/redis-server.log
dir /tmp
EOF
      if $tls; then
          cat >>/_sentinel.conf<<EOF
tls-port 26380
tls-replication yes
tls-cert-file /etc/certs/redis.crt
tls-key-file /etc/certs/redis.key
tls-ca-cert-file /etc/certs/ca.crt
sentinel monitor mymaster $LOCAL_IP 8000 1
EOF
      else
          cat >>/_sentinel.conf<<EOF
sentinel monitor mymaster $LOCAL_IP 7000 1
EOF
      fi
      redis-server /_sentinel.conf --sentinel
    fi
    REDIS_LOAD_FLG=false
done

exit 0;

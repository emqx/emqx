#!/bin/bash

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
    -t|--tls-enabled)
    tls="$2"
    shift # past argument
    shift # past value
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
    /data/conf/nodes.7002.conf ;

if [ "${node}" = "cluster" ] ; then
  if $tls ; then
    redis-server /data/conf/redis-tls.conf --port 7000 --cluster-config-file /data/conf/nodes.7000.conf \
                                           --tls-port 8000 --cluster-enabled yes ;
    redis-server /data/conf/redis-tls.conf --port 7001 --cluster-config-file /data/conf/nodes.7001.conf \
                                           --tls-port 8001 --cluster-enabled yes;
    redis-server /data/conf/redis-tls.conf --port 7002 --cluster-config-file /data/conf/nodes.7002.conf \
                                           --tls-port 8002 --cluster-enabled yes;
  else
    redis-server /data/conf/redis.conf --port 7000 --cluster-config-file /data/conf/nodes.7000.conf --cluster-enabled yes;
    redis-server /data/conf/redis.conf --port 7001 --cluster-config-file /data/conf/nodes.7001.conf --cluster-enabled yes;
    redis-server /data/conf/redis.conf --port 7002 --cluster-config-file /data/conf/nodes.7002.conf --cluster-enabled yes;
  fi
elif [ "${node}" = "sentinel" ] ; then
    redis-server /data/conf/redis.conf --port 7000 --cluster-config-file /data/conf/nodes.7000.conf \
                                       --cluster-enabled no;
    redis-server /data/conf/redis.conf --port 7001 --cluster-config-file /data/conf/nodes.7001.conf \
                                       --cluster-enabled no --slaveof 172.16.239.10 7000;
    redis-server /data/conf/redis.conf --port 7002 --cluster-config-file /data/conf/nodes.7002.conf \
                                       --cluster-enabled no --slaveof 172.16.239.10 7000;
fi
REDIS_LOAD_FLG=true;

while $REDIS_LOAD_FLG;
do
    sleep 1;
    redis-cli -p 7000 info 1> /data/conf/r7000i.log 2> /dev/null;
    if [ -s /data/conf/r7000i.log ]; then
        :
    else
        continue;
    fi
    redis-cli -p 7001 info 1> /data/conf/r7001i.log 2> /dev/null;
    if [ -s /data/conf/r7001i.log ]; then
        :
    else
        continue;
    fi
    redis-cli -p 7002 info 1> /data/conf/r7002i.log 2> /dev/null;
    if [ -s /data/conf/r7002i.log ]; then
        :
    else
        continue;
    fi
    if [ "${node}" = "cluster" ] ; then
      yes "yes" | redis-cli --cluster create 172.16.239.10:7000 172.16.239.10:7001 172.16.239.10:7002;
    elif [ "${node}" = "sentinel" ] ; then
      cp /data/conf/sentinel.conf /_sentinel.conf
      redis-server /_sentinel.conf --sentinel;
    fi
    REDIS_LOAD_FLG=false;
done

exit 0;

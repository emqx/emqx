#!/usr/bin/env bash

set -euo pipefail


TIMEOUT=60

echo "+++++++ Sleep for a while to make sure that old keytab and truststore is deleted ++++++++"

sleep 5

echo "+++++++ Wait until Kerberos Keytab is created ++++++++"

timeout $TIMEOUT bash -c 'until [ -f /var/lib/secret/kafka.keytab ]; do sleep 1; done'


echo "+++++++ Wait until SSL certs are generated ++++++++"

timeout $TIMEOUT bash -c 'until [ -f /var/lib/secret/kafka.truststore.jks ]; do sleep 1; done'

sleep 3

echo "+++++++ Starting Kafka ++++++++"

start-kafka.sh &

SERVER=localhost
PORT1=9092
PORT2=9093
TIMEOUT=60

echo "+++++++ Wait until Kafka ports are up ++++++++"

# shellcheck disable=SC2016
timeout $TIMEOUT bash -c 'until printf "" 2>>/dev/null >>/dev/tcp/$0/$1; do sleep 1; done' $SERVER $PORT1

# shellcheck disable=SC2016
timeout $TIMEOUT bash -c 'until printf "" 2>>/dev/null >>/dev/tcp/$0/$1; do sleep 1; done' $SERVER $PORT2

echo "+++++++ Run config commands ++++++++"

kafka-configs.sh --bootstrap-server localhost:9092 --alter --add-config 'SCRAM-SHA-256=[iterations=8192,password=password],SCRAM-SHA-512=[password=password]' --entity-type users --entity-name emqxuser

echo "+++++++ Wait until Kafka ports are down ++++++++"

bash -c 'while printf "" 2>>/dev/null >>/dev/tcp/$0/$1; do sleep 1; done' $SERVER $PORT1

echo "+++++++ Kafka ports are down ++++++++"


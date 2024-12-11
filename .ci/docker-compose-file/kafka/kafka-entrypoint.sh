#!/usr/bin/env bash

set -euo pipefail


TIMEOUT=60

echo "+++++++ Sleep for a while to make sure that old keytab and truststore is deleted ++++++++"

sleep 5

echo "+++++++ Wait until Kerberos Keytab is created ++++++++"

timeout $TIMEOUT bash -c 'until [ -f /var/lib/secret-ro/kafka.keytab ]; do sleep 1; done'


echo "+++++++ Wait until SSL certs are generated ++++++++"

timeout $TIMEOUT bash -c 'until [ -f /var/lib/secret-ro/kafka.truststore.jks ]; do sleep 1; done'

mkdir -p /var/lib/secret

## make our own copy for this container
cp /var/lib/secret-ro/* /var/lib/secret/

keytool -list -v -keystore /var/lib/secret/kafka.keystore.jks -storepass password

sleep 3

echo "+++++++ Starting Kafka ++++++++"

# fork start Kafka
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

# create topics after re-configuration
# there seem to be a race condition when creating the topics (too early)
if [[ "$KAFKA_BROKER_ID" == 1 ]]; then
  echo "+++++++ Creating Kafka Topics ++++++++"

  env KAFKA_CREATE_TOPICS="$KAFKA_CREATE_TOPICS_NG" KAFKA_PORT="$PORT1" create-topics.sh

  # create a topic with max.message.bytes=100
  /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server "${SERVER}:${PORT1}" --topic max-100-bytes --partitions 1 --replication-factor 1 --config max.message.bytes=100
fi

echo "+++++++ Wait until Kafka ports are down ++++++++"

bash -c 'while printf "" 2>>/dev/null >>/dev/tcp/$0/$1; do sleep 1; done' $SERVER $PORT1

echo "+++++++ Kafka ports are down ++++++++"

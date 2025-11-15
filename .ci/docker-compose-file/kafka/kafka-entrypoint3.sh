#!/usr/bin/env bash

set -euo pipefail

SERVER=localhost
PORT1=9092

# fork start Kafka
start-kafka.sh &

if [[ "$KAFKA_BROKER_ID" == 3 ]]; then
  echo "+++++++ Creating Kafka Topics ++++++++"

  env KAFKA_CREATE_TOPICS="$KAFKA_CREATE_TOPICS_NG" KAFKA_PORT="$PORT1" create-topics.sh
fi

echo "+++++++ Wait until Kafka ports are down ++++++++"

bash -c 'while printf "" 2>>/dev/null >>/dev/tcp/$0/$1; do sleep 1; done' $SERVER $PORT1

echo "+++++++ Kafka ports are down ++++++++"

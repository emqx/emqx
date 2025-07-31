#!/bin/bash

set -ex

topic="$1"
keys="$2"

if [ -z "$topic" ] || [ -z "$keys" ]; then
    echo "Usage: $0 <topic> <compaction_key_num>"
    exit 1
fi

n=0

while true; do
    n=$((n + 1))
    compaction_key="key$(($RANDOM % $keys))"
    payload="dummy message $n $compaction_key"
    mosquitto_pub -t "$topic" -m "$payload" -D publish user-property mq-compaction-key "$compaction_key"
    sleep 0.3
done

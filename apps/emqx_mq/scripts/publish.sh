#!/bin/bash

set -ex

topic="$1"
keys="$2"

if [ -z "$topic" ] || [ -z "$keys" ]; then
    echo "Usage: $0 <topic> <key_num>"
    exit 1
fi

n=0

while true; do
    n=$((n + 1))
    key="key$((RANDOM % keys))"
    payload="dummy message $n $key"
    mosquitto_pub -t "$topic" -m "$payload" -D publish user-property mq-key "$key"
    sleep 0.3
done

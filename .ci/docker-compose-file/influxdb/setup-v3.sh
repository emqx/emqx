#!/usr/bin/env bash

set -exu

ls -la /var/lib/secret_container

# copy to shared mount
cp "/var/lib/secret_container/influxv3.json" "/var/lib/secret/influxv3_$MODE.json"

exec bash -c "$@"

#!/usr/bin/env bash

NET='emqx.influx.io'
NODE2_FQDN="emqx@emqx_2.$NET"
NODE1_CONTAINER_NAME="emqx_1"
NODE2_CONTAINER_NAME="emqx_2"
INFLUXDB_CONTAINER_NAME="influxdb_server"
export EMQX_IMAGE_TAG="${EMQX_IMAGE_TAG:-latest}"

cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" || exit

docker rm -f "$NODE1_CONTAINER_NAME" "$NODE2_CONTAINER_NAME" "$INFLUXDB_CONTAINER_NAME"
docker-compose up -d

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

wait_for_emqx "$NODE1_CONTAINER_NAME" 30
wait_for_emqx "$NODE2_CONTAINER_NAME" 30

echo

docker exec "$NODE1_CONTAINER_NAME" emqx_ctl cluster join "$NODE2_FQDN"

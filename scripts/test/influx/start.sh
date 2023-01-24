#!/usr/bin/env bash

NET='emqx.influx.io'
NODE1="emqx@emqx_1.$NET"
NODE2="emqx@emqx_2.$NET"
export EMQX_IMAGE_TAG="${EMQX_IMAGE_TAG:-latest}"

cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")"

docker rm -f emqx_1 emqx_2 influxdb_server
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

wait_for_emqx emqx_1 30
wait_for_emqx emqx_2 30

echo

docker exec emqx_1 emqx_ctl cluster join "$NODE2"

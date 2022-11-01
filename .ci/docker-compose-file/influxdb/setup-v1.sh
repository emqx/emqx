#!/usr/bin/env bash

set -e

# influx v1 dbrp create \
#   --bucket-id ${DOCKER_INFLUXDB_INIT_BUCKET_ID} \
#   --db ${V1_DB_NAME} \
#   --rp ${V1_RP_NAME} \
#   --default \
#   --org ${DOCKER_INFLUXDB_INIT_ORG}

influx v1 auth create \
  --username "${DOCKER_INFLUXDB_INIT_USERNAME}" \
  --password "${DOCKER_INFLUXDB_INIT_PASSWORD}" \
  --write-bucket "${DOCKER_INFLUXDB_INIT_BUCKET_ID}" \
  --org "${DOCKER_INFLUXDB_INIT_ORG}"

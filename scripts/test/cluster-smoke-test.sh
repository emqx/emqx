#!/usr/bin/env bash

set -euo pipefail

[ $# -ne 1 ] && { echo "Usage: $0 DOCKER_IMAGE_TAG"; exit 1; }

DOCKER_IMAGE_TAG="$1"

check_cluster_status () {
    docker exec -it 'node1.emqx.io' emqx ctl cluster status --json
}

cd -P -- "$(dirname -- "$0")"

## can join cluster by default
echo '======== Allow clustering with "evaluation" license'
env LICENSE_KEY1='evaluation' \
    LICENSE_KEY2='default' \
./start-two-nodes-in-docker.sh "${DOCKER_IMAGE_TAG}"

## cannot join cluster if node1 does not have a license
echo '======== Do not allow clustering with "default" license'
! env LICENSE_KEY1='default' \
      LICENSE_KEY2='default' \
  ./start-two-nodes-in-docker.sh "${DOCKER_IMAGE_TAG}" || {
    echo "ERROR: 'default' license allowed cluster join, but expected to fail"
    exit 1
}

echo '======== Allow clustering with "default" license if peer node is before 5.9'
## new (>= 5.9.0) can join cluster with old (< 5.9.0)
IMAGE1='emqx/emqx-enterprise:5.8.6'
env LICENSE_KEY1='default' \
    LICENSE_KEY2='default' \
./start-two-nodes-in-docker.sh "${IMAGE1}" "${DOCKER_IMAGE_TAG}"

## Clenaup
./start-two-nodes-in-docker.sh -c

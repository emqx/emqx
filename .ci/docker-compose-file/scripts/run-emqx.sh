#!/bin/bash
set -euxo pipefail

# _EMQX_DOCKER_IMAGE_TAG is shared with docker-compose file
export _EMQX_DOCKER_IMAGE_TAG="$1"
_EMQX_TEST_DB_BACKEND="${2:-${_EMQX_TEST_DB_BACKEND:-mnesia}}"

case "$_EMQX_TEST_DB_BACKEND" in
  rlog)
    CLUSTER_OVERRIDES=".ci/docker-compose-file/docker-compose-emqx-cluster-rlog.override.yaml"
    ;;
  mnesia)
    CLUSTER_OVERRIDES=".ci/docker-compose-file/docker-compose-emqx-cluster-mnesia.override.yaml"
    ;;
  *)
    echo "ERROR: Unknown DB backend: ${_EMQX_TEST_DB_BACKEND}"
    exit 1
    ;;
esac

{
  echo "HOCON_ENV_OVERRIDE_PREFIX=EMQX_"
  echo "EMQX_ZONES__DEFAULT__MQTT__RETRY_INTERVAL=2s"
  echo "EMQX_ZONES__DEFAULT__MQTT__MAX_TOPIC_ALIAS=10"
  echo "EMQX_AUTHORIZATION__SOURCES=[]"
  echo "EMQX_AUTHORIZATION__NO_MATCH=allow"
} >> .ci/docker-compose-file/conf.cluster.env

is_node_up() {
  local node="$1"
  docker exec -i "$node" \
         bash -c "emqx eval-erl \"['emqx@node1.emqx.io','emqx@node2.emqx.io'] = maps:get(running_nodes, ekka_cluster:info()).\"" > /dev/null 2>&1
}

is_node_listening() {
  local node="$1"
  docker exec -i "$node" \
         emqx ctl listeners | \
    grep -A6 'tcp:default' | \
    grep -qE 'running *: true'
}

is_cluster_up() {
  is_node_up node1.emqx.io && \
    is_node_up node2.emqx.io && \
    is_node_listening node1.emqx.io && \
    is_node_listening node2.emqx.io
}

docker-compose \
  -f .ci/docker-compose-file/docker-compose-emqx-cluster.yaml \
  -f "$CLUSTER_OVERRIDES" \
  -f .ci/docker-compose-file/docker-compose-python.yaml \
  up -d

while ! is_cluster_up; do
  echo "['$(date -u +"%Y-%m-%dT%H:%M:%SZ")']:waiting emqx";
  sleep 5;
done

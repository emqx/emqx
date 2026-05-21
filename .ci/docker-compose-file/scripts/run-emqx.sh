#!/bin/bash
set -euxo pipefail

# _EMQX_DOCKER_IMAGE_TAG is shared with docker-compose file
export _EMQX_DOCKER_IMAGE_TAG="$1"

emqx_db_backend="${2:-${_EMQX_TEST_DB_BACKEND:-mnesia}}"
emqx_node1_envfile=.ci/docker-compose-file/conf.node1.env
emqx_node2_envfile=.ci/docker-compose-file/conf.node2.env

case "${emqx_db_backend}" in
  rlog)
    {
      echo "EMQX_NODE__DB_BACKEND=rlog"
      echo "EMQX_NODE__DB_ROLE=core"
      echo "EMQX_CLUSTER__STATIC__SEEDS=[emqx@node1.emqx.io]"
    } >> ${emqx_node1_envfile}
    {
      echo "EMQX_NODE__DB_BACKEND=rlog"
      echo "EMQX_NODE__DB_ROLE=replicant"
      echo "EMQX_CLUSTER__CORE_NODES=emqx@node1.emqx.io"
      echo "EMQX_CLUSTER__STATIC__SEEDS=[emqx@node1.emqx.io]"
    } >> ${emqx_node2_envfile}
    ;;
  mnesia)
    {
      echo "EMQX_NODE__DB_BACKEND=mnesia"
    } | tee -a ${emqx_node1_envfile} ${emqx_node2_envfile} >/dev/null
    ;;
  *)
    echo "ERROR: Unknown DB backend: ${emqx_db_backend}"
    exit 1
    ;;
esac

{
  echo "EMQX_MQTT__RETRY_INTERVAL=2s"
  echo "EMQX_MQTT__MAX_TOPIC_ALIAS=10"
  echo "EMQX_AUTHORIZATION__SOURCES=[]"
  echo "EMQX_AUTHORIZATION__NO_MATCH=allow"
} | tee -a ${emqx_node1_envfile} ${emqx_node2_envfile} >/dev/null

is_node_up() {
  local node="$1"
  docker exec -i "$node" \
         bash -c "emqx eval \"['emqx@node1.emqx.io','emqx@node2.emqx.io'] = maps:get(running_nodes, ekka_cluster:info()).\"" > /dev/null 2>&1
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
  -f .ci/docker-compose-file/docker-compose-python.yaml \
  up -d

while ! is_cluster_up; do
  echo "['$(date -u +"%Y-%m-%dT%H:%M:%SZ")']:waiting emqx";
  sleep 5;
done

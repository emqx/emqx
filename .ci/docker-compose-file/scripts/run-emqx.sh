#!/bin/bash
set -euxo pipefail

# _EMQX_DOCKER_IMAGE_TAG is shared with docker-compose file
export _EMQX_DOCKER_IMAGE_TAG="$1"
_EMQX_TEST_DB_BACKEND="${2:-${_EMQX_TEST_DB_BACKEND:-mnesia}}"

if [ "$_EMQX_TEST_DB_BACKEND" = "rlog" ]
then
  CLUSTER_OVERRIDES="-f .ci/docker-compose-file/docker-compose-emqx-cluster-rlog.override.yaml"
else
  CLUSTER_OVERRIDES=""
fi

{
  echo "HOCON_ENV_OVERRIDE_PREFIX=EMQX_"
  echo "EMQX_ZONES__DEFAULT__MQTT__RETRY_INTERVAL=2s"
  echo "EMQX_ZONES__DEFAULT__MQTT__MAX_TOPIC_ALIAS=10"
} >> .ci/docker-compose-file/conf.cluster.env

is_node_up() {
  local node
  node="$1"
  docker exec -i "$node" \
         bash -c "emqx eval \"['emqx@node1.emqx.io','emqx@node2.emqx.io'] = maps:get(running_nodes, ekka_cluster:info()).\"" > /dev/null 2>&1
}

is_node_listening() {
  local node
  node="$1"
  docker exec -i "$node" \
         emqx eval "ok = case gen_tcp:connect(\"localhost\", 1883, []) of {ok, P} -> gen_tcp:close(P), ok; _ -> exit(1) end." > /dev/null 2>&1
}

is_cluster_up() {
  is_node_up node1.emqx.io && \
    is_node_up node2.emqx.io && \
    is_node_listening node1.emqx.io && \
    is_node_listening node2.emqx.io
}

docker-compose \
  -f .ci/docker-compose-file/docker-compose-emqx-cluster.yaml \
  $CLUSTER_OVERRIDES \
  -f .ci/docker-compose-file/docker-compose-python.yaml \
  up -d

while ! is_cluster_up; do
  echo "['$(date -u +"%Y-%m-%dT%H:%M:%SZ")']:waiting emqx";
  sleep 5;
done

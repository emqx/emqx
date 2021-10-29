#!/bin/bash
set -euxo pipefail

if [ "$EMQX_TEST_DB_BACKEND" = "rlog" ]
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

is_cluster_up() {
  docker exec -i node1.emqx.io \
         bash -c "emqx eval \"['emqx@node1.emqx.io','emqx@node2.emqx.io'] = maps:get(running_nodes, ekka_cluster:info()).\"" > /dev/null 2>&1
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

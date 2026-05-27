#!/bin/bash
set -euo pipefail

emqx_node1_envfile=.ci/docker-compose-file/conf.node1.env
emqx_node2_envfile=.ci/docker-compose-file/conf.node2.env

emqx_image_tag=""
emqx_db_backend="mnesia"
emqx_env_overrides=()
emqx_tcp_backend=""

usage() {
  cat <<'EOF'
Usage:
  run-emqx.sh IMAGE_TAG [OPTIONS]

Options:
  --db-backend BACKEND    EMQX DB backend: 'mnesia' or 'rlog'. Defaults to 'mnesia'.
  --override KEY=VAL      Append an EMQX env override to both nodes. Can be repeated.
  --tcp-backend NAME      Set tcp_backend for both TCP listeners: default and haproxy.
  --help                  Show this help.
EOF
}

usage_error() {
  echo "ERROR: $1" >&2
  usage >&2
  exit 1
}

append_conf() {
  local target="$1"
  shift
  case "${target}" in
    node1)
      printf '%s\n' "$@" >> "${emqx_node1_envfile}"
      ;;
    node2)
      printf '%s\n' "$@" >> "${emqx_node2_envfile}"
      ;;
    all)
      printf '%s\n' "$@" | tee -a "${emqx_node1_envfile}" "${emqx_node2_envfile}" >/dev/null
      ;;
  esac
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --db-backend)
      [ -z "${2:-}" ] && usage_error "${1} requires a value"
      emqx_db_backend="$2"
      shift 2
      ;;
    --override)
      [[ "${2:-}" != EMQX_*=* ]] && usage_error "override must be in EMQX_KEY=VALUE form: ${2}"
      emqx_env_overrides+=("$2")
      shift 2
      ;;
    --tcp-backend)
      [ -z "${2:-}" ] && usage_error "${1} requires a value"
      emqx_tcp_backend="$2"
      shift 2
      ;;
    --help)
      usage
      exit 0
      ;;
    -*)
      usage_error "unknown option: $1"
      ;;
    *)
      if [ -z "${emqx_image_tag}" ]; then
        emqx_image_tag="$1"
      else
        usage_error "unexpected positional argument: $1"
      fi
      shift
      ;;
  esac
done

if [ -z "${emqx_image_tag}" ]; then
  usage_error "Docker image tag is required"
fi

case "${emqx_db_backend}" in
  rlog)
    append_conf node1 "EMQX_NODE__DB_BACKEND=rlog"                     \
                      "EMQX_NODE__DB_ROLE=core"                        \
                      "EMQX_CLUSTER__STATIC__SEEDS=[emqx@node1.emqx.io]"
    append_conf node2 "EMQX_NODE__DB_BACKEND=rlog"                     \
                      "EMQX_NODE__DB_ROLE=replicant"                   \
                      "EMQX_CLUSTER__CORE_NODES=emqx@node1.emqx.io"    \
                      "EMQX_CLUSTER__STATIC__SEEDS=[emqx@node1.emqx.io]"
    ;;
  mnesia)
    append_conf all "EMQX_NODE__DB_BACKEND=mnesia"
    ;;
  *)
    usage_error "Unknown DB backend: ${emqx_db_backend}"
    ;;
esac

append_conf all "EMQX_MQTT__RETRY_INTERVAL=2s"     \
                "EMQX_MQTT__MAX_TOPIC_ALIAS=10"    \
                "EMQX_AUTHORIZATION__SOURCES=[]"   \
                "EMQX_AUTHORIZATION__NO_MATCH=allow"

if [ -n "${emqx_tcp_backend}" ]; then
  append_conf all "EMQX_LISTENERS__TCP__DEFAULT__TCP_BACKEND=${emqx_tcp_backend}" \
                  "EMQX_LISTENERS__TCP__HAPROXY__TCP_BACKEND=${emqx_tcp_backend}"
fi

append_conf all "${emqx_env_overrides[@]}"

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

# _EMQX_DOCKER_IMAGE_TAG is shared with docker-compose file
export _EMQX_DOCKER_IMAGE_TAG="${emqx_image_tag}"
docker-compose \
  -f .ci/docker-compose-file/docker-compose-emqx-cluster.yaml \
  -f .ci/docker-compose-file/docker-compose-python.yaml \
  up -d

while ! is_cluster_up; do
  echo "['$(date -u +"%Y-%m-%dT%H:%M:%SZ")']:waiting emqx";
  sleep 5;
done

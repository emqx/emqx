#!/usr/bin/env bash

## This script runs CT (and necessary dependencies) in docker container(s)

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

help() {
    echo
    echo "-h|--help:              To display this usage info"
    echo "--app lib_dir/app_name: Print apps in json"
    echo "--console:              Start EMQX in console mode"
}

WHICH_APP='novalue'
CONSOLE='no'
while [ "$#" -gt 0 ]; do
    case $1 in
        -h|--help)
            help
            exit 0
            ;;
        --app)
            WHICH_APP="$2"
            shift 2
            ;;
        --console)
            CONSOLE='yes'
            shift 1
            ;;
        *)
            echo "unknown option $1"
            exit 1
            ;;
    esac
done

if [ "${WHICH_APP}" = 'novalue' ]; then
    echo "must provide --app arg"
    exit 1
fi

ERLANG_CONTAINER='erlang24'
DOCKER_CT_ENVS_FILE="${WHICH_APP}/docker-ct"

if [ -f "$DOCKER_CT_ENVS_FILE" ]; then
    # shellcheck disable=SC2002
    CT_DEPS="$(cat "$DOCKER_CT_ENVS_FILE" | xargs)"
fi
CT_DEPS="${ERLANG_CONTAINER} ${CT_DEPS}"

FILES=( )

for dep in ${CT_DEPS}; do
    case "${dep}" in
        erlang24)
            FILES+=( '.ci/docker-compose-file/docker-compose.yaml' )
            ;;
        mongo)
            FILES+=( '.ci/docker-compose-file/docker-compose-mongo-single-tcp.yaml'
                     '.ci/docker-compose-file/docker-compose-mongo-single-tls.yaml' )
            ;;
        redis)
            FILES+=( '.ci/docker-compose-file/docker-compose-redis-single-tcp.yaml'
                     '.ci/docker-compose-file/docker-compose-redis-single-tls.yaml'
                     '.ci/docker-compose-file/docker-compose-redis-sentinel-tcp.yaml'
                     '.ci/docker-compose-file/docker-compose-redis-sentinel-tls.yaml' )
            ;;
        mysql)
            FILES+=( '.ci/docker-compose-file/docker-compose-mysql-tcp.yaml'
                     '.ci/docker-compose-file/docker-compose-mysql-tls.yaml' )
            ;;
        pgsql)
            FILES+=( '.ci/docker-compose-file/docker-compose-pgsql-tcp.yaml'
                     '.ci/docker-compose-file/docker-compose-pgsql-tls.yaml' )
            ;;
        *)
            echo "unknown_ct_dependency $dep"
            exit 1
            ;;
    esac
done

F_OPTIONS=""

for file in "${FILES[@]}"; do
    F_OPTIONS="$F_OPTIONS -f $file"
done

# shellcheck disable=2086 # no quotes for F_OPTIONS
docker-compose $F_OPTIONS up -d --build

# /emqx is where the source dir is mounted to the Erlang container
# in .ci/docker-compose-file/docker-compose.yaml
TTY=''
if [[ -t 1 ]]; then
    TTY='-t'
fi
docker exec -i $TTY "$ERLANG_CONTAINER" bash -c 'git config --global --add safe.directory /emqx'

if [ "$CONSOLE" = 'yes' ]; then
    docker exec -i $TTY "$ERLANG_CONTAINER" bash -c "make run"
else
    set +e
    docker exec -i $TTY "$ERLANG_CONTAINER" bash -c "make ${WHICH_APP}-ct"
    RESULT=$?
    # shellcheck disable=2086 # no quotes for F_OPTIONS
    docker-compose $F_OPTIONS down
    exit $RESULT
fi

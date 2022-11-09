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

# Passing $UID to docker-compose to be used in erlang container
# as owner of the main process to avoid git repo permissions issue.
# Permissions issue happens because we are mounting local filesystem
# where files are owned by $UID to docker container where it's using
# root (UID=0) by default, and git is not happy about it.
# shellcheck disable=2086 # no quotes for F_OPTIONS
UID_GID="$UID:$UID" docker-compose $F_OPTIONS up -d --build

# /emqx is where the source dir is mounted to the Erlang container
# in .ci/docker-compose-file/docker-compose.yaml
TTY=''
if [[ -t 1 ]]; then
    TTY='-t'
fi

# rebar and hex cache directory need to be writable by $UID
docker exec -i $TTY -u root:root "$ERLANG_CONTAINER" bash -c "mkdir /.cache && chown $UID:$UID /.cache"
# need to initialize .erlang.cookie manually here because / is not writable by $UID
docker exec -i $TTY -u root:root "$ERLANG_CONTAINER" bash -c "openssl rand -base64 16 > /.erlang.cookie && chown $UID:$UID /.erlang.cookie && chmod 0400 /.erlang.cookie"
if [ "$CONSOLE" = 'yes' ]; then
    docker exec -i $TTY "$ERLANG_CONTAINER" bash -c "make run"
else
    set +e
    docker exec -i $TTY "$ERLANG_CONTAINER" bash -c "make ${WHICH_APP}-ct"
    RESULT=$?
    # shellcheck disable=2086 # no quotes for F_OPTIONS
    UID_GID="$UID:$UID" docker-compose $F_OPTIONS down
    exit $RESULT
fi

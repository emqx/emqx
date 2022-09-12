#!/usr/bin/env bash

## This script runs CT (and necessary dependencies) in docker container(s)

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

help() {
    echo
    echo "-h|--help:              To display this usage info"
    echo "--app lib_dir/app_name: For which app to run start docker-compose, and run common tests"
    echo "--suites SUITE1,SUITE2: Comma separated SUITE names to run. e.g. apps/emqx/test/emqx_SUITE.erl"
    echo "--console:              Start EMQX in console mode"
    echo "--attach:               Attach to the Erlang docker container without running any test case"
    echo "--only-up:              Only start the testbed but do not run CT"
    echo "--keep-up:              Keep the testbed running after CT"
}

WHICH_APP='novalue'
CONSOLE='no'
KEEP_UP='no'
ONLY_UP='no'
SUITES=''
ATTACH='no'
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
        --only-up)
            ONLY_UP='yes'
            shift 1
            ;;
        --keep-up)
            KEEP_UP='yes'
            shift 1
            ;;
        --attach)
            ATTACH='yes'
            shift 1
            ;;
        --console)
            CONSOLE='yes'
            shift 1
            ;;
        --suites)
            SUITES="$2"
            shift 2
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

case "${WHICH_APP}" in
    lib-ee*)
        ## ensure enterprise profile when testing lib-ee applications
        export PROFILE='emqx-enterprise'
        ;;
    *)
        true
        ;;
esac

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
        mongo_rs_sharded)
            FILES+=( '.ci/docker-compose-file/docker-compose-mongo-replicaset-tcp.yaml'
                     '.ci/docker-compose-file/docker-compose-mongo-sharded-tcp.yaml' )
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
        kafka)
            FILES+=( '.ci/docker-compose-file/docker-compose-kafka.yaml' )
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

if [ "$ONLY_UP" = 'yes' ]; then
    exit 0
fi

if [ "$ATTACH" = 'yes' ]; then
    docker exec -it "$ERLANG_CONTAINER" bash
elif [ "$CONSOLE" = 'yes' ]; then
    docker exec -i $TTY "$ERLANG_CONTAINER" bash -c "make run"
else
    set +e
    docker exec -i $TTY -e EMQX_CT_SUITES="$SUITES" "$ERLANG_CONTAINER" bash -c "BUILD_WITHOUT_QUIC=1 make ${WHICH_APP}-ct"
    RESULT=$?
    if [ "$KEEP_UP" = 'yes' ]; then
        exit $RESULT
    else
        # shellcheck disable=2086 # no quotes for F_OPTIONS
        docker-compose $F_OPTIONS down
        exit $RESULT
    fi
fi

#!/usr/bin/env bash

## This script runs CT (and necessary dependencies) in docker container(s)

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

help() {
    echo
    echo "-h|--help:              To display this usage info"
    echo "--app lib_dir/app_name: For which app to run start docker-compose, and run common tests"
    echo "--console:              Start EMQX in console mode but do not run test cases"
    echo "--attach:               Attach to the Erlang docker container without running any test case"
    echo "--stop:                 Stop running containers for the given app"
    echo "--only-up:              Only start the testbed but do not run CT"
    echo "--keep-up:              Keep the testbed running after CT"
    echo "--ci:                   Set this flag in GitHub action to enforce no tests are skipped"
    echo "--:                     If any, all args after '--' are passed to rebar3 ct"
    echo "                        otherwise it runs the entire app's CT"
}

set +e
if docker compose version; then
    DC='docker compose'
elif command -v docker-compose; then
    DC='docker-compose'
else
    echo 'Neither "docker compose" or "docker-compose" are available, stop.'
    exit 1
fi
set -e

WHICH_APP='novalue'
CONSOLE='no'
KEEP_UP='no'
ONLY_UP='no'
ATTACH='no'
STOP='no'
IS_CI='no'
SQLSERVER_ODBC_REQUEST='no'
SNOWFLAKE_ODBC_REQUEST='no'
UP='up --wait'
while [ "$#" -gt 0 ]; do
    case $1 in
        -h|--help)
            help
            exit 0
            ;;
        --app)
            WHICH_APP="${2%/}"
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
        --stop)
            STOP='yes'
            shift 1
            ;;
        --console)
            CONSOLE='yes'
            shift 1
            ;;
        --ci)
            IS_CI='yes'
            UP='up --wait --quiet-pull'
            shift 1
            ;;
        --)
            shift 1
            REBAR3CT="$*"
            shift $#
            ;;
        *)
            echo "unknown option $1"
            exit 1
            ;;
    esac
done

if [ "${WHICH_APP}" = 'novalue' ]; then
    echo "must provide --app arg"
    help
    exit 1
fi

if [ ! -d "${WHICH_APP}" ]; then
    echo "must provide an existing path for --app arg"
    help
    exit 1
fi

ERLANG_CONTAINER='erlang'
DOCKER_CT_ENVS_FILE="${WHICH_APP}/docker-ct"
PROFILE='emqx-enterprise'

if [ -f "$DOCKER_CT_ENVS_FILE" ]; then
    # shellcheck disable=SC2002
    CT_DEPS="$(cat "$DOCKER_CT_ENVS_FILE" | xargs)"
fi
CT_DEPS="${ERLANG_CONTAINER} ${CT_DEPS:-}"

FILES=( )

for dep in ${CT_DEPS}; do
    case "${dep}" in
        erlang)
            FILES+=( '.ci/docker-compose-file/docker-compose.yaml' )
            ;;
        toxiproxy)
            FILES+=( '.ci/docker-compose-file/docker-compose-toxiproxy.yaml' )
            ;;
        influxdb)
            FILES+=( '.ci/docker-compose-file/docker-compose-influxdb-tcp.yaml'
                     '.ci/docker-compose-file/docker-compose-influxdb-tls.yaml' )
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
        redis_cluster)
            FILES+=( '.ci/docker-compose-file/docker-compose-redis-cluster-tcp.yaml'
                     '.ci/docker-compose-file/docker-compose-redis-cluster-tls.yaml' )
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
        tdengine)
            FILES+=( '.ci/docker-compose-file/docker-compose-tdengine-restful.yaml' )
            ;;
        clickhouse)
            FILES+=( '.ci/docker-compose-file/docker-compose-clickhouse.yaml' )
            ;;
        dynamo)
            FILES+=( '.ci/docker-compose-file/docker-compose-dynamo.yaml' )
            ;;
        rocketmq)
            FILES+=( '.ci/docker-compose-file/docker-compose-rocketmq.yaml'
                     '.ci/docker-compose-file/docker-compose-rocketmq-ssl.yaml' )
            ;;
        cassandra)
            FILES+=( '.ci/docker-compose-file/docker-compose-cassandra.yaml' )
            ;;
        sqlserver)
            SQLSERVER_ODBC_REQUEST='yes'
            FILES+=( '.ci/docker-compose-file/docker-compose-sqlserver.yaml' )
            ;;
        opents)
            FILES+=( '.ci/docker-compose-file/docker-compose-opents.yaml' )
            ;;
        pulsar)
            FILES+=( '.ci/docker-compose-file/docker-compose-pulsar.yaml' )
            ;;
        oracle)
            FILES+=( '.ci/docker-compose-file/docker-compose-oracle.yaml' )
            ;;
        iotdb)
            FILES+=( '.ci/docker-compose-file/docker-compose-iotdb.yaml' )
            ;;
        rabbitmq)
            FILES+=( '.ci/docker-compose-file/docker-compose-rabbitmq.yaml' )
            ;;
        minio)
            FILES+=( '.ci/docker-compose-file/docker-compose-minio-tcp.yaml'
                     '.ci/docker-compose-file/docker-compose-minio-tls.yaml' )
            ;;
        gcp_emulator)
            FILES+=( '.ci/docker-compose-file/docker-compose-gcp-emulator.yaml' )
            ;;
        kinesis)
            FILES+=( '.ci/docker-compose-file/docker-compose-kinesis.yaml' )
            ;;
        greptimedb)
            FILES+=( '.ci/docker-compose-file/docker-compose-greptimedb.yaml' )
            ;;
        ldap)
            FILES+=( '.ci/docker-compose-file/docker-compose-ldap.yaml' )
            ;;
        otel)
            FILES+=( '.ci/docker-compose-file/docker-compose-otel.yaml' )
            ;;
        elasticsearch)
            FILES+=( '.ci/docker-compose-file/docker-compose-elastic-search-tls.yaml' )
            ;;
        azurite)
            FILES+=( '.ci/docker-compose-file/docker-compose-azurite.yaml' )
            ;;
        couchbase)
            FILES+=( '.ci/docker-compose-file/docker-compose-couchbase.yaml' )
            ;;
        kdc)
            FILES+=( '.ci/docker-compose-file/docker-compose-kdc.yaml' )
            ;;
        datalayers)
            FILES+=( '.ci/docker-compose-file/docker-compose-datalayers-tcp.yaml'
                     '.ci/docker-compose-file/docker-compose-datalayers-tls.yaml' )
            ;;
        snowflake)
            if [[ -z "${SNOWFLAKE_ACCOUNT_ID:-}" ]]; then
                echo "Snowflake environment requested, but SNOWFLAKE_ACCOUNT_ID is undefined"
                echo "Will NOT install Snowflake's ODBC drivers"
            else
                SNOWFLAKE_ODBC_REQUEST='yes'
            fi
            ;;
        schema-registry)
          FILES+=( '.ci/docker-compose-file/docker-compose-confluent-schema-registry.yaml' )
            ;;
        iceberg)
            FILES+=( '.ci/docker-compose-file/docker-compose-iceberg.yaml' )
            ;;
        doris)
            FILES+=( '.ci/docker-compose-file/docker-compose-doris.yaml'
                     '.ci/docker-compose-file/docker-compose-doris-tls.yaml' )
            ;;
        bigquery)
            FILES+=( '.ci/docker-compose-file/docker-compose-bigquery.yaml' )
            ;;
        alloydb)
            FILES+=( '.ci/docker-compose-file/docker-compose-alloydb.yaml' )
            ;;
        cockroachdb)
            FILES+=( '.ci/docker-compose-file/docker-compose-cockroachdb.yaml' )
            ;;
        *)
            echo "unknown_ct_dependency $dep"
            exit 1
            ;;
    esac
done

if [ "$SQLSERVER_ODBC_REQUEST" = 'yes' ] && [ "$STOP" = 'no' ]; then
    INSTALL_SQLSERVER_ODBC="./scripts/install-msodbc-driver.sh"
else
    INSTALL_SQLSERVER_ODBC="echo 'msodbc driver not requested'"
fi

if [ "$SNOWFLAKE_ODBC_REQUEST" = 'yes' ] && [ "$STOP" = 'no' ]; then
    INSTALL_SNOWFLAKE_ODBC="./scripts/install-snowflake-driver.sh"
else
    INSTALL_SNOWFLAKE_ODBC="echo 'snowflake driver not requested'"
fi

for file in "${FILES[@]}"; do
    DC="$DC -f $file"
done

DOCKER_USER="$(id -u)"
export DOCKER_USER

TTY=''
if [[ -t 1 ]]; then
    TTY='-t'
fi

# ensure directory with secrets is created by current user before running compose
mkdir -p /tmp/emqx-ci/emqx-shared-secret

if [ "$STOP" = 'no' ]; then
    # some left-over log file has to be deleted before a new docker-compose up
    rm -f '.ci/docker-compose-file/redis/*.log'
    set +e
    # shellcheck disable=2086 # no quotes for UP
    $DC $UP -d -t 0 --build --remove-orphans
    RESULT=$?
    if [ $RESULT -ne 0 ]; then
        mkdir -p _build/test/logs
        LOG='_build/test/logs/docker-compose.log'
        echo "Dumping docker-compose log to $LOG"
        $DC logs --no-color --timestamps > "$LOG"
        exit 1
    fi
    set -e
fi

if [ "$DOCKER_USER" != "root" ]; then
    # the user must exist inside the container for `whoami` to work
  docker exec -i $TTY -u root:root \
         -e "SFACCOUNT=${SFACCOUNT:-myorg-myacc}" \
         "$ERLANG_CONTAINER" bash -c \
         "useradd --uid $DOCKER_USER -M -d / emqx || true && \
          mkdir -p /.cache /.hex /.mix && \
          chown $DOCKER_USER /.cache /.hex /.mix && \
          openssl rand -base64 -hex 16 > /.erlang.cookie && \
          chown $DOCKER_USER /.erlang.cookie && \
          chmod 0400 /.erlang.cookie && \
          $INSTALL_SQLSERVER_ODBC && \
          $INSTALL_SNOWFLAKE_ODBC" || true
fi

if [ "$ONLY_UP" = 'yes' ]; then
    exit 0
fi

set +e

if [ "$STOP" = 'yes' ]; then
    $DC down -t 0 --remove-orphans
elif [ "$ATTACH" = 'yes' ]; then
    docker exec -it "$ERLANG_CONTAINER" bash
elif [ "$CONSOLE" = 'yes' ]; then
    docker exec -e PROFILE="$PROFILE" -i $TTY "$ERLANG_CONTAINER" bash -c "make run"
else
    if [ -z "${REBAR3CT:-}" ]; then
        docker exec -e IS_CI="$IS_CI" \
                    -e PROFILE="$PROFILE" \
                    -e SUITEGROUP="${SUITEGROUP:-}" \
                    -e ENABLE_COVER_COMPILE="${ENABLE_COVER_COMPILE:-}" \
                    -e CT_COVER_EXPORT_PREFIX="${CT_COVER_EXPORT_PREFIX:-}" \
                    -i $TTY "$ERLANG_CONTAINER" \
                    bash -c "make ${WHICH_APP}-ct"
    else
        # this is an ad-hoc run
        docker exec -e IS_CI="$IS_CI" \
                    -e PROFILE="$PROFILE" \
                    -i $TTY "$ERLANG_CONTAINER" \
                    bash -c "./rebar3 ct $REBAR3CT"
    fi
    RESULT=$?
    if [ "$RESULT" -ne 0 ]; then
        LOG='_build/test/logs/docker-compose.log'
        echo "Dumping docker-compose log to $LOG"
        $DC logs --no-color --timestamps > "$LOG"
    fi
    if [ "$KEEP_UP" != 'yes' ]; then
        $DC down
    fi
    exit "$RESULT"
fi

#!/usr/bin/env bash

DEBUG="${DEBUG:-0}"
if [ "$DEBUG" -eq 1 ]; then
    set -x
fi

ROOT_DIR="$(cd "$(dirname "$(readlink "$0" || echo "$0")")"/.. || exit 1; pwd -P)"
# shellcheck disable=SC1090,SC1091
. "$ROOT_DIR"/releases/emqx_vars

# defined in emqx_vars
export RUNNER_ROOT_DIR
export RUNNER_ETC_DIR
export REL_VSN

export RUNNER_SCRIPT="$RUNNER_BIN_DIR/$REL_NAME"
export CODE_LOADING_MODE="${CODE_LOADING_MODE:-embedded}"
export REL_DIR="$RUNNER_ROOT_DIR/releases/$REL_VSN"
export SCHEMA_MOD=emqx_conf_schema

WHOAMI=$(whoami)
export WHOAMI

# Make sure data/configs exists
export CONFIGS_DIR="$RUNNER_DATA_DIR/configs"

# hocon try to read environment variables starting with "EMQX_"
export HOCON_ENV_OVERRIDE_PREFIX='EMQX_'

export ROOTDIR="$RUNNER_ROOT_DIR"
export ERTS_DIR="$ROOTDIR/erts-$ERTS_VSN"
export BINDIR="$ERTS_DIR/bin"
export EMU="beam"
export PROGNAME="erl"
export ERTS_LIB_DIR="$ERTS_DIR/../lib"
export DYNLIBS_DIR="$RUNNER_ROOT_DIR/dynlibs"

## backward compatible
if [ -d "$ERTS_DIR/lib" ]; then
    export LD_LIBRARY_PATH="$ERTS_DIR/lib:$LD_LIBRARY_PATH"
fi

# EPMD_ARG="-start_epmd true $PROTO_DIST_ARG"
NO_EPMD="-start_epmd false -epmd_module ekka_epmd -proto_dist ekka"
EPMD_ARG="${EPMD_ARG:-${NO_EPMD}}"

# Warn the user if ulimit -n is less than 1024
ULIMIT_F=$(ulimit -n)
if [ "$ULIMIT_F" -lt 1024 ]; then
    echo "!!!!"
    echo "!!!! WARNING: ulimit -n is ${ULIMIT_F}; 1024 is the recommended minimum."
    echo "!!!!"
fi

SED_REPLACE="sed -i "
case $(sed --help 2>&1) in
    *GNU*) SED_REPLACE="sed -i ";;
    *BusyBox*) SED_REPLACE="sed -i ";;
    *) SED_REPLACE=(sed -i '' );;
esac
export SED_REPLACE

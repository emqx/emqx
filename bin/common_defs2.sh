#!/bin/bash

## NOTE: those are defined separately from `common_defs.sh` because
## they require `common_functions.sh` to be sourced prior to sourcing
## this file.  Basically, the definitions below depend on the function
## `call_hocon`, which is defined in `common_functions.sh`.  Also,
## they require the variable `IS_BOOT_COMMAND` to be set to either
## `yes` or `no` for the name definition to be done properly.

## This should be sourced after the calling script has define the
## `$COMMAND` variable.

## make EMQX_NODE_COOKIE right
if [ -n "${EMQX_NODE_NAME:-}" ]; then
    export EMQX_NODE__NAME="${EMQX_NODE_NAME}"
    unset EMQX_NODE_NAME
fi

## Possible ways to configure emqx node name:
## 1. configure node.name in emqx.conf
## 2. override with environment variable EMQX_NODE__NAME
## Node name is either short-name (without '@'), e.g. 'emqx'
## or long name (with '@') e.g. 'emqx@example.net' or 'emqx@127.0.0.1'
NAME="${EMQX_NODE__NAME:-}"
if [ -z "$NAME" ]; then
    if [ "$IS_BOOT_COMMAND" = 'yes' ]; then
        # for boot commands, inspect emqx.conf for node name
        NAME="$(call_hocon -s "$SCHEMA_MOD" -I "$CONFIGS_DIR/" -c "$RUNNER_ETC_DIR"/emqx.conf get node.name | tr -d \")"
    else
        vm_args_file="$(latest_vm_args 'EMQX_NODE__NAME')"
        NAME="$(grep -E '^-s?name' "${vm_args_file}" | awk '{print $2}')"
    fi
fi

# force to use 'emqx' short name
[ -z "$NAME" ] && NAME='emqx'
export MNESIA_DATA_DIR="$RUNNER_DATA_DIR/mnesia/$NAME"

case "$NAME" in
    *@*)
        NAME_TYPE='-name'
       ;;
    *)
        NAME_TYPE='-sname'
esac
export NAME_TYPE
SHORT_NAME="$(echo "$NAME" | awk -F'@' '{print $1}')"
export ESCRIPT_NAME="$SHORT_NAME"

PIPE_DIR="${PIPE_DIR:-/$RUNNER_DATA_DIR/${WHOAMI}_erl_pipes/$NAME/}"

## make EMQX_NODE_COOKIE right
if [ -n "${EMQX_NODE_COOKIE:-}" ]; then
    export EMQX_NODE__COOKIE="${EMQX_NODE_COOKIE}"
    unset EMQX_NODE_COOKIE
fi
COOKIE="${EMQX_NODE__COOKIE:-}"
if [ -z "$COOKIE" ]; then
    if [ "$IS_BOOT_COMMAND" = 'yes' ]; then
        COOKIE="$(call_hocon -s "$SCHEMA_MOD" -I "$CONFIGS_DIR/" -c "$RUNNER_ETC_DIR"/emqx.conf get node.cookie | tr -d \")"
    else
        vm_args_file="$(latest_vm_args 'EMQX_NODE__COOKIE')"
        COOKIE="$(grep -E '^-setcookie' "${vm_args_file}" | awk '{print $2}')"
    fi
fi

if [ -z "$COOKIE" ]; then
    die "Please set node.cookie in $RUNNER_ETC_DIR/emqx.conf or override from environment variable EMQX_NODE__COOKIE"
fi

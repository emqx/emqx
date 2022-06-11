#!/usr/bin/env bash
## EMQ docker image start script
# Huang Rui <vowstar@gmail.com>
# EMQX Team <support@emqx.io>

## Shell setting
if [[ -n "$DEBUG" ]]; then
    set -ex
else
    set -e
fi

shopt -s nullglob

## Local IP address setting

LOCAL_IP=$(hostname -i | grep -oE '((25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])\.){3}(25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])' | head -n 1)

if [[ -z "$EMQX_NODE_NAME" ]]; then
    EMQX_NAME="${EMQX_NAME:-emqx}"
    if [[ -z "$EMQX_HOST" ]]; then
        if [[ "$EMQX_CLUSTER__K8S__ADDRESS_TYPE" == "dns" ]] && [[ -n "$EMQX_CLUSTER__K8S__NAMESPACE" ]]; then
            EMQX_CLUSTER__K8S__SUFFIX=${EMQX_CLUSTER__K8S__SUFFIX:-"pod.cluster.local"}
            EMQX_HOST="${LOCAL_IP//./-}.$EMQX_CLUSTER__K8S__NAMESPACE.$EMQX_CLUSTER__K8S__SUFFIX"
        elif [[ "$EMQX_CLUSTER__K8S__ADDRESS_TYPE" == 'hostname' ]] && [[ -n "$EMQX_CLUSTER__K8S__NAMESPACE" ]]; then
            EMQX_CLUSTER__K8S__SUFFIX=${EMQX_CLUSTER__K8S__SUFFIX:-'svc.cluster.local'}
            EMQX_HOST=$(grep -h "^$LOCAL_IP" /etc/hosts | grep -o "$(hostname).*.$EMQX_CLUSTER__K8S__NAMESPACE.$EMQX_CLUSTER__K8S__SUFFIX")
        else
            EMQX_HOST="$LOCAL_IP"
        fi
    fi
    export EMQX_NODE_NAME="$EMQX_NAME@$EMQX_HOST"
    unset EMQX_NAME
    unset EMQX_HOST
fi

# The default rpc port discovery 'stateless' is mostly for clusters
# having static node names. So it's troulbe-free for multiple emqx nodes
# running on the same host.
# When start emqx in docker, it's mostly one emqx node in one container
# i.e. use port 5369 (or per tcp_server_port | ssl_server_port config) for gen_rpc
export EMQX_RPC__PORT_DISCOVERY="${EMQX_RPC__PORT_DISCOVERY:-manual}"

exec "$@"

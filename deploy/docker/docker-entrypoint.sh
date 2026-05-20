#!/usr/bin/env bash

## EMQX docker image start script

if [[ -n "$DEBUG" ]]; then
    set -ex
else
    set -e
fi

shopt -s nullglob

## Local IP address setting

IP_REGEX='((25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])\.){3}(25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])'
LOCAL_IP=$(hostname -i 2>/dev/null | grep -oE "$IP_REGEX" | head -n 1)
if [[ -z "$LOCAL_IP" ]]; then
    LOCAL_IP=$(hostname -I 2>/dev/null | grep -oE "$IP_REGEX" | head -n 1)
fi

failed_to_determine_node_host() {
    echo "ERROR: Failed to determine EMQX node host. Set EMQX_NODE_NAME, EMQX_NODE__NAME, or EMQX_HOST." >&2
    exit 1
}

ensure_local_ip() {
    if [[ -z "$LOCAL_IP" ]]; then
        failed_to_determine_node_host
    fi
}

ensure_emqx_host() {
    if [[ -z "${EMQX_HOST:-}" ]]; then
        failed_to_determine_node_host
    fi
}

export EMQX_NAME="${EMQX_NAME:-emqx}"

## EMQX_NODE_NAME or EMQX_NODE__NAME to indicate the full node name to be used by EMQX
## If both are set EMQX_NODE_NAME takes higher precedence than EMQX_NODE__NAME
if [[ -z "${EMQX_NODE_NAME:-}" ]] && [[ -z "${EMQX_NODE__NAME:-}" ]]; then
    # No node name is provide from environment variables
    # try to resolve from other settings
    if [[ -z "$EMQX_HOST" ]]; then
        if [[ "$EMQX_CLUSTER__DISCOVERY_STRATEGY" == "dns" ]] && \
            [[ "$EMQX_CLUSTER__DNS__RECORD_TYPE" == "srv" ]] && \
            grep -q "$(hostname).$EMQX_CLUSTER__DNS__NAME" /etc/hosts; then
                EMQX_HOST="$(hostname).$EMQX_CLUSTER__DNS__NAME"
        elif [[ "$EMQX_CLUSTER__DISCOVERY_STRATEGY" == "k8s" ]] && \
            [[ "$EMQX_CLUSTER__K8S__ADDRESS_TYPE" == "dns" ]] && \
            [[ -n "$EMQX_CLUSTER__K8S__NAMESPACE" ]]; then
                ensure_local_ip
                EMQX_CLUSTER__K8S__SUFFIX=${EMQX_CLUSTER__K8S__SUFFIX:-"pod.cluster.local"}
                EMQX_HOST="${LOCAL_IP//./-}.$EMQX_CLUSTER__K8S__NAMESPACE.$EMQX_CLUSTER__K8S__SUFFIX"
        elif [[ "$EMQX_CLUSTER__DISCOVERY_STRATEGY" == "k8s" ]] && \
            [[ "$EMQX_CLUSTER__K8S__ADDRESS_TYPE" == 'hostname' ]] && \
            [[ -n "$EMQX_CLUSTER__K8S__NAMESPACE" ]]; then
                ensure_local_ip
                EMQX_CLUSTER__K8S__SUFFIX=${EMQX_CLUSTER__K8S__SUFFIX:-'svc.cluster.local'}
                EMQX_HOST=$(grep -h "^$LOCAL_IP" /etc/hosts | grep -o "$(hostname).*.$EMQX_CLUSTER__K8S__NAMESPACE.$EMQX_CLUSTER__K8S__SUFFIX" || true)
        else
            ensure_local_ip
            EMQX_HOST="$LOCAL_IP"
        fi
        ensure_emqx_host
        export EMQX_HOST
    fi
    export EMQX_NODE_NAME="$EMQX_NAME@$EMQX_HOST"
fi

# The default rpc port discovery 'stateless' is mostly for clusters
# having static node names. So it's troulbe-free for multiple emqx nodes
# running on the same host.
# When start emqx in docker, it's mostly one emqx node in one container
# i.e. use port 5369 (or per tcp_server_port | ssl_server_port config) for gen_rpc
export EMQX_RPC__PORT_DISCOVERY="${EMQX_RPC__PORT_DISCOVERY:-manual}"

exec "$@"

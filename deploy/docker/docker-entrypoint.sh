#!/bin/bash
## EMQ docker image start script
# Huang Rui <vowstar@gmail.com>
# EMQ X Team <support@emqx.io>

## Shell setting
if [[ -n "$DEBUG" ]]; then
    set -ex
else
    set -e
fi

shopt -s nullglob

## Local IP address setting

LOCAL_IP=$(hostname -i | grep -oE '((25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])\.){3}(25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])' | head -n 1)

## EMQ Base settings and plugins setting
# Base settings in /opt/emqx/etc/emqx.conf
# Plugin settings in /opt/emqx/etc/plugins

_EMQX_HOME='/opt/emqx'

if [[ -z "$EMQX_NAME" ]]; then
    EMQX_NAME="emqx"
fi

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

if [[ -z "$EMQX_NODE__NAME" ]]; then
    export EMQX_NODE__NAME="$EMQX_NAME@$EMQX_HOST"
fi

# prevent interpretation as config values
EMQX_NAME=null
EMQX_HOST=null

# Set hosts to prevent cluster mode failed

if [[ -z "$EMQX_NODE__PROCESS_LIMIT" ]]; then
    export EMQX_NODE__PROCESS_LIMIT=2097152
fi

if [[ -z "$EMQX_NODE__MAX_PORTS" ]]; then
    export EMQX_NODE__MAX_PORTS=1048576
fi

if [[ -z "$EMQX_NODE__MAX_ETS_TABLES" ]]; then
    export EMQX_NODE__MAX_ETS_TABLES=2097152
fi

if [[ -z "$EMQX_LOG__TO" ]]; then
    export EMQX_LOG__TO='console'
fi

if [[ -z "$EMQX_LISTENER__TCP__EXTERNAL__ACCEPTORS" ]]; then
    export EMQX_LISTENER__TCP__EXTERNAL__ACCEPTORS=64
fi

if [[ -z "$EMQX_LISTENER__TCP__EXTERNAL__MAX_CONNECTIONS" ]]; then
    export EMQX_LISTENER__TCP__EXTERNAL__MAX_CONNECTIONS=1000000
fi

if [[ -z "$EMQX_LISTENER__SSL__EXTERNAL__ACCEPTORS" ]]; then
    export EMQX_LISTENER__SSL__EXTERNAL__ACCEPTORS=32
fi

if [[ -z "$EMQX_LISTENER__SSL__EXTERNAL__MAX_CONNECTIONS" ]]; then
    export EMQX_LISTENER__SSL__EXTERNAL__MAX_CONNECTIONS=500000
fi

if [[ -z "$EMQX_LISTENER__WS__EXTERNAL__ACCEPTORS" ]]; then
    export EMQX_LISTENER__WS__EXTERNAL__ACCEPTORS=16
fi

if [[ -z "$EMQX_LISTENER__WS__EXTERNAL__MAX_CONNECTIONS" ]]; then
    export EMQX_LISTENER__WS__EXTERNAL__MAX_CONNECTIONS=250000
fi

# Fix issue #42 - export env EMQX_DASHBOARD__DEFAULT_USER__PASSWORD to configure
# 'dashboard.default_user.password' in etc/plugins/emqx_dashboard.conf
if [[ -n "$EMQX_ADMIN_PASSWORD" ]]; then
    export EMQX_DASHBOARD__DEFAULT_USER__PASSWORD=$EMQX_ADMIN_PASSWORD
fi

# echo value of $VAR hiding secrets if any
# SYNOPSIS
#     echo_value KEY VALUE
echo_value() {
    # get MASK_CONFIG
    MASK_CONFIG_FILTER="$MASK_CONFIG_FILTER|password|passwd|key|token|secret"
    FORMAT_MASK_CONFIG_FILTER=$(echo "$MASK_CONFIG_FILTER" | sed -r -e 's/^[^A-Za-z0-9_]+//' -e 's/[^A-Za-z0-9_]+$//' -e 's/[^A-Za-z0-9_]+/|/g')
    local key=$1
    local value=$2
    # check if contains sensitive value
    if echo "$key" | grep -iqwE "$FORMAT_MASK_CONFIG_FILTER"; then
        echo "$key=***secret***"
    else
        echo "$key=$value"
    fi
}

# fill config on specific file if the key exists
# SYNOPSIS
#     try_fill_config FILE KEY VALUE
override_config() {
    local key=$1
    local value=$2
    local maybe_plugin_conf="$_EMQX_HOME/etc/plugins/emqx_${key//.*/}.conf"
    local conf
    if [ -f "$maybe_plugin_conf" ]; then
      conf=$maybe_plugin_conf
    else
      conf="$_EMQX_HOME/etc/emqx.conf"
    fi
    if [[ -z "$value" ]]; then
      echo "$key = null" >> "$conf.override"
    else
      echo "$key = $value" >> "$conf.override"
    fi
}

for VAR in $(compgen -e); do
    # Config normal keys such like node.name = emqx@127.0.0.1
    if echo "$VAR" | grep -q '^EMQX_'; then
        VAR_NAME=$(echo "$VAR" | sed -e 's/^EMQX_//' -e 's/__/./g' | tr '[:upper:]' '[:lower:]' | tr -d '[:cntrl:]')
        VAR_VALUE=$(echo "${!VAR}" | tr -d '[:cntrl:]')
        # Config in emqx.conf
        override_config "$VAR_NAME" "$VAR_VALUE"
    fi
done

# fill tuples on specific file
# SYNOPSIS
#     fill_tuples FILE [ELEMENTS ...]
fill_tuples() {
    local file=$1
    local elements=${*:2}
    for var in $elements; do
        if grep -qE "\{\s*$var\s*,\s*(true|false)\s*\}\s*\." "$file"; then
            sed -i -r "s/\{\s*($var)\s*,\s*(true|false)\s*\}\s*\./{\1, true}./1" "$file"
        elif grep -q "$var\s*\." "$file"; then
            # backward compatible.
            sed -i -r "s/($var)\s*\./{\1, true}./1" "$file"
        else
            sed -i '$a'\\ "$file"
            echo "{$var, true}." >>"$file"
        fi
    done
}

## EMQX Plugin load settings
# Plugins loaded by default
LOADED_PLUGINS="$_EMQX_HOME/data/loaded_plugins"
if [[ -n "$EMQX_LOADED_PLUGINS" ]]; then
    EMQX_LOADED_PLUGINS=$(echo "$EMQX_LOADED_PLUGINS" | tr -d '[:cntrl:]' | sed -r -e 's/^[^A-Za-z0-9_]+//g' -e 's/[^A-Za-z0-9_]+$//g' -e 's/[^A-Za-z0-9_]+/ /g')
    echo "EMQX_LOADED_PLUGINS=$EMQX_LOADED_PLUGINS"
    # Parse module names and place `{module_name, true}.` tuples in `loaded_plugins`.
    fill_tuples "$LOADED_PLUGINS" "$EMQX_LOADED_PLUGINS"
fi

## EMQX Modules load settings
# Modules loaded by default
LOADED_MODULES="$_EMQX_HOME/data/loaded_modules"
if [[ -n "$EMQX_LOADED_MODULES" ]]; then
    EMQX_LOADED_MODULES=$(echo "$EMQX_LOADED_MODULES" | tr -d '[:cntrl:]' | sed -r -e 's/^[^A-Za-z0-9_]+//g' -e 's/[^A-Za-z0-9_]+$//g' -e 's/[^A-Za-z0-9_]+/ /g')
    echo "EMQX_LOADED_MODULES=$EMQX_LOADED_MODULES"
    # Parse module names and place `{module_name, true}.` tuples in `loaded_modules`.
    fill_tuples "$LOADED_MODULES" "$EMQX_LOADED_MODULES"
fi

exec "$@"

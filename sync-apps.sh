#!/bin/bash

set -euo pipefail

force="${1:-no}"

apps=(
"emqx_auth_http"
"emqx_auth_jwt"
"emqx_auth_ldap"
"emqx_auth_mnesia"
"emqx_auth_mongo"
"emqx_auth_mysql"
"emqx_auth_pgsql"
"emqx_auth_redis"
"emqx_bridge_mqtt"
"emqx_coap"
"emqx_dashboard"
"emqx_exhook"
"emqx_exproto"
"emqx_lua_hook"
"emqx_lwm2m"
"emqx_management"
"emqx_plugin_template"
"emqx_prometheus"
"emqx_psk_file"
"emqx_recon"
"emqx_retainer"
"emqx_rule_engine"
"emqx_sasl"
"emqx_sn"
"emqx_stomp"
"emqx_telemetry"
"emqx_web_hook")

if git status --porcelain | grep -qE 'apps/'; then
    echo 'apps dir is not git-clear, refuse to sync'
#    exit 1
fi

rm -rf apps/emqx_*
mkdir -p tmp/

download_zip() {
    local app="$1"
    local ref="$2"
    local vsn="$(echo "$ref" | tr '/' '-')"
    local file="tmp/${app}-${vsn}.zip"
    if [ -f "$file" ] && [ "$force" != "force" ]; then
        return 0
    fi
    local repo="$(echo "$app" | sed 's#_#-#g')"
    local url="https://github.com/emqx/$repo/archive/$ref.zip"
    echo "downloading ${url}"
    curl -fLsS -o "$file" "$url"
}

default_vsn="dev/v4.3.0"
download_zip "emqx_passwd" "v1.1.1"
for app in ${apps[@]}; do
    download_zip "$app" "$default_vsn"
done

extract_zip(){
    local app="$1"
    local ref="$2"
    local vsn_arg="${3:-}"
    local vsn_dft="$(echo "$ref" | tr '/' '-')"
    local vsn
    if [ -n "$vsn_arg" ]; then
        vsn="$vsn_arg"
    else
        vsn="$vsn_dft"
    fi
    local file="tmp/${app}-${vsn_dft}.zip"
    local repo="$(echo "$app" | sed 's#_#-#g')"
    unzip "$file" -d apps/
    mv "apps/${repo}-${vsn}/" "apps/$app/"
}

extract_zip "emqx_passwd" "v1.1.1" "1.1.1"
for app in ${apps[@]}; do
    extract_zip "$app" "$default_vsn"
done

cleanup_app(){
    local app="$1"
    rm -f "apps/$app/Makefile"
    rm -f "apps/$app/rebar.config.script"
}

apps+=( "emqx_passwd" )
for app in ${apps[@]}; do
    cleanup_app $app
done

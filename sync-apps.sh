#!/bin/bash

set -euo pipefail

force="${1:-no}"

apps=(
# "emqx_auth_http" # permanently diverged
# "emqx_web_hook" # permanently diverged
"emqx_auth_jwt"
"emqx_auth_ldap"
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
"emqx_prometheus"
"emqx_psk_file"
"emqx_recon"
"emqx_retainer"
"emqx_rule_engine"
"emqx_sasl"
"emqx_sn"
"emqx_stomp"
"emqx_telemetry"
)

if git status --porcelain | grep -qE 'apps/'; then
    echo 'apps dir is not git-clear, refuse to sync'
#    exit 1
fi

mkdir -p tmp/

download_zip() {
    local app="$1"
    local ref="$2"
    local vsn
    vsn="$(echo "$ref" | tr '/' '-')"
    local file="tmp/${app}-${vsn}.zip"
    if [ -f "$file" ] && [ "$force" != "force" ]; then
        return 0
    fi
    local repo
    repo=${app//_/-}
    local url="https://github.com/emqx/$repo/archive/$ref.zip"
    echo "downloading ${url}"
    curl -fLsS -o "$file" "$url"
}

default_vsn="dev/v4.3.0"
download_zip "emqx_auth_mnesia" "e4.2.3"
for app in "${apps[@]}"; do
    download_zip "$app" "$default_vsn"
done

extract_zip(){
    local app="$1"
    local ref="$2"
    local vsn_arg="${3:-}"
    local vsn_dft
    vsn_dft="$(echo "$ref" | tr '/' '-')"
    local vsn
    if [ -n "$vsn_arg" ]; then
        vsn="$vsn_arg"
    else
        vsn="$vsn_dft"
    fi
    local file="tmp/${app}-${vsn_dft}.zip"
    local repo
    repo=${app//_/-}
    rm -rf "apps/${app}/"
    unzip "$file" -d apps/
    mv "apps/${repo}-${vsn}/" "apps/$app/"
}

extract_zip "emqx_auth_mnesia" "e4.2.3" "e4.2.3"
for app in "${apps[@]}"; do
    extract_zip "$app" "$default_vsn"
done

cleanup_app(){
    local app="$1"
    pushd "apps/$app"
    rm -f Makefile rebar.config.script LICENSE src/*.app.src.script src/*.appup.src
    rm -rf ".github" ".ci"
    # restore rebar.config and app.src
    git checkout rebar.config
    git checkout src/*.app.src
    popd
}

apps+=( "emqx_auth_mnesia" )
for app in "${apps[@]}"; do
    cleanup_app "$app"
done

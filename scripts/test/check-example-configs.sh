#!/usr/bin/env bash

set -euo pipefail

cd -P -- "$(dirname -- "$0")/../.."
# shellcheck disable=SC1091
source ./env.sh

PROFILE="${PROFILE:-emqx-enterprise}"
DIR_NAME='examples'
SCHEMA_MOD='emqx_enterprise_schema'

IFS=$'\n' read -r -d '' -a FILES < <(find "rel/config/${DIR_NAME}" -name "*.example" 2>/dev/null | sort && printf '\0')

prepare_erl_libs() {
    local libs_dir="$1"
    local erl_libs="${ERL_LIBS:-}"
    local sep=':'
    for app in "${libs_dir}"/*; do
        if [ -d "${app}/ebin" ]; then
            if [ -n "$erl_libs" ]; then
                erl_libs="${erl_libs}${sep}${app}"
            else
                erl_libs="${app}"
            fi
        fi
    done
    export ERL_LIBS="$erl_libs"
}

# This is needed when checking schema
export EMQX_ETC_DIR="apps/emqx/etc"

prepare_erl_libs "_build/$PROFILE/lib"

check_file() {
    local file="$1"
    erl -noshell -eval \
        "File=\"$file\",
         case emqx_hocon:load_and_check($SCHEMA_MOD, File) of
            {ok, _} ->
                io:format(\"check_example_config_ok: ~s~n\", [File]),
                halt(0);
            {error, Reason} ->
                io:format(\"failed_to_check_example_config: ~s~n~p~n\", [File, Reason]),
                halt(1)
        end."
}

for file in "${FILES[@]}"; do
    check_file "$file"
done

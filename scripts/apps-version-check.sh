#!/bin/bash
set -euo pipefail

latest_release=$(git describe --abbrev=0 --tags)

bad_app_count=0

get_vsn() {
    commit="$1"
    app_src_file="$2"
    if [ "$commit" = 'HEAD' ]; then
        if [ -f "$app_src_file" ]; then
            grep vsn "$app_src_file" | grep -oE '"[0-9]+.[0-9]+.[0-9]+"' | tr -d '"' || true
        fi
    else
        git show "$commit":"$app_src_file" 2>/dev/null | grep vsn | grep -oE '"[0-9]+.[0-9]+.[0-9]+"' | tr -d '"' || true
    fi
}

check_apps() {
    while read -r app_path; do
        app=$(basename "$app_path")
        src_file="$app_path/src/$app.app.src"
        old_app_version="$(get_vsn "$latest_release" "$src_file")"
        ## TODO: delete it after new version is released with emqx app in apps dir
        if [ "$app" = 'emqx' ] && [ "$old_app_version" = '' ]; then
            old_app_version="$(get_vsn "$latest_release" 'src/emqx.app.src')"
        fi
        now_app_version="$(get_vsn 'HEAD' "$src_file")"
        ## TODO: delete it after new version is released with emqx app in apps dir
        if [ "$app" = 'emqx' ] && [ "$now_app_version" = '' ]; then
            now_app_version="$(get_vsn 'HEAD' 'src/emqx.app.src')"
        fi
        if [ -z "$now_app_version" ]; then
            echo "failed_to_get_new_app_vsn for $app"
            exit 1
        fi
        if [ -z "${old_app_version:-}" ]; then
            echo "skiped checking new app ${app}"
        elif [ "$old_app_version" = "$now_app_version" ]; then
            lines="$(git diff --name-only "$latest_release"...HEAD \
                        -- "$app_path/src" \
                        -- "$app_path/priv" \
                        -- "$app_path/c_src")"
            if [ "$lines" != '' ]; then
                echo "$src_file needs a vsn bump (old=$old_app_version)"
                echo "changed: $lines"
                bad_app_count=$(( bad_app_count + 1))
            fi
        fi
    done < <(./scripts/find-apps.sh)

    if [ $bad_app_count -gt 0 ]; then
        exit 1
    else
        echo "apps version check successfully"
    fi
}

_main() {
    if echo "${latest_release}" |grep -oE '[0-9]+.[0-9]+.[0-9]+' > /dev/null 2>&1; then
        check_apps
    else
        echo "skiped unstable tag: ${latest_release}"
    fi
}

_main

#!/bin/bash
set -euo pipefail

remote="refs/remote/$(git remote -v | grep fetch | grep 'emqx/emqx' | awk '{print $1}')"
latest_release=$(git describe --tags "$(git rev-list --tags --max-count=1 --remotes="$remote")")

bad_app_count=0

get_vsn() {
    commit="$1"
    app_src_file="$2"
    git show "$commit":"$app_src_file" 2>/dev/null | grep vsn | grep -oE '"[0-9]+.[0-9]+.[0-9]+"' | tr -d '"' || true
}

while read -r app_path; do
    app=$(basename "$app_path")
    src_file="$app_path/src/$app.app.src"
    old_app_version="$(get_vsn "$latest_release" "$src_file")"
    ## TODO: delete it after new version is released with emqx app in apps dir
    if [ "$app" = 'emqx' ] && [ "$old_app_version" = '' ]; then
        old_app_version="$(get_vsn "$latest_release" 'src/emqx.app.src')"
    fi
    if [ -z "$old_app_version" ]; then
        echo "failed_to_get_old_app_vsn for $app"
        exit 1
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
    if [ "$old_app_version" = "$now_app_version" ]; then
        changed="$(git diff --name-only "$latest_release"...HEAD \
                    -- "$app_path/src" \
                    -- "$app_path/priv" \
                    -- "$app_path/c_src" | wc -l)"
        if [ "$changed" -gt 0 ]; then
            echo "$src_file needs a vsn bump"
            bad_app_count=$(( bad_app_count + 1))
        fi
    fi
done < <(./scripts/find-apps.sh)

if [ $bad_app_count -gt 0 ]; then
    exit 1
else
    echo "apps version check successfully"
fi

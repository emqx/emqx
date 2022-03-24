#!/usr/bin/env bash
set -euo pipefail

latest_release=$(git describe --abbrev=0 --tags --exclude '*rc*' --exclude '*alpha*' --exclude '*beta*')

bad_app_count=0

while read -r app; do
    if [ "$app" != "emqx" ]; then
        app_path="$app"
    else
        app_path="."
    fi
    src_file="$app_path/src/$(basename "$app").app.src"
    old_app_version="$(git show "$latest_release":"$src_file" | grep vsn | grep -oE '"[0-9]+.[0-9]+.[0-9]+"' | tr -d '"')"
    now_app_version=$(grep -E 'vsn' "$src_file" | grep -oE '"[0-9]+\.[0-9]+\.[0-9]+"' | tr -d '"')
    if [ "$old_app_version" = "$now_app_version" ]; then
        changed="$(git diff --name-only "$latest_release"...HEAD \
                    -- "$app_path/src" \
                    -- "$app_path/priv" \
                    -- "$app_path/c_src" | { grep -v -E 'appup\.src' || true; } | wc -l)"
        if [ "$changed" -gt 0 ]; then
            echo "$src_file needs a vsn bump"
            bad_app_count=$(( bad_app_count + 1))
        elif [[ ${app_path} = *emqx_dashboard* ]]; then
            ## emqx_dashboard is ensured to be upgraded after all other plugins
            ## at the end of its appup instructions, there is the final instruction
            ## {apply, {emqx_plugins, load, []}
            ## since we don't know which plugins are stopped during the upgrade
            ## for safty, we just force a dashboard version bump for each and every release
            ## even if there is nothing changed in the app
            echo "$src_file needs a vsn bump to ensure plugins loaded after upgrade"
            bad_app_count=$(( bad_app_count + 1))
        fi
    fi
done < <(./scripts/find-apps.sh)

if [ $bad_app_count -gt 0 ]; then
    exit 1
else
    echo "apps version check successfully"
fi

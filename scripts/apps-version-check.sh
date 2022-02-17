#!/usr/bin/env bash
set -euo pipefail

## compare to the latest release version tag:
##   match rel-e4.4.0, v4.4.*, or e4.4.* tags
##   but do not include alpha, beta and rc versions
##
## NOTE: 'rel-' tags are the merge base of enterprise release in opensource repo.
## i.e. if we are to release a new enterprise without cutting a new opensource release
## we should tag rel-e4.4.X in the opensource repo, and merge this tag to enterprise
## then cut a release from the enterprise repo.
latest_release="$(git describe --abbrev=0 --tags --match 'rel-e4.4.*' --match '[v|e]4.4*' --exclude '*beta*' --exclude '*alpha*' --exclude '*rc*')"
echo "Compare base: $latest_release"

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
            elif [ "$app" = 'emqx_dashboard' ]; then
                ## emqx_dashboard is ensured to be upgraded after all other plugins
                ## at the end of its appup instructions, there is the final instruction
                ## {apply, {emqx_plugins, load, []}
                ## since we don't know which plugins are stopped during the upgrade
                ## for safety, we just force a dashboard version bump for each and every release
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
}

_main() {
    if echo "${latest_release}" |grep -oE '[0-9]+.[0-9]+.[0-9]+' > /dev/null 2>&1; then
        check_apps
    else
        echo "skiped unstable tag: ${latest_release}"
    fi
}

_main

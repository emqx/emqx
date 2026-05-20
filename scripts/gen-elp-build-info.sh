#!/bin/bash

# This script generates a build_info.json file that contains detailed
# information about all applications and dependencies in the EMQX project.

set -e

beginfmt='\033[1m'
endfmt='\033[0m'

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

usage() {
    echo    "Usage: $0 [-h | -t | -w]"
    echo    "  -h: Show this help message."
    echo -e "  -t: Test generation and show the output via ${beginfmt}'less'${endfmt}. Does not write any files."
    echo -e "  -w: Write the output to ${beginfmt}build_info.json${endfmt} (Will overwrite the existing file)"
    echo -e "      And create/check ${beginfmt}.elp.toml${endfmt}."
}

# This function contains the original, working logic for processing an app directory.
process_app() {
    local app_path=$1
    if [ ! -d "$app_path" ]; then
        return
    fi
    # shellcheck disable=SC2155
    local app_name=$(basename "$app_path")
    local src_dirs_json="[]"
    if [ -d "$app_path/src" ]; then src_dirs_json='["src"]'; fi
    local include_dirs_json="[]"
    if [ -d "$app_path/include" ]; then include_dirs_json='["include"]'; fi
    local extra_src_dirs_json="[]"
    if [ -d "$app_path/test" ]; then extra_src_dirs_json='["test"]'; fi
    local macros_json='{}'

    jq -n \
      --arg name "$app_name" --arg dir "$app_path" \
      --argjson src_dirs "$src_dirs_json" --argjson extra_src_dirs "$extra_src_dirs_json" \
      --argjson include_dirs "$include_dirs_json" --argjson macros "$macros_json" \
      '{name: $name, dir: $dir, src_dirs: $src_dirs, extra_src_dirs: $extra_src_dirs, include_dirs: $include_dirs, macros: $macros}'
}

plugin_packages_ready() {
    local plugin_app
    if [ ! -d "plugins" ]; then
        return 0
    fi
    if [ ! -d "_build/plugins" ]; then
        return 1
    fi

    for plugin_dir in plugins/*;
    do
        if [ ! -d "$plugin_dir" ] || [ ! -f "$plugin_dir/mix.exs" ]; then
            continue
        fi
        if ! grep -q "emqx_plugin:" "$plugin_dir/mix.exs"; then
            continue
        fi

        plugin_app=$(basename "$plugin_dir")
        if ! find _build/plugins -maxdepth 1 -type f -name "${plugin_app}-*.tar.gz" | grep -q .; then
            return 1
        fi
    done

    return 0
}

# This function wraps the entire discovery and generation process.
generate_json_content() {
    # shellcheck disable=SC2155
    local TMP_APPS_FILE=$(mktemp)
    # shellcheck disable=SC2155
    local TMP_DEPS_FILE=$(mktemp)
    trap 'rm -f "$TMP_APPS_FILE" "$TMP_DEPS_FILE"' RETURN

    # 1. Process in-project applications
    echo -e "Processing project applications in ${beginfmt}apps/${endfmt}"
    find apps -mindepth 1 -maxdepth 1 -type d | while read -r app_dir;
    do
        process_app "$app_dir" >> "$TMP_APPS_FILE"
    done

    # 2. Process in-project plugin applications
    if [ -d "plugins" ]; then
        echo -e "Processing plugin applications in ${beginfmt}plugins/${endfmt}"
        find plugins -mindepth 1 -maxdepth 1 -type d | while read -r plugin_dir;
        do
            if [ -f "$plugin_dir/mix.exs" ] && grep -q "emqx_plugin:" "$plugin_dir/mix.exs"; then
                process_app "$plugin_dir" >> "$TMP_APPS_FILE"
            fi
        done
    fi

    # 3. Conditionally compile dependencies and plugins
    if [ -d "_build/emqx-enterprise/lib" ] && [ -d "_build/emqx-enterprise-test/lib" ]; then
        echo -e "Build directories found, skipping compilation."
    else
        echo -e "Build directories not found or incomplete. Running ${beginfmt}'make test-compile'...${endfmt}\n"
        make test-compile
    fi

    if ! plugin_packages_ready; then
        echo -e "Plugin packages not found or incomplete. Running ${beginfmt}'make plugins'...${endfmt}\n"
        make plugins
    fi

    # 4. Process dependencies from the 'deps' directory
    local DEP_ROOTS=('deps')
    echo -e "Processing dependencies in ${beginfmt}${DEP_ROOTS[*]}${endfmt}..."
    for dep_root in "${DEP_ROOTS[@]}";
    do
        if [ ! -d "$dep_root" ]; then
            echo -e "Warning: Dependency directory ${beginfmt}'$dep_root'${endfmt} not found. Did you run 'make test-compile'?"
            continue
        fi
        find "$dep_root" -mindepth 1 -maxdepth 1 -not -name ".rebar3" | while read -r dep_path;
        do
            # With mix, the distinction is simple, in_project_apps: `apps/` and dependencies: `deps/`. No complex checks needed.
            # The logic for nested apps within a dep is still needed.
            if [ -d "$dep_path/src" ]; then
                process_app "$dep_path" >> "$TMP_DEPS_FILE"
            elif [ -d "$dep_path/apps" ]; then
                find "$dep_path/apps" -mindepth 1 -maxdepth 1 -type d | while read -r sub_app_path;
                do
                    process_app "$sub_app_path" >> "$TMP_DEPS_FILE"
                done
            else
                process_app "$dep_path" >> "$TMP_DEPS_FILE"
            fi
        done
    done

    # 5. Assemble and output the final JSON
    echo "Assembling final JSON..."
    json_content=$(jq -n --slurpfile apps "$TMP_APPS_FILE" --slurpfile deps "$TMP_DEPS_FILE" \
                        '{apps: $apps, deps: $deps}')
}

default_elp_toml() {
    echo '[build_info]' >> .elp.toml
    echo 'file = "build_info.json"' >> .elp.toml
}
# Main Execution

if [ -z "$1" ]; then
    usage
    exit 0
fi

case "$1" in
    -h)
        usage
        ;;
    -t)
        generate_json_content
        echo "$json_content" | jq -C | less
        ;;
    -w)
        generate_json_content
        echo -e "Writing to ${beginfmt}build_info.json...${endfmt}"
        echo "$json_content" > build_info.json

        if [ -f ".elp.toml" ]; then
            beginfmt='\033[1;33m'
            echo -e "${beginfmt}Note: .elp.toml already exists. You may need to manually update it.${endfmt}"
        else
            echo -e "Creating ${beginfmt}.elp.toml${endfmt}..."
            default_elp_toml;
        fi
        ;;
    *)
        echo "Error: Invalid option '$1'"
        echo ""
        usage
        exit 1
        ;;
esac

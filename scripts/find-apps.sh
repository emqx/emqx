#!/usr/bin/env bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "$0")/.."

help() {
    echo
    echo "-h|--help:        To display this usage info"
    echo "--ci:             Print apps in json format for github ci matrix"
    echo "--base-ref:       Base ref for git diff (default: use GITHUB_BASE_REF or merge-base)"
}

MODE='list'
BASE_REF=""
while [ "$#" -gt 0 ]; do
    case $1 in
        -h|--help)
            help
            exit 0
            ;;
        --ci)
            MODE='ci'
            shift 1
            ;;
        --base-ref)
            BASE_REF="$2"
            shift 2
            ;;
        *)
            echo "unknown option $1"
            exit 1
            ;;
    esac
done

if [ "$(./scripts/get-distro.sh)" = 'windows' ]; then
    # Otherwise windows may resolve to find.exe
    FIND="/usr/bin/find"
else
    FIND='find'
fi

find_app() {
    local appdir="$1"
    "$FIND" "${appdir}" -mindepth 1 -maxdepth 1 -type d
}

# Get changed apps based on git diff
get_changed_apps() {
    local base_ref="$1"
    local changed_files
    changed_files=$(git diff --name-only "${base_ref}" HEAD || true)

    # Check if rebar.config at root is changed
    if echo "${changed_files}" | grep -q "^rebar\.config$"; then
        # If rebar.config changed, return all apps
        find_app 'apps'
        return 0
    fi

    # Check if .github or .ci directories are changed
    if echo "${changed_files}" | grep -qE "^(\.github|\.ci)/"; then
        # If CI configuration changed, return all apps
        find_app 'apps'
        return 0
    fi

    # Get changed files in apps/ directory
    local changed_app_files
    changed_app_files=$(echo "${changed_files}" | grep "^apps/" || true)

    if [ -z "${changed_app_files}" ]; then
        # No changes in apps/, return empty
        return 0
    fi

    # Extract unique app names from changed paths
    echo "${changed_app_files}" | sed 's|^apps/\([^/]*\).*|\1|' | sort -u
}

# Get apps that use the changed apps (transitive closure)
get_apps_to_test() {
    local changed_apps="$1"
    local deps_file="deps.txt"

    if [ ! -f "${deps_file}" ]; then
        echo "Warning: ${deps_file} not found, falling back to all apps" >&2
        find_app 'apps'
        return 0
    fi

    # If no changed apps, return empty
    if [ -z "${changed_apps}" ]; then
        return 0
    fi

    # Use an associative array (set) to track apps to test for deduplication
    # Store app names without "apps/" prefix
    declare -A apps_set
    local found_all=false

    while IFS= read -r app_path; do
        # Skip if empty
        [ -z "${app_path}" ] && continue

        # Normalize: strip "apps/" prefix if present to get just the app name
        local app="${app_path#apps/}"

        # Find apps that use this app from deps.txt
        # Format: app_name: user1 user2 ... or app_name: all or app_name: none
        local line
        line=$(grep "^${app}:" "${deps_file}" || echo "")

        if [ -z "${line}" ]; then
            # App not found in deps.txt, just add the changed app itself
            apps_set["${app}"]=1
            continue
        fi

        local users
        users=$(echo "${line}" | cut -d: -f2- | xargs || echo "")

        if [ "${users}" = "all" ]; then
            # If app is used by all, we need to test all apps
            found_all=true
            break
        elif [ "${users}" != "none" ] && [ -n "${users}" ]; then
            # Add users to the set
            for user in ${users}; do
                apps_set["${user}"]=1
            done
        fi

        # Also add the changed app itself
        apps_set["${app}"]=1
    done <<< "${changed_apps}"

    if [ "${found_all}" = true ]; then
        # If any changed app is used by all, test all apps
        find_app 'apps'
    else
        # Convert set to sorted list and add "apps/" prefix to match find_app format
        printf '%s\n' "${!apps_set[@]}" | sort | sed 's|^|apps/|'
    fi
}

if [ "$MODE" = 'list' ]; then
    find_app 'apps'
    exit 0
fi

##################################################
###### now deal with the github action's matrix.
##################################################

# In CI mode, determine which apps to test based on git diff
if [ "$MODE" = 'ci' ]; then
    # Determine base ref (should be merge base commit SHA)
    if [ -n "${BASE_REF}" ] && git rev-parse --verify "${BASE_REF}" >/dev/null 2>&1; then
        # Get changed apps
        CHANGED_APPS=$(get_changed_apps "${BASE_REF}")

        if [ -n "${CHANGED_APPS}" ]; then
            # Get apps to test based on dependencies
            APPS_TO_TEST=$(get_apps_to_test "${CHANGED_APPS}")

            if [ -n "${APPS_TO_TEST}" ]; then
                APPS_FOR_CI="${APPS_TO_TEST}"
            else
                # If no apps to test (e.g., only non-app files changed), test nothing
                APPS_FOR_CI=""
            fi
        else
            # No changes in apps/ and rebar.config not changed, test nothing
            APPS_FOR_CI=""
        fi
    else
        # If BASE_REF is not available or invalid, fall back to all apps
        APPS_FOR_CI="$(find_app 'apps')"
    fi
else
    echo "Unknown env: MODE=${MODE}"
    exit 1
fi

format_app_entry() {
    local groups="$2"
    local group=0
    while [ "$groups" -gt $group ]; do
        if [ $group -gt 0 ]; then
            echo ", "
        fi
        group=$(( group + 1 ))
        ## prefix is for github actions (they don't like slash in variables)
        local prefix=${1//\//_}
        echo -n -e "$(
    cat <<END
        {"app": "${1}", "suitegroup": "${group}_${groups}", "profile": "${3}", "runner": "${4}", "prefix": "${prefix}"}
END
        )"
    done
}

matrix() {
    local runner
    local profile
    local entries=()

    # If no apps to test, return empty array
    if [ -z "${APPS_FOR_CI}" ]; then
        echo -n "[]"
        return 0
    fi

    for app in ${APPS_FOR_CI}; do
        if [ -f "${app}/docker-ct" ]; then
            runner="docker"
        else
            runner="host"
        fi
        case "${app}" in
            apps/emqx)
                entries+=("$(format_app_entry "$app" 10 emqx "$runner")")
                entries+=("$(format_app_entry "$app" 10 emqx-enterprise "$runner")")
                ;;
            apps/emqx_bridge)
                entries+=("$(format_app_entry "$app" 1 emqx "$runner")")
                entries+=("$(format_app_entry "$app" 1 emqx-enterprise "$runner")")
                ;;
            apps/emqx_connector)
                entries+=("$(format_app_entry "$app" 1 emqx "$runner")")
                entries+=("$(format_app_entry "$app" 1 emqx-enterprise "$runner")")
                ;;
            apps/emqx_dashboard)
                entries+=("$(format_app_entry "$app" 1 emqx "$runner")")
                entries+=("$(format_app_entry "$app" 1 emqx-enterprise "$runner")")
                ;;
            apps/emqx_prometheus)
                entries+=("$(format_app_entry "$app" 1 emqx "$runner")")
                entries+=("$(format_app_entry "$app" 1 emqx-enterprise "$runner")")
                ;;
            apps/emqx_rule_engine)
                entries+=("$(format_app_entry "$app" 1 emqx "$runner")")
                entries+=("$(format_app_entry "$app" 1 emqx-enterprise "$runner")")
                ;;
            apps/emqx_management)
                entries+=("$(format_app_entry "$app" 1 emqx "$runner")")
                entries+=("$(format_app_entry "$app" 1 emqx-enterprise "$runner")")
                ;;
            apps/emqx_auth_http)
                entries+=("$(format_app_entry "$app" 1 emqx "$runner")")
                entries+=("$(format_app_entry "$app" 1 emqx-enterprise "$runner")")
                ;;
            apps/*)
                if [[ -f "${app}/BSL.txt" ]]; then
                    profile='emqx-enterprise'
                else
                    profile='emqx'
                fi
                entries+=("$(format_app_entry "$app" 1 "$profile" "$runner")")
                ;;
            *)
                echo "unknown app: $app"
                exit 1
                ;;
        esac
    done
    echo -n "[$(IFS=,; echo "${entries[*]}")]"
}

matrix

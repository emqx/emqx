#!/usr/bin/env bash
set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

log_red() {
  local RED='\033[0;31m' # Red
  local NC='\033[0m' # No Color
  echo -e "${RED}${1}${NC}"
}

# match any official release tag 'e*' and 'v*'
latest_release="$(env PREV_TAG_MATCH_PATTERN='*' ./scripts/find-prev-rel-tag.sh)"
echo "Version check compare base: $latest_release"

bad_app_count=0

no_comment_re='(^[^\s?%])'
# For .proto files: exclude comment lines starting with // (after optional whitespace)
proto_no_comment_re='(^[^/\s]|^[[:space:]]+[^/\s])'
## TODO: c source code comments re (in $app_path/c_src dirs)

parse_semver() {
    echo "$1" | tr '.|-' ' '
}

is_allowed_non_strict() {
    local src_file="$1"
    local from="$2"
    local to="$3"
    log_red "WARN: $src_file vsn bump from $from to $to"
    return 0
}

APPS="$(./scripts/find-apps.sh)"
for app in ${APPS}; do
    if [ "$app" != "emqx" ]; then
        app_path="$app"
    else
        app_path="."
    fi
    src_file="$app_path/src/$(basename "$app").app.src"
    if git show "$latest_release":"$src_file" >/dev/null 2>&1; then
        old_app_version="$(git show "$latest_release":"$src_file" | grep vsn | grep -oE '"[0-9]+\.[0-9]+\.[0-9]+"' | tr -d '"')"
    else
        old_app_version='not_found'
    fi
    if [ ! -f "$src_file" ]; then
        # app is deleted
        continue
    fi
    now_app_version=$(grep -E 'vsn' "$src_file" | grep -oE '"[0-9]+\.[0-9]+\.[0-9]+"' | tr -d '"')

    if [ "$old_app_version" = 'not_found' ]; then
        echo "IGNORE: $src_file is newly added"
        true
    elif [ "$old_app_version" = "$now_app_version" ]; then
        # Check for changes - get changed files first, then filter out proto and appup files
        changed_files="$(git diff "$latest_release" --name-only \
                             -- "$app_path/src" \
                             -- "$app_path/include" \
                             -- "$app_path/priv" \
                             -- "$app_path/c_src" \
                         | grep -v '\.appup\.src$' \
                         | grep -v '\.proto$' || true)"

        # Now check if any of the remaining files have actual code changes (not just comments)
        changed_lines=0
        if [ -n "$changed_files" ]; then
            # shellcheck disable=SC2086
            changed_lines="$(git diff "$latest_release" --ignore-blank-lines -G "$no_comment_re" \
                             -- $changed_files | wc -l)"
        fi
        # Check .proto files separately with regex that ignores comment lines
        proto_changed_lines=0
        proto_files="$(find "$app_path/priv" -name "*.proto" -type f 2>/dev/null || true)"
        if [ -n "$proto_files" ]; then
            # shellcheck disable=SC2086
            proto_changed_lines="$(git diff "$latest_release" --ignore-blank-lines -G "$proto_no_comment_re" \
                                 $proto_files | wc -l)"
        fi
        total_changed=$((changed_lines + proto_changed_lines))
        if [ "$total_changed" -gt 0 ]; then
            log_red "ERROR: $src_file needs a vsn bump"
            bad_app_count=$(( bad_app_count + 1))
        fi
    else
        # shellcheck disable=SC2207
        old_app_version_semver=($(parse_semver "$old_app_version"))
        # shellcheck disable=SC2207
        now_app_version_semver=($(parse_semver "$now_app_version"))
        if  [ "${old_app_version_semver[0]}" = "${now_app_version_semver[0]}" ] && \
            [ "${old_app_version_semver[1]}" = "${now_app_version_semver[1]}" ] && \
            [ "$(( old_app_version_semver[2] + 1 ))" = "${now_app_version_semver[2]}" ]; then
            true
        elif [ "${old_app_version_semver[0]}" = "${now_app_version_semver[0]}" ] && \
             [ "$(( old_app_version_semver[1] + 1 ))" = "${now_app_version_semver[1]}" ] && \
             [ "${now_app_version_semver[2]}" = "0" ]; then
            true
        else
            if ! is_allowed_non_strict "$src_file" "$old_app_version" "$now_app_version"; then
                echo "$src_file: non-strict semver version bump from $old_app_version to $now_app_version"
                bad_app_count=$(( bad_app_count + 1))
            fi
        fi
    fi
done

if [ $bad_app_count -gt 0 ]; then
    exit 1
else
    echo "apps version check passed successfully"
fi

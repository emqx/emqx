#!/usr/bin/env bash

set -euo pipefail
shopt -s inherit_errexit

[ "${DEBUG:-0}" = 1 ] && set -x

usage() {
    cat <<EOF
$0 [MAJOR.MINOR | FILE]

Delete changelog entries in changes/ee which are already released.

  no argument:  delete the entries released in the previous release tag.
  MAJOR.MINOR:  delete the entries released in any version before MAJOR.MINOR.
                e.g. '$0 5.8' deletes the entries released in 5.7.x or earlier.
  FILE:         delete FILE if it is released in the previous release tag.

Release tags must be fetched first.
EOF
}

top_dir="$(git rev-parse --show-toplevel)"
arg="${1:-}"
if [ -f "$arg" ]; then
    arg="$(realpath --relative-to="$top_dir" "$arg")"
fi
cd "$top_dir"

## print release tags (e.g. e5.7.2, v5.7.2, 6.0.1) of versions before $1.$2
tags_before() {
    local major="$1" minor="$2"
    git tag -l | sed -nE 's/^[ev]?([0-9]+)\.([0-9]+)\.[0-9]+$/\1 \2 &/p' |
        awk -v major="$major" -v minor="$minor" \
            '$1 < major || ($1 == major && $2 < minor) { print $3 }'
}

## print the changelog files which were added by a commit reachable from any of the given tags.
## Renames are followed, so an entry moved from changes/ce to changes/ee is checked by its original commit.
released_files() {
    local released_commits
    released_commits="$(mktemp)"
    # shellcheck disable=SC2064
    trap "rm -f '$released_commits'" RETURN
    git rev-list "$@" > "$released_commits"
    ## walk the history from new to old, map each earlier name to the current file,
    ## and print "<commit> <current-file>" for each commit which added it
    git log -M --diff-filter=AR --name-status --format='C %H' -- changes/ |
        awk -v files="$FILES" '
            BEGIN { n = split(files, fs, "\n"); for (i = 1; i <= n; i++) if (fs[i] != "") alias[fs[i]] = fs[i] }
            $1 == "C" { commit = $2; next }
            $1 ~ /^R/ && ($3 in alias) { alias[$2] = alias[$3]; next }
            $1 == "A" && ($2 in alias) { print commit, alias[$2] }
        ' |
        awk 'NR == FNR { released[$1] = 1; next } ($1 in released) { print $2 }' "$released_commits" - |
        sort -u
}

## usage: delete_released <label> <tag>...
delete_released() {
    local label="$1" file to_delete
    shift
    to_delete="$(released_files "$@")"
    while read -r file; do
        [ -n "$file" ] || continue
        echo "Deleting $file, released $label"
        rm -f "$file"
    done <<< "$to_delete"
}

case "$arg" in
    -h|--help)
        usage
        exit 0
        ;;
    '')
        FILES="$(find changes/ee -type f -name '*.en.md')"
        prev_tag="$("$top_dir"/scripts/find-prev-rel-tag.sh)"
        delete_released "in $prev_tag" "$prev_tag"
        ;;
    *)
        if [[ "$arg" =~ ^([0-9]+)\.([0-9]+)$ ]]; then
            tags="$(tags_before "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}")"
            if [ -z "$tags" ]; then
                echo "No release tag found before $arg" 1>&2
                exit 1
            fi
            FILES="$(find changes/ee -type f -name '*.en.md')"
            # shellcheck disable=SC2086
            delete_released "before $arg" $tags
        elif [ -f "$arg" ]; then
            FILES="$arg"
            prev_tag="$("$top_dir"/scripts/find-prev-rel-tag.sh)"
            delete_released "in $prev_tag" "$prev_tag"
        else
            usage 1>&2
            exit 1
        fi
        ;;
esac

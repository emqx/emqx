#!/usr/bin/env bash

set -euo pipefail

[ "${DEBUG:-0}" = 1 ] && set -x

top_dir="$(git rev-parse --show-toplevel)"
prev_tag="$("$top_dir"/scripts/find-prev-rel-tag.sh)"

## check if a file's first commit is contained in the previous release
is_released() {
    file="$1"
    prev_tag="$2"
    first_commit=$(git log --pretty=format:"%h" "$file" | tail -1)
    git merge-base --is-ancestor "$first_commit" "$prev_tag"
}

## check if a file's first commit is contained in the previous release
## and delete the file if it is
check_and_delete_file() {
    file="$1"
    if is_released "$file" "$prev_tag"; then
        echo "Deleting $file, released in $prev_tag"
        rm -f "$file"
    fi
}

## if a file is provided as an argument, only delete the first commit of that file
file_in_arg="${1:-}"
if [ -n "$file_in_arg" ]; then
    check_and_delete_file "$file_in_arg"
    exit 0
fi

## loop over files in $top_dir/changes/ee
## and delete the ones that are included in the previous release
## capture first: a find failure inside a process substitution is not caught by set -e
changelog_files="$(find "$top_dir/changes/ee" -type f -name '*.en.md')"
while read -r file; do
    [ -n "$file" ] || continue
    check_and_delete_file "$file"
done <<< "$changelog_files"

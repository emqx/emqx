#!/usr/bin/env bash

set -euo pipefail

[ "${DEBUG:-0}" = 1 ] && set -x

top_dir="$(git rev-parse --show-toplevel)"
prev_ce_tag="$("$top_dir"/scripts/find-prev-rel-tag.sh 'emqx')"
prev_ee_tag="$("$top_dir"/scripts/find-prev-rel-tag.sh 'emqx-enterprise')"

## check if a file is included in the previous release
is_released() {
    file="$1"
    prev_tag="$2"
    # check if file exists in the previous release
    if git show "$prev_tag:$file" >/dev/null 2>&1; then
        return 1
    else
        return 0
    fi
}

## loop over files in $top_dir/changes/ce
## and delete the ones that are included in the previous ce and ee releases
while read -r file; do
    if is_released "$file" "$prev_ce_tag" && is_released "$file" "$prev_ee_tag"; then
        echo "deleting $file, released in $prev_ce_tag and $prev_ee_tag"
        rm -f "$file"
    fi
done < <(find "$top_dir/changes/ce" -type f -name '*.md')

## loop over files in $top_dir/changes/ee
## and delete the ones taht are included in the previous ee release
while read -r file; do
    if is_released "$file" "$prev_ee_tag"; then
        echo "deleting $file, released in $prev_ee_tag"
        rm -f "$file"
    fi
done < <(find "$top_dir/changes/ee" -type f -name '*.md')

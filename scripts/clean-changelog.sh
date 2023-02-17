#!/bin/bash
set -euo pipefail
shopt -s nullglob
export LANG=C.UTF-8

top_dir="$(git rev-parse --show-toplevel)"
changes_dir="$top_dir/changes"
ce_dir=$"$changes_dir/ce"
ee_dir=$"$changes_dir/ee"
ce_name="emqx"
ee_name="emqx-enterprise"

get_dir () {
    local version="$1"
    if [ "emqx" == "$version" ]; then
        echo "$ce_dir"
    else
        echo "$ee_dir"
    fi
}

get_merged () {
    local version dir prefix
    version="$1"
    if [ "emqx" == "$version" ]; then
        prefix="v"
    else
        prefix="e"
    fi
    find "$changes_dir" -name "$prefix*.md" -print0 |xargs -0 grep -E -o -h "\\[#[0-9]+\\]" |sed -E 's/\[#([0-9]+)\]/\1/g' | sort | uniq
}

get_uncleaned () {
    local dir
    dir=$(get_dir "$1")
    find "$dir" -name "*.md" | sed -E 's/.*-([0-9]+)\..*/\1/g' | sort | uniq
}

do_clean () {
    local target fst_check_name snd_check_name fst_merged snd_merged uncleaned dir
    target="$1"
    dir=$(get_dir "$target")
    fst_check_name="$2"
    snd_check_name="${3:-}"
    fst_merged=$(get_merged "$fst_check_name")
    [ -n "$snd_check_name" ] && snd_merged=$(get_merged "$snd_check_name")
    uncleaned=$(get_uncleaned "$target")
    echo "$uncleaned" | while read -r PR; do
        flag=0
        [[ "$fst_merged" =~ $PR ]] && flag=1
        [ -n "$snd_check_name" ] && [[ ! "$snd_merged" =~ $PR ]] && flag=0
        if [[ $flag -eq 1 ]]; then
            echo "deleted $target PR: #$PR"
            find "$dir" -name "*$PR*.md" -type f -delete
        fi
    done
    git add "$dir"
}

do_clean "$ee_name" "$ee_name"

## An opensource changelog can only be deleted if it was both tag in opensource and enterprise verion
do_clean "$ce_name" "$ce_name" "$ee_name"

[ -n "$(git status -s)" ] && git commit -m "chore: clean changelogs"

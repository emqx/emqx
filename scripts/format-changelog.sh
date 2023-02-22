#!/bin/bash
set -euo pipefail
shopt -s nullglob
export LANG=C.UTF-8

[ "$#" -ne 4 ] && {
    echo "Usage $0 <emqx|emqx-enterprise> <LAST TAG> <VERSION> <OUTPUT DIR>" 1>&2;
    exit 1
}

profile="${1}"
last_tag="${2}"
version="${3}"
output_dir="${4}"
languages=("en" "zh")
top_dir="$(git rev-parse --show-toplevel)"
templates_dir="$top_dir/scripts/changelog-lang-templates"
declare -a changes
changes=("")

echo "generated changelogs from tag:${last_tag} to HEAD"

item() {
    local filename pr indent
    filename="${1}"
    pr="$(echo "${filename}" | sed -E 's/.*-([0-9]+)\.[a-z]+\.md$/\1/')"
    indent="- [#${pr}](https://github.com/emqx/emqx/pull/${pr}) "
    while read -r line; do
        echo "${indent}${line}"
        indent="  "
    done < "${filename}"
    echo
}

section() {
    local prefix=$1
    for file in "${changes[@]}"; do
        if [[ $file =~ .*$prefix-.*$language.md ]]; then
            item "$file"
        fi
    done
}

generate() {
    local language=$1
    local output="$output_dir/${version}_$language.md"
    local template_file="$templates_dir/$language"
    local template
    if [ -f "$template_file" ]; then
        template=$(cat "$template_file")
        eval "echo \"$template\" > $output"
    else
        echo "Invalid language ${language}" 1>&2;
        exit 1
    fi
}

changes_dir=("$top_dir/changes/ce")
if [ "$profile" == "emqx-enterprise" ]; then
    changes_dir+=("$top_dir/changes/ee")
fi

while read -d "" -r file; do
   changes+=("$file")
done < <(git diff --name-only -z -a "tags/${last_tag}...HEAD" "${changes_dir[@]}")

for language in "${languages[@]}"; do
    generate "$language"
done

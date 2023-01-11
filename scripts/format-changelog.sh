#!/bin/bash
set -euo pipefail
shopt -s nullglob
export LANG=C.UTF-8

[ "$#" -ne 2 ] && {
    echo "Usage $0 <EMQX version> <en|zh>" 1>&2;
    exit 1
}

version="${1}"
language="${2}"

changes_dir="$(git rev-parse --show-toplevel)/changes/${version}"

item() {
    local filename pr indent
    filename="${1}"
    pr="$(echo "${filename}" | sed -E 's/.*-([0-9]+)\.(en|zh)\.md$/\1/')"
    indent="- [#${pr}](https://github.com/emqx/emqx/pull/${pr}) "
    while read -r line; do
        echo "${indent}${line}"
        indent="  "
    done < "${filename}"
    echo
}

section() {
    local section_name=$1
    for i in "${changes_dir}"/"${section_name}"-*."${language}".md; do
        item "${i}"
    done
}

if [ "${language}" = "en" ]; then
    cat <<EOF
# ${version}

## Enhancements

$(section feat)

## Bug fixes

$(section fix)
EOF
elif [ "${language}" = "zh" ]; then
     cat <<EOF
# ${version}

## 增强

$(section feat)

## 修复

$(section fix)
EOF
else
    echo "Invalid language ${language}" 1>&2;
    exit 1
fi

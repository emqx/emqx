#!/bin/bash
set -euo pipefail
shopt -s nullglob
export LANG=C.UTF-8

[ "${DEBUG:-}" = 1 ] && set -x

logerr() {
    echo "$(tput setaf 1)ERROR: $1$(tput sgr0)" 1>&2
}

usage() {
    cat <<EOF
$0 [option]
options:
  -h|--help: print this usages info
  -b|--base:
    The git tag of compare base to find included changes.
    e.g. v5.0.18, e5.0.0 etc.
  -v|--version:
    The tag to be released
    e.g. v5.0.19, e5.0.1-alpha.1 etc.
EOF
}

while [ "$#" -gt 0 ]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -b|--base)
            shift
            BASE_TAG="$1"
            shift
            ;;
        -v|--version)
            shift
            TEMPLATE_VSN_HEADING="$1"
            shift
            ;;
        *)
            logerr "Unknown option $1"
            exit 1
            ;;
    esac
done

LANGUAGE='en'

case "${BASE_TAG:-}" in
    v*)
        PROFILE="emqx"
        ;;
    e*)
        PROFILE="emqx-enterprise"
        ;;
    *)
        logerr "Unsupported -b|--base option, must be v* or e*"
        exit 1
        ;;
esac

TEMPLATE_VSN_HEADING="${TEMPLATE_VSN_HEADING:-<VSN-TAG>}"

top_dir="$(git rev-parse --show-toplevel)"
declare -a PRS
PRS=("")

format_one_pr() {
    local filename pr_num indent
    filename="${1}"
    pr_num="$(echo "${filename}" | sed -E 's/.*-([0-9]+)\.[a-z]+\.md$/\1/')"
    re='^[0-9]+$'
    if ! [[ $pr_num =~ $re ]]; then
        logerr "bad filename format: $filename"
    fi
    indent="- [#${pr_num}](https://github.com/emqx/emqx/pull/${pr_num}) "
    while read -r line; do
        if [ "${line}" != '' ]; then
            echo "${indent}${line}"
        else
            echo ''
        fi
        indent="  "
    done < "${filename}"
    echo
}

section() {
    local prefix=$1
    for file in "${PRS[@]}"; do
        if [[ $file =~ .*$prefix-.*$LANGUAGE.md ]]; then
            format_one_pr "$file"
        fi
    done
}

changes_dir=("$top_dir/changes/ce")
if [ "$PROFILE" == "emqx-enterprise" ]; then
    changes_dir+=("$top_dir/changes/ee")
fi

while read -r file; do
   PRS+=("$file")
done < <(git diff --diff-filter=A --name-only "tags/${BASE_TAG}...HEAD" "${changes_dir[@]}")

TEMPLATE_FEAT_CHANGES="$(section 'feat')"
TEMPLATE_PERF_CHANGES="$(section 'perf')"
TEMPLATE_FIX_CHANGES="$(section 'fix')"
TEMPLATE_BREAKING_CHANGES="$(section 'breaking')"

TEMPLATE_ENH_HEADING="Enhancements"
TEMPLATE_FIX_HEADING="Bug Fixes"
TEMPLATE_BREAKING_HEADING="Breaking Changes"

cat <<EOF
# ${TEMPLATE_VSN_HEADING}

## ${TEMPLATE_ENH_HEADING}

${TEMPLATE_FEAT_CHANGES}

${TEMPLATE_PERF_CHANGES}

## ${TEMPLATE_FIX_HEADING}

${TEMPLATE_FIX_CHANGES}

## ${TEMPLATE_BREAKING_HEADING}

${TEMPLATE_BREAKING_CHANGES}
EOF

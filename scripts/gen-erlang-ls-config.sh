#!/bin/bash
set -eou pipefail
shopt -s nullglob

# Our fork of rebar3 copies those directories from apps to
# _build/default/lib rather than just making a symlink. Now erlang_ls
# sees the same module twice and if you're just navigating through the
# call stack you accidentally end up editing files in
# _build/default/lib rather than apps

usage() {
    cat <<EOF
Generate configuration for erlang_ls

USAGE:

   $(basename "${0}") [-t TEMPLATE_FILE]

OPTIONS:

   -h  Get this help
   -t  Path to the YAML file containing additional options for erlang ls
       that will be added to the generated file
EOF
    exit 1
}

default_template()
{
    cat <<EOF
diagnostics:
  enabled:
    - bound_var_in_pattern
    - elvis
    - unused_includes
    - unused_macros
    - crossref
    # - dialyzer
    - compiler
  disabled:
    - dialyzer
    # - crossref
    # - compiler
lenses:
  disabled:
    # - show-behaviour-usages
    # - ct-run-test
    - server-info
  enable:
    - show-behaviour-usages
    - ct-run-test
macros:
  - name: EMQX_RELEASE_EDITION
    value: ee
code_reload:
  node: emqx@127.0.0.1
formatting:
  formatter: erlfmt
EOF
}

template="$(default_template)"

while getopts "ht:" opt; do
    case "${opt}" in
        h) usage ;;
        t) template=$(cat "${OPTARG}") ;;
        *) ;;
    esac
done

deps_dirs()
{
    local app
    for dir in _build/default/lib/*; do
        app=$(basename "${dir}")
        ## Only add applications that are not part of EMQX umbrella project:
        [ -d "apps/${app}" ] ||
            echo "  - \"${dir}\""
    done
}

find_plt() {
    for plt in emqx_dialyzer_*_plt; do
        cat <<EOF
plt_path:
  - "${plt}"
EOF
        break
    done
}

cat <<EOF
apps_dirs:
  - "apps/*"
deps_dirs:
$(deps_dirs)
  - "_build/test/lib/bbmustache"
  - "_build/test/lib/meck"
  - "_build/test/lib/proper"
  - "_build/test/lib/erl_csv"
include_dirs:
  - "apps/"
  - "apps/*/include"
  - "_build/default/lib/typerefl/include"
  - "_build/default/lib/"
  - "_build/default/lib/*/include"
  - "_build/test/lib/bbmustache"
  - "_build/test/lib/meck"
  - "_build/test/lib/proper"
exclude_unused_includes:
  - "typerefl/include/types.hrl"
  - "logger.hrl"
$(find_plt)
${template}
EOF

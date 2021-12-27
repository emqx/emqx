#!/usr/bin/env bash

set -euo pipefail

vsn="${1}"
workdir="demo_src"
target_name="emqx_plugin_template-${vsn}.tar.gz"
target="$workdir/_build/default/emqx_plugrel/${target_name}"
if [ -f "${target}" ]; then
    cp "$target" ./
    exit 0
fi

# cleanup
rm -rf "${workdir}"

git clone https://github.com/emqx/emqx-plugin-template.git -b "${vsn}" ${workdir}
make -C "$workdir" rel

cp "$target" ./

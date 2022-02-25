#!/usr/bin/env bash

set -euo pipefail

vsn="${1}"
target_path="${2}"
release_name="${3}"
git_url="${4}"
workdir="${5}"

target_name="${release_name}-${vsn}.tar.gz"
target="$workdir/${target_path}/${target_name}"
if [ -f "${target}" ]; then
    cp "$target" ./
    exit 0
fi

# cleanup
rm -rf "${workdir}"

git clone "${git_url}" -b "${vsn}" "${workdir}"
make -C "$workdir" rel

cp "$target" ./

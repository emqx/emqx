#!/usr/bin/env bash

set -euo pipefail

vsn="${1}"
target_path="${2}"
release_name="${3}"
git_url="${4}"
workdir="${5}"
artifact_download_url="${6:-}"

target_name="${release_name}-${vsn}.tar.gz"
target="$workdir/${target_path}/${target_name}"

if [ -n "${artifact_download_url}" ]
then
  echo "downloading plugin from artifact url..."
  curl --show-error -L -o "${target_name}" "${artifact_download_url}"
else
  echo "building plugin from source..."

  if [ -f "${target}" ]; then
      cp "$target" ./
      exit 0
  fi

  # cleanup
  rm -rf "${workdir}"

  git clone "${git_url}" -b "${vsn}" "${workdir}"
  make -C "$workdir" rel

  cp "$target" ./
fi

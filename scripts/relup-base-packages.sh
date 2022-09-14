#!/usr/bin/env bash

## This script helps to download relup base version packages

if [[ -n "$DEBUG" ]]; then
    set -x
fi
set -euo pipefail

PROFILE="${1}"
if [ "$PROFILE" = "" ]; then
    PROFILE="emqx"
fi

case $PROFILE in
    "emqx")
        DIR='emqx-ce'
        EDITION='community'
        ;;
    "emqx-ee")
        DIR='emqx-ee'
        EDITION='enterprise'
        ;;
    "emqx-edge")
        DIR='emqx-edge'
        EDITION='edge'
        ;;
esac

SYSTEM="${SYSTEM:-$(./scripts/get-distro.sh)}"

ARCH="${ARCH:-$(uname -m)}"
case "$ARCH" in
    x86_64)
        ARCH='amd64'
        ;;
    aarch64)
        ARCH='arm64'
        ;;
    arm*)
        ARCH=arm
        ;;
esac

SHASUM="sha256sum"
if [ "$SYSTEM" = "macos" ]; then
    SHASUM="shasum -a 256"
fi

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

mkdir -p _upgrade_base
pushd _upgrade_base

for tag in $(../scripts/relup-base-vsns.sh $EDITION | xargs echo -n); do
    filename="$PROFILE-$SYSTEM-${tag#[e|v]}-$ARCH.zip"
    url="https://packages.emqx.io/$DIR/$tag/$filename"
    echo "downloading base package from ${url} ..."
    if [ -f "$filename" ]; then
        echo "file $filename already downloaded; skikpped"
        continue
    fi
    curl -L -o "${filename}" "${url}"
    if [ "$SYSTEM" != "centos6" ]; then
        curl -L -o "${filename}.sha256" "${url}.sha256"
        SUMSTR=$(cat "${filename}.sha256")
        echo "got sha265sum: ${SUMSTR}"
        ## https://askubuntu.com/questions/1202208/checking-sha256-checksum
        echo "${SUMSTR}  ${filename}" | $SHASUM -c || exit 1
    fi
done

popd

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
OTP_VSN="${OTP_VSN:-$(./scripts/get-otp-vsn.sh)}"

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


case "$SYSTEM" in
    windows*)
        echo "WARNING: skipped downloading relup base for windows because we do not support relup for windows yet."
        exit 0
        ;;
    macos*)
        SHASUM="shasum -a 256"
        ;;
    *)
        SHASUM="sha256sum"
        ;;
esac

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

mkdir -p _upgrade_base
pushd _upgrade_base

# For 4.5+, we upgrade from OTP 24.1.5-3 to 24.3.4.2-1, so we must manually
# check the old OTP releases.
otp_vsn_for() {
  case "${1#[e|v]}" in
    4.4.*)
      echo "24.1.5-3"
      ;;
    4.5.*)
      echo "$OTP_VSN"
      ;;
  esac
}

for tag in $(../scripts/relup-base-vsns.sh $EDITION | xargs echo -n); do
    filename="$PROFILE-${tag#[e|v]}-otp$(otp_vsn_for "$tag")-$SYSTEM-$ARCH.zip"
    url="https://packages.emqx.io/$DIR/$tag/$filename"
    if [ ! -f "$filename" ] && curl -L -I -m 10 -o /dev/null -s -w "%{http_code}" "${url}" | grep -q -oE "^[23]+" ; then
        echo "downloading base package from ${url} ..."
        curl -L -o "${filename}" "${url}"
        if [ "$SYSTEM" != "centos6" ]; then
            echo "downloading sha256 sum from ${url}.sha256 ..."
            curl -L -o "${filename}.sha256" "${url}.sha256"
            SUMSTR=$(cat "${filename}.sha256")
            echo "got sha265sum: ${SUMSTR}"
            ## https://askubuntu.com/questions/1202208/checking-sha256-checksum
            echo "${SUMSTR}  ${filename}" | $SHASUM -c || exit 1
        fi
    fi
done

popd

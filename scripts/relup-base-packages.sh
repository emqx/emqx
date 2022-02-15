#!/usr/bin/env bash
if [[ -n "$DEBUG" ]]; then
    set -x
fi
set -euo pipefail

PROFILE="${1}"
if [ "$PROFILE" = "" ]; then
    $PROFILE="emqx"
fi

case $PROFILE in
    "emqx")
        BROKER="broker"
        ;;
    "emqx-ee")
        BROKER="enterprise"
        ;;
    "emqx-edge")
        BROKER="edge"
        ;;
esac

SYSTEM="$(./scripts/get-distro.sh)"

ARCH="$(uname -m)"
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

for tag in $(../scripts/relup-base-vsns.sh community | xargs echo -n); do
    if [ ! -f "$PROFILE-$SYSTEM-${tag#[e|v]}-$ARCH.zip" ] \
    && [ ! -z "$(echo $(curl -I -m 10 -o /dev/null -s -w %{http_code} https://www.emqx.com/downloads/$BROKER/$tag/$PROFILE-$SYSTEM-${tag#[e|v]}-$ARCH.zip) | grep -oE "^[23]+")" ];then
        wget --no-verbose https://www.emqx.com/downloads/$BROKER/$tag/$PROFILE-$SYSTEM-${tag#[e|v]}-$ARCH.zip
        if [ "$SYSTEM" != "centos6" ]; then
            wget --no-verbose https://www.emqx.com/downloads/$BROKER/$tag/$PROFILE-$SYSTEM-${tag#[e|v]}-$ARCH.zip.sha256
            ## https://askubuntu.com/questions/1202208/checking-sha256-checksum
            echo "$(cat $PROFILE-$SYSTEM-${tag#[e|v]}-$ARCH.zip.sha256)  $PROFILE-$SYSTEM-${tag#[e|v]}-$ARCH.zip" | $SHASUM -c || exit 1
        fi
    fi
done 

popd

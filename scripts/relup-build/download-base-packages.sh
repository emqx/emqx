#!/usr/bin/env bash

## This script helps to download relup base version packages

if [[ -n "$DEBUG" ]]; then set -x; fi
set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
ROOT_DIR="$(pwd)"

PROFILE="${1:-emqx-enterprise}"
export PROFILE

case $PROFILE in
    "emqx-enterprise")
        DIR='enterprise'
        EDITION='enterprise'
        ;;
    "emqx")
        echo "No relup for opensource edition"
        exit 0
        ;;
    *)
        echo "Unknown profile $PROFILE"
        exit 1
        ;;
esac

SYSTEM="$(./scripts/get-distro.sh)"
case "$SYSTEM" in
    windows*)
        echo "NOTE: no_relup_for_windows"
        exit 0
        ;;
    macos*)
        SHASUM="shasum -a 256"
        ;;
    *)
        SHASUM="sha256sum"
        ;;
esac

BASE_VERSIONS="$("${ROOT_DIR}"/scripts/relup-build/base-vsns.sh "$EDITION" | xargs echo -n)"

fullvsn() {
    env PKG_VSN="$1" "${ROOT_DIR}"/pkg-vsn.sh "$PROFILE" --long
}

mkdir -p _upgrade_base
pushd _upgrade_base >/dev/null
for tag in ${BASE_VERSIONS}; do
    filename="$PROFILE-$(fullvsn "${tag#[e|v]}").tar.gz"
    url="https://www.emqx.com/downloads/$DIR/$tag/$filename"
    echo "downloading ${filename} ..."
    ## if the file does not exist (not downloaded yet)
    ## and there is such a package to downlaod
    if [ ! -f "$filename" ] && curl -I -m 10 -o /dev/null -s -w "%{http_code}" "${url}" | grep -q -oE "^[23]+" ; then
        curl -L -o "${filename}" "${url}"
        curl -L -o "${filename}.sha256" "${url}.sha256"
        ## https://askubuntu.com/questions/1202208/checking-sha256-checksum
        echo "$(cat "${filename}.sha256")  ${filename}" | $SHASUM -c || exit 1
    fi
done

popd >/dev/null

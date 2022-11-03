#!/bin/bash

USAGE="$0 profile vsn old_vsn package_path"
EXAMPLE="$0 emqx 4.3.8-b3bb6075 v4.3.2 /home/alice/relup_dubug/downloaded_packages"

if [[ $# -ne 4 ]]; then
    echo "$USAGE"
    echo "$EXAMPLE"
    exit 1
fi

set -ex

PROFILE="$1"
VSN="$2"
OLD_VSN="$3"
PACKAGE_PATH="$4"
FROM_OTP_VSN="${5:-24.3.4.2-1}"
TO_OTP_VSN="${6:-24.3.4.2-1}"

TEMPDIR=$(mktemp -d)
trap '{ rm -rf -- "$TEMPDIR"; }' EXIT

git clone --branch=master "https://github.com/terry-xiaoyu/one_more_emqx.git" "$TEMPDIR/one_more_emqx"
cp -r "$PACKAGE_PATH" "$TEMPDIR/packages"
cp relup.lux "$TEMPDIR/"
cp -r http_server "$TEMPDIR/http_server"

exec docker run \
    -v "$TEMPDIR:/relup_test" \
    -w "/relup_test" \
    -e REBAR_COLOR=none \
    -it emqx/relup-test-env:erl23.2.7.2-emqx-3-ubuntu20.04 \
        lux \
        --progress verbose \
        --case_timeout infinity \
        --var PROFILE="$PROFILE" \
        --var PACKAGE_PATH="/relup_test/packages" \
        --var ONE_MORE_EMQX_PATH="/relup_test/one_more_emqx" \
        --var VSN="$VSN" \
        --var OLD_VSN="$OLD_VSN" \
        --var FROM_OTP_VSN="$FROM_OTP_VSN" \
        --var TO_OTP_VSN="$TO_OTP_VSN" \
        relup.lux

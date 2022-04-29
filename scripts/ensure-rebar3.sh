#!/usr/bin/env bash

set -euo pipefail

## rebar3 tag 3.18.0-emqx-1 is compiled using otp24.1.5.
## we have to use an otp24-compiled rebar3 because the defination of record #application{}
## in systools.hrl is changed in otp24.
OTP_VSN="${OTP_VSN:-$(./scripts/get-otp-vsn.sh)}"
case ${OTP_VSN} in
    23*)
        VERSION="3.16.1-emqx-1"
        ;;
    24*)
        VERSION="3.18.0-emqx-1"
        ;;
    *)
        echo "Unsupporetd Erlang/OTP version $OTP_VSN"
        exit 1
        ;;
esac

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

DOWNLOAD_URL='https://github.com/emqx/rebar3/releases/download'

download() {
    echo "downloading rebar3 ${VERSION}"
    curl -f -L "${DOWNLOAD_URL}/${VERSION}/rebar3" -o ./rebar3
}

# get the version number from the second line of the escript
# because command `rebar3 -v` tries to load rebar.config
# which is slow and may print some logs
version() {
    head -n 2 ./rebar3 | tail -n 1 | tr ' ' '\n' | grep -E '^.+-emqx-.+'
}

if [ -f 'rebar3' ] && [ "$(version)" = "$VERSION" ]; then
    exit 0
fi

download
chmod +x ./rebar3

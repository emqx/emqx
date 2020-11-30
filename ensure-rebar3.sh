#!/bin/bash

set -euo pipefail

VERSION="$1"

DOWNLOAD_URL='https://github.com/emqx/rebar3/releases/download'
download() {
    curl -L "${DOWNLOAD_URL}/${VERSION}/rebar3" -o ./rebar3
}

version() {
    ./rebar3 -v | grep -v '===' | grep 'rebar.*Erlang' | awk '{print $2}'
}

if [ -f 'rebar3' ] && [ "$(version)" == "$VERSION" ]; then
    exit 0
fi

download
chmod +x ./rebar3

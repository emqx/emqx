#!/bin/bash

set -euo pipefail

VERSION="$1"

download() {
    curl -L "https://s3-us-west-2.amazonaws.com/packages.emqx/rebar/rebar3-${VERSION}" -o ./rebar3
}

version() {
    ./rebar3 -v | grep -v '===' | grep 'rebar.*Erlang' | awk '{print $2}'
}

if [ -f 'rebar3' ] && [ "$(version)" == "$VERSION" ]; then
    exit 0
fi

download
chmod +x ./rebar3

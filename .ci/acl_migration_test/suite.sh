#!/bin/bash

set -xe

export EMQX_PATH="$1"
export EMQX_BASE="$2"

export TEST_PATH="emqx_test"

./build.sh

VERSION=$("$EMQX_PATH"/pkg-vsn.sh)
export VERSION

./prepare.sh

./test.sh

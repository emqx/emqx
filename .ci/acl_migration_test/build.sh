#!/bin/bash

set -xe

cd "$EMQX_PATH"

rm -rf _build _upgrade_base

mkdir _upgrade_base
pushd _upgrade_base
    wget "https://s3-us-west-2.amazonaws.com/packages.emqx/emqx-ce/v${EMQX_BASE}/emqx-ubuntu20.04-${EMQX_BASE}-amd64.zip"
popd

make emqx-zip

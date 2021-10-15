#!/bin/bash

set -xe

mkdir -p "$TEST_PATH"
cd "$TEST_PATH"

cp ../"$EMQX_PATH"/_upgrade_base/*.zip ./
unzip ./*.zip

cp ../"$EMQX_PATH"/_packages/emqx/*.zip ./emqx/releases/

git clone --depth 1 https://github.com/terry-xiaoyu/one_more_emqx.git

./one_more_emqx/one_more_emqx.sh emqx2

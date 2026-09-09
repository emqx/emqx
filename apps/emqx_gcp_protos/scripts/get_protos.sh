#!/usr/bin/env bash
set -xeuo pipefail

## Simple script to clone the repository containing bigtable grpc schemas and copying them
## to the bridge's `script` dir.  Update `$REF` to the desired commit ref.

REF="8bd905897f61fb6f2e7d8b7cb3e2ca41d0cbc9c8"

PRIV_DIR="$(realpath "$(dirname -- "${BASH_SOURCE[0]}")")"

cd /tmp

if [[ ! -d googleapis ]]; then
  git clone https://github.com/googleapis/googleapis.git
  cd googleapis
  git checkout "$REF"
fi

for path in api bigtable/v2 rpc type; do
  mkdir -p "$PRIV_DIR/protos/google/$path"
  cp -r /tmp/googleapis/google/$path/*.proto "$PRIV_DIR/protos/google/$path/"
done

rm -rf googleapis

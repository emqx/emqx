#!/usr/bin/env bash

[[ -n "$WITHOUT_UPDATE_BOM" ]] && exit 0

set -euo pipefail

PROFILE="$1"
REL_DIR="$2"

./rebar3 as "$PROFILE" sbom -f -o "$REL_DIR/bom.json"

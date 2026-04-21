#!/usr/bin/env bash

## Build a single monorepo plugin package.
##
## Driven entirely from the umbrella build: the plugin is compiled by
## the root `make` into `_build/$PROFILE/lib/<name>/`, and this wrapper
## delegates to `scripts/build-plugin.escript` to assemble the tarball
## with the same on-disk layout as `rebar3 emqx_plugrel tar`.
##
## The escript reads `plugins/<name>/rebar.config` for the `emqx_plugrel`
## metadata block and `plugins/<name>/VERSION` for the release version.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $(basename "$0") <plugin_app>" >&2
    exit 1
fi

APP="$1"

if [[ ! "$APP" =~ ^[a-z][a-z0-9_]*$ ]]; then
    echo "Invalid plugin app name: $APP" >&2
    exit 1
fi

ROOT_DIR="$(cd -P -- "$(dirname -- "$0")/.." && pwd)"
PROFILE="${PROFILE:-emqx-enterprise}"

exec env ROOT_DIR="$ROOT_DIR" PROFILE="$PROFILE" \
    "$ROOT_DIR/scripts/build-plugin.escript" "$APP"

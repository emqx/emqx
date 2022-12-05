#!/usr/bin/env bash
set -euo pipefail

if [ "${DEBUG:-}" = 1 ]; then
    set -x
fi

# ensure dir
cd -P -- "$(dirname -- "$0")/../.."

PROFILE="$1"
CHART_FILE="deploy/charts/${PROFILE}/Chart.yaml"

if [ ! -f "$CHART_FILE" ]; then
    echo "Chart file $CHART_FILE is not found"
    echo "Current working dir: $(pwd)"
    exit 1
fi

CHART_VSN="$(grep -oE '^version:.*' "$CHART_FILE" | cut -d ':' -f 2 | tr -d ' ')"
APP_VSN="$(grep -oE '^appVersion:.*' "$CHART_FILE" | cut -d ':' -f 2 | tr -d ' ')"

if [ "$CHART_VSN" != "$APP_VSN" ]; then
    echo "Chart version and app version mismatch in $CHART_FILE"
    exit 2
fi

PKG_VSN="$(./pkg-vsn.sh "$PROFILE" | cut -d '-' -f 1)"

if [ "$CHART_VSN" != "$PKG_VSN" ]; then
    echo "Chart version in $CHART_FILE is not in sync with release version."
    echo "Chart version: $CHART_VSN"
    echo "Release version: $PKG_VSN"
    exit 3
fi

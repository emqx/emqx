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
RELEASE_VSN="$(./pkg-vsn.sh "$PROFILE" --release)"
## The chart version is a semver form of the release version; see chart-vsn.sh.
EXPECTED_CHART_VSN="$(./scripts/rel/chart-vsn.sh "$RELEASE_VSN")"

if [ "$APP_VSN" != "$RELEASE_VSN" ]; then
    echo "Chart appVersion in $CHART_FILE is not in sync with release version."
    echo "Chart appVersion: $APP_VSN"
    echo "Release version: $RELEASE_VSN"
    exit 2
fi

if [ "$CHART_VSN" != "$EXPECTED_CHART_VSN" ]; then
    echo "Chart version in $CHART_FILE is not in sync with release version."
    echo "Chart version: $CHART_VSN"
    echo "Expected chart version: $EXPECTED_CHART_VSN"
    echo "Release version: $RELEASE_VSN"
    exit 3
fi

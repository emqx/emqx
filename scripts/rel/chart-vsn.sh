#!/usr/bin/env bash

## Print the Helm chart version for an EMQX release version.
##
## Helm requires the chart version to be a semver string. A hotfix release
## version Major.Minor.Patch.N is not one, so the chart carries it as build
## metadata: Major.Minor.Patch+N. Every other release version is used as is.
## The chart appVersion always carries the release version unchanged.

set -euo pipefail

RELEASE_VSN="${1:-}"
if [ -z "$RELEASE_VSN" ]; then
    echo "usage: $0 RELEASE_VERSION" >&2
    exit 1
fi

if [[ "$RELEASE_VSN" =~ ^([0-9]+\.[0-9]+\.[0-9]+)\.([0-9]+)$ ]]; then
    echo "${BASH_REMATCH[1]}+${BASH_REMATCH[2]}"
else
    echo "$RELEASE_VSN"
fi

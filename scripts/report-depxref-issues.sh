#!/usr/bin/env bash

set -euo pipefail

base="${1:-}"
if [ "${base}" = "" ]; then
    echo "Usage $0 <git-compare-base-ref>"
    exit 1
fi

# determine affected apps
apps="$(./scripts/find-changed-apps.sh "${base}")"
if [ "${apps}" = "" ]; then
    echo "Skipping: no apps changed since ${base}" && exit 0
fi

# use release profile
export PROFILE=emqx-enterprise

# shellcheck disable=SC2086
mix emqx.depxref --report-md=depxref-report.md --summarize ${apps}

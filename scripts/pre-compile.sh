#!/usr/bin/env bash

set -euo pipefail

# NOTE: PROFILE_STR may not be exactly PROFILE (emqx or emqx-enterprise)
# it might be with suffix such as -pkg etc.
PROFILE_STR="${1}"

case "$PROFILE_STR" in
    *enterprise*)
        dashboard_version="$EMQX_EE_DASHBOARD_VERSION"
        ;;
    *)
        dashboard_version="$EMQX_DASHBOARD_VERSION"
        ;;
esac

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

./scripts/get-dashboard.sh "$dashboard_version"
./scripts/merge-config.escript
./scripts/merge-i18n.escript

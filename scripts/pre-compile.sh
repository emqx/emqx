#!/usr/bin/env bash

set -euo pipefail

[ "${DEBUG:-0}" -eq 1 ] && set -x

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

# generate merged config files and English translation of the desc (desc.en.hocon)
./scripts/merge-config.escript

I18N_REPO_BRANCH="v$(./pkg-vsn.sh "${PROFILE_STR}" | tr -d '.' | cut -c 1-2)"

# download desc (i18n) translations
curl -L --silent --show-error \
     --output "apps/emqx_dashboard/priv/desc.zh.hocon" \
     "https://raw.githubusercontent.com/emqx/emqx-i18n/${I18N_REPO_BRANCH}/desc.zh.hocon"

# TODO
# make sbom a build artifcat
# ./scripts/update-bom.sh "$PROFILE_STR" ./rel

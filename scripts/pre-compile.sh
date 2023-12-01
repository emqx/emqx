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

DOWNLOAD_I18N_TRANSLATIONS=${DOWNLOAD_I18N_TRANSLATIONS:-true}
# download desc (i18n) translations
if [ "$DOWNLOAD_I18N_TRANSLATIONS" = "true" ]; then
  echo "downloading i18n translation from emqx/emqx-i18n"
  start=$(date +%s)
  curl -L --fail --silent --show-error \
       --output "apps/emqx_dashboard/priv/desc.zh.hocon" \
       "https://raw.githubusercontent.com/emqx/emqx-i18n/${I18N_REPO_BRANCH}/desc.zh.hocon"
  end=$(date +%s)
  duration=$(echo "$end $start" | awk '{print $1 - $2}')
  echo "downloaded i18n translation in $duration seconds, set DOWNLOAD_I18N_TRANSLATIONS=false to skip"
else
  echo "skipping to download i18n translation from emqx/emqx-i18n, set DOWNLOAD_I18N_TRANSLATIONS=true to update"
fi

# TODO
# make sbom a build artifact
# ./scripts/update-bom.sh "$PROFILE_STR" ./rel

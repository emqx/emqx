#!/usr/bin/env bash

set -euo pipefail

[ "${DEBUG:-0}" -eq 1 ] && set -x

# NOTE: PROFILE_STR may not be exactly PROFILE (emqx or emqx-enterprise)
# it might be with suffix such as -pkg etc.
PROFILE_STR="${1:-emqx-enterprise}"

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

I18N_REPO_BRANCH="v$(./pkg-vsn.sh "${PROFILE_STR}" | cut -d'.' -f1,2 | tr -d '.')"

DOWNLOAD_I18N_TRANSLATIONS=${DOWNLOAD_I18N_TRANSLATIONS:-true}
# download desc (i18n) translations
beginfmt='\033[1m'
endfmt='\033[0m'
if [ "$DOWNLOAD_I18N_TRANSLATIONS" = "true" ]; then
  echo "Downloading i18n translation from emqx/emqx-i18n..."
  start=$(date +%s%N)
  curl -L --fail --silent --show-error \
       --output "apps/emqx_dashboard/priv/desc.zh.hocon" \
       "https://raw.githubusercontent.com/emqx/emqx-i18n/${I18N_REPO_BRANCH}/desc.zh.hocon"
  end=$(date +%s%N)
  duration=$(echo "$end $start" | awk '{printf "%.f\n", (($1 - $2)/ 1000000)}')
  if [ "$duration" -gt 1000 ]; then beginfmt='\033[1;33m'; fi
  echo -e "Downloaded i18n translation in $duration milliseconds.\nSet ${beginfmt}DOWNLOAD_I18N_TRANSLATIONS=false${endfmt} to skip"
else
  echo -e "Skipping to download i18n translation from emqx/emqx-i18n.\nSet ${beginfmt}DOWNLOAD_I18N_TRANSLATIONS=true${endfmt} to update"
fi

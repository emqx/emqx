#!/usr/bin/env bash

set -euo pipefail

[ "${DEBUG:-0}" -eq 1 ] && set -x

# NOTE: PROFILE_STR may not be exactly PROFILE (emqx or emqx-enterprise)
# it might be with suffix such as -pkg etc.
PROFILE_STR="${1:-emqx-enterprise}"

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

./scripts/get-dashboard.sh "$EMQX_DASHBOARD_VERSION"

# generate merged config files and English translation of the desc (desc.en.hocon)
./scripts/merge-config.escript

I18N_REPO_BRANCH="v$(./pkg-vsn.sh "${PROFILE_STR}" | cut -d'.' -f1,2 | tr -d '.')"

# The language tag below is both the value accepted by `dashboard.i18n_lang` and
# the tag emqx_dashboard_desc_cache derives from the "desc.<lang>.hocon" file
# name, so the two must match exactly (the lookup is a case sensitive binary
# match). A text missing from one language shows the English one.

# Downloaded from emqx/emqx-i18n: "<language tag>:<file name in that repo>"
I18N_DOWNLOAD_LANGS=(
  "zh:desc.zh.hocon"
)

# Maintained in this repository: "<language tag>:<path from the repo root>"
I18N_LOCAL_LANGS=(
  "zh-TW:rel/i18n-tr/desc.zh-TW.hocon"
)

DOWNLOAD_I18N_TRANSLATIONS=${DOWNLOAD_I18N_TRANSLATIONS:-true}
# download desc (i18n) translations
beginfmt='\033[1m'
endfmt='\033[0m'
if [ "$DOWNLOAD_I18N_TRANSLATIONS" = "true" ]; then
  echo "Downloading i18n translation from emqx/emqx-i18n..."
  start=$(date +%s%N)
  for lang_spec in "${I18N_DOWNLOAD_LANGS[@]}"; do
    lang="${lang_spec%%:*}"
    src_file="${lang_spec#*:}"
    curl -L --fail --silent --show-error \
         --retry 3 --retry-delay 2 \
         --output "apps/emqx_dashboard/priv/desc.${lang}.hocon" \
         "https://raw.githubusercontent.com/emqx/emqx-i18n/${I18N_REPO_BRANCH}/${src_file}"
  done
  end=$(date +%s%N)
  duration=$(echo "$end $start" | awk '{printf "%.f\n", (($1 - $2)/ 1000000)}')
  if [ "$duration" -gt 1000 ]; then beginfmt='\033[1;33m'; fi
  echo -e "Downloaded i18n translation in $duration milliseconds.\nSet ${beginfmt}DOWNLOAD_I18N_TRANSLATIONS=false${endfmt} to skip"
else
  echo -e "Skipping to download i18n translation from emqx/emqx-i18n.\nSet ${beginfmt}DOWNLOAD_I18N_TRANSLATIONS=true${endfmt} to update"
fi

# install the translations kept in this repository, no network needed
for lang_spec in "${I18N_LOCAL_LANGS[@]}"; do
  lang="${lang_spec%%:*}"
  src_file="${lang_spec#*:}"
  if [ -f "$src_file" ]; then
    cp -f "$src_file" "apps/emqx_dashboard/priv/desc.${lang}.hocon"
    echo "Installed ${lang} translation from ${src_file}"
  else
    echo -e "\033[1;33mWARNING: ${src_file} not found, ${lang} will fall back to another language\033[0m"
  fi
done

#!/usr/bin/env bash

# arg1: profile, e.g. emqx | emqx-enterprise

set -euo pipefail

EMQX_CE_DASHBOARD_VERSION='v1.0.5'
EMQX_EE_DASHBOARD_VERSION='e1.0.0'

case "$1" in
    emqx-enterprise|emqx)
        PROFILE=$1
        ;;
    *)
        echo Invalid profile "$1"
        exit 1
esac

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

case "${PROFILE}" in
    emqx-enterprise)
        VERSION="${EMQX_EE_DASHBOARD_VERSION}"
        RELEASE_ASSET_FILE="emqx-enterprise-dashboard.zip"
        ;;
    emqx)
        VERSION="${EMQX_CE_DASHBOARD_VERSION}"
        RELEASE_ASSET_FILE="emqx-dashboard.zip"
        ;;
esac
DASHBOARD_PATH='apps/emqx_dashboard/priv'
DASHBOARD_REPO='emqx-dashboard-web-new'
DIRECT_DOWNLOAD_URL="https://github.com/emqx/${DASHBOARD_REPO}/releases/download/${VERSION}/${RELEASE_ASSET_FILE}"

case $(uname) in
    *Darwin*) SED="sed -E";;
    *) SED="sed -r";;
esac

version() {
    grep -oE 'github_ref: (.*)' "$DASHBOARD_PATH/www/version" |  $SED 's|github_ref: refs/tags/(.*)|\1|g'
}

if [ -d "$DASHBOARD_PATH/www" ] && [ "$(version)" = "$VERSION" ]; then
    exit 0
fi

echo "Downloading dashboard from: $DIRECT_DOWNLOAD_URL"
curl -L --silent --show-error \
     --header "Accept: application/octet-stream" \
     --output "${RELEASE_ASSET_FILE}" \
     "$DIRECT_DOWNLOAD_URL"

unzip -q "$RELEASE_ASSET_FILE" -d "$DASHBOARD_PATH"
rm -rf "$DASHBOARD_PATH/www"
mv "$DASHBOARD_PATH/dist" "$DASHBOARD_PATH/www"
rm -f "$RELEASE_ASSET_FILE"

#!/usr/bin/env bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

PKG_VSN="${PKG_VSN:-$(./pkg-vsn.sh)}"
case "${PKG_VSN}" in
    4.3*)
        EMQX_CE_DASHBOARD_VERSION='v4.3.12'
        EMQX_EE_DASHBOARD_VERSION='v4.3.30'
        ;;
    4.4*)
        # keep the above 4.3 untouched, otherwise conflicts!
        EMQX_CE_DASHBOARD_VERSION='v4.4.11'
        EMQX_EE_DASHBOARD_VERSION='v4.4.26'
        ;;
    *)
        echo "Unsupported version $PKG_VSN" >&2
        exit 1
        ;;
esac

RELEASE_ASSET_FILE="emqx-dashboard.zip"

if [ -f 'EMQX_ENTERPRISE' ]; then
    VERSION="${EMQX_EE_DASHBOARD_VERSION}"
    DASHBOARD_PATH='lib-ee/emqx_dashboard/priv'
    DASHBOARD_REPO='emqx-dashboard-web'
    DIRECT_DOWNLOAD_URL="https://github.com/emqx/${DASHBOARD_REPO}/releases/download/${VERSION}/${RELEASE_ASSET_FILE}"
else
    VERSION="${EMQX_CE_DASHBOARD_VERSION}"
    DASHBOARD_PATH='lib-ce/emqx_dashboard/priv'
    DASHBOARD_REPO='emqx-dashboard-frontend'
    DIRECT_DOWNLOAD_URL="https://github.com/emqx/${DASHBOARD_REPO}/releases/download/${VERSION}/${RELEASE_ASSET_FILE}"
fi

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

echo "Downloading dashboard from $DIRECT_DOWNLOAD_URL"
curl -L --silent --show-error \
     --header "Accept: application/octet-stream" \
     --output "${RELEASE_ASSET_FILE}" \
     "$DIRECT_DOWNLOAD_URL"

unzip -q "$RELEASE_ASSET_FILE" -d "$DASHBOARD_PATH"
rm -rf "$DASHBOARD_PATH/www"
mv "$DASHBOARD_PATH/dist" "$DASHBOARD_PATH/www"
rm -f "$RELEASE_ASSET_FILE"

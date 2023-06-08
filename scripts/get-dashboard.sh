#!/usr/bin/env bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

VERSION="${1}"
case "$VERSION" in
    v*)
        RELEASE_ASSET_FILE="emqx-dashboard-$VERSION.zip"
        ;;
    e*)
        RELEASE_ASSET_FILE="emqx-enterprise-dashboard-$VERSION.zip"
        ;;
    *)
        echo "Unknown version $VERSION"
        exit 1
        ;;
esac

DASHBOARD_PATH='apps/emqx_dashboard/priv'
DASHBOARD_REPO='emqx-dashboard5'
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

unzip -o -q "$RELEASE_ASSET_FILE" -d "$DASHBOARD_PATH"
rm -rf "$DASHBOARD_PATH/www"
mv "$DASHBOARD_PATH/dist" "$DASHBOARD_PATH/www"
rm -f "$RELEASE_ASSET_FILE"

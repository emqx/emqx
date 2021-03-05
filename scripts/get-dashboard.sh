#!/usr/bin/env bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

RELEASE_ASSET_FILE="emqx-dashboard.zip"

if [ -f 'EMQX_ENTERPRISE' ]; then
    VERSION="${EMQX_EE_DASHBOARD_VERSION}"
    DASHBOARD_PATH='lib-ee/emqx_dashboard/priv'
    DASHBOARD_REPO='emqx-enterprise-dashboard-frontend-src'
    AUTH="Authorization: token $(cat scripts/git-token)"
else
    VERSION="${EMQX_CE_DASHBOARD_VERSION}"
    DASHBOARD_PATH='lib-ce/emqx_dashboard/priv'
    DASHBOARD_REPO='emqx-dashboard-frontend'
    AUTH=""
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

get_assets(){
    # Get the download URL of our desired asset
    download_url="$(curl --silent --show-error \
        --header "${AUTH}" \
        --header "Accept: application/vnd.github.v3+json" \
        "https://api.github.com/repos/emqx/${DASHBOARD_REPO}/releases/tags/${VERSION}" \
        | jq --raw-output ".assets[] | select(.name==\"${RELEASE_ASSET_FILE}\").url")"
    # Get GitHub's S3 redirect URL
    redirect_url=$(curl --silent --show-error \
        --header "${AUTH}" \
        --header "Accept: application/octet-stream" \
        --write-out "%{redirect_url}" \
        "$download_url")
    curl --silent --show-error \
         --header "Accept: application/octet-stream" \
         --output "${RELEASE_ASSET_FILE}" \
         "$redirect_url"
}

get_assets
unzip -q "$RELEASE_ASSET_FILE" -d "$DASHBOARD_PATH"
rm -rf "$DASHBOARD_PATH/www"
mv "$DASHBOARD_PATH/dist" "$DASHBOARD_PATH/www"
rm -rf "$RELEASE_ASSET_FILE"

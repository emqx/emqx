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
    # have to be resolved with auth and redirect
    DIRECT_DOWNLOAD_URL=""
else
    VERSION="${EMQX_CE_DASHBOARD_VERSION}"
    DASHBOARD_PATH='lib-ce/emqx_dashboard/priv'
    DASHBOARD_REPO='emqx-dashboard-frontend'
    AUTH=""
    DIRECT_DOWNLOAD_URL="https://github.com/emqx/${DASHBOARD_REPO}/releases/download/${VERSION}/emqx-dashboard.zip"
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

find_url() {
    # Get the download URL of our desired asset
    release_url="https://api.github.com/repos/emqx/${DASHBOARD_REPO}/releases/tags/${VERSION}"
    release_info="$(curl --silent --show-error --header "${AUTH}" --header "Accept: application/vnd.github.v3+json" "$release_url")"
    if ! download_url="$(echo "$release_info" | jq --raw-output ".assets[] | select(.name==\"${RELEASE_ASSET_FILE}\").url" | tr -d '\n' | tr -d '\r')"; then
        echo "failed to query $release_url"
        echo "${release_info}"
        exit 1
    fi
    # Get GitHub's S3 redirect URL
    curl --silent --show-error \
         --header "${AUTH}" \
         --header "Accept: application/octet-stream" \
         --write-out "%{redirect_url}" \
         "$download_url"
}

if [ -z "$DIRECT_DOWNLOAD_URL" ]; then
    DIRECT_DOWNLOAD_URL="$(find_url)"
fi

curl -L --silent --show-error \
     --header "Accept: application/octet-stream" \
     --output "${RELEASE_ASSET_FILE}" \
     "$DIRECT_DOWNLOAD_URL"

unzip -q "$RELEASE_ASSET_FILE" -d "$DASHBOARD_PATH"
rm -rf "$DASHBOARD_PATH/www"
mv "$DASHBOARD_PATH/dist" "$DASHBOARD_PATH/www"
rm -f "$RELEASE_ASSET_FILE"

#!/bin/bash

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

if [[ "$1" == https://* ]]; then
    VERSION='*' # alwyas download
    DOWNLOAD_URL="$1"
else
    VERSION="$1"
    DOWNLOAD_URL="https://github.com/emqx/emqx-dashboard-frontend/releases/download/${VERSION}/emqx-dashboard.zip"
fi

if [ "${EMQX_ENTERPRISE:-}" = 'true' ] || [ "${EMQX_ENTERPRISE:-}" == '1' ]; then
    DASHBOARD_PATH='lib-ee/emqx_dashboard/priv'
else
    DASHBOARD_PATH='lib-ce/emqx_dashboard/priv'
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

curl -f -L "${DOWNLOAD_URL}" -o ./emqx-dashboard.zip
unzip -q ./emqx-dashboard.zip -d "$DASHBOARD_PATH"
rm -rf "$DASHBOARD_PATH/www"
mv "$DASHBOARD_PATH/dist" "$DASHBOARD_PATH/www"
rm -rf emqx-dashboard.zip

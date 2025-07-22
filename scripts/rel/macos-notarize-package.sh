#!/usr/bin/env bash

set -x

set -euo pipefail

# intended to run on MacOS only
if [ "$(uname)" != 'Darwin' ]; then
    echo 'Not macOS, exiting';
    exit 0;
fi

if [ "${APPLE_SIGN_BINARIES:-0}" == 0 ]; then
    echo "Signing Apple binaries is disabled, exiting"
    exit 0
fi

if [[ "${APPLE_ID:-0}" == 0 || "${APPLE_ID_PASSWORD:-0}" == 0 || "${APPLE_TEAM_ID:-0}" == 0 ]]; then
    echo "Apple ID is not configured, skipping notarization."
    exit 0
fi

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <path-to-zip-package>"
    exit 1
fi

ZIP_PACKAGE_PATH="${1}"

echo 'Submitting the package for notarization to Apple (normally takes 1-2 minutes)'
notarytool_output="$(xcrun notarytool submit \
                                           --apple-id "${APPLE_ID}" \
                                           --password "${APPLE_ID_PASSWORD}" \
                                           --team-id "${APPLE_TEAM_ID}" "${ZIP_PACKAGE_PATH}" \
                                           --no-progress \
                                           --wait)"
echo "$notarytool_output"
echo "$notarytool_output" | grep -q 'status: Accepted' || {
    echo 'Notarization failed';
    submission_id=$(echo "$notarytool_output" | grep 'id: ' | awk '{print $2}')
    # find out what went wrong
    xcrun notarytool log \
        --apple-id "${APPLE_ID}" \
        --password "${APPLE_ID_PASSWORD}" \
        --team-id "${APPLE_TEAM_ID}" "$submission_id"
    exit 1;
}

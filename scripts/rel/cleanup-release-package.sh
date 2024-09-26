#!/usr/bin/env bash

# Remove unnecessary files to reduce the package size

set -euo pipefail

find "${RELX_TEMP_DIR}" -name 'swagger*.js.map' -exec rm {} +
find "${RELX_TEMP_DIR}" -name 'swagger*.css.map' -exec rm {} +

#!/usr/bin/env bash

# Install the QuasarDB ODBC driver on a Debian-based system.
# https://doc.quasar.ai/master/user-guide/integration/odbc.html

set -xeuo pipefail

if [ ! -f /etc/debian_version ]; then
  echo "This script is only intended for Debian-based systems"
  exit 1
fi

function maybe_download() {
  local FILE URL HASH
  FILE="$1"
  URL="$2"
  HASH="$3"
  if [[ -f "${FILE}" && $(echo "$HASH *$FILE" | sha256sum -c) ]]; then
    echo "${FILE} is already downloaded"
  else
    curl -fsSL -O --retry 5 "$URL"
    echo "$HASH *$FILE" | sha256sum -c
  fi
}

VERSION_FAMILY="3.14"
VERSION="${VERSION_FAMILY}.1"

## Note: 3.14.1 only offers a single driver/api file flavor.  3.14.2 apparently offers
## different files for amd64/aarch64/arm64 architectures, but currently does not have a
## 3.14.2 odbc driver option....  The available file patterns are kind of a mess.
API_FILE="qdb-api_${VERSION}.deb"
API_URL="https://download.quasar.ai/quasardb/${VERSION_FAMILY}/${VERSION}/api/c/${API_FILE}"
API_SHA256="fd0fccfc10cf79b06a2838991558353eb8db45a6d9122fead7ffc3495601bd33"
DRIVER_FILE="qdb-${VERSION}-linux-64bit-odbc-driver.tar.gz"
DRIVER_URL="https://download.quasar.ai/quasardb/${VERSION_FAMILY}/${VERSION}/api/odbc/${DRIVER_FILE}"
DRIVER_SHA256="cc04452f14c2b29c0c3e1e70abe999ae1834c4ac577820d63f609bfb52266c7d"

maybe_download "${API_FILE}" "${API_URL}" "${API_SHA256}"
maybe_download "${DRIVER_FILE}" "${DRIVER_URL}" "${DRIVER_SHA256}"

apt-get install -yqq "./${API_FILE}"
DRIVER_OUT_PATH="/tmp/qdb_odbc_driver"
mkdir "$DRIVER_OUT_PATH"
tar -C "$DRIVER_OUT_PATH" -xf "./${DRIVER_FILE}"

DRIVER_PATH="$DRIVER_OUT_PATH/lib/libqdb_odbc_driver.so"

ODBC_INST_INI_CONTENT="[qdb_odbc_driver]
Description=Quasardb ODBC Driver
Driver=$DRIVER_PATH
Setup=$DRIVER_PATH
APILevel=1
ConnectFunctions=YYY
DriverODBCVer=3.14.1
FileUsage=0
SQLLevel=1
UsageCount=1"

if ! grep -Fxq "$ODBC_INST_INI_CONTENT" /etc/odbcinst.ini; then
    cat >>/etc/odbcinst.ini <<EOF
$ODBC_INST_INI_CONTENT
EOF
fi

mkdir -p /tmp/qdb/log
ODBC_INI_CONTENT="[qdb]
Driver = qdb_odbc_driver
Description = QuasarDB ODBC Data Source
#URI = qdb://172.100.239.30:2836
#UID = user_name
#PWD = user_key
#KEY = cluster_public_key
LogDir = /tmp/qdb/log"

if ! grep -Fxq "$ODBC_INI_CONTENT" /etc/odbc.ini; then
    cat >>/etc/odbc.ini <<EOF
$ODBC_INI_CONTENT
EOF
fi

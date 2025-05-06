#!/usr/bin/env bash

# Install the Snowflake ODBC driver on a Debian-based system.
# https://developers.snowflake.com/odbc/

set -xeuo pipefail

if [ ! -f /etc/debian_version ]; then
  echo "This script is only intended for Debian-based systems"
  exit 1
fi

if dpkg -l snowflake-odbc 1>/dev/null 2>/dev/null ; then
  echo "Snowflake ODBC driver is already installed"
  exit 0
fi

VERSION="3.4.1"
BASE_URL="https://sfc-repo.snowflakecomputing.com/odbc"
ARCH="$(uname -m)"
FILE="snowflake-odbc-${VERSION}.${ARCH}.deb"
if [ "${ARCH}" == x86_64 ]; then
  URL="${BASE_URL}/linux/${VERSION}/${FILE}"
  SHA256="a96fcc89a3d3f2d16fc492976a01fefae57029bcd2e238ff4eff5a6693fa4f74"
elif [ "${ARCH}" == aarch64 ]; then
  URL="${BASE_URL}/linux${ARCH}/${VERSION}/${FILE}"
  SHA256="c9f5c60ace416640683693037d5949aefea9555b9aa62501b6b6c692d140989c"
else
  echo "Unsupported architecture: ${ARCH}"
  exit 1
fi

if [[ -f "${FILE}" && $(echo "$SHA256 *$FILE" | sha256sum -c) ]]; then
    echo "Snowflake ODBC driver package is already downloaded"
else
    apt-get -qq update && env DEBIAN_FRONTEND=noninteractive apt-get install -yqq curl
    curl -fsSL -O --retry 5 "$URL"
    echo "$SHA256 *$FILE" | sha256sum -c
fi

apt-get -qq update
apt-get install -yqq unixodbc-dev unixodbc odbcinst curl libreadline8
apt-get install -yqq "./${FILE}"

# Fix the path to the libodbcinst.so library in the simba.snowflake.ini file.
ODBC_INST_LIB="/usr/lib/${ARCH}-linux-gnu/libodbcinst.so"
sed -i -e "s#^ODBCInstLib=.*#ODBCInstLib=$ODBC_INST_LIB#" /usr/lib/snowflake/odbc/lib/simba.snowflake.ini

# Remove references to SF_ACCOUNT from the odbc.ini file.
# It will be configured dynamically.
sed -i '/^SERVER/d' /etc/odbc.ini
sed -i '/^ACCOUNT/d' /etc/odbc.ini

ODBC_INI_CONTENT="[ODBC Data Sources]
snowflake = SnowflakeDSIIDriver"

if ! grep -Fxq "$ODBC_INI_CONTENT" /etc/odbc.ini; then
    cat >>/etc/odbc.ini <<EOF
$ODBC_INI_CONTENT
EOF
fi

#!/usr/bin/env bash

set -xeuo pipefail

## Specify your organization - account name as the account identifier
SFACCOUNT=${SFACCOUNT:-myorganization-myaccount}
VERSION="3.3.2"
FILE="snowflake-odbc-${VERSION}.x86_64.deb"
URL="https://sfc-repo.snowflakecomputing.com/odbc/linux/${VERSION}/${FILE}"
SHA256="fdcf83aadaf92ec135bed0699936fa4ef2cf2d88aef5a4657a96877ae2ba232d"

if [[ -f "${FILE}" && $(sha256sum "${FILE}" | cut -f1 -d' ') == "${SHA256}" ]]; then
  echo "snowflake package already downloaded"
else
  echo "downloading snowflake package"
  wget -nc "$URL"
fi

function configure() {
  ODBC_INST_LIB=/usr/lib/x86_64-linux-gnu/libodbcinst.so

  sed -i -e "s#^ODBCInstLib=.*#ODBCInstLib=$ODBC_INST_LIB#" /usr/lib/snowflake/odbc/lib/simba.snowflake.ini

  sed -i -e "s#SF_ACCOUNT#${SFACCOUNT}#" /etc/odbc.ini

  cat >>/etc/odbc.ini  <<EOF
[ODBC Data Sources]
snowflake = SnowflakeDSIIDriver
EOF
}

if ! dpkg -l snowflake-odbc 1>/dev/null 2>/dev/null ; then
  apt update && apt install -yyq unixodbc-dev odbcinst
  dpkg -i "${FILE}"
  apt install -f

  configure

  echo "installed and configured snowflake"
else
  echo "snowflake odbc already installed; not attempting to configure it"
fi

#!/usr/bin/env bash

# Install the MS SQL Server ODBC driver on a Debian-based system.
# https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server

set -xeuo pipefail

if [ ! -f /etc/debian_version ]; then
  echo "This script is only intended for Debian-based systems"
  exit 1
fi

apt-get -qq update && env DEBIAN_FRONTEND=noninteractive apt-get install -yqq curl gpg

# shellcheck disable=SC1091
. /etc/os-release

# ubuntu
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc > /etc/apt/trusted.gpg.d/microsoft.asc
# debian
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --batch --yes --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
curl -fsSL "https://packages.microsoft.com/config/${ID}/${VERSION_ID}/prod.list" > /etc/apt/sources.list.d/mssql-release.list

apt-get -qq update
env DEBIAN_FRONTEND=noninteractive ACCEPT_EULA=Y apt-get install -yqq msodbcsql18 unixodbc-dev
## and not needed to modify /etc/odbcinst.ini
## docker-compose will mount one in .ci/docker-compose-file/odbc
sed -i 's/ODBC Driver 18 for SQL Server/ms-sql/g' /etc/odbcinst.ini

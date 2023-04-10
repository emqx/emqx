#!/usr/bin/env bash

## this script install msodbcsql17 and unixodbc-dev on ci environment
## specific to ubuntu 16.04, 18.04, 20.04, 22.04
set -euo pipefail

# install msodbcsql17
if ! [[ "16.04 18.04 20.04 22.04" == *"$(lsb_release -rs)"* ]];
then
    echo "Ubuntu $(lsb_release -rs) is not currently supported.";
    exit 1;
fi

curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
## TODO: upgrade builder image
apt-get update && \
ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev mssql-tools && \
## and not needed to modify /etc/odbcinst.ini
## docker-compose will mount one in .ci/docker-compose-file/odbc
sed -i 's/ODBC Driver 17 for SQL Server/ms-sql/g' /etc/odbcinst.ini

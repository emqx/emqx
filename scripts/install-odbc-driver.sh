#!/usr/bin/env bash

## this script install msodbcsql17 and unixodbc-dev on ci environment
set -euo pipefail

# install msodbcsql17
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev && \
## and not needed to modify /etc/odbcinst.ini
## docker-compose will mount one in .ci/docker-compose-file/odbc
sed -i 's/ODBC Driver 17 for SQL Server/ms-sql/g' /etc/odbcinst.ini

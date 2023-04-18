#!/usr/bin/env bash

## this script install msodbcsql17 and unixodbc-dev on ci environment
## specific to ubuntu 16.04, 18.04, 20.04, 22.04
set -euo pipefail

# # install freetds as MS SQL Server driver

apt-get update && \
apt-get install -y unixodbc unixodbc-dev tdsodbc freetds-bin freetds-common freetds-dev

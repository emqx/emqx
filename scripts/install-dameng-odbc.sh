#!/usr/bin/env bash

# Configure the OTP `odbc' client (unixODBC + the Dameng DM8 driver) on THIS
# machine, so that the Erlang `odbc' application (whose `odbcserver' port
# program reads /etc/) can connect to a DM8 database.
#
# The DM8 client libraries are expected to be already present in
# $DM_INSTALL_PATH/bin: the docker testbed copies them from the `dameng'
# container image (see scripts/ct/run.sh), because the vendor download
# (download.dameng.com) is not reachable from GitHub-hosted runners.
#
# Environment:
#   DM_INSTALL_PATH  where the DM8 client libraries live (default /opt/dmdbms)
#   DM_CONN_HOST     host the ODBC client connects to (default localhost)
#   DM_PORT          DM8 port (default 5236)
#   DM_USER          user of the `[dm8]' DSN (default SYSDBA)
#   DM_PASSWORD      password of the `[dm8]' DSN (default SYSDBA001)

set -euo pipefail

DM_INSTALL_PATH="${DM_INSTALL_PATH:-/opt/dmdbms}"
DM_PORT="${DM_PORT:-5236}"
DM_USER="${DM_USER:-SYSDBA}"
DM_PASSWORD="${DM_PASSWORD:-SYSDBA001}"
DM_CONN_HOST="${DM_CONN_HOST:-localhost}"

DRIVER_SO="$DM_INSTALL_PATH/bin/libdodbc.so"

echo "=== install-dameng-odbc.sh (ODBC client setup) ==="
echo "DM_INSTALL_PATH=$DM_INSTALL_PATH DM_CONN_HOST=$DM_CONN_HOST DM_PORT=$DM_PORT"

install_unixodbc() {
    if command -v odbcinst >/dev/null 2>&1; then
        return 0
    fi
    echo "==> installing unixODBC"
    if command -v apt-get >/dev/null 2>&1; then
        apt-get -qq update && apt-get install -yqq unixodbc unixodbc-dev
    elif command -v yum >/dev/null 2>&1; then
        yum install -y unixODBC unixODBC-devel
    else
        echo "Neither apt-get nor yum available" >&2
        exit 1
    fi
}

# The DM8 driver links against sibling libraries in the same directory
# (libdmdpi.so, libdmfldr.so, ...) and has no RPATH, so the directory must be on
# the dynamic loader path.
register_driver_dir() {
    local DIR="$1"
    echo "==> registering $DIR with the dynamic loader"
    echo "$DIR" > /etc/ld.so.conf.d/dameng.conf
    ldconfig
}

# Write the ODBC driver registry + DSN, appending if not already present.
# NOTE: the Erlang `odbc' `odbcserver' port program reads /etc/, so we write
# /etc/odbcinst.ini and /etc/odbc.ini (NOT /usr/local/etc/).
write_odbc_config() {
    echo "==> writing /etc/odbcinst.ini and /etc/odbc.ini (driver=$DRIVER_SO)"
    if ! grep -Fxq "[DM8 ODBC DRIVER]" /etc/odbcinst.ini 2>/dev/null; then
        cat >> /etc/odbcinst.ini <<EOF

[DM8 ODBC DRIVER]
Description=ODBC DRIVER FOR DM8
Driver=$DRIVER_SO
EOF
    fi
    if ! grep -Fxq "[dm8]" /etc/odbc.ini 2>/dev/null; then
        cat >> /etc/odbc.ini <<EOF

[dm8]
Description=DM ODBC DSN
Driver=DM8 ODBC DRIVER
SERVER=$DM_CONN_HOST
UID=$DM_USER
PWD=$DM_PASSWORD
TCP_PORT=$DM_PORT
EOF
    fi
}

install_unixodbc

if [ ! -f "$DRIVER_SO" ]; then
    echo "DM8 driver not found at $DRIVER_SO" >&2
    exit 1
fi
register_driver_dir "$DM_INSTALL_PATH/bin"
write_odbc_config
echo "==> odbcinst -j"
odbcinst -j || true
echo "==> ODBC client setup done"

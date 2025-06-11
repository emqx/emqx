#!/usr/bin/env bash

set -exuo pipefail

echo 'enable_ssl = true' >> /opt/apache-doris/fe/conf/fe.conf

bash init_fe.sh

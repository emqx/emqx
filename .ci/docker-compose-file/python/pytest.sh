#!/bin/sh

## This script is to run emqx cluster smoke tests (fvt) in github action
## This script is executed in pacho_client

set -x
set +e

LB="haproxy"

apk update && apk add git curl
git clone -b develop-4.0 https://github.com/emqx/paho.mqtt.testing.git /paho.mqtt.testing
pip install pytest==6.2.5

pytest -v /paho.mqtt.testing/interoperability/test_client/V5/test_connect.py -k test_basic --host "$LB"
RESULT=$?

pytest -v /paho.mqtt.testing/interoperability/test_client --host "$LB"
RESULT=$(( RESULT + $? ))

# pytest -v /paho.mqtt.testing/interoperability/test_cluster --host1 "node1.emqx.io" --host2 "node2.emqx.io"
# RESULT=$(( RESULT + $? ))

exit $RESULT

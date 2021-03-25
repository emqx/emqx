#!/bin/sh

## This script is to run emqx cluster smoke tests (fvt) in github action
## This script is executed in pacho_client

set -x
set +e

NODE1="node1.emqx.io"
NODE2="node2.emqx.io"

apk update && apk add git curl
git clone -b develop-4.0 https://github.com/emqx/paho.mqtt.testing.git /paho.mqtt.testing
pip install pytest
pytest -v /paho.mqtt.testing/interoperability/test_client/V5/test_connect.py -k test_basic --host "$NODE1"
RESULT=$?
pytest -v /paho.mqtt.testing/interoperability/test_cluster --host1 "$NODE1" --host2 "$NODE2"
RESULT=$(( RESULT + $? ))
pytest -v /paho.mqtt.testing/interoperability/test_client --host "$NODE1"
RESULT=$(( RESULT + $? ))

exit $RESULT

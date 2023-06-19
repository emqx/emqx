#!/bin/sh

## This script is to run emqx cluster smoke tests (fvt) in github action
## This script is executed in paho_client

set -x
set +e

EMQX_TEST_DB_BACKEND=$1
if [ "$EMQX_TEST_DB_BACKEND" = "rlog" ]
then
  # TODO: target only replica to avoid replication races
  # see: https://github.com/emqx/emqx/issues/6094
  TARGET_HOST="node2.emqx.io"
else
  # use loadbalancer
  TARGET_HOST="haproxy"
fi

apk update && apk add git curl
## Use 5.1.0 to bypass the newly added test cases in
## https://github.com/emqx/paho.mqtt.testing/pull/13
## This is a temporary solution for 5.1.0 release. After 5.1.0 release, we should use
## the develop-5.0 branch
git clone -b 5.1.0 https://github.com/emqx/paho.mqtt.testing.git /paho.mqtt.testing

pip install pytest==7.1.2 pytest-retry

pytest --retries 3 -v /paho.mqtt.testing/interoperability/test_client/V5/test_connect.py -k test_basic --host "$TARGET_HOST"
RESULT=$?

pytest --retries 3 -v /paho.mqtt.testing/interoperability/test_client --host "$TARGET_HOST"
RESULT=$(( RESULT + $? ))

# pytest -v /paho.mqtt.testing/interoperability/test_cluster --host1 "node1.emqx.io" --host2 "node2.emqx.io"
# RESULT=$(( RESULT + $? ))

exit $RESULT

#!/bin/sh

## This script is to run emqx cluster smoke tests (fvt) in github action
## This script is executed in paho_client

set -x
set +e

# shellcheck disable=SC3028 disable=SC3054
SCRIPT_DIR="$( dirname -- "$( readlink -f -- "$0"; )"; )"

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
git clone -b develop-5.0 https://github.com/emqx/paho.mqtt.testing.git /paho.mqtt.testing

pip install --require-hashes -r "$SCRIPT_DIR/requirements.txt"

pytest --retries 3 -v /paho.mqtt.testing/interoperability/test_client/V5/test_connect.py -k test_basic --host "$TARGET_HOST"
RESULT=$?

pytest --retries 3 -v /paho.mqtt.testing/interoperability/test_client --host "$TARGET_HOST"
RESULT=$(( RESULT + $? ))

# pytest -v /paho.mqtt.testing/interoperability/test_cluster --host1 "node1.emqx.io" --host2 "node2.emqx.io"
# RESULT=$(( RESULT + $? ))

exit $RESULT

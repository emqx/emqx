#!/bin/bash

set -e

EMQX_ENDPOINT="http://localhost:8081/api/v4/acl"
EMQX2_ENDPOINT="http://localhost:8917/api/v4/acl"

function run() {
    emqx="$1"
    shift

    echo "[$emqx]" "$@"

    pushd "$TEST_PATH/$emqx"
        "$@"
    popd
}

function post_rule() {
    endpoint="$1"
    rule="$2"
    echo -n "->($endpoint) "
    curl -s -u admin:public -X POST "$endpoint" -d "$rule"
    echo
}

function verify_clientid_rule() {
    endpoint="$1"
    id="$2"
    echo -n "<-($endpoint) "
    curl -s -u admin:public "$endpoint/clientid/$id" | grep "$id" || (echo "verify rule for client $id failed" && return 1)
}

# Run nodes

run emqx ./bin/emqx start
run emqx2 ./bin/emqx start

run emqx ./bin/emqx_ctl plugins load emqx_auth_mnesia
run emqx2 ./bin/emqx_ctl plugins load emqx_auth_mnesia

run emqx2 ./bin/emqx_ctl cluster join 'emqx@127.0.0.1'

# Add ACL rule to unupgraded EMQX nodes

post_rule "$EMQX_ENDPOINT" '{"clientid": "CLIENT1_A","topic": "t", "action": "pub", "access": "allow"}'
post_rule "$EMQX2_ENDPOINT" '{"clientid": "CLIENT1_B","topic": "t", "action": "pub", "access": "allow"}'

# Upgrade emqx2 node

run emqx2 ./bin/emqx install "$VERSION"
sleep 60

# Verify upgrade blocked

run emqx2 ./bin/emqx eval 'emqx_acl_mnesia_migrator:is_old_table_migrated().' | grep false || (echo "emqx2 shouldn't have migrated" && exit 1)

# Verify old rules on both nodes

verify_clientid_rule "$EMQX_ENDPOINT" 'CLIENT1_A'
verify_clientid_rule "$EMQX2_ENDPOINT" 'CLIENT1_A'

verify_clientid_rule "$EMQX_ENDPOINT" 'CLIENT1_B'
verify_clientid_rule "$EMQX2_ENDPOINT" 'CLIENT1_B'

# Add ACL on OLD and NEW node, verify on all nodes

post_rule "$EMQX_ENDPOINT" '{"clientid": "CLIENT2_A","topic": "t", "action": "pub", "access": "allow"}'
post_rule "$EMQX2_ENDPOINT" '{"clientid": "CLIENT2_B","topic": "t", "action": "pub", "access": "allow"}'

verify_clientid_rule "$EMQX_ENDPOINT" 'CLIENT2_A'
verify_clientid_rule "$EMQX2_ENDPOINT" 'CLIENT2_A'

verify_clientid_rule "$EMQX_ENDPOINT" 'CLIENT2_B'
verify_clientid_rule "$EMQX2_ENDPOINT" 'CLIENT2_B'

# Upgrade emqx node

run emqx ./bin/emqx install "$VERSION"

# Wait for upgrade

sleep 60

# Verify if upgrade occured

run emqx ./bin/emqx eval 'emqx_acl_mnesia_migrator:is_old_table_migrated().' | grep true || (echo "emqx should have migrated" && exit 1)
run emqx2 ./bin/emqx eval 'emqx_acl_mnesia_migrator:is_old_table_migrated().' | grep true || (echo "emqx2 should have migrated" && exit 1)

# Verify rules are kept

verify_clientid_rule "$EMQX_ENDPOINT" 'CLIENT1_A'
verify_clientid_rule "$EMQX2_ENDPOINT" 'CLIENT1_A'

verify_clientid_rule "$EMQX_ENDPOINT" 'CLIENT1_B'
verify_clientid_rule "$EMQX2_ENDPOINT" 'CLIENT1_B'

verify_clientid_rule "$EMQX_ENDPOINT" 'CLIENT2_A'
verify_clientid_rule "$EMQX2_ENDPOINT" 'CLIENT2_A'

verify_clientid_rule "$EMQX_ENDPOINT" 'CLIENT2_B'
verify_clientid_rule "$EMQX2_ENDPOINT" 'CLIENT2_B'

# Add ACL on OLD and NEW node, verify on all nodes

post_rule "$EMQX_ENDPOINT" '{"clientid": "CLIENT3_A","topic": "t", "action": "pub", "access": "allow"}'
post_rule "$EMQX2_ENDPOINT" '{"clientid": "CLIENT3_B","topic": "t", "action": "pub", "access": "allow"}'

verify_clientid_rule "$EMQX_ENDPOINT" 'CLIENT3_A'
verify_clientid_rule "$EMQX2_ENDPOINT" 'CLIENT3_A'

verify_clientid_rule "$EMQX_ENDPOINT" 'CLIENT3_B'
verify_clientid_rule "$EMQX2_ENDPOINT" 'CLIENT3_B'

# Stop nodes

run emqx ./bin/emqx stop
run emqx2 ./bin/emqx stop

echo "Success!"


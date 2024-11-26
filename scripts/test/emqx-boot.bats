#!/usr/bin/env bats

# https://github.com/bats-core/bats-core
# env PROFILE=emqx bats -t -p --verbose-run scripts/test/emqx-boot.bats

@test "PROFILE must be set" {
    [[ -n "$PROFILE" ]]
}

@test "emqx boot with invalid node name" {
    output="$(env EMQX_NODE_NAME="invliadename#" ./_build/$PROFILE/rel/emqx/bin/emqx console 2>&1|| true)"
    [[ "$output" =~ "ERROR: Invalid node name,".+ ]]
}

@test "corrupted cluster-override.conf" {
    conffile="./_build/$PROFILE/rel/emqx/data/configs/cluster-override.conf"
    echo "{" > $conffile
    run ./_build/$PROFILE/rel/emqx/bin/emqx console
    [[ $status -ne 0 ]]
    rm -f $conffile
}

@test "corrupted cluster.hocon" {
    conffile="./_build/$PROFILE/rel/emqx/data/configs/cluster.hocon"
    echo "{" > $conffile
    run ./_build/$PROFILE/rel/emqx/bin/emqx console
    [[ $status -ne 0 ]]
    rm -f $conffile
}

@test "corrupted base.hocon" {
    conffile="./_build/$PROFILE/rel/emqx/etc/base.hocon"
    echo "{" > $conffile
    run ./_build/$PROFILE/rel/emqx/bin/emqx console
    [[ $status -ne 0 ]]
    rm -f $conffile
}

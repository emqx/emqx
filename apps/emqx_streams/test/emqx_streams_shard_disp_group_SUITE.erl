%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_shard_disp_group_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

-include("../src/emqx_streams_internal.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, """
            durable_storage.streams_states {
                backend = builtin_local
                n_shards = 4
            }
            """},
            {emqx_streams, """
            streams.enable = true
            """}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok = emqx_streams_app:wait_readiness(5_000),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

%%

-define(sgroup, atom_to_binary(?FUNCTION_NAME)).

t_smoke(_Config) ->
    ?assertEqual(
        #db_sgroup_st{vsn = undefined, leases = #{}, consumers = #{}, shards = #{}},
        emqx_streams_state_db:shard_leases_dirty(?sgroup)
    ).

t_lease_release(_Config) ->
    Shards = [<<"S1">>, <<"S2">>],
    G1 = emqx_streams_shard_disp_group:new(),
    [{lease, Shard} | _] = provision_changes(<<"c1">>, mk_provision(?sgroup, Shards, 0)),
    %% Initial lease progress:
    G2 = run_progress(<<"c1">>, ?sgroup, Shard, 0, 42, G1),
    ?assertMatch(#{}, G2),
    V1 = emqx_streams_state_db:alloc_vsn_dirty(?sgroup),
    %% Regular shard progress:
    G3 = run_progress(<<"c1">>, ?sgroup, Shard, 111, 43, G2),
    ?assertMatch(#{}, G3),
    V2 = emqx_streams_state_db:alloc_vsn_dirty(?sgroup),
    ?assertEqual(V1, V2),
    %% Shard release:
    G4 = run_release(<<"c1">>, ?sgroup, Shard, 111, G3),
    ?assertMatch(#{}, G4),
    V3 = emqx_streams_state_db:alloc_vsn_dirty(?sgroup),
    ?assertNotEqual(V2, V3),
    %% Retry:
    ?assertMatch(#{}, run_release(<<"c1">>, ?sgroup, Shard, 111, G3)).

t_lease_retry(_Config) ->
    Shards = [<<"S1">>, <<"S2">>],
    G1 = emqx_streams_shard_disp_group:new(),
    [{lease, Shard} | _] = provision_changes(<<"c1">>, mk_provision(?sgroup, Shards, 0)),
    %% Initial lease progress:
    {tx, Ref1, _, _G2} =
        emqx_streams_shard_disp_group:progress(<<"c1">>, ?sgroup, Shard, 0, 42, G1),
    %% Lost the reply somehow:
    ?assertReceive(?ds_tx_commit_reply(Ref1, _)),
    %% Retry with the same lease:
    {tx, _Ref, _, _} =
        Tx =
        emqx_streams_shard_disp_group:progress(<<"c1">>, ?sgroup, Shard, 10, 43, G1),
    G2 = run_tx(<<"c1">>, ?sgroup, Tx, true),
    ?assertMatch(#{}, G2),
    %% Releasing also works cleanly:
    ?assertMatch(
        #{},
        run_release(<<"c1">>, ?sgroup, Shard, 20, G2)
    ).

t_lease_conflict(_Config) ->
    Shard1 = <<"S1">>,
    Shard2 = <<"S2">>,
    G0 = emqx_streams_shard_disp_group:new(),
    G1 = run_progress(<<"c1">>, ?sgroup, Shard1, 0, 42, G0),
    _G = run_progress(<<"c1">>, ?sgroup, Shard2, 0, 43, G1),
    ?assertMatch(
        {invalid, {leased, <<"c1">>}, #{}},
        run_progress(<<"c2">>, ?sgroup, Shard1, 0, 44, G0)
    ).

t_lease_initial_conflict_avoidance(_Config) ->
    Shards = [<<"S1">>, <<"S2">>, <<"S3">>, <<"S4">>, <<"S5">>],
    G0 = emqx_streams_shard_disp_group:new(),
    %% Ask for initial leases concurrently:
    %% Consumers do not know about each other at this point.
    P1 = mk_provision(?sgroup, Shards, 0),
    [{lease, Shard1} | _] = provision_changes(<<"c1">>, P1),
    [{lease, Shard2} | _] = provision_changes(<<"c2">>, P1),
    %% Conflict should likely be avoided:
    %% NOTE: Sensitive to number of shards / consumer IDs.
    GA1 = run_progress(<<"c1">>, ?sgroup, Shard1, 0, 42, G0),
    ?assertMatch(#{}, GA1),
    GB1 = run_progress(<<"c2">>, ?sgroup, Shard2, 0, 42, G0),
    ?assertMatch(#{}, GB1),
    %% Now they know about each other, rest of shards proposed cooperatively:
    P2 = mk_provision(?sgroup, Shards, 0),
    Provisions1 = provision_changes(<<"c1">>, P2),
    Provisions2 = provision_changes(<<"c2">>, P2),
    ?assertMatch(
        [_, _, _],
        Provisions1 ++ Provisions2
    ),
    ?assertEqual(
        [],
        ordsets:intersection(ordsets:from_list(Provisions1), ordsets:from_list(Provisions2))
    ).

t_rebalance(_Config) ->
    SGroup = ?sgroup,
    _ = emqx_pd:inc_counter(SGroup, 1),
    NextOffset = fun() -> emqx_pd:inc_counter(SGroup, 1) end,
    Shards = [<<"S1">>, <<"S2">>, <<"S3">>, <<"S4">>, <<"S5">>],
    G0 = emqx_streams_shard_disp_group:new(),
    %% Ask for initial leases:
    Leases1 = provision_changes(<<"c1">>, mk_provision(SGroup, Shards, 0)),
    GA1 = lists:foldl(
        fun({lease, Shard}, G1A) ->
            #{} = run_progress(<<"c1">>, SGroup, Shard, NextOffset(), 1, G1A)
        end,
        G0,
        Leases1
    ),
    %% Provision another consumer:
    ?assertEqual([], provision_changes(<<"c2">>, mk_provision(SGroup, Shards, 0))),
    %% Nothing to do, announce the consumer:
    GB1 = emqx_streams_shard_disp_group:announce(<<"c2">>, SGroup, 2, 60, G0),
    %% Find out if rebalancing is advertised:
    Releases = provision_changes(<<"c1">>, mk_provision(SGroup, Shards, 0)),
    ?assertMatch([_, _], Releases),
    _GA = lists:foldl(
        fun({release, Shard}, GAcc) ->
            #{} = run_release(<<"c1">>, SGroup, Shard, NextOffset(), GAcc)
        end,
        GA1,
        Releases
    ),
    %% Retry another consumer:
    Leases2 = provision_changes(<<"c2">>, mk_provision(SGroup, Shards, 0)),
    ?assertMatch([_, _], Leases2),
    _GB = lists:foldl(
        fun({lease, Shard}, GAcc) ->
            #{} = run_progress(<<"c2">>, SGroup, Shard, NextOffset(), 3, GAcc)
        end,
        GB1,
        Leases2
    ),
    %% Should be stable now:
    ?assertEqual(
        [],
        provision_changes(<<"c1">>, mk_provision(SGroup, Shards, 0))
    ).

t_rebalance_stale_announcement(_Config) ->
    Shards = [<<"S1">>, <<"S2">>, <<"S3">>, <<"S4">>, <<"S5">>],
    G0 = emqx_streams_shard_disp_group:new(),
    %% Announce C1 with Heartbeat Timestamp = 10:
    _GA = emqx_streams_shard_disp_group:announce(<<"c1">>, ?sgroup, 10, 60, G0),
    %% Announce C2 with Heartbeat Timestamp = 20:
    _GB = emqx_streams_shard_disp_group:announce(<<"c2">>, ?sgroup, 20, 60, G0),
    %% Compute the provision at C2 later, at Time = 15:
    %% All the shards are provosioned to C2 since C1's announcement has expired.
    ?assertSameSet(
        [{lease, S} || S <- Shards],
        provision_changes(<<"c2">>, mk_provision(?sgroup, Shards, 15))
    ).

t_takeover(_Config) ->
    Shards = [<<"S1">>, <<"S2">>, <<"S3">>, <<"S4">>, <<"S5">>],
    G0 = emqx_streams_shard_disp_group:new(),
    %% Ask for a lease:
    [{lease, ShardA1} | _] = provision_changes(<<"A">>, mk_provision(?sgroup, Shards, 0)),
    GA1 = #{} = run_progress(<<"A">>, ?sgroup, ShardA1, 1, 10, G0),
    %% Concurrently, ask for leases as another consumer and take them:
    GTProvisions = provision_changes(<<"TO">>, mk_provision(?sgroup, Shards, 0)),
    GTHeartbeat = 20,
    _GT = lists:foldl(
        fun({lease, Shard}, GAcc) ->
            #{} = run_progress(<<"TO">>, ?sgroup, Shard, 2, GTHeartbeat, GAcc)
        end,
        G0,
        GTProvisions
    ),
    %% Let other consumers take the rest:
    GBProvisions = provision_changes(<<"B">>, mk_provision(?sgroup, Shards, 0)),
    _GB = lists:foldl(
        fun({lease, Shard}, GAcc) ->
            #{} = run_progress(<<"B">>, ?sgroup, Shard, 3, 30, GAcc)
        end,
        G0,
        GBProvisions
    ),
    GAProvisions = provision_changes(<<"A">>, mk_provision(?sgroup, Shards, 0)),
    _GA = lists:foldl(
        fun({lease, Shard}, GAcc) ->
            #{} = run_progress(<<"A">>, ?sgroup, Shard, 4, 40, GAcc)
        end,
        GA1,
        GAProvisions
    ),
    %% Every shard is allocated (at Time = 0):
    P0 = mk_provision(?sgroup, Shards, 0),
    ?assertEqual(
        [[], [], []],
        [provision_changes(C, P0) || C <- [<<"A">>, <<"B">>, <<"T0">>]]
    ),
    %% Fast-forward to Time = 25, assuming C is no longer with us:
    %% Provisions are still empty...
    P25 = mk_provision(?sgroup, Shards, 25),
    ?assertEqual(
        [[], []],
        [provision_changes(C, P25) || C <- [<<"A">>, <<"B">>]]
    ),
    %% ...But there proposed takeovers:
    ?assertSameSet(
        [{takeover, S, <<"TO">>, GTHeartbeat} || {lease, S} <- GTProvisions],
        lists:append([provision_takeovers(C, P25) || C <- [<<"A">>, <<"B">>]])
    ).

t_progress_backwards(_Config) ->
    Shard = <<"S1">>,
    G0 = emqx_streams_shard_disp_group:new(),
    G1 = run_progress(<<"c1">>, ?sgroup, Shard, 0, 42, G0),
    G2 = run_progress(<<"c1">>, ?sgroup, Shard, 10, 42, G1),
    ?assertMatch(#{}, G2),
    %% Group notices it:
    ?assertMatch(
        {invalid, {offset_going_backwards, 10}, _G},
        emqx_streams_shard_disp_group:progress(<<"c1">>, ?sgroup, Shard, 8, 43, G2)
    ),
    %% Database notices it (start from G1):
    ?assertMatch(
        {invalid, {offset_ahead, 10}, _G},
        run_progress(<<"c1">>, ?sgroup, Shard, 9, 43, G1)
    ).

%%

t_concurrent(_Config) ->
    SGroup = ?sgroup,
    Shards = [<<"S1">>, <<"S2">>, <<"S3">>, <<"S4">>, <<"S5">>],
    Consumers = [<<"c1">>, <<"c2">>, <<"c3">>],
    Actors = [
        erlang:spawn_monitor(
            fun() -> run_consumer(C, SGroup, Shards, 1, emqx_streams_shard_disp_group:new()) end
        )
     || C <- Consumers
    ],
    [?assertReceive({'DOWN', MRef, process, Pid, normal}) || {Pid, MRef} <- Actors],
    DBGroupSt = emqx_streams_state_db:shard_leases_dirty(SGroup),
    Alloc = emqx_streams_shard_disp_group:current_allocation(Shards, DBGroupSt),
    ?assertMatch(
        [{_}, {_}, {_}, {_}, {_}],
        [emqx_streams_allocation:lookup_resource(S, Alloc) || S <- Shards]
    ).

run_consumer(C, SGroup, Shards, Step, G1) ->
    Provision = mk_provision(SGroup, Shards, _HBWatermark = 0),
    case provision_changes(C, Provision) of
        [{lease, Shard} | _Rest] ->
            Offset = Step,
            HB = hb_timestamp(),
            case run_progress(C, SGroup, Shard, Offset, HB, G1) of
                #{} = G2 ->
                    run_consumer(C, SGroup, Shards, Step, G2);
                {invalid, Reason = {leased, _}, G2} ->
                    ct:pal("(retry) [~s] ~p", [C, Reason]),
                    run_consumer(C, SGroup, Shards, Step, G2);
                {invalid, Reason = conflict, G2} ->
                    ct:pal("(retry) [~s] ~p", [C, Reason]),
                    run_consumer(C, SGroup, Shards, Step, G2);
                Other ->
                    ct:pal("(fatal) [~s] ~p", [C, Other]),
                    exit(Other)
            end;
        [] ->
            ok
    end.

hb_timestamp() ->
    erlang:system_time(second).

%%

mk_provision(SGroup, Shards, HBWatermark) ->
    DBGroupSt = emqx_streams_state_db:shard_leases_dirty(SGroup),
    emqx_streams_shard_disp_group:new_provision(Shards, HBWatermark, DBGroupSt).

provision_changes(Consumer, Provision) ->
    emqx_streams_shard_disp_group:provision_changes(Consumer, Provision).

provision_takeovers(Consumer, Provision) ->
    emqx_streams_shard_disp_group:provision_takeovers(Consumer, Provision).

run_progress(Consumer, SGroup, Shard, Offset, HB, G0) ->
    run_tx(
        Consumer,
        SGroup,
        emqx_streams_shard_disp_group:progress(Consumer, SGroup, Shard, Offset, HB, G0),
        true
    ).

run_release(Consumer, SGroup, Shard, Offset, G0) ->
    run_tx(
        Consumer,
        SGroup,
        emqx_streams_shard_disp_group:release(Consumer, SGroup, Shard, Offset, G0),
        false
    ).

run_tx(Consumer, SGroup, {tx, Ref, Ctx, GSt}, true) ->
    ?ds_tx_commit_reply(Ref, Reply) = ?assertReceive(?ds_tx_commit_reply(Ref, _)),
    run_tx(
        Consumer,
        SGroup,
        emqx_streams_shard_disp_group:handle_tx_reply(Consumer, SGroup, Ref, Reply, Ctx, GSt),
        false
    );
run_tx(Consumer, SGroup, {tx, Ref, Ctx, GSt}, false) ->
    ?ds_tx_commit_reply(Ref, Reply) = ?assertReceive(?ds_tx_commit_reply(Ref, _)),
    emqx_streams_shard_disp_group:handle_tx_reply(Consumer, SGroup, Ref, Reply, Ctx, GSt);
run_tx(_Consumer, _SGroup, Ret, _) ->
    Ret.

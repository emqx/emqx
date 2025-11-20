%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_extrouter_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/asserts.hrl").

-include_lib("emqx/include/emqx.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%%

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    ok = init_db(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(TC, Config) ->
    emqx_common_test_helpers:init_per_testcase(?MODULE, TC, Config).

end_per_testcase(TC, Config) ->
    gc_actors(env(10_000_000)),
    emqx_common_test_helpers:end_per_testcase(?MODULE, TC, Config).

init_db() ->
    mria:wait_for_tables(emqx_cluster_link_extrouter:create_tables()).

init_db_nodes(Nodes) ->
    ok = lists:foreach(
        fun(Node) -> ok = erpc:call(Node, ?MODULE, init_db, []) end,
        Nodes
    ).

%%

t_consistent_routing_view(_Config) ->
    Cluster = ?FUNCTION_NAME,
    Actor1 = a1,
    Actor2 = a2,
    Actor3 = a3,
    {true, AS10} = actor_init(Cluster, Actor1, 1),
    {true, AS20} = actor_init(Cluster, Actor2, 1),
    {true, _AS3} = actor_init(Cluster, Actor3, 1),
    {false, AS30} = actor_init(Cluster, Actor3, 1),
    %% Add few routes originating from different actors.
    %% Also test that route operations are idempotent.
    AS11 = apply_operation({add, {<<"t/client/#">>, id}}, AS10),
    _AS11 = apply_operation({add, {<<"t/client/#">>, id}}, AS10),
    AS21 = apply_operation({add, {<<"t/client/#">>, id}}, AS20),
    AS31 = apply_operation({add, {<<"t/client/+/+">>, id1}}, AS30),
    AS32 = apply_operation({add, {<<"t/client/+/+">>, id2}}, AS31),
    _AS22 = apply_operation({delete, {<<"t/client/#">>, id}}, AS21),
    AS12 = apply_operation({add, {<<"t/client/+/+">>, id1}}, AS11),
    AS33 = apply_operation({delete, {<<"t/client/+/+">>, id1}}, AS32),
    _AS34 = apply_operation({delete, {<<"t/client/+/+">>, id2}}, AS33),
    ?assertSameSet(
        [<<"t/client/#">>, <<"t/client/+/+">>],
        emqx_cluster_link_extrouter:topics()
    ),
    ?assertEqual(
        [#route{topic = <<"t/client/#">>, dest = Cluster}],
        emqx_cluster_link_extrouter:match_routes(<<"t/client/42">>)
    ),
    %% Remove all routes from the actors.
    AS13 = apply_operation({delete, {<<"t/client/#">>, id}}, AS12),
    AS14 = apply_operation({delete, {<<"t/client/+/+">>, id1}}, AS13),
    AS14 = apply_operation({delete, {<<"t/client/+/+">>, id1}}, AS13),
    ?assertEqual(
        [],
        emqx_cluster_link_extrouter:topics()
    ).

t_actor_reincarnation(_Config) ->
    Cluster = ?FUNCTION_NAME,
    Actor1 = a1,
    Actor2 = a2,
    {true, AS10} = actor_init(Cluster, Actor1, 1),
    {true, AS20} = actor_init(Cluster, Actor2, 1),
    AS11 = apply_operation({add, {<<"topic/#">>, id}}, AS10),
    AS12 = apply_operation({add, {<<"topic/42/+">>, id}}, AS11),
    AS21 = apply_operation({add, {<<"topic/#">>, id}}, AS20),
    ?assertSameSet(
        [<<"topic/#">>, <<"topic/42/+">>],
        emqx_cluster_link_extrouter:topics()
    ),
    {true, _AS3} = actor_init(Cluster, Actor1, 2),
    ?assertError(
        _IncarnationMismatch,
        apply_operation({add, {<<"toolate/#">>, id}}, AS12)
    ),
    ?assertSameSet(
        [<<"topic/#">>],
        emqx_cluster_link_extrouter:topics()
    ),
    {true, _AS4} = actor_init(Cluster, Actor2, 2),
    ?assertError(
        _IncarnationMismatch,
        apply_operation({add, {<<"toolate/#">>, id}}, AS21)
    ),
    ?assertEqual(
        [],
        emqx_cluster_link_extrouter:topics()
    ).

t_actor_gc(_Config) ->
    Cluster1 = "t_actor_gc:1",
    Cluster2 = "t_actor_gc:2",
    Actor1 = a1,
    Actor2 = a2,
    Actor3 = a3,
    {true, AS10} = actor_init(Cluster1, Actor1, 1),
    {true, AS20} = actor_init(Cluster1, Actor2, 1),
    {true, AS30} = actor_init(Cluster2, Actor3, 1),
    %% Insert few routes: 2 by Actor1, 1 by Actor2, 1 by Actor3:
    AS11 = apply_operation({add, {<<"topic/#">>, id}}, AS10),
    AS12 = apply_operation({add, {<<"topic/42/+">>, id}}, AS11),
    AS21 = apply_operation({add, {<<"global/#">>, id}}, AS20),
    AS31 = apply_operation({add, {<<"broker">>, id}}, AS30),
    ?assertSameSet(
        [<<"broker">>, <<"global/#">>, <<"topic/#">>, <<"topic/42/+">>],
        emqx_cluster_link_extrouter:topics()
    ),
    %% Bump lifetime of Actor2 and Actor3:
    _AS22 = apply_operation(heartbeat, AS21, 60_000),
    _AS32 = apply_operation(heartbeat, AS31, 60_000),
    %% Perform GC at T = 60s, exactly 1 actor should be collected:
    ?assertEqual(
        1,
        gc_actors(env(60_000))
    ),
    %% Routes of Actor1 are collected:
    ?assertSameSet(
        [<<"broker">>, <<"global/#">>],
        emqx_cluster_link_extrouter:topics()
    ),
    %% Garbage-collected actor should be unable to do anything:
    ?assertError(
        _IncarnationMismatch,
        apply_operation({add, {<<"toolate/#">>, id}}, AS12)
    ),
    %% Perform GC at T = 120s, rest of the actors are collected now:
    ?assertEqual(
        2,
        gc_actors(env(120_000))
    ),
    ?assertEqual(
        [],
        emqx_cluster_link_extrouter:topics()
    ).

t_consistent_routing_view_concurrent_updates(_Config) ->
    Cluster = ?FUNCTION_NAME,
    A1Seq = repeat(10, [
        reincarnate,
        {add, {<<"t/client/#">>, id}},
        {add, {<<"t/client/+/+">>, id1}},
        {add, {<<"t/client/+/+">>, id1}},
        {delete, {<<"t/client/#">>, id}}
    ]),
    A2Seq = repeat(10, [
        {add, {<<"global/#">>, id}},
        {add, {<<"t/client/+/+">>, id1}},
        {add, {<<"t/client/+/+">>, id2}},
        {delete, {<<"t/client/+/+">>, id1}},
        heartbeat
    ]),
    A3Seq = repeat(10, [
        {add, {<<"global/#">>, id}},
        {delete, {<<"global/#">>, id}},
        {add, {<<"t/client/+/+">>, id1}},
        {delete, {<<"t/client/+/+">>, id1}},
        {add, {<<"t/client/+/+">>, id2}},
        {delete, {<<"t/client/+/+">>, id2}},
        reincarnate
    ]),
    A4Seq = repeat(10, [
        gc,
        {sleep, 1}
    ]),
    _ = emqx_utils:pmap(
        fun run_actor/1,
        [
            {Cluster, a1, A1Seq},
            {Cluster, a2, A2Seq},
            {Cluster, a3, A3Seq},
            {Cluster, gc, A4Seq}
        ],
        infinity
    ),
    ?assertSameSet(
        [<<"global/#">>, <<"t/client/+/+">>, <<"t/client/+/+">>],
        emqx_cluster_link_extrouter:topics()
    ).

t_consistent_routing_view_concurrent_cluster_updates('init', Config) ->
    Specs = [
        {emqx_cluster_link_extrouter1, #{role => core}},
        {emqx_cluster_link_extrouter2, #{role => core}},
        {emqx_cluster_link_extrouter3, #{role => core}}
    ],
    Cluster = emqx_cth_cluster:start(
        Specs,
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    ok = init_db_nodes(Cluster),
    [{cluster, Cluster} | Config];
t_consistent_routing_view_concurrent_cluster_updates('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(cluster, Config)).

t_consistent_routing_view_concurrent_cluster_updates(Config) ->
    Cluster = ?FUNCTION_NAME,
    [N1, N2, N3] = ?config(cluster, Config),
    A1Seq = repeat(10, [
        reincarnate,
        {add, {<<"t/client/#">>, id}},
        {add, {<<"t/client/+/+">>, id1}},
        {add, {<<"t/client/+/+">>, id1}},
        {delete, {<<"t/client/#">>, id}}
    ]),
    A2Seq = repeat(10, [
        {add, {<<"global/#">>, id}},
        {add, {<<"t/client/+/+">>, id1}},
        {add, {<<"t/client/+/+">>, id2}},
        {delete, {<<"t/client/+/+">>, id1}},
        heartbeat
    ]),
    A3Seq = repeat(10, [
        {add, {<<"global/#">>, id}},
        {delete, {<<"global/#">>, id}},
        {add, {<<"t/client/+/+">>, id1}},
        {delete, {<<"t/client/+/+">>, id1}},
        {add, {<<"t/client/+/+">>, id2}},
        {delete, {<<"t/client/+/+">>, id2}},
        reincarnate
    ]),
    A4Seq = repeat(10, [
        gc,
        {sleep, 1}
    ]),
    Runners = lists:map(
        fun run_remote_actor/1,
        [
            {N1, {Cluster, a1, A1Seq}},
            {N2, {Cluster, a2, A2Seq}},
            {N3, {Cluster, a3, A3Seq}},
            {N3, {Cluster, gc, A4Seq}}
        ]
    ),
    [?assertReceive({'DOWN', MRef, _, Pid, normal}) || {Pid, MRef} <- Runners],
    ?assertSameSet(
        [<<"global/#">>, <<"t/client/+/+">>, <<"t/client/+/+">>],
        erpc:call(N1, emqx_cluster_link_extrouter, topics, [])
    ).

t_consistent_routing_view_concurrent_cluster_replicant_updates('init', Config) ->
    Specs = [
        {emqx_cluster_link_extrouter_repl1, #{role => core}},
        {emqx_cluster_link_extrouter_repl2, #{role => core}},
        {emqx_cluster_link_extrouter_repl3, #{role => replicant}}
    ],
    Cluster = emqx_cth_cluster:start(
        Specs,
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    ok = init_db_nodes(Cluster),
    [{cluster, Cluster} | Config];
t_consistent_routing_view_concurrent_cluster_replicant_updates('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(cluster, Config)).

t_consistent_routing_view_concurrent_cluster_replicant_updates(Config) ->
    t_consistent_routing_view_concurrent_cluster_updates(Config).

run_remote_actor({Node, Run}) ->
    erlang:spawn_monitor(Node, ?MODULE, run_actor, [Run]).

run_actor({Cluster, Actor, Seq}) ->
    {true, AS0} = actor_init(Cluster, Actor, 0),
    lists:foldl(
        fun
            ({TS, {add, _} = Op}, AS) ->
                apply_operation(Op, AS, TS);
            ({TS, {delete, _} = Op}, AS) ->
                apply_operation(Op, AS, TS);
            ({TS, heartbeat}, AS) ->
                apply_operation(heartbeat, AS, TS);
            ({TS, gc}, AS) ->
                _NC = emqx_cluster_link_extrouter:actor_gc(env(TS)),
                AS;
            ({_TS, {sleep, MS}}, AS) ->
                ok = timer:sleep(MS),
                AS;
            ({TS, reincarnate}, _AS) ->
                {true, AS} = actor_init(Actor, TS, TS),
                AS
        end,
        AS0,
        lists:enumerate(Seq)
    ).

%%

actor_init(Cluster, Actor, Incarnation) ->
    actor_init(Cluster, Actor, Incarnation, _TS = 0).

actor_init(Cluster, Actor, Incarnation, TS) ->
    emqx_cluster_link_extrouter:actor_init(Cluster, Actor, Incarnation, env(TS)).

apply_operation(Op, AS) ->
    apply_operation(Op, AS, _TS = 42).

apply_operation(Op, AS, TS) ->
    emqx_cluster_link_extrouter:actor_apply_operation(Op, AS, env(TS)).

gc_actors(Env) ->
    case emqx_cluster_link_extrouter:actor_gc(Env) of
        1 -> 1 + gc_actors(Env);
        0 -> 0
    end.

env() ->
    env(42).

env(TS) ->
    #{timestamp => TS}.

%%

repeat(N, L) ->
    lists:flatten(lists:duplicate(N, L)).

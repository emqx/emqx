%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_timer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("../src/internals.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

%% This testcase verifies lazy initialization sequence of the
%% application. It is expected that the application is started without
%% creating any durable databases until the first timer type is
%% registered.
%%
%% Additionally, this testcase doesn't create any timer data, so it's
%% used to verify that the logic handles lack of any timers.
t_010_lazy_initialization({init, Config}) ->
    Env = #{
        <<"durable_storage">> =>
            #{
                <<"timers">> =>
                    #{
                        <<"backend">> => builtin_local,
                        <<"n_shards">> => 1
                    }
            },
        <<"cluster">> =>
            #{
                <<"heartbeat_interval">> => 1000,
                <<"missed_heartbeats">> => 2
            }
    },
    Cluster = cluster(?FUNCTION_NAME, Config, 1, Env),
    [{cluster, Cluster} | Config];
t_010_lazy_initialization({stop, Config}) ->
    Config;
t_010_lazy_initialization(Config) ->
    Cluster = proplists:get_value(cluster, Config),
    ?check_trace(
        #{timetrap => 30_000},
        begin
            [Node] = emqx_cth_cluster:start(Cluster),
            %% Application is started but dormant. Databases don't exist yet:
            ?ON(
                Node,
                begin
                    {DBs, _} = lists:unzip(emqx_ds:which_dbs()),
                    ?defer_assert(?assertNot(lists:member(?DB_GLOB, DBs)))
                end
            ),
            %%
            %% Now let's register a timer handler. This should
            %% activate the application. Expect: node adds its
            %% metadata to the tables.
            %%
            ?assertMatch(
                ok,
                ?ON(
                    Node,
                    begin
                        %% This operation should be idempotent:
                        ok = emqx_durable_test_timer:init(),
                        ok = emqx_durable_test_timer:init()
                    end
                )
            ),
            %% Verify that the node has opened the DB:
            ?ON(
                Node,
                begin
                    {DBs, _} = lists:unzip(emqx_ds:which_dbs()),
                    ?defer_assert(?assert(lists:member(?DB_GLOB, DBs)))
                end
            ),
            %%
            %% The node should create a new epoch:
            %%
            wait_heartbeat(Node),
            Epoch1 = ?ON(
                Node, emqx_durable_timer:epoch()
            ),
            [{Epoch1, true, T1_1}] = ?ON(Node, emqx_durable_timer_dl:dirty_ls_epochs(Node)),
            %% Also verify ls_epochs/0 API:
            ?assertMatch(
                [{Node, Epoch1, true, _}],
                ?ON(Node, emqx_durable_timer_dl:dirty_ls_epochs())
            ),
            %% Verify that heartbeat is periodically incrementing:
            wait_heartbeat(Node),
            [{Epoch1, true, T1_2}] = ?ON(Node, emqx_durable_timer_dl:dirty_ls_epochs(Node)),
            ?assert(is_integer(T1_1), T1_1),
            ?assert(is_integer(T1_2) andalso T1_2 > T1_1, T1_2),
            ct:pal("DB state before reboot: ~p", [db_dump(Node)]),
            %%
            %% Restart the node. Expect that node will create a new
            %% epoch and close the old one. Disable the automatic
            %% clean up of epochs before enabling the app.
            %%
            ?tp(notice, test_restart_cluster, #{}),
            emqx_cth_cluster:restart(Cluster),
            ?ON(
                Node,
                begin
                    meck:new(emqx_durable_timer_dl, [no_history, passthrough, no_link]),
                    meck:expect(
                        emqx_durable_timer_dl,
                        delete_epoch_if_empty,
                        fun(_) ->
                            false
                        end
                    ),
                    ?assertMatch(ok, emqx_durable_test_timer:init())
                end
            ),
            wait_heartbeat(Node),
            %% Restart complete.
            %% Verify that the new epoch has been created:
            ct:pal("DB state after reboot: ~p", [db_dump(Node)]),
            Epoch2 = ?ON(Node, emqx_durable_timer:epoch()),
            ?assertNot(Epoch1 =:= Epoch2),
            [{Epoch1, false, T1_3}, {Epoch2, true, T2_1}] =
                ?ON(Node, emqx_durable_timer_dl:dirty_ls_epochs(Node)),
            wait_heartbeat(Node),
            %% Heartbeat for Epoch1 stays the same and Epoch2 is ticking:
            [{Epoch1, false, T1_3}, {Epoch2, true, T2_2}] = ?ON(
                Node, emqx_durable_timer_dl:dirty_ls_epochs(Node)
            ),
            ?assert(is_integer(T2_2) andalso T2_2 >= T2_1, {T2_2, T2_1}),
            %% Register a new type, verify that workers have been started:
            ?tp(notice, test_register2, #{}),
            ?assertMatch(
                ok,
                ?ON(Node, emqx_durable_test_timer2:init())
            ),
            wait_heartbeat(Node),
            %%
            %% Test node isolation and recovery
            %%
            ?tp(notice, test_isolate, #{}),
            ?wait_async_action(
                isolate_node(Node),
                #{?snk_kind := ?tp_state_change, to := {isolated, _}}
            ),
            %% Verify that node keeps running heartbeats while isolated
            wait_heartbeat(Node),
            ?tp(notice, test_recover_isolated, #{}),
            ?wait_async_action(
                recover_isolated(Node),
                #{?snk_kind := ?tp_state_change, to := normal}
            ),
            %% A new epoch should be created:
            wait_heartbeat(Node),
            Epoch3 = ?ON(
                Node, emqx_durable_timer:epoch()
            ),
            ?assertNot(Epoch3 =:= Epoch2),
            {Epoch1, Epoch2, Epoch3}
        end,
        [
            %% fun no_unexpected/1, % Disabled due to some disturbance introduced by mock
            fun ?MODULE:no_read_conflicts/1,
            fun ?MODULE:no_replay_failures/1,
            fun ?MODULE:no_abnormal_terminate/1,
            fun ?MODULE:verify_timers/1,
            {"Workers lifetime", fun({Epoch1, Epoch2, Epoch3}, Trace) ->
                %% Trace before restart:
                {BeforeRestart, T1} = ?split_trace_at(#{?snk_kind := test_restart_cluster}, Trace),
                %% Trace before registration of the second type:
                {AfterRestart, T2} = ?split_trace_at(#{?snk_kind := test_register2}, T1),
                %% Trace before isolation:
                {AfterRegister, AfterRecovery} = ?split_trace_at(#{?snk_kind := test_isolate}, T2),
                ?assertMatch(
                    [#{type := 16#fffffffe, kind := active}],
                    started_workers(Epoch1, BeforeRestart)
                ),
                %% After restart:
                ?assertMatch(
                    [#{type := 16#fffffffe, kind := active}],
                    started_workers(Epoch2, AfterRestart)
                ),
                ?assertMatch(
                    [
                        #{type := 16#fffffffe, kind := {closed, dead_hand}, epoch := Epoch1},
                        #{type := 16#fffffffe, kind := {closed, started}, epoch := Epoch1}
                    ],
                    started_workers(Epoch1, AfterRestart)
                ),
                %% Verify that workers have been spawned after type 2 was registered:
                ?assertMatch(
                    [#{type := 16#ffffffff, kind := active}],
                    started_workers(Epoch2, AfterRegister)
                ),
                ?assertMatch(
                    [
                        #{type := 16#ffffffff, kind := {closed, dead_hand}},
                        #{type := 16#ffffffff, kind := {closed, started}}
                    ],
                    started_workers(Epoch1, AfterRegister)
                ),
                %% Verify that workers (re)stared when node recovered from isolated state:
                ?assertMatch(
                    [
                        #{type := 16#fffffffe, kind := active},
                        #{type := 16#ffffffff, kind := active}
                    ],
                    started_workers(Epoch3, AfterRecovery)
                ),
                ?assertMatch(
                    [
                        #{type := 16#fffffffe, kind := {closed, dead_hand}},
                        #{type := 16#fffffffe, kind := {closed, started}},
                        #{type := 16#ffffffff, kind := {closed, dead_hand}},
                        #{type := 16#ffffffff, kind := {closed, started}}
                    ],
                    started_workers(Epoch2, AfterRecovery)
                )
            end}
        ]
    ).

started_workers(Epoch, Trace) ->
    OrderBy = fun(#{kind := K1, type := T1}, #{kind := K2, type := T2}) ->
        {T1, K1} =< {T2, K2}
    end,
    lists:sort(OrderBy, [
        M
     || #{?snk_kind := ?tp_worker_started, ?snk_meta := M = #{epoch := E}} <- Trace, E =:= Epoch
    ]).

%% This testcase verifies normal operation of the timers when the
%% owner node is not restarted.
t_020_normal_execution({init, Config}) ->
    Env = #{
        <<"durable_storage">> =>
            #{
                <<"timers">> =>
                    #{<<"n_shards">> => 2}
            },
        <<"cluster">> =>
            #{
                <<"durable_timers">> =>
                    #{<<"batch_size">> => 2}
            }
    },
    Cluster = cluster(?FUNCTION_NAME, Config, 1, Env),
    [{cluster, Cluster} | Config];
t_020_normal_execution({stop, Config}) ->
    Config;
t_020_normal_execution(Config) ->
    Cluster = proplists:get_value(cluster, Config),
    Type = emqx_durable_test_timer:durable_timer_type(),
    ?check_trace(
        #{timetrap => 15_000},
        begin
            [Node] = emqx_cth_cluster:start(Cluster),
            ?assertMatch(ok, ?ON(Node, emqx_durable_test_timer:init())),
            %%
            %% Test a timer with 0 delay:
            ?wait_async_action(
                ?assertMatch(
                    ok,
                    ?ON(
                        Node,
                        emqx_durable_test_timer:apply_after(<<>>, <<>>, 0)
                    )
                ),
                #{?snk_kind := ?tp_fire, key := <<>>, val := <<>>},
                infinity
            ),
            %%
            %% Test a timer with 1s delay:
            ?wait_async_action(
                ?assertMatch(
                    ok,
                    ?ON(
                        Node,
                        emqx_durable_test_timer:apply_after(<<1>>, <<1>>, 1_000)
                    )
                ),
                #{?snk_kind := ?tp_fire, key := <<1>>, val := <<1>>},
                infinity
            ),
            %%
            %% Test overriding of timers:
            ?wait_async_action(
                ?ON(
                    Node,
                    begin
                        %% 1. Start a timer with key <<2>>
                        ok = emqx_durable_test_timer:apply_after(<<2>>, <<1>>, 500),
                        %% 2. Then override delay and value:
                        ok = emqx_durable_test_timer:apply_after(<<2>>, <<2>>, 1_000)
                    end
                ),
                #{?snk_kind := ?tp_fire, key := <<2>>, val := <<2>>},
                infinity
            ),
            %%
            %% Test overriding of dead hand:
            ?wait_async_action(
                ?ON(
                    Node,
                    begin
                        %% Derive topic name:
                        DHT = emqx_durable_timer_dl:dead_hand_topic(
                            Type, emqx_durable_timer:epoch(), '+'
                        ),
                        %% 1. Set up a dead hand timer with key <<3>>
                        ok = emqx_durable_test_timer:dead_hand(<<3>>, <<1>>, 0),
                        ?assertMatch([_], emqx_ds:dirty_read(?DB_GLOB, DHT)),
                        %% 2. Override it with a regular timer:
                        ok = emqx_durable_test_timer:apply_after(<<3>>, <<2>>, 100),
                        %% First version of the timer is gone:
                        ?assertMatch([], emqx_ds:dirty_read(?DB_GLOB, DHT))
                    end
                ),
                #{?snk_kind := ?tp_fire, key := <<3>>, val := <<2>>},
                infinity
            ),
            ct:sleep(1000),
            %% Verify that all timers have been GC'd:
            ?assertMatch(
                [],
                ?ON(
                    Node,
                    emqx_ds:dirty_read(
                        ?DB_GLOB, emqx_durable_timer_dl:dead_hand_topic(Type, '+', '+')
                    )
                )
            ),
            ?assertMatch(
                [],
                ?ON(
                    Node,
                    emqx_ds:dirty_read(
                        ?DB_GLOB, emqx_durable_timer_dl:started_topic(Type, '+', '+')
                    )
                )
            ),
            ok
        end,
        [
            fun ?MODULE:no_unexpected/1,
            fun ?MODULE:no_abnormal_terminate/1,
            fun ?MODULE:no_read_conflicts/1,
            fun ?MODULE:no_replay_failures/1,
            fun ?MODULE:verify_timers/1
        ]
    ).

%% This testcase verifies the functionality related to timer
%% cancellation.
t_030_cancellation({init, Config}) ->
    Env = #{},
    Cluster = cluster(?FUNCTION_NAME, Config, 1, Env),
    [{cluster, Cluster} | Config];
t_030_cancellation({stop, Config}) ->
    Config;
t_030_cancellation(Config) ->
    Cluster = proplists:get_value(cluster, Config),
    ?check_trace(
        #{timetrap => 30_000},
        try
            [Node] = emqx_cth_cluster:start(Cluster),
            ?assertMatch(ok, ?ON(Node, emqx_durable_test_timer:init())),
            %%
            %% 1. Test dead hand functionality. It's fairly trivial as
            %% long as the node doesn't restart: we just need to
            %% verify that the record is deleted.
            ?ON(
                Node,
                begin
                    DHT = emqx_durable_timer_dl:dead_hand_topic(
                        emqx_durable_test_timer:durable_timer_type(),
                        emqx_durable_timer:epoch(),
                        '+'
                    ),
                    emqx_durable_test_timer:dead_hand(<<0>>, <<0>>, 5_000),
                    ?assertMatch([_], emqx_ds:dirty_read(?DB_GLOB, DHT)),
                    %% Cancel it and verify that the record is deleted:
                    emqx_durable_test_timer:cancel(<<0>>),
                    ?assertMatch([], emqx_ds:dirty_read(?DB_GLOB, DHT))
                end
            ),
            %% 2. Test apply_after cancellation.
            Delay = 2_000,
            AAKey = <<1>>,
            ?ON(
                Node,
                begin
                    AAT = emqx_durable_timer_dl:started_topic(
                        emqx_durable_test_timer:durable_timer_type(),
                        emqx_durable_timer:epoch(),
                        '+'
                    ),
                    emqx_durable_test_timer:apply_after(AAKey, <<1>>, Delay),
                    ?assertMatch([_], emqx_ds:dirty_read(?DB_GLOB, AAT)),
                    %% Cancel it and verify that the record is deleted:
                    emqx_durable_test_timer:cancel(AAKey),
                    ?assertMatch([], emqx_ds:dirty_read(?DB_GLOB, AAT))
                end
            ),
            ct:sleep(2 * Delay)
        after
            emqx_cth_cluster:stop(Cluster)
        end,
        [
            fun ?MODULE:no_unexpected/1,
            fun ?MODULE:no_abnormal_terminate/1,
            fun ?MODULE:no_read_conflicts/1,
            fun ?MODULE:no_replay_failures/1,
            fun ?MODULE:verify_timers/1,
            {"Verify that cancelled timers didn't fire", fun(Trace) ->
                ?assertMatch([], ?of_kind(?tp_fire, Trace))
            end}
        ]
    ).

%% This testcase verifies basic functionality of dead hand timer.
t_040_dead_hand({init, Config}) ->
    Env = #{
        <<"durable_storage">> =>
            #{
                <<"timers">> =>
                    #{
                        <<"n_shards">> => 1,
                        <<"replication_factor">> => 3
                    }
            },
        <<"cluster">> =>
            #{
                <<"heartbeat_interval">> => 1_000,
                <<"missed_heartbeats">> => 3,
                <<"durable_timers">> => #{<<"batch_size">> => 2}
            }
    },
    Cluster = cluster(?FUNCTION_NAME, Config, 3, Env),
    [{cluster, Cluster} | Config];
t_040_dead_hand({stop, Config}) ->
    Config;
t_040_dead_hand(Config) ->
    Cluster = proplists:get_value(cluster, Config),
    ?check_trace(
        #{timetrap => 30_000},
        begin
            %% Prepare system:
            [N1, _N2, _N3] = Nodes = emqx_cth_cluster:start(Cluster),
            [?assertMatch(ok, ?ON(N, emqx_durable_test_timer:init())) || N <- Nodes],
            [
                ?assertMatch(
                    ok, ?ON(N1, emqx_durable_test_timer:dead_hand(<<I>>, <<I>>, (I div 3) * 100))
                )
             || I <- lists:seq(0, 10)
            ],
            %% Shut down the first node and wait for the full replay of its dead_hand timers:
            ?wait_async_action(
                emqx_cth_cluster:stop([N1]),
                #{?snk_kind := ?tp_test_fire, key := <<10>>, val := <<10>>}
            )
        end,
        [
            fun ?MODULE:no_unexpected/1,
            fun ?MODULE:no_abnormal_terminate/1,
            fun ?MODULE:no_read_conflicts/1,
            fun ?MODULE:no_replay_failures/1,
            fun ?MODULE:verify_timers/1,
            {"All dead hand timers fire once", fun(Trace) ->
                {_, AfterRestart} = ?split_trace_at(
                    #{?snk_kind := ?tp_update_epoch, up := false}, Trace
                ),
                ?assertEqual(
                    [{<<I>>, <<I>>} || I <- lists:seq(0, 10)],
                    ?projection([key, val], ?of_kind(?tp_test_fire, AfterRestart))
                )
            end}
        ]
    ).

%% This testcase verifies that "apply_after" timers are continued to
%% be replayed by other nodes after the original node goes down.
t_050_apply_after_postmortem_replay({init, Config}) ->
    Env = #{
        <<"durable_storage">> =>
            #{
                <<"timers">> =>
                    #{
                        <<"n_shards">> => 1,
                        <<"replication_factor">> => 3
                    }
            },
        <<"cluster">> =>
            #{
                <<"heartbeat_interval">> => 1_000,
                <<"missed_heartbeats">> => 3,
                <<"durable_timers">> => #{<<"batch_size">> => 2}
            }
    },
    Cluster = cluster(?FUNCTION_NAME, Config, 3, Env),
    [{cluster, Cluster} | Config];
t_050_apply_after_postmortem_replay({stop, Config}) ->
    Config;
t_050_apply_after_postmortem_replay(Config) ->
    Cluster = proplists:get_value(cluster, Config),
    ?check_trace(
        #{timetrap => 30_000},
        begin
            %% Prepare system:
            [N1, _N2, _N3] = Nodes = emqx_cth_cluster:start(Cluster),
            [?assertMatch(ok, ?ON(N, emqx_durable_test_timer:init())) || N <- Nodes],
            [
                ?assertMatch(
                    ok,
                    ?ON(
                        N1,
                        emqx_durable_test_timer:apply_after(<<I>>, <<I>>, 1_000 + (I div 3) * 100)
                    )
                )
             || I <- lists:seq(0, 10)
            ],
            %% Shut down the first node and wait for the full replay of its apply_after timers:
            ?wait_async_action(
                emqx_cth_cluster:stop([N1]),
                #{?snk_kind := ?tp_test_fire, key := <<10>>, val := <<10>>}
            )
        end,
        [
            fun ?MODULE:no_unexpected/1,
            fun ?MODULE:no_abnormal_terminate/1,
            fun ?MODULE:no_read_conflicts/1,
            fun ?MODULE:no_replay_failures/1,
            fun ?MODULE:verify_timers/1,
            {"All scheduled timers fire once", fun(Trace) ->
                {_, AfterRestart} = ?split_trace_at(
                    #{?snk_kind := ?tp_update_epoch, up := false}, Trace
                ),
                ?assertEqual(
                    [{<<I>>, <<I>>} || I <- lists:seq(0, 10)],
                    ?projection([key, val], ?of_kind(?tp_test_fire, AfterRestart))
                )
            end}
        ]
    ).

%% This testcase verifies leader selection and standby worker functionality
t_060_standby({init, Config}) ->
    Env = #{
        <<"durable_storage">> =>
            #{
                <<"timers">> =>
                    #{
                        <<"n_shards">> => 1,
                        <<"replication_factor">> => 5
                    }
            },
        <<"cluster">> =>
            #{
                <<"heartbeat_interval">> => 100,
                <<"missed_heartbeats">> => 3,
                <<"durable_timers">> => #{<<"batch_size">> => 2}
            }
    },
    Cluster = cluster(?FUNCTION_NAME, Config, 5, Env),
    [{cluster, Cluster} | Config];
t_060_standby({stop, Config}) ->
    Config;
t_060_standby(Config) ->
    Cluster = proplists:get_value(cluster, Config),
    ?check_trace(
        #{timetrap => 30_000},
        begin
            %% Prepare system:
            [N1 | _] = Nodes = emqx_cth_cluster:start(Cluster),
            [?assertMatch(ok, ?ON(N, emqx_durable_test_timer:init())) || N <- Nodes],
            ?ON(
                N1,
                begin
                    emqx_durable_test_timer:dead_hand(<<1>>, <<1>>, 0),
                    %% Note: spread between the events should be >
                    %% than time for establishing the leader.
                    emqx_durable_test_timer:dead_hand(<<2>>, <<2>>, 10_000)
                end
            ),
            %% Shut down N1, wait for the leader to replay the first
            %% timer, and find out who is the leader for N1's last
            %% epoch:
            ?tp(notice, test_stop_n1, #{}),
            {_, {ok, #{?snk_meta := #{node := Leader1}}}} =
                ?wait_async_action(
                    emqx_cth_peer:stop(N1),
                    #{?snk_kind := ?tp_test_fire, key := <<1>>}
                ),
            %% Give the old leader some time to delete old data:
            ct:sleep(100),
            ?tp(notice, test_stop_leader1, #{leader => Leader1}),
            %% Some other node should take over and continue replay of
            %% the epoch:
            {_, {ok, #{?snk_meta := #{node := Leader2}}}} =
                ?wait_async_action(
                    emqx_cth_peer:stop(Leader1),
                    #{?snk_kind := ?tp_test_fire, key := <<2>>}
                ),
            %% Eventually, the old epoch should be completely removed:
            ct:sleep(100),
            ?assertMatch(
                [],
                ?ON(Leader2, emqx_durable_timer_dl:dirty_ls_epochs(N1))
            ),
            Leader1
        end,
        [
            fun ?MODULE:no_unexpected/1,
            fun ?MODULE:no_read_conflicts/1,
            fun ?MODULE:no_replay_failures/1,
            fun ?MODULE:verify_timers/1,
            {"Each timer was executed once", fun(Trace) ->
                ?assertMatch(
                    [<<1>>, <<2>>],
                    ?projection(key, ?of_kind(?tp_test_fire, Trace))
                )
            end}
        ]
    ).

%% This testcase verifies sharding
t_070_multiple_shards({init, Config}) ->
    Env = #{
        <<"durable_storage">> =>
            #{
                <<"timers">> =>
                    #{
                        <<"n_shards">> => 16,
                        <<"replication_factor">> => 5
                    }
            },
        <<"cluster">> =>
            #{<<"durable_timers">> => #{<<"batch_size">> => 2}}
    },
    Cluster = cluster(?FUNCTION_NAME, Config, 5, Env),
    [{cluster, Cluster} | Config];
t_070_multiple_shards({stop, Config}) ->
    Config;
t_070_multiple_shards(Config) ->
    Cluster = proplists:get_value(cluster, Config),
    ?check_trace(
        #{timetrap => 30_000},
        begin
            %% Prepare system:
            Nodes = emqx_cth_cluster:start(Cluster),
            [?assertMatch(ok, ?ON(N, emqx_durable_test_timer:init())) || N <- Nodes],
            %% Create data in multiple shards:
            Keys = [
                begin
                    NodeBin = atom_to_binary(N),
                    Key = <<NodeBin/binary, I:32>>,
                    ?ON(N, emqx_durable_test_timer:apply_after(Key, <<>>, 3000 - I * 1000)),
                    Key
                end
             || N <- Nodes,
                I <- lists:seq(1, 3)
            ],
            ct:sleep(3_000),
            Keys
        end,
        fun(Keys, Trace) ->
            snabbkaffe_diff:assert_lists_eq(
                lists:sort(Keys),
                lists:sort(?projection(key, ?of_kind(?tp_fire, Trace)))
            )
        end
    ).

%%------------------------------------------------------------------------------
%% Trace specs
%%------------------------------------------------------------------------------

%% Verify that the workes didn't have to retry fetching batches. This
%% property should hold as long as DS state is not degraded.
no_replay_failures(Trace) ->
    ?assertMatch([], ?of_kind(?tp_replay_failed, Trace)).

%% Verify that none of the workers encountered an unknwon message.
%% This should always hold.
no_unexpected(Trace) ->
    Unknown = ?of_kind(?tp_unknown_event, Trace),
    Unexpected = lists:filter(
        fun
            (#{info := {'DOWN', _, {error, _}, _, commit_timeout}}) ->
                %% These errors may happen due to known
                %% limitations. Ignore them.
                false;
            (_) ->
                true
        end,
        Unknown
    ),
    ?assertMatch([], Unexpected).

%% Verify that workers only terminated with normal reason
no_abnormal_terminate(Trace) ->
    ?assertMatch([], [
        I
     || I = #{?snk_kind := ?tp_terminate, reason := Reason} <- Trace,
        Reason =/= normal,
        Reason =/= shutdown
    ]).

%% Active Timer State:
-record(ats, {
    val :: binary(),
    min_ts :: integer()
}).

%% Verifier State:
-record(vs, {
    active = #{},
    dead_hand = #{},
    stats = [],
    errors = []
}).

%% Verify arbitrary sequence of operations with the timers, node
%% restarts and side effects.
verify_timers(Trace) ->
    do_verify_timers(Trace, #vs{}).

do_verify_timers([], #vs{errors = Errors, stats = Stats}) ->
    case Stats of
        [] ->
            ok;
        _ ->
            Min = lists:min(Stats),
            Max = lists:max(Stats),
            Median = median(Stats),
            ct:pal(
                """
                Deviation statistics (ms):
                   Min:    ~p
                   Max:    ~p
                   Median: ~p
                """,
                [Min, Max, Median]
            )
    end,
    ?assertMatch([], Errors);
do_verify_timers(
    [TP = #{?snk_kind := Kind, ?snk_meta := #{time := TimeUs}} | Trace], S0
) ->
    TS =
        erlang:convert_time_unit(TimeUs, microsecond, millisecond) +
            erlang:time_offset(millisecond),
    #vs{active = A0, dead_hand = DH0, errors = Err0, stats = Stats0} = S0,
    S =
        case Kind of
            ?tp_new_apply_after ->
                #{type := T, key := K, val := Val, delay := Delay} = TP,
                S0#vs{
                    active = A0#{{T, K} => #ats{val = Val, min_ts = Delay + TS}},
                    dead_hand = maps:remove(K, DH0)
                };
            ?tp_new_dead_hand ->
                #{type := T, key := K, val := Val, delay := Delay, epoch := Epoch} = TP,
                S0#vs{
                    active = maps:remove({T, K}, A0),
                    dead_hand = DH0#{{T, K} => {Epoch, Val, Delay}}
                };
            ?tp_delete ->
                #{type := T, key := K} = TP,
                S0#vs{
                    active = maps:remove({T, K}, A0),
                    dead_hand = maps:remove({T, K}, DH0)
                };
            ?tp_update_epoch ->
                #{epoch := Epoch, up := IsUp, hb := LastHeartbeat} = TP,
                case IsUp of
                    true ->
                        S0;
                    false ->
                        epoch_down(Epoch, LastHeartbeat, S0)
                end;
            ?tp_fire ->
                #{type := T, key := K, val := ValGot} = TP,
                case maps:take({T, K}, A0) of
                    {#ats{val = ValExpected, min_ts = MinTS}, A} ->
                        %% Check value:
                        Err1 =
                            case ValGot of
                                ValExpected ->
                                    Err0;
                                _ ->
                                    [
                                        #{
                                            '_' => value_mismath,
                                            k => K,
                                            expected => ValExpected,
                                            got => ValGot
                                        }
                                        | Err0
                                    ]
                            end,
                        %% Check delay:
                        Diff = TS - MinTS,
                        Err =
                            case Diff >= 0 of
                                true ->
                                    Err1;
                                false ->
                                    [
                                        #{
                                            '_' => fired_too_early,
                                            k => K,
                                            expected => MinTS,
                                            t => TS,
                                            v => ValGot,
                                            dt => Diff
                                        }
                                        | Err1
                                    ]
                            end,
                        S0#vs{
                            active = A,
                            stats = [Diff | Stats0],
                            errors = Err
                        };
                    error ->
                        S0#vs{
                            errors = [{unexpected, TP} | Err0]
                        }
                end;
            _ ->
                S0
        end,
    do_verify_timers(
        Trace,
        S
    ).

epoch_down(Epoch, TS, S = #vs{dead_hand = DH0, active = A}) ->
    New = maps:filtermap(
        fun(_Key, {E, Val, Delay}) ->
            case E of
                Epoch ->
                    {true, #ats{val = Val, min_ts = Delay + TS}};
                _ ->
                    false
            end
        end,
        DH0
    ),
    DH = maps:without(maps:keys(New), DH0),
    S#vs{
        active = maps:merge(A, New),
        dead_hand = DH
    }.

%% This should hold as long as testcase doesn't issue operations that
%% update the same key in parallel.
no_read_conflicts(Trace) ->
    ?assertMatch(
        [],
        [E || E = #{?snk_kind := emqx_ds_tx_retry, reason := {read_conflict, _}} <- Trace]
    ).

isolate_node(Node) ->
    ?ON(
        Node,
        begin
            meck:new(emqx_ds, [no_history, passthrough, no_link]),
            meck:expect(
                emqx_ds,
                trans,
                fun(_, _) ->
                    {error, recoverable, mock_error}
                end
            )
        end
    ).

recover_isolated(Node) ->
    ?ON(Node, meck:unload(emqx_ds)).

wait_heartbeat(Node) ->
    ?block_until(
        #{?snk_kind := ?tp_heartbeat, ?snk_meta := #{node := Node}}, infinity, 0
    ).

median(L0) ->
    L = lists:sort(L0),
    N = length(L),
    case N rem 2 of
        1 ->
            lists:nth(N div 2 + 1, L);
        0 ->
            [A, B | _] = lists:nthtail(N div 2 - 1, L),
            (A + B) / 2
    end.

db_dump(Node) ->
    ?ON(Node, emqx_ds:dirty_read(?DB_GLOB, ['#'])).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

suite() -> [{timetrap, {minutes, 1}}].

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    case emqx_common_test_helpers:is_standalone_test() of
        false ->
            Config;
        true ->
            {skip, standalone_not_supported}
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    ?MODULE:TC({init, Config}).

end_per_testcase(TC, Config) ->
    ?MODULE:TC({stop, Config}),
    case proplists:get_value(cluster, Config) of
        undefined ->
            ok;
        Cluster ->
            %% Give it time to flush logs:
            %% ct:sleep(1000),
            emqx_cth_cluster:stop(Cluster)
    end,
    emqx_cth_suite:clean_work_dir(emqx_cth_suite:work_dir(TC, Config)),
    ok.

cluster(TC, Config, Nnodes, Env) ->
    AppSpecs = [
        {emqx_conf, #{config => Env}},
        {emqx_ds_builtin_raft, #{n_sites => Nnodes}},
        {emqx_durable_timer, #{
            override_env => Env,
            after_start => fun fix_logging/0
        }}
    ],
    NodeSpecs =
        [
            {list_to_atom("emqx_durable_timer_SUITE" ++ integer_to_list(I)), #{
                role => core,
                apps => AppSpecs
            }}
         || I <- lists:seq(1, Nnodes)
        ],
    emqx_cth_cluster:mk_nodespecs(
        NodeSpecs,
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ).

fix_logging() ->
    %% NOTE: cth_peer module often stops the node too abruptly before
    %% it flushes the logs. When investigating a remote crash, it's
    %% necessary to give node some time waiting for the flush.
    logger:set_primary_config(level, debug),
    logger:remove_handler(default),
    logger:add_handler(
        default,
        logger_std_h,
        #{
            config => #{file => "erlang.log", max_no_files => 1}
        }
    ).
